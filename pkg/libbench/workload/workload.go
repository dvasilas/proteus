package workload

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/pkg/libbench/config"
	"github.com/dvasilas/proteus/pkg/libbench/distributions"
	"github.com/dvasilas/proteus/pkg/libbench/operations"
	"github.com/dvasilas/proteus/pkg/perf"
	"golang.org/x/sync/semaphore"
)

// Workload ...
type Workload struct {
	config       *config.BenchmarkConfig
	ops          *operations.Operations
	measurements *perf.Perf
	storySemPhs  []*semaphore.Weighted
}

// NewWorkload ...
func NewWorkload(conf *config.BenchmarkConfig) (*Workload, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	ops, err := operations.NewOperations(conf)
	if err != nil {
		return nil, err
	}

	storySemPhs := make([]*semaphore.Weighted, conf.Preload.RecordCount.Stories)
	for i := range storySemPhs {
		storySemPhs[i] = semaphore.NewWeighted(1)
	}

	return &Workload{
		config:       conf,
		ops:          ops,
		measurements: perf.New(),
		storySemPhs:  storySemPhs,
	}, nil
}

// Run ...
func (w Workload) Run(measurementBufferSize int64) (map[string][]time.Duration, map[string]int64, time.Time, time.Time) {
	durations := make(map[string][]time.Duration, measurementBufferSize)
	durations["getHomepage"] = make([]time.Duration, measurementBufferSize)
	durations["vote"] = make([]time.Duration, measurementBufferSize)

	perOpCnt := make(map[string]int64)
	perOpCnt["getHomepage"] = 0
	perOpCnt["vote"] = 0
	var opCnt int64
	var st time.Time
	var respTime time.Duration
	var err error
	timerStarted := false
	warmingUp, warmupTimeout := w.config.Benchmark.DoWarmup, time.After(time.Duration(w.config.Benchmark.Warmup)*time.Second)
	for timeIsUp, timeout := true, time.After(time.Duration(w.config.Benchmark.Runtime)*time.Second); timeIsUp; {

		select {
		case <-timeout:
			timeIsUp = false
		case <-warmupTimeout:
			warmingUp = false
		default:
		}

		if !timerStarted && !warmingUp {
			timerStarted = true
			st = time.Now()
		}
		if opCnt == w.config.Benchmark.OpCount {
			break
		}

		r := rand.Float64()
		if r < w.config.Operations.WriteRatio {
			vote := rand.Float64()
			if vote < w.config.Operations.DownVoteRatio {
				respTime, err = w.DownVoteStory(0)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				respTime, err = w.UpVoteStory(0)
				if err != nil {
					log.Fatal(err)
				}
			}
			if !warmingUp {
				durations["vote"][perOpCnt["vote"]] = respTime
				perOpCnt["vote"]++
			}
		} else {
			respTime, err = w.GetHomepage()
			if err != nil {
				log.Fatal(err)
			}
			if !warmingUp {
				durations["getHomepage"][perOpCnt["getHomepage"]] = respTime
				perOpCnt["getHomepage"]++
			}
		}
		opCnt++
	}
	return durations, perOpCnt, st, time.Now()
}

// Preload ...
func (w Workload) Preload() error {
	fmt.Println("Preloading ..")
	// start from 1 because when MySQL automaticall assigns ids
	// it starts from 1
	// ¯\_(ツ)_/¯
	for i := 1; i <= w.config.Preload.RecordCount.Users; i++ {
		if err := w.AddUser(); err != nil {
			return err
		}
	}

	fmt.Printf("Created %d users\n", w.config.Preload.RecordCount.Users)

	for i := 1; i <= w.config.Preload.RecordCount.Stories; i++ {
		if err := w.AddStory(); err != nil {
			return err
		}
		if _, err := w.UpVoteStory(i); err != nil {
			return err
		}
	}

	fmt.Printf("Created %d stories\n", w.config.Preload.RecordCount.Stories)

	for i := 1; i <= w.config.Preload.RecordCount.Comments; i++ {
		if err := w.AddComment(); err != nil {
			return err
		}
		if _, err := w.UpVoteComment(i); err != nil {
			return err
		}
	}

	fmt.Printf("Created %d comments\n", w.config.Preload.RecordCount.Comments)

	preadloadThreads := 10
	var wg sync.WaitGroup

	for t := 1; t <= preadloadThreads; t++ {
		wg.Add(1)
		go func(voteCount int) {
			defer wg.Done()
			for i := 1; i <= voteCount; i++ {
				vote := rand.Float64()
				if vote < w.config.Operations.DownVoteRatio {
					if _, err := w.DownVoteStory(0); err != nil {
						panic(err)
					}
				} else {
					if _, err := w.UpVoteStory(0); err != nil {
						panic(err)
					}
				}
				if i%1000 == 0 {
					fmt.Printf("Created %d votes\n", i)
				}
			}
		}(w.config.Preload.RecordCount.StoryVotes / preadloadThreads)
	}

	wg.Wait()

	fmt.Printf("Created %d votes\n", w.config.Preload.RecordCount.StoryVotes)
	fmt.Println("Preloading done")
	return nil
}

// GetHomepage ...
func (w *Workload) GetHomepage() (time.Duration, error) {
	st := time.Now()

	_, err := w.ops.GetHomepage()

	return time.Since(st), err
}

// AddUser ...
func (w *Workload) AddUser() error {
	return w.ops.AddUser()
}

// AddStory ...
func (w *Workload) AddStory() error {
	w.ops.State.UserMutex.RLock()
	userRecords := w.ops.State.UserRecords
	w.ops.State.UserMutex.RUnlock()
	userID, err := distributions.SelectUser(userRecords)
	if err != nil {
		return err
	}

	return w.ops.AddStory(userID)
}

// AddComment ...
func (w *Workload) AddComment() error {
	w.ops.State.UserMutex.RLock()
	userRecords := w.ops.State.UserRecords
	storyRecords := w.ops.State.StoryRecords
	w.ops.State.UserMutex.RUnlock()

	userID, err := distributions.SelectUser(userRecords)
	if err != nil {
		return err
	}
	storyID, err := distributions.SelectStory(storyRecords)
	if err != nil {
		return err
	}

	return w.ops.AddComment(userID, storyID)

}

// UpVoteStory ...
func (w *Workload) UpVoteStory(storyID int) (time.Duration, error) {
	w.ops.State.UserMutex.RLock()
	userRecords := w.ops.State.UserRecords
	storyRecords := w.ops.State.StoryRecords
	w.ops.State.UserMutex.RUnlock()

	userID, err := distributions.SelectUser(userRecords)
	if err != nil {
		return -1, err
	}

	if storyID == 0 {
		window := 5
		storyID, err = distributions.SelectStory(storyRecords)
		if err != nil {
			return -1, err
		}
		for !w.storySemPhs[storyID].TryAcquire(1) {
			storyID, err = distributions.SelectStory(storyRecords)
			if err != nil {
				return -1, err
			}
			window *= 2
			waitFor := time.Duration(rand.Intn(window))
			time.Sleep(waitFor * time.Millisecond)
		}
		defer w.storySemPhs[storyID].Release(1)
	}

	st := time.Now()
	err = w.ops.UpVoteStory(userID, storyID)

	return time.Since(st), err
}

// DownVoteStory ...
func (w *Workload) DownVoteStory(storyID int) (time.Duration, error) {
	w.ops.State.UserMutex.RLock()
	userRecords := w.ops.State.UserRecords
	storyRecords := w.ops.State.StoryRecords
	w.ops.State.UserMutex.RUnlock()

	userID, err := distributions.SelectUser(userRecords)
	if err != nil {
		return -1, err
	}

	if storyID == 0 {
		window := 5
		storyID, err = distributions.SelectStory(storyRecords)
		if err != nil {
			return -1, err
		}
		for !w.storySemPhs[storyID].TryAcquire(1) {
			storyID, err = distributions.SelectStory(storyRecords)
			if err != nil {
				return -1, err
			}
			window *= 2
			waitFor := time.Duration(rand.Intn(window))
			time.Sleep(waitFor * time.Millisecond)
		}
		defer w.storySemPhs[storyID].Release(1)
	}

	st := time.Now()
	err = w.ops.DownVoteStory(userID, storyID)

	return time.Since(st), err
}

// UpVoteComment ...
func (w *Workload) UpVoteComment(commentID int) (time.Duration, error) {
	w.ops.State.UserMutex.RLock()
	userRecords := w.ops.State.UserRecords
	commentRecords := w.ops.State.CommentRecords
	w.ops.State.UserMutex.RUnlock()

	userID, err := distributions.SelectUser(userRecords)
	if err != nil {
		return -1, err
	}

	if commentID == 0 {
		commentID, err = distributions.SelectComment(commentRecords)
		if err != nil {
			return -1, err
		}
	}

	st := time.Now()
	err = w.ops.UpVoteComment(userID, commentID)

	return time.Since(st), err
}

// DownVoteComment ...
func (w *Workload) DownVoteComment(commentID int) (time.Duration, error) {
	w.ops.State.UserMutex.RLock()
	userRecords := w.ops.State.UserRecords
	commentRecords := w.ops.State.CommentRecords
	w.ops.State.UserMutex.RUnlock()

	userID, err := distributions.SelectUser(userRecords)
	if err != nil {
		return -1, err
	}

	if commentID == 0 {
		commentID, err = distributions.SelectComment(commentRecords)
		if err != nil {
			return -1, err
		}
	}

	st := time.Now()
	err = w.ops.DownVoteComment(userID, commentID)

	return time.Since(st), err
}

// Close ...
func (w Workload) Close() {
	w.ops.Close()
}
