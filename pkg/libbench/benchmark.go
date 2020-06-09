package libbench

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/jamiealquiza/tachymeter"
)

const credentialsAccessKeyID = "user"
const credentialsSecretAccessKey = "123456"
const datastoreEndpoint = "127.0.0.1:3307"
const datastoreDB = "proteus_lobsters_db"

// Benchmark ...
type Benchmark struct {
	config       *benchmarkConfig
	measurements *measurements
}

type measurements struct {
	ops        *operations
	tachymeter *tachymeter.Tachymeter
}

type operations struct {
	config *benchmarkConfig
	state  *benchmarkState
	qe     queryEngine
	ds     datastore
}

type benchmarkState struct {
	userRecords    int
	storyRecords   int
	commentRecords int
	userMutex      sync.RWMutex
	storyMutex     sync.RWMutex
	commentMutex   sync.RWMutex
}

type queryEngine interface {
	query(limit int) (interface{}, error)
}

// NewBenchmark ...
func NewBenchmark(configFile, system string, preload bool) (Benchmark, error) {
	rand.Seed(time.Now().UnixNano())

	conf, err := getConfig(configFile)
	if err != nil {
		return Benchmark{}, err
	}
	conf.doPreload = preload
	conf.measuredSystem = system

	libqpu.Trace("config", map[string]interface{}{"conf": conf})

	measurements, err := newMeasurements(&conf)
	if err != nil {
		return Benchmark{}, err
	}

	return Benchmark{
		config:       &conf,
		measurements: measurements,
	}, nil
}

// PrintMeasurements ...
func (b Benchmark) PrintMeasurements() {
	fmt.Println(b.measurements.tachymeter.Calc())

}

// Run ...
func (b Benchmark) Run(threadCount int) error {
	var wg sync.WaitGroup

	wallTimeStart := time.Now()

	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go b.runWorkload(&wg)
	}

	wg.Wait()

	b.measurements.tachymeter.SetWallTime(time.Since(wallTimeStart))

	return nil
}

func (b Benchmark) runWorkload(wg *sync.WaitGroup) {
	defer wg.Done()

	opCnt := 0
	warmupPeriod, warmupTimeout := b.config.DoWarmup, time.After(time.Duration(b.config.Warmup)*time.Second)
	for timeIsUp, timeout := true, time.After(time.Duration(b.config.Runtime)*time.Second); timeIsUp; {

		select {
		case <-timeout:
			timeIsUp = false
		case <-warmupTimeout:
			warmupPeriod = false
		default:
		}
		r := rand.Float64()
		if r < b.config.Operations.WriteRatio {
			vote := rand.Float64()
			if vote < b.config.Operations.DownVoteRatio {
				if err := b.measurements.downVoteStory(0); err != nil {
					log.Fatal(err)
				}
			} else {
				if err := b.measurements.upVoteStory(0); err != nil {
					log.Fatal(err)
				}
			}
		} else {
			err := b.measurements.getHomepage(!warmupPeriod)
			if err != nil {
				log.Fatal(err)
			}
		}
		opCnt++
		if opCnt == b.config.OpCount {
			break
		}
	}
}

// Preload ...
func (b Benchmark) Preload() error {
	// start from 1 because when MySQL automaticall assigns ids
	// it starts from 1
	// ¯\_(ツ)_/¯
	for i := 1; i <= b.config.Preload.RecordCount.Users; i++ {
		if err := b.measurements.addUser(); err != nil {
			return err
		}
	}

	for i := 1; i <= b.config.Preload.RecordCount.Stories; i++ {
		if err := b.measurements.addStory(); err != nil {
			return err
		}
		if err := b.measurements.upVoteStory(i); err != nil {
			return err
		}
	}

	for i := 1; i <= b.config.Preload.RecordCount.Comments; i++ {
		if err := b.measurements.addComment(); err != nil {
			return err
		}
		if err := b.measurements.upVoteComment(i); err != nil {
			return err
		}
	}

	return nil
}
