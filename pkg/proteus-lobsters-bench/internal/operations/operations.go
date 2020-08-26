package operations

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strconv"
	"sync"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	queryengine "github.com/dvasilas/proteus-lobsters-bench/internal/query-engine"
)

// Operations ...
type Operations struct {
	config *config.BenchmarkConfig
	State  *BenchmarkState
	qe     queryengine.QueryEngine
	ds     datastore.Datastore
}

// BenchmarkState ...
type BenchmarkState struct {
	UserRecords    int
	StoryRecords   int
	CommentRecords int
	UserMutex      sync.RWMutex
	StoryMutex     sync.RWMutex
	CommentMutex   sync.RWMutex
}

// Homepage ...
type Homepage struct {
	Stories []Story
}

// Story ...
type Story struct {
	Title       string
	Description string
	ShortID     string
	VoteCount   int64
}

// NewOperations ...
func NewOperations(conf *config.BenchmarkConfig) (*Operations, error) {
	ds, err := datastore.NewDatastore(conf.Connection.Endpoint, conf.Connection.Database, conf.Connection.AccessKeyID, conf.Connection.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	var qe queryengine.QueryEngine
	if !conf.Benchmark.DoPreload {
		switch conf.Benchmark.MeasuredSystem {
		case "proteus":
			qe, err = queryengine.NewProteusQueryEngine(conf.Tracing)
			if err != nil {
				return nil, err
			}
		case "mysql_plain":
			qe = queryengine.NewMySQLPlainQE(&ds)
		case "mysql_mv":
			qe = queryengine.NewMySQLWithViewsQE(&ds)
		default:
			return nil, errors.New("invalid 'system' argument")
		}
	}

	state := BenchmarkState{}
	if !conf.Benchmark.DoPreload {
		state.UserRecords = conf.Preload.RecordCount.Users
		state.StoryRecords = conf.Preload.RecordCount.Stories
		state.CommentRecords = conf.Preload.RecordCount.Comments
	}

	return &Operations{
		config: conf,
		qe:     qe,
		ds:     ds,
		State:  &state,
	}, nil
}

func (st *BenchmarkState) addUser() {
	st.StoryMutex.Lock()
	st.UserRecords++
	st.StoryMutex.Unlock()
}

func (st *BenchmarkState) addStory() {
	st.StoryMutex.Lock()
	st.StoryRecords++
	st.StoryMutex.Unlock()
}

func (st *BenchmarkState) addComment() {
	st.StoryMutex.Lock()
	st.CommentRecords++
	st.StoryMutex.Unlock()
}

// GetHomepage ...
func (op *Operations) GetHomepage() (Homepage, error) {
	resp, err := op.qe.Query(op.config.Operations.Homepage.StoriesLimit)
	if err != nil {
		return Homepage{}, err
	}

	var hp Homepage

	switch op.config.Benchmark.MeasuredSystem {
	case "proteus":
		// response := resp.(*qpu_api.QueryResp)
		// fmt.Println(response)

		// stories := make([]story, len(response))
		// for i, entry := range response {
		// 	stories[i] = story{
		// 		title:       entry.State["title"],
		// 		description: entry.State["description"],
		// 		shortID:     entry.State["short_id"],
		// 	}

		// 	val, err := strconv.ParseInt(entry.State["vote_sum"], 10, 64)
		// 	if err != nil {
		// 		return homepage{}, err
		// 	}
		// 	stories[i].voteCount = val
		// }
		// hp.stories = stories
	case "mysql_plain":
		response := resp.([]map[string]string)
		stories := make([]Story, len(response))
		for i, entry := range response {
			stories[i] = Story{
				Title:       entry["title"],
				Description: entry["description"],
				ShortID:     entry["short_id"],
			}

			val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
			if err != nil {
				return Homepage{}, err
			}
			stories[i].VoteCount = val
		}
		hp.Stories = stories
	case "mysql_mv":
		response := resp.([]map[string]string)
		stories := make([]Story, len(response))
		for i, entry := range response {
			stories[i] = Story{
				Title:       entry["title"],
				Description: entry["description"],
				ShortID:     entry["short_id"],
			}

			val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
			if err != nil {
				return Homepage{}, err
			}
			stories[i].VoteCount = val
		}
		hp.Stories = stories
	}

	return hp, nil
}

// AddUser ...
func (op *Operations) AddUser() error {
	userName, err := randString(10)
	if err != nil {
		return err
	}
	if err := op.ds.Insert(
		"users",
		map[string]interface{}{"username": userName},
	); err != nil {
		return err
	}

	op.State.addUser()

	return nil
}

// AddStory ...
func (op *Operations) AddStory(userID int) error {
	title, err := randString(10)
	if err != nil {
		return err
	}
	description, err := randString(30)
	if err != nil {
		return err
	}
	shortID, err := randString(5)
	if err != nil {
		return err
	}

	if err := op.ds.Insert(
		"stories",
		map[string]interface{}{
			"user_id":     userID,
			"title":       title,
			"description": description,
			"short_id":    shortID[0:4],
		},
	); err != nil {
		return err
	}

	op.State.addStory()

	return nil
}

// AddComment ...
func (op *Operations) AddComment(userID, storyID int) error {
	comment, err := randString(20)
	if err != nil {
		return err
	}

	if err := op.ds.Insert(
		"comments",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"comment":  comment,
		},
	); err != nil {
		return err
	}

	op.State.addComment()

	return nil
}

// UpVoteStory ...
func (op *Operations) UpVoteStory(userID, storyID int) error {
	return op.ds.Insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     1,
		})
}

// DownVoteStory ...
func (op *Operations) DownVoteStory(userID, storyID int) error {
	return op.ds.Insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     -1,
		})
}

// UpVoteComment ...
func (op *Operations) UpVoteComment(userID, commentID int) error {
	storyID, err := op.ds.Get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}

	return op.ds.Insert(
		"votes",
		map[string]interface{}{
			"user_id":    userID,
			"story_id":   storyID,
			"comment_id": commentID,
			"vote":       1,
		})
}

// DownVoteComment ...
func (op *Operations) DownVoteComment(userID, commentID int) error {
	storyID, err := op.ds.Get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}
	return op.ds.Insert(
		"votes",
		map[string]interface{}{
			"user_id":    userID,
			"story_id":   storyID,
			"comment_id": commentID,
			"vote":       -1,
		})
}

// Close ...
func (op *Operations) Close() {
	op.qe.Close()
}

func randString(length int) (string, error) {
	b, err := generateRandomBytes(length)
	return base64.URLEncoding.EncodeToString(b), err
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}
