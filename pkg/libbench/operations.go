package libbench

import (
	"errors"
	"math/rand"
	"strconv"

	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type homepage struct {
	stories []story
}

type story struct {
	title       string
	description string
	shortID     string
	voteCount   int64
}

func newOperations(conf *benchmarkConfig) (*operations, error) {
	ds, err := newDatastore(datastoreEndpoint, datastoreDB, credentialsAccessKeyID, credentialsSecretAccessKey)
	if err != nil {
		return nil, err
	}

	var qe queryEngine
	if !conf.doPreload {
		if conf.measuredSystem == "proteus" {
			qe, err = newProteusQueryEngine()
		} else if conf.measuredSystem == "mysql_plain" {
			qe, err = newMySQLPlainQE(&ds)
		} else if conf.measuredSystem == "mysql_views" {
			qe, err = neMySQLWithViewsQE(&ds)
		} else {
			return nil, errors.New("invalid 'system' argument")
		}
	}

	state := benchmarkState{}
	if !conf.doPreload {
		state.userRecords = conf.Preload.RecordCount.Users
		state.storyRecords = conf.Preload.RecordCount.Stories
		state.commentRecords = conf.Preload.RecordCount.Comments
	}

	return &operations{
		config: conf,
		qe:     qe,
		ds:     ds,
		state:  &state,
	}, nil
}

func (st *benchmarkState) addUser() {
	st.storyMutex.Lock()
	st.userRecords++
	st.storyMutex.Unlock()
}

func (st *benchmarkState) addStory() {
	st.storyMutex.Lock()
	st.storyRecords++
	st.storyMutex.Unlock()
}

func (st *benchmarkState) addComment() {
	st.storyMutex.Lock()
	st.commentRecords++
	st.storyMutex.Unlock()
}

func (op *operations) getHomepage() (homepage, error) {
	resp, err := op.qe.query(op.config.Operations.Homepage.StoriesLimit)
	if err != nil {
		return homepage{}, err
	}

	var hp homepage

	switch op.config.measuredSystem {
	case "proteus":
		response := resp.([]proteusclient.ResponseRecord)
		stories := make([]story, len(response))
		for i, entry := range response {
			stories[i] = story{
				title:       entry.State["title"],
				description: entry.State["description"],
				shortID:     entry.State["short_id"],
			}

			val, err := strconv.ParseInt(entry.State["vote_sum"], 10, 64)
			if err != nil {
				return homepage{}, err
			}
			stories[i].voteCount = val
		}
		hp.stories = stories
	default:
	case "mysql_plain":
		response := resp.([]map[string]string)
		stories := make([]story, len(response))
		for i, entry := range response {
			stories[i] = story{
				title:       entry["title"],
				description: entry["description"],
				shortID:     entry["short_id"],
			}

			val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
			if err != nil {
				return homepage{}, err
			}
			stories[i].voteCount = val
		}
		hp.stories = stories
	}

	return hp, nil
}

func (op *operations) addUser() error {
	if err := op.ds.insert(
		"users",
		map[string]interface{}{"username": randString(10)},
	); err != nil {
		return err
	}

	return nil
}

func (op *operations) addStory(userID int) error {
	if err := op.ds.insert(
		"stories",
		map[string]interface{}{
			"user_id":     userID,
			"title":       randString(10),
			"description": randString(30),
			"short_id":    randString(5),
		},
	); err != nil {
		return err
	}

	return nil
}

func (op *operations) addComment(userID, storyID int) error {
	if err := op.ds.insert(
		"comments",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"comment":  randString(20),
		},
	); err != nil {
		return err
	}

	return nil
}

func (op *operations) upVoteStory(userID, storyID int) error {
	return op.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     1,
		})
}

func (op *operations) downVoteStory(userID, storyID int) error {
	return op.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     -1,
		})
}

func (op *operations) upVoteComment(userID, commentID int) error {
	storyID, err := op.ds.get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}

	return op.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":    userID,
			"story_id":   storyID,
			"comment_id": commentID,
			"vote":       1,
		})
}

func (op *operations) downVoteComment(userID, commentID int) error {
	storyID, err := op.ds.get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}
	return op.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":    userID,
			"story_id":   storyID,
			"comment_id": commentID,
			"vote":       -1,
		})
}

func randString(length int) string {
	str := make([]rune, length)
	for i := range str {
		str[i] = letters[rand.Intn(len(letters))]
	}
	return string(str)
}
