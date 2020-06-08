package libbench

import (
	"math/rand"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Preload ...
func (b *Benchmark) Preload() error {
	// start from 1 because when MySQL automaticall assigns ids
	// it starts from 1
	// ¯\_(ツ)_/¯
	for i := 1; i <= b.config.Preload.RecordCount.Users; i++ {
		if err := b.addUser(); err != nil {
			return err
		}
	}
	b.state.userRecords = b.config.Preload.RecordCount.Users

	for i := 1; i <= b.config.Preload.RecordCount.Stories; i++ {
		if err := b.addStory(b.selectUser()); err != nil {
			return err
		}
		if err := b.upVoteStory(b.selectUser(), i); err != nil {
			return err
		}
	}
	b.state.storyRecords = b.config.Preload.RecordCount.Stories

	for i := 1; i <= b.config.Preload.RecordCount.Comments; i++ {

		if err := b.addComment(b.selectUser(), b.selectStory()); err != nil {
			return err
		}
		if err := b.upVoteComment(b.selectUser(), i); err != nil {
			return err
		}
	}
	b.state.commentRecords = b.config.Preload.RecordCount.Comments

	return nil
}

func (b *Benchmark) addUser() error {
	if err := b.ds.insert(
		"users",
		map[string]interface{}{"username": randString(10)},
	); err != nil {
		return err
	}

	b.state.userMutex.Lock()
	b.state.userRecords++
	b.state.userMutex.Unlock()

	return nil
}

func (b *Benchmark) addStory(userID int) error {
	if err := b.ds.insert(
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

	b.state.storyMutex.Lock()
	b.state.storyRecords++
	b.state.storyMutex.Unlock()

	return nil
}

func (b *Benchmark) addComment(userID, storyID int) error {
	if err := b.ds.insert(
		"comments",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"comment":  randString(20),
		},
	); err != nil {
		return err
	}

	b.state.commentMutex.Lock()
	b.state.commentRecords++
	b.state.commentMutex.Unlock()

	return nil
}

func (b *Benchmark) upVoteStory(userID, storyID int) error {
	return b.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     1,
		})
}

func (b *Benchmark) downVoteStory(userID, storyID int) error {
	return b.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     -1,
		})
}

func (b *Benchmark) upVoteComment(userID, commentID int) error {
	storyID, err := b.ds.get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}

	return b.ds.insert(
		"votes",
		map[string]interface{}{
			"user_id":    userID,
			"story_id":   storyID,
			"comment_id": commentID,
			"vote":       1,
		})
}

func (b *Benchmark) downVoteComment(userID, commentID int) error {
	storyID, err := b.ds.get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}
	return b.ds.insert(
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
