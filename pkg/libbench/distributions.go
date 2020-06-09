package libbench

import "math/rand"

func (op *operations) selectUser() int {
	op.state.userMutex.RLock()
	userID := rand.Intn(op.state.userRecords-1) + 1
	op.state.userMutex.RUnlock()

	return userID
}

func (op *operations) selectStory() int {
	op.state.storyMutex.RLock()
	storyID := rand.Intn(op.state.storyRecords-1) + 1
	op.state.storyMutex.RUnlock()

	return storyID
}

func (op *operations) selectComment() int {
	op.state.commentMutex.RLock()
	commentID := rand.Intn(op.state.commentRecords-1) + 1
	op.state.commentMutex.RUnlock()

	return commentID
}
