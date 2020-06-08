package libbench

import "math/rand"

func (b *Benchmark) selectUser() int {
	b.state.userMutex.RLock()
	userID := rand.Intn(b.state.userRecords-1) + 1
	b.state.userMutex.RUnlock()

	return userID
}

func (b *Benchmark) selectStory() int {
	b.state.storyMutex.RLock()
	storyID := rand.Intn(b.state.storyRecords-1) + 1
	b.state.storyMutex.RUnlock()

	return storyID
}
