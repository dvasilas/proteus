package distributions

import (
	"crypto/rand"
	"math/big"
)

// SelectUser ...
func SelectUser(userCount int) (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(userCount)))
	if err != nil {
		return -1, err
	}
	return int(n.Int64()) + 1, nil
}

// SelectStory ...
func SelectStory(storyCount int) (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(storyCount)))
	if err != nil {
		return -1, err
	}
	return int(n.Int64()) + 1, nil
}

// SelectComment ...
func SelectComment(commentCount int) (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(commentCount)))
	if err != nil {
		return -1, err
	}
	return int(n.Int64()) + 1, nil
}
