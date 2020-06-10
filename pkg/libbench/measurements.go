package libbench

import (
	"fmt"
	"time"

	"github.com/jamiealquiza/tachymeter"
)

func newMeasurements(conf *benchmarkConfig) (*measurements, error) {
	ops, err := newOperations(conf)
	if err != nil {
		return nil, err
	}

	t := tachymeter.New(&tachymeter.Config{Size: conf.Benchmark.OpCount})

	return &measurements{
		ops:        ops,
		tachymeter: t,
	}, nil
}

func (m *measurements) getHomepage(measure bool) error {
	st := time.Now()

	_, err := m.ops.getHomepage()

	if measure {
		m.tachymeter.AddTime(time.Since(st))
	}

	return err
}

func (m *measurements) addUser() error {
	err := m.ops.addUser()

	m.ops.state.addUser()

	return err
}

func (m *measurements) addStory() error {
	userID := m.ops.selectUser()

	err := m.ops.addStory(userID)

	m.ops.state.addStory()

	return err
}

func (m *measurements) addComment() error {
	userID := m.ops.selectUser()
	storyID := m.ops.selectStory()

	err := m.ops.addComment(userID, storyID)

	m.ops.state.addComment()

	return err
}

func (m *measurements) upVoteStory(storyID int) error {
	userID := m.ops.selectUser()
	if storyID == 0 {
		storyID = m.ops.selectStory()
	}

	err := m.ops.upVoteStory(userID, storyID)

	return err
}

func (m *measurements) downVoteStory(storyID int) error {
	userID := m.ops.selectUser()
	if storyID == 0 {
		storyID = m.ops.selectStory()
	}

	err := m.ops.downVoteStory(userID, storyID)

	return err
}

func (m *measurements) upVoteComment(commentID int) error {
	userID := m.ops.selectUser()
	if commentID == 0 {
		commentID = m.ops.selectStory()
	}

	err := m.ops.upVoteComment(userID, commentID)

	return err
}

func (m *measurements) downVoteComment() error {
	userID := m.ops.selectUser()
	commentID := m.ops.selectStory()

	err := m.ops.downVoteComment(userID, commentID)

	return err
}

func printHomepage(hp homepage) error {
	fmt.Println("----------------------")
	for _, story := range hp.stories {
		fmt.Printf("%s | (%s) \n %s \n %d \n", story.title, story.shortID, story.description, story.voteCount)
	}
	fmt.Println("----------------------")
	// for i, attributeKey := range qe.projection {
	// 	if val, found := respRecord.State[attributeKey]; found {
	// 		fmt.Printf("%s: %s", attributeKey, val)
	// 		if i < len(qe.projection)-1 {
	// 			fmt.Printf(", ")
	// 		}
	// 	} else {
	// 		log.Fatal("attribute not found")
	// 	}
	// }
	// fmt.Printf(" \n")

	return nil
}
