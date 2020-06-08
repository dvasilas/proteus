package libbench

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
)

const credentialsAccessKeyID = "user"
const credentialsSecretAccessKey = "123456"
const datastoreEndpoint = "127.0.0.1:3307"
const datastoreDB = "proteus_lobsters_db"

// Benchmark ...
type Benchmark struct {
	config benchmarkConfig
	qe     queryEngine
	ds     datastore
	state  *benchmarkState
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
	getHomepage() error
}

// NewBenchmark ...
func NewBenchmark(configFile, system string, preload bool) (*Benchmark, error) {
	rand.Seed(time.Now().UnixNano())

	conf, err := getConfig(configFile)
	if err != nil {
		return nil, err
	}
	libqpu.Trace("config", map[string]interface{}{"conf": conf})

	ds, err := newDatastore(datastoreEndpoint, datastoreDB, credentialsAccessKeyID, credentialsSecretAccessKey)
	if err != nil {
		return nil, err
	}

	var qe queryEngine
	if system == "proteus" {
		qe, err = newProteusQueryEngine()
	} else if system == "mysql" {
		qe, err = newMySQLQueryEngine(&ds)
	} else {
		return nil, errors.New("invalid 'system' argument")
	}

	if err != nil {
		return nil, err
	}

	state := benchmarkState{}
	if !preload {
		state.userRecords = conf.Preload.RecordCount.Users
		state.storyRecords = conf.Preload.RecordCount.Stories
		state.commentRecords = conf.Preload.RecordCount.Comments
	}

	return &Benchmark{
		config: conf,
		ds:     ds,
		qe:     qe,
		state:  &state,
	}, nil
}

// Run ...
func (b *Benchmark) Run() error {
	for i := 0; i < 100; i++ {
		f := rand.Float32()
		fmt.Println(f)

		if f < 0.1 {
			if err := b.upVoteStory(b.selectUser(), b.selectStory()); err != nil {
				return err
			}
		} else {
			b.qe.getHomepage()
		}
	}

	return nil
	//
}
