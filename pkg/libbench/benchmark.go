package libbench

import (
	"errors"
	"sync"

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
	state  benchmarkState
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
func NewBenchmark(configFile, system string) (*Benchmark, error) {
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
	return &Benchmark{
		config: conf,
		ds:     ds,
		qe:     qe,
		state:  benchmarkState{},
	}, nil
}

// GetHomepage ...
func (b *Benchmark) GetHomepage() error {
	return b.qe.getHomepage()
}
