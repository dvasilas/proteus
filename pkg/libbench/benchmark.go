package libbench

import (
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/pkg/perf"
	log "github.com/sirupsen/logrus"
)

// Benchmark ...
type Benchmark struct {
	config       *benchmarkConfig
	workload     *workload
	measurements *perf.Perf
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
func NewBenchmark(configFile, system string, preload bool, threadCnt int) (Benchmark, error) {
	rand.Seed(time.Now().UnixNano())

	conf, err := getConfig(configFile)
	if err != nil {
		return Benchmark{}, err
	}
	conf.Benchmark.doPreload = preload
	conf.Benchmark.measuredSystem = system
	if threadCnt > 0 {
		conf.Benchmark.ThreadCount = threadCnt
	}

	log.WithFields(log.Fields{"conf": conf}).Info("configuration")

	workload, err := newWorkload(&conf)
	if err != nil {
		return Benchmark{}, err
	}

	return Benchmark{
		config:       &conf,
		workload:     workload,
		measurements: perf.New(),
	}, nil
}

// PrintMeasurements ...
func (b Benchmark) PrintMeasurements() {

}

// Run ...
func (b Benchmark) Run() error {
	var wg sync.WaitGroup

	for i := 0; i < b.config.Benchmark.ThreadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			measurements, measurementBufferSize, startTime, endTime := b.workload.run(b.config.Benchmark.OpCount)
			b.measurements.ReportMeasurements(measurements, measurementBufferSize, startTime, endTime)
		}()
	}

	wg.Wait()

	b.measurements.CalculateMetrics()

	return nil
}

// Preload ...
func (b Benchmark) Preload() error {
	return b.workload.preload()
}
