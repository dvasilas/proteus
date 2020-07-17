package libbench

import (
	"fmt"
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
	close()
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
	b.config.print()
	metrics := b.measurements.CalculateMetrics()

	fmt.Printf("Runtime: %.3f\n", metrics.Runtime)
	for opType, metrics := range metrics.PerOpMetrics {
		fmt.Printf("[%s] Operation count: %d\n", opType, metrics.OpCount)
		fmt.Printf("[%s] Throughput(old): %.5f\n", opType, metrics.Throughput)
		fmt.Printf("[%s] Throughput(norm): %.5f\n", opType, metrics.ThroughputNorm)
		fmt.Printf("[%s] p50(ms): %.5f\n", opType, metrics.P50)
		fmt.Printf("[%s] p90(ms): %.5f\n", opType, metrics.P90)
		fmt.Printf("[%s] p95(ms): %.5f\n", opType, metrics.P95)
		fmt.Printf("[%s] p99(ms): %.5f\n", opType, metrics.P99)
	}
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

	b.workload.close()

	return nil
}

// Preload ...
func (b Benchmark) Preload() error {
	return b.workload.preload()
}
