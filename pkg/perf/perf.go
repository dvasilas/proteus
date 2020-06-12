package perf

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type durations []time.Duration

func (d durations) Len() int           { return len(d) }
func (d durations) Less(i, j int) bool { return int64(d[i]) < int64(d[j]) }
func (d durations) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func (d durations) percentile(p float64) time.Duration {
	return d[int(float64(d.Len())*p+0.5)-1]
}

type threadRawMeasurements struct {
	start   time.Time
	end     time.Time
	buf     map[string][]time.Duration
	opCount map[string]int64
}

// Perf ...
type Perf struct {
	sync.Mutex
	measurementsBuf []threadRawMeasurements
	runTime         time.Duration
}

// Metrics ...
type Metrics struct {
	// measurements           durations
	// aggregatedMeasurements durations
}

// New ...
func New() *Perf {
	return &Perf{
		measurementsBuf: make([]threadRawMeasurements, 0),
	}
}

//ReportMeasurements ...
func (p *Perf) ReportMeasurements(m map[string][]time.Duration, opCount map[string]int64, st, en time.Time) {
	p.Lock()
	p.measurementsBuf = append(p.measurementsBuf, threadRawMeasurements{
		start:   st,
		end:     en,
		buf:     m,
		opCount: opCount,
	})
	p.Unlock()
}

// CalculateMetrics ...
func (p *Perf) CalculateMetrics() Metrics {
	aggregatedMeasurements := make(map[string]durations)
	aggregatedOpCount := make(map[string]int64)
	var experimentSt, experimentEn time.Time
	for threadID, threadReport := range p.measurementsBuf {
		if threadID == 0 {
			experimentSt = threadReport.start
			experimentEn = threadReport.end
		}
		if threadReport.start.Before(experimentSt) {
			experimentSt = threadReport.start
		}
		if threadReport.end.After(experimentEn) {
			experimentEn = threadReport.end
		}
		for measurementType, rawMeasurements := range p.measurementsBuf[threadID].buf {
			aggregatedMeasurements[measurementType] = append(
				aggregatedMeasurements[measurementType],
				rawMeasurements[:threadReport.opCount[measurementType]]...,
			)
			aggregatedOpCount[measurementType] += threadReport.opCount[measurementType]
		}
	}

	runTime := experimentEn.Sub(experimentSt)

	for _, threadMeasurements := range aggregatedMeasurements {
		sort.Sort(durations(threadMeasurements))
	}

	// fmt.Println()
	// fmt.Println("aggregatedMeasurements ", aggregatedMeasurements)
	// fmt.Println()

	fmt.Printf("Runtime: %.3f\n", runTime.Seconds())

	for opType, threadMeasurements := range aggregatedMeasurements {
		fmt.Printf("[%s] Operation count: %d\n", opType, aggregatedOpCount[opType])
		fmt.Printf("[%s] Throughput: %.5f\n", opType, float64(aggregatedOpCount[opType])/float64(runTime.Seconds()))
		fmt.Printf("[%s] p50(ms): %.5f\n", opType, durationToMillis(threadMeasurements[threadMeasurements.Len()/2]))
		fmt.Printf("[%s] p90(ms): %.5f\n", opType, durationToMillis(threadMeasurements.percentile(0.9)))
		fmt.Printf("[%s] p95(ms): %.5f\n", opType, durationToMillis(threadMeasurements.percentile(0.95)))
		fmt.Printf("[%s] p99(ms): %.5f\n", opType, durationToMillis(threadMeasurements.percentile(0.99)))
	}

	// 	m.aggregatedMeasurements = make([]time.Duration, 0)
	// 	var totalOpCount int64
	// 	for _, thread := range m.measurements {
	// 		totalOpCount += thread.size
	// 		m.aggregatedMeasurements = append(m.aggregatedMeasurements, thread.durations[:thread.size]...)
	// 	}

	// fmt.Printf("p50(ms): %.5f\n", durationToMillis(m.aggregatedMeasurements[m.aggregatedMeasurements.Len()/2]))
	// 	fmt.Printf("p90(ms): %.5f\n", durationToMillis(m.aggregatedMeasurements.percentile(0.9)))
	// 	fmt.Printf("p95(ms): %.5f\n", durationToMillis(m.aggregatedMeasurements.percentile(0.95)))
	// 	fmt.Printf("p99(ms): %.5f\n", durationToMillis(m.aggregatedMeasurements.percentile(0.99)))

	return Metrics{}
}

func calcExperimentStart(measurements map[string][]time.Duration) time.Duration {
	var min time.Duration
	for _, opType := range measurements {
		if min == 0 || opType[0] < min {
			min = opType[0]
		}
	}
	return min
}

func calcExperimentEnd(measurements map[string][]time.Duration) time.Duration {
	var max time.Duration
	for _, opType := range measurements {
		if opType[len(opType)-1] > max {
			max = opType[len(opType)-1]
		}
	}
	return max
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
