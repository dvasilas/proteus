package perf

import (
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
}

// OpMetrics ...
type OpMetrics struct {
	OpCount        int64
	Throughput     float64
	ThroughputNorm float64
	P50            float64
	P90            float64
	P95            float64
	P99            float64
}

// Metrics ...
type Metrics struct {
	Runtime      float64
	PerOpMetrics map[string]OpMetrics
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
	aggregateMeasurements := make(map[string]durations)
	aggregateOpCount := make(map[string]int64)

	var aggregateRuntime time.Duration
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

		aggregateRuntime += threadReport.end.Sub(threadReport.start)

		for measurementType, rawMeasurements := range p.measurementsBuf[threadID].buf {
			aggregateMeasurements[measurementType] = append(
				aggregateMeasurements[measurementType],
				rawMeasurements[:threadReport.opCount[measurementType]]...,
			)
			aggregateOpCount[measurementType] += threadReport.opCount[measurementType]
		}
	}

	runTime := experimentEn.Sub(experimentSt)

	for _, threadMeasurements := range aggregateMeasurements {
		sort.Sort(threadMeasurements)
	}

	m := Metrics{
		PerOpMetrics: make(map[string]OpMetrics),
	}

	m.Runtime = runTime.Seconds()

	for opType, threadMeasurements := range aggregateMeasurements {
		if aggregateOpCount[opType] > 0 {
			opMetrics := OpMetrics{
				OpCount:        aggregateOpCount[opType],
				Throughput:     float64(aggregateOpCount[opType]) / runTime.Seconds(),
				ThroughputNorm: (float64(aggregateOpCount[opType]) / aggregateRuntime.Seconds()) * float64(len(p.measurementsBuf)),
				P50:            durationToMillis(threadMeasurements[threadMeasurements.Len()/2]),
				P90:            durationToMillis(threadMeasurements.percentile(0.9)),
				P95:            durationToMillis(threadMeasurements.percentile(0.95)),
				P99:            durationToMillis(threadMeasurements.percentile(0.99)),
			}
			m.PerOpMetrics[opType] = opMetrics
		}
	}

	return m
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
