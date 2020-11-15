package metrics

import (
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/benchmark/stats"
)

var (
	histogramOpts = stats.HistogramOptions{
		// up to 2s
		NumBuckets:   200000,
		GrowthFactor: .01,
	}
)

// LatencyM ...
type LatencyM struct {
	hist *stats.Histogram
}

// NewLatencyM ...
func NewLatencyM() LatencyM {
	return LatencyM{
		hist: stats.NewHistogram(histogramOpts),
	}
}

// AddFromTs ...
func (m LatencyM) AddFromTs(t0 time.Time) (err error) {
	return m.hist.Add(time.Since(t0).Nanoseconds())
}

// AddFromOp ...
func (m LatencyM) AddFromOp(logOp libqpu.LogOperation) (err error) {
	var t0, t1 time.Time
	t1 = time.Now()

	for _, v := range logOp.GetTimestamp().GetVc() {
		t0, err = ptypes.Timestamp(v)
		if err != nil {
			return err
		}
	}

	if t1.After(t0) {
		return m.hist.Add(t1.Sub(t0).Nanoseconds())
	}
	return nil
}

// GetMetrics ...
func (m LatencyM) GetMetrics() (float64, float64, float64, float64) {
	return durationToMillis(time.Duration(pepcentile(.5, m.hist))),
		durationToMillis(time.Duration(pepcentile(.9, m.hist))),
		durationToMillis(time.Duration(pepcentile(.95, m.hist))),
		durationToMillis(time.Duration(pepcentile(.99, m.hist)))
}

func pepcentile(percentile float64, h *stats.Histogram) int64 {
	percentileCount := int64(float64(h.Count) * percentile)
	currentCount := int64(0)
	for _, bucket := range h.Buckets {
		if currentCount+bucket.Count >= percentileCount {
			lastBuckedFilled := float64(percentileCount-currentCount) / float64(bucket.Count)
			return int64((1.0-lastBuckedFilled)*bucket.LowBound + lastBuckedFilled*bucket.LowBound*(1.0+histogramOpts.GrowthFactor))
		}
		currentCount += bucket.Count
	}
	panic("should have found a bound")
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// FreshnessLatency ...
func FreshnessLatency(writeLog []libqpu.WriteLogEntry) (float64, float64, float64, float64) {
	hist := stats.NewHistogram(histogramOpts)

	for _, e := range writeLog {
		hist.Add(e.T1.Sub(e.T0).Nanoseconds())
	}

	return durationToMillis(time.Duration(pepcentile(.5, hist))),
		durationToMillis(time.Duration(pepcentile(.9, hist))),
		durationToMillis(time.Duration(pepcentile(.95, hist))),
		durationToMillis(time.Duration(pepcentile(.99, hist)))
}
