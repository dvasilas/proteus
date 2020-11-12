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

// NotificationLatencyM ...
type NotificationLatencyM struct {
	hist *stats.Histogram
}

// NewNotificationLatencyM ...
func NewNotificationLatencyM() NotificationLatencyM {
	return NotificationLatencyM{
		hist: stats.NewHistogram(histogramOpts),
	}
}

// Add ...
func (m NotificationLatencyM) Add(logOp libqpu.LogOperation) (err error) {
	var t0, t1 time.Time
	t1 = time.Now()

	for _, v := range logOp.GetTimestamp().GetVc() {
		t0, err = ptypes.Timestamp(v)
		if err != nil {
			return err
		}
	}

	return m.hist.Add(t1.Sub(t0).Nanoseconds())
}

// GetMetrics ...
func (m NotificationLatencyM) GetMetrics() (float64, float64, float64, float64) {
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
