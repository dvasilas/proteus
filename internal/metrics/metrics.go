package metrics

import (
	"sort"
	"strconv"
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

type opType int

const (
	writeDB      opType = iota
	writeApplied opType = iota
	query        opType = iota
)

type opLogEntry struct {
	recordWritten int64
	recordsRead   []int64
	ts            time.Time
	opType        opType
}

type opLog []opLogEntry

func (t opLog) Len() int           { return len(t) }
func (t opLog) Less(i, j int) bool { return t[i].ts.Before(t[j].ts) }
func (t opLog) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

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

// FreshnessVersions ...
func FreshnessVersions(queryLog []libqpu.QueryLogEntry, writeLog []libqpu.WriteLogEntry) (float64, float64, float64, float64, error) {
	opLog, err := constructOpLog(queryLog, writeLog)
	if err != nil {
		return -1, -1, -1, -1, err
	}
	versionReadLog := postMortem(opLog)

	hist := stats.NewHistogram(stats.HistogramOptions{
		NumBuckets:   10,
		GrowthFactor: 1,
	})

	for _, e := range versionReadLog {
		hist.Add(int64(e))
	}

	return float64(hist.Buckets[0].Count) / float64(len(versionReadLog)),
		float64(hist.Buckets[1].Count) / float64(len(versionReadLog)),
		float64(hist.Buckets[2].Count) / float64(len(versionReadLog)),
		float64(hist.Buckets[3].Count) / float64(len(versionReadLog)),
		nil
}

func constructOpLog(queryLog []libqpu.QueryLogEntry, writeLog []libqpu.WriteLogEntry) (opLog, error) {
	log := make([]opLogEntry, 2*len(writeLog)+len(queryLog))
	i := 0
	for _, entry := range writeLog {
		log[i] = opLogEntry{recordWritten: entry.RowID, ts: entry.T0, opType: writeDB}
		i++
		log[i] = opLogEntry{recordWritten: entry.RowID, ts: entry.T1, opType: writeApplied}
		i++
	}

	for _, entry := range queryLog {
		rec := opLogEntry{
			recordsRead: make([]int64, len(entry.RowIDs)),
			ts:          entry.Ts,
			opType:      query,
		}

		for j, id := range entry.RowIDs {
			rID, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				return log, err
			}
			rec.recordsRead[j] = rID
		}
		log[i] = rec
		i++
	}

	sort.Sort(opLog(log))

	return log, nil
}

func postMortem(opLog opLog) []int {
	snapshotDatastore := make(map[int64]int)
	snapshotQPU := make(map[int64]int)
	stalenessLog := make([]int, 0)

	for _, entry := range opLog {
		switch entry.opType {
		case writeDB:
			_, ok := snapshotDatastore[entry.recordWritten]
			if ok {
				snapshotDatastore[entry.recordWritten]++
			} else {
				snapshotDatastore[entry.recordWritten] = 1
			}
		case writeApplied:
			_, ok := snapshotQPU[entry.recordWritten]
			if ok {
				snapshotQPU[entry.recordWritten]++
			} else {
				snapshotQPU[entry.recordWritten] = 1
			}
		case query:
			for _, rID := range entry.recordsRead {
				if _, ok := snapshotQPU[rID]; ok {
					stalenessLog = append(stalenessLog, int(snapshotDatastore[rID]-snapshotQPU[rID]))
				}
			}
		}
	}

	return stalenessLog
}
