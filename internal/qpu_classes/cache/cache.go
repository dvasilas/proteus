package cacheqpu

import (
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	lrucache "github.com/dvasilas/proteus/internal/qpu_classes/cache/lruCache"
	"github.com/golang/protobuf/ptypes"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"
)

// Describes the interface that any cache implementation needs to expose
// to work with this module.
type cacheImplementation interface {
	Put(query string, response *qpuextapi.QueryResp, size int, client libqpu.APIClient) error
	Get(query string) (*qpuextapi.QueryResp, bool)
}

// CacheQPU ...
type CacheQPU struct {
	cache         cacheImplementation
	adjacentQPUs  []*libqpu.AdjacentQPU
	conf          *libqpu.QPUConfig
	logTimestamps bool
	writeLog      writeLog
	queryLog      queryLog
}

type writeLog struct {
	sync.Mutex
	entries []libqpu.WriteLogEntry
}

type queryLog struct {
	sync.Mutex
	entries []libqpu.QueryLogEntry
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*CacheQPU, error) {
	cqpu := &CacheQPU{
		cache:         lrucache.New(qpu.Config),
		adjacentQPUs:  qpu.AdjacentQPUs,
		conf:          qpu.Config,
		logTimestamps: qpu.Config.Evaluation.LogTimestamps,
		writeLog: writeLog{
			entries: make([]libqpu.WriteLogEntry, 0),
		},
		queryLog: queryLog{
			entries: make([]libqpu.QueryLogEntry, 0),
		},
	}

	go func() {
		time.Sleep(2)
		catchUpDoneCh <- 0
	}()

	return cqpu, nil
}

// ProcessQuerySnapshot ...
func (q *CacheQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	return nil, nil
}

// ClientQuery ...
func (q *CacheQPU) ClientQuery(query libqpu.ASTQuery, queryStr string, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	snapshotTs := time.Now()

	cachedResult, hit := q.cache.Get(queryStr)
	if hit {
		// fmt.Println("hit")
		if q.logTimestamps {
			qLogEntry := libqpu.QueryLogEntry{
				RowIDs: make([]string, len(cachedResult.GetRespRecord())),
				Ts:     snapshotTs,
			}

			for i, respRecord := range cachedResult.GetRespRecord() {
				qLogEntry.RowIDs[i] = respRecord.GetRecordId()
			}
			q.queryLog.Lock()
			q.queryLog.entries = append(q.queryLog.entries, qLogEntry)
			q.queryLog.Unlock()

			// fmt.Println("queryLog", q.queryLog.entries)
		}

		return cachedResult, nil
	}
	// fmt.Println("miss")
	resp, err := q.adjacentQPUs[0].APIClient.QueryUnary(queryStr)
	if err != nil {
		return nil, utils.Error(err)
	}

	cacheEntrySize := 0
	for _, e := range resp.GetRespRecord() {
		cacheEntrySize += len(e.GetAttributes())
	}

	if err := q.cache.Put(queryStr, resp, cacheEntrySize, q.adjacentQPUs[0].APIClient); err != nil {
		return nil, utils.Error(err)
	}

	if q.logTimestamps {
		var t0, t1 time.Time
		t1 = time.Now()

		entries := make([]libqpu.WriteLogEntry, len(resp.GetRespRecord()))
		for _, respRecord := range resp.GetRespRecord() {
			i := 0
			for _, v := range respRecord.GetTimestamp() {
				t0, err = ptypes.Timestamp(v)
				if err != nil {
					return nil, utils.Error(err)
				}
			}
			entries[i] = libqpu.WriteLogEntry{
				RowID: respRecord.GetRecordId(),
				T0:    t0,
				T1:    t1,
			}
			i++
		}
		q.writeLog.Lock()
		q.writeLog.entries = append(q.writeLog.entries, entries...)
		q.writeLog.Unlock()

		// fmt.Println("writeLog" q.writeLog.entries)
	}

	return resp, nil
}

// ClientQuery1 ...
func (q *CacheQPU) ClientQuery1(query libqpu.ASTQuery, queryStr string) (*qpuextapi.QueryResp1, error) {
	return nil, nil
}

// ProcessQuerySubscribe ...
func (q *CacheQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *CacheQPU) RemovePersistentQuery(table string, queryID int) {
}

// GetMetrics ...
func (q *CacheQPU) GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	var err error
	var FL50, FL90, FL95, FL99 float64
	var FV0, FV1, FV2, FV4 float64

	if q.logTimestamps {
		FL50, FL90, FL95, FL99 = metrics.FreshnessLatency(q.writeLog.entries)

		FV0, FV1, FV2, FV4, err = metrics.FreshnessVersions(q.queryLog.entries, q.writeLog.entries)
		if err != nil {
			return nil, err
		}
	}

	return &qpuextapi.MetricsResponse{
		FreshnessLatencyP50: FL50,
		FreshnessLatencyP90: FL90,
		FreshnessLatencyP95: FL95,
		FreshnessLatencyP99: FL99,
		FreshnessVersions0:  FV0,
		FreshnessVersions1:  FV1,
		FreshnessVersions2:  FV2,
		FreshnessVersions4:  FV4,
	}, nil
}

// ---------------- Internal Functions --------------

func (q *CacheQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) error {
	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}

	if respRecordType == libqpu.EndOfStream {
		close(recordCh)
	} else {
		recordCh <- respRecord
	}

	return nil
}

// GetConfig ...
func (q CacheQPU) GetConfig() *qpuapi.ConfigResponse {
	return &qpuapi.ConfigResponse{}
}
