package cacheqpu

import (
	"context"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	lrucache "github.com/dvasilas/proteus/internal/qpu_classes/cache/lruCache"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"
)

// Describes the interface that any cache implementation needs to expose
// to work with this module.
type cacheImplementation interface {
	Put(query string, response *qpuextapi.QueryResp, size int, client libqpu.APIClient) error
	Get(query string) (*qpuextapi.QueryResp, bool)
	WaitInvalidate(qpuapi.QPUAPI_QuerySubscribeClient, context.CancelFunc, string)
}

// CacheQPU ...
type CacheQPU struct {
	cache         cacheImplementation
	adjacentQPUs  []*libqpu.AdjacentQPU
	conf          *libqpu.QPUConfig
	logTimestamps bool
	writeLog      writeLog
	queryLog      queryLog
	putInCacheLog writeLog
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

		putInCacheLog: writeLog{
			entries: make([]libqpu.WriteLogEntry, 0),
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
		}

		return cachedResult, nil
	}
	resp, err := q.adjacentQPUs[0].APIClient.QueryUnary(queryStr)
	if err != nil {
		return nil, utils.Error(err)
	}

	cacheEntrySize := 0

	if q.logTimestamps {
		q.putInCacheLog.Lock()
	}
	for _, e := range resp.GetRespRecord() {
		cacheEntrySize += len(e.GetAttributes())

		if q.logTimestamps {
			q.putInCacheLog.entries = append(q.putInCacheLog.entries, libqpu.WriteLogEntry{
				RowID: e.GetRecordId(),
				T1:    time.Now(),
			})
		}
	}
	if q.logTimestamps {
		q.putInCacheLog.Unlock()
	}

	if err := q.cache.Put(queryStr, resp, cacheEntrySize, q.adjacentQPUs[0].APIClient); err != nil {
		return nil, utils.Error(err)
	}

	// QuerySubscribe
	stream, cancel, err := q.adjacentQPUs[0].APIClient.QuerySubscribe(queryStr)
	if err != nil {
		return nil, utils.Error(err)
	}

	q.cache.WaitInvalidate(stream, cancel, queryStr)

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
	}

	return resp, nil
}

// ProcessQuerySubscribe ...
func (q *CacheQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *CacheQPU) RemovePersistentQuery(table string, queryID int) {
}

// QuerySubscribe  ...
func (q *CacheQPU) QuerySubscribe(query libqpu.ASTQuery, res *qpuextapi.QueryReq) (chan libqpu.LogOperation, chan bool, chan error) {
	return nil, nil, nil
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
