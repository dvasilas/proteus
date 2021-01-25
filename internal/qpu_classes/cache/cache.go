package cacheqpu

import (
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
}

// CacheQPU ...
type CacheQPU struct {
	cache        cacheImplementation
	adjacentQPUs []*libqpu.AdjacentQPU
	conf         *libqpu.QPUConfig
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*CacheQPU, error) {
	cqpu := &CacheQPU{
		cache:        lrucache.New(qpu.Config),
		adjacentQPUs: qpu.AdjacentQPUs,
		conf:         qpu.Config,
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
	cachedResult, hit := q.cache.Get(queryStr)
	if hit {
		return cachedResult, nil
	}

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
	return nil, nil
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
