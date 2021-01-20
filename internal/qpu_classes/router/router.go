package routerqpu

import (
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"
)

// RouterQPU ...
type RouterQPU struct {
	adjacentQPUs []*libqpu.AdjacentQPU
	conf         *libqpu.QPUConfig
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*RouterQPU, error) {

	rqpu := &RouterQPU{
		adjacentQPUs: qpu.AdjacentQPUs,
		conf:         qpu.Config,
	}

	go func() {
		time.Sleep(2)
		catchUpDoneCh <- 0
	}()

	return rqpu, nil
}

// ProcessQuerySnapshot ...
func (q *RouterQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	return nil, nil
}

// ClientQuery ...
func (q *RouterQPU) ClientQuery(query libqpu.ASTQuery, queryStr string, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	queryRespCh := make(chan qpuextapi.QueryResp)
	errorCh := make(chan error)

	respRecords := make([]*qpuextapi.QueryRespRecord, 0)
	subQueryCount := len(q.adjacentQPUs)

	for _, adjQPU := range q.adjacentQPUs {
		go func(to *libqpu.AdjacentQPU) {
			resp, err := to.APIClient.QueryUnary(queryStr)
			if err != nil {
				errorCh <- err
				return
			}
			queryRespCh <- *resp
		}(adjQPU)
	}

	returnedCount := 0
	for {
		select {
		case resp := <-queryRespCh:
			respRecords = append(respRecords, resp.GetRespRecord()...)
			returnedCount++
			if returnedCount == subQueryCount {
				close(queryRespCh)
				close(errorCh)
				return &qpuextapi.QueryResp{
					RespRecord: respRecords,
				}, nil
			}
		case err := <-errorCh:
			return nil, err
		}
	}
}

// ProcessQuerySubscribe ...
func (q *RouterQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *RouterQPU) RemovePersistentQuery(table string, queryID int) {
}

// GetMetrics ...
func (q *RouterQPU) GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	return nil, nil
}

// ---------------- Internal Functions --------------

func (q *RouterQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
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
func (q RouterQPU) GetConfig() *qpuapi.ConfigResponse {
	return &qpuapi.ConfigResponse{}
}
