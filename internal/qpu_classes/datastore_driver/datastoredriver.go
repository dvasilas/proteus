package datastoredriver

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
	s3driver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/s3"
	"github.com/dvasilas/proteus/internal/queries"
	"github.com/opentracing/opentracing-go"
)

const stateDatabase = "stateDB"

// DsDriverQPU ...
type DsDriverQPU struct {
	state             libqpu.QPUState
	datastore         dataStore
	persistentQueries map[string]map[int]respChannels
	inputSchema       libqpu.Schema
}

type respChannels struct {
	id      int
	logOpCh chan libqpu.LogOperation
	errCh   chan error
}

type dataStore interface {
	GetSnapshot(string, []string, []string, []string) (<-chan libqpu.LogOperation, <-chan error)
	SubscribeOps(string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error)
	GetNotificationLanency() (float64, float64, float64, float64)
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*DsDriverQPU, error) {
	var ds dataStore
	var err error
	switch qpu.Config.DatastoreConfig.Type {
	case libqpu.MYSQL:
		ds, err = mysqldriver.NewDatastore(qpu.Config, qpu.InputSchema)
		if err != nil {
			return &DsDriverQPU{}, err
		}
	case libqpu.S3:
		ds, err = s3driver.NewDatastore(qpu.Config, qpu.InputSchema)
		if err != nil {
			return &DsDriverQPU{}, err
		}
	default:
		return &DsDriverQPU{}, utils.Error(errors.New("unknown datastore type"))
	}

	go func() {
		catchUpDoneCh <- 0
	}()

	return &DsDriverQPU{
		state:             qpu.State,
		datastore:         ds,
		persistentQueries: make(map[string]map[int]respChannels),
		inputSchema:       qpu.InputSchema,
	}, nil
}

// ProcessQuerySnapshot ...
func (q *DsDriverQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	isNull, isNotNull := query.GetPredicateContains()
	return q.datastore.GetSnapshot(query.GetTable(), query.GetProjection(), isNull, isNotNull)
}

// ProcessQuerySubscribe ...
func (q *DsDriverQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		errCh <- err
		close(logOpCh)
		close(errCh)
	}
	id := int(n.Int64())

	if _, found := q.persistentQueries[query.GetTable()]; !found {
		logOpChFromStore, cancel, errChFromStore := q.datastore.SubscribeOps(query.GetTable())
		q.persistentQueries[query.GetTable()] = make(map[int]respChannels, 1)

		q.persistentQueries[query.GetTable()][id] = respChannels{
			id:      id,
			logOpCh: logOpCh,
			errCh:   errCh,
		}
		go func() {
			for {
				select {
				case logOp, ok := <-logOpChFromStore:
					if !ok {
						logOpCh = nil
					} else {
						// utils.Trace("datastore received", map[string]interface{}{"logOp": logOp, "table": query.GetTable()})
						if len(q.persistentQueries[query.GetTable()]) == 0 {
							cancel()
							delete(q.persistentQueries, query.GetTable())
							return
						}
						for _, respChs := range q.persistentQueries[query.GetTable()] {
							respChs.logOpCh <- logOp
						}
					}
				case err, ok := <-errChFromStore:
					if !ok {
						errCh = nil
					} else {
						if len(q.persistentQueries[query.GetTable()]) == 0 {
							cancel()
							delete(q.persistentQueries, query.GetTable())
							return
						}
						for _, respChs := range q.persistentQueries[query.GetTable()] {
							respChs.errCh <- err
							close(respChs.logOpCh)
							close(respChs.errCh)
						}
						return
					}
				}
				if logOpCh == nil && errCh == nil {
					for _, respChs := range q.persistentQueries[query.GetTable()] {
						close(respChs.logOpCh)
						close(respChs.errCh)
					}
				}
			}
		}()
	} else {

		q.persistentQueries[query.GetTable()][id] = respChannels{
			id:      id,
			logOpCh: logOpCh,
			errCh:   errCh,
		}
	}

	return id, logOpCh, errCh
}

// ClientQuery ...
func (q *DsDriverQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	respRecords := make([]*qpuextapi.QueryRespRecord, 0)

	isNull, isNotNull := query.GetPredicateContains()
	snapshotCh, errCh := q.datastore.GetSnapshot(query.GetTable(), query.GetProjection(), isNull, isNotNull)
	for {
		select {
		case logOp, ok := <-snapshotCh:
			if !ok {
				snapshotCh = nil
			} else {
				// utils.Trace("datastore driver received", map[string]interface{}{"dataItem": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, query)
				// utils.Trace("SatisfiesPredicate", map[string]interface{}{"dataItem": logOp, "ok": ok})
				if err != nil {
					return nil, utils.Error(err)
				}
				if ok {
					attributes := make(map[string]string)
					for k, v := range logOp.GetAttributes() {
						valStr, err := utils.ValueToStr(v)
						if err != nil {
							return nil, err
						}
						attributes[k] = valStr
					}
					respRecords = append(respRecords, &qpuextapi.QueryRespRecord{
						RecordId:   logOp.GetObjectId(),
						Attributes: attributes,
						Timestamp:  logOp.GetTimestamp().GetVc(),
					})
				}
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else {
				return nil, err
			}
		}
		if snapshotCh == nil && errCh == nil {
			break
		}
	}

	return &qpuextapi.QueryResp{
		RespRecord: respRecords,
	}, nil
}

// RemovePersistentQuery ...
func (q *DsDriverQPU) RemovePersistentQuery(table string, queryID int) {
	if _, found := q.persistentQueries[table][queryID]; found {
		close(q.persistentQueries[table][queryID].logOpCh)
		close(q.persistentQueries[table][queryID].errCh)
		delete(q.persistentQueries[table], queryID)
	}
}

// GetConfig ...
func (q *DsDriverQPU) GetConfig() *qpuapi.ConfigResponse {
	schemaTables := make([]string, len(q.inputSchema))
	i := 0
	for table := range q.inputSchema {
		schemaTables[i] = table
		i++
	}
	return &qpuapi.ConfigResponse{
		Schema: schemaTables,
	}
}

// GetMetrics ...
func (q *DsDriverQPU) GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	p50, p90, p95, p99 := q.datastore.GetNotificationLanency()
	return &qpuextapi.MetricsResponse{
		NotificationLatencyP50: p50,
		NotificationLatencyP90: p90,
		NotificationLatencyP95: p95,
		NotificationLatencyP99: p99,
	}, nil
}
