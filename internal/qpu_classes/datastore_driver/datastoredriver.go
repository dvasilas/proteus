package datastoredriver

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
	s3driver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/s3"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"github.com/opentracing/opentracing-go"
)

const stateDatabase = "stateDB"

// DsDriverQPU ...
type DsDriverQPU struct {
	state             libqpu.QPUState
	datastore         dataStore
	persistentQueries map[string]map[int]respChannels
	inputSchema       libqpu.Schema
	logTimestamps     bool
}

type respChannels struct {
	id      int
	logOpCh chan libqpu.LogOperation
	errCh   chan error
}

type dataStore interface {
	GetSnapshot(string, []string, []string, []string) (<-chan libqpu.LogOperation, <-chan error)
	SubscribeOps(string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error)
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

	if qpu.Config.Evaluation.LogTimestamps {
		if err := qpu.State.Init(stateDatabase, "", ""); err != nil {
			return &DsDriverQPU{}, err
		}
	}

	go func() {
		catchUpDoneCh <- 0
	}()

	return &DsDriverQPU{
		state:             qpu.State,
		datastore:         ds,
		persistentQueries: make(map[string]map[int]respChannels),
		inputSchema:       qpu.InputSchema,
		logTimestamps:     qpu.Config.Evaluation.LogTimestamps,
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
						if q.logTimestamps {
							rowID, err := getRowID(logOp)
							if err != nil {
								panic(utils.Error(err))
							}
							go q.state.LogReceivedUpdateRec(rowID, logOp.GetTimestamp().GetVc(), time.Now())
						}

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
func (q *DsDriverQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*pb.QueryResp, error) {
	return nil, nil
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
func (q *DsDriverQPU) GetConfig() *qpu_api.ConfigResponse {
	schemaTables := make([]string, len(q.inputSchema))
	i := 0
	for table := range q.inputSchema {
		schemaTables[i] = table
		i++
	}
	return &qpu_api.ConfigResponse{
		Schema: schemaTables,
	}
}

// this is use case specific
// should not be hardcoded here
func getRowID(logOp libqpu.LogOperation) (interface{}, error) {
	switch logOp.GetTable() {
	case "votes":
		return logOp.GetAttributes()["story_id"].GetInt(), nil
	default:
		return nil, errors.New("unknown bucket")
	}
}
