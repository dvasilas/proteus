package datastoredriver

import (
	"context"
	"math/rand"

	"github.com/dvasilas/proteus/internal/libqpu"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
)

// DatastoreDriverQPU ...
type DatastoreDriverQPU struct {
	datastore         dataStore
	persistentQueries map[string]map[int]respChannels
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
func InitClass(qpu *libqpu.QPU) (*DatastoreDriverQPU, error) {
	var ds dataStore
	var err error
	switch qpu.Config.DatastoreConfig.Type {
	case libqpu.MYSQL:
		ds, err = mysqldriver.NewDatastore(qpu.Config, qpu.Schema)
		if err != nil {
			return &DatastoreDriverQPU{}, err
		}
	default:
		return &DatastoreDriverQPU{}, libqpu.Error("unknown datastore type")
	}

	return &DatastoreDriverQPU{
		datastore:         ds,
		persistentQueries: make(map[string]map[int]respChannels),
	}, nil
}

// ProcessQuerySnapshot ...
func (q *DatastoreDriverQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {

	isNull, isNotNull := query.GetPredicateContains()
	return q.datastore.GetSnapshot(query.GetTable(), query.GetProjection(), isNull, isNotNull)
}

// ProcessQuerySubscribe ...
func (q *DatastoreDriverQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)
	id := rand.Int()

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
						libqpu.Trace("datastore received", map[string]interface{}{"logOp": logOp, "table": query.GetTable()})
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

// RemovePersistentQuery ...
func (q *DatastoreDriverQPU) RemovePersistentQuery(table string, queryID int) {
	if _, found := q.persistentQueries[table][queryID]; found {
		close(q.persistentQueries[table][queryID].logOpCh)
		close(q.persistentQueries[table][queryID].errCh)
		delete(q.persistentQueries[table], queryID)
	}
}
