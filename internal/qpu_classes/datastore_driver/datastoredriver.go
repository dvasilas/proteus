package datastoredriver

import (
	"context"

	"github.com/dvasilas/proteus/internal/libqpu"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
)

// DatastoreDriverQPU ...
type DatastoreDriverQPU struct {
	datastore               dataStore
	perTableSubscribeStream map[string][]respChannels
}

type respChannels struct {
	logOpCh chan libqpu.LogOperation
	errCh   chan error
}

type dataStore interface {
	GetSnapshot(string, []string) (<-chan libqpu.LogOperation, <-chan error)
	SubscribeOps(string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error)
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU) (DatastoreDriverQPU, error) {
	var ds dataStore
	var err error
	switch qpu.Config.DatastoreConfig.Type {
	case libqpu.MYSQL:
		ds, err = mysqldriver.NewDatastore(qpu.Config, qpu.Schema)
		if err != nil {
			return DatastoreDriverQPU{}, err
		}
	default:
		return DatastoreDriverQPU{}, libqpu.Error("unknown datastore type")
	}

	return DatastoreDriverQPU{
		datastore:               ds,
		perTableSubscribeStream: make(map[string][]respChannels),
	}, nil
}

// ProcessQuerySnapshot ...
func (q DatastoreDriverQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	return q.datastore.GetSnapshot(query.GetTable(), query.GetProjection())
}

// ProcessQuerySubscribe ...
func (q DatastoreDriverQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	if _, found := q.perTableSubscribeStream[query.GetTable()]; !found {
		logOpChFromStore, _, errChFromStore := q.datastore.SubscribeOps(query.GetTable())
		q.perTableSubscribeStream[query.GetTable()] = make([]respChannels, 1)
		q.perTableSubscribeStream[query.GetTable()][0] = respChannels{
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
						for _, respChs := range q.perTableSubscribeStream[query.GetTable()] {
							respChs.logOpCh <- logOp
						}
					}
				case err, ok := <-errChFromStore:
					if !ok {
						errCh = nil
					} else {
						for _, respChs := range q.perTableSubscribeStream[query.GetTable()] {
							respChs.errCh <- err
						}
						return
					}
				}
				if logOpCh == nil && errCh == nil {
					for _, respChs := range q.perTableSubscribeStream[query.GetTable()] {
						close(respChs.logOpCh)
						close(respChs.errCh)
					}
				}
			}
		}()
	} else {
		q.perTableSubscribeStream[query.GetTable()] = append(
			q.perTableSubscribeStream[query.GetTable()],
			respChannels{
				logOpCh: logOpCh,
				errCh:   errCh,
			},
		)
	}

	return logOpCh, errCh
}
