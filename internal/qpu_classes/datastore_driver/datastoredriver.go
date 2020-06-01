package datastoredriver

import (
	"context"

	"github.com/dvasilas/proteus/internal/libqpu"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
)

// DatastoreDriverQPU ...
type DatastoreDriverQPU struct {
	datastore dataStore
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
		datastore: ds,
	}, nil
}

// ProcessQuerySnapshot ...
func (q DatastoreDriverQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	return q.datastore.GetSnapshot(query.GetTable(), query.GetProjection())
}

// ProcessQuerySubscribe ...
func (q DatastoreDriverQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh, _, errCh := q.datastore.SubscribeOps(query.GetTable())
	return logOpCh, errCh
}
