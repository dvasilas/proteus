package datastoredriver

import (
	"context"
	"fmt"

	"github.com/dvasilas/proteus/internal/libqpu"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
	"github.com/dvasilas/proteus/internal/queries"
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
		return DatastoreDriverQPU{}, libqpu.Error("unkown datastore type")
	}

	return DatastoreDriverQPU{
		datastore: ds,
	}, nil
}

// ProcessQuery ...
func (q DatastoreDriverQPU) ProcessQuery(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) error {
	if queries.IsSubscribeToAllQuery(query) {
		return q.opConsumer(query, stream)
	} else if queries.IsGetSnapshotQuery(query) {
		return q.snapshotConsumer(query, stream)
	}
	return libqpu.Error("invalid query for datastore_driver QPU")
}

// ---------------- Internal Functions --------------

func (q *DatastoreDriverQPU) snapshotConsumer(query libqpu.InternalQuery, stream libqpu.RequestStream) error {
	logOpCh, errCh := q.datastore.GetSnapshot(query.GetTable(), query.GetProjection())

	var seqID int64
	processSnashopRecord := func(logOp libqpu.LogOperation, stream libqpu.RequestStream, seqID *int64) error {
		ok, err := queries.SatisfiesPredicate(logOp, query)
		if err != nil {
			return err
		}
		if ok {
			fmt.Println("Sending ..")
			if err := stream.Send(*seqID, libqpu.State, logOp); err != nil {
				return err
			}
			*seqID++
		}
		return nil
	}

	return logOpConsumer(logOpCh, errCh, nil, processSnashopRecord, &seqID, stream)
}

func (q DatastoreDriverQPU) opConsumer(query libqpu.InternalQuery, stream libqpu.RequestStream) error {
	fmt.Println("opConsumer")
	logOpCh, cancel, errCh := q.datastore.SubscribeOps(query.GetTable())

	var seqID int64
	processOpRecord := func(logOp libqpu.LogOperation, stream libqpu.RequestStream, seqID *int64) error {
		ok, err := queries.SatisfiesPredicate(logOp, query)
		if err != nil {
			return err
		}
		if ok {
			if err := stream.Send(*seqID, libqpu.Delta, logOp); err != nil {
				return err
			}
			*seqID++
		}
		return nil
	}
	return logOpConsumer(logOpCh, errCh, cancel, processOpRecord, &seqID, stream)
}

func logOpConsumer(logOpCh <-chan libqpu.LogOperation, errCh <-chan error, cancel context.CancelFunc, processOp func(libqpu.LogOperation, libqpu.RequestStream, *int64) error, seqID *int64, stream libqpu.RequestStream) error {
	for {
		select {
		case logOp, ok := <-logOpCh:
			fmt.Println(logOp)
			if !ok {
				logOpCh = nil
			} else {
				err := processOp(logOp, stream, seqID)
				if err != nil {
					if cancel != nil {
						cancel()
					}
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else {
				if cancel != nil {
					cancel()
				}
				return err
			}
		}
		if logOpCh == nil && errCh == nil {
			return stream.Send(
				*seqID,
				libqpu.EndOfStream,
				libqpu.LogOperation{},
			)
		}
	}
}
