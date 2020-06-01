package mysqldriver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/mysql"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"google.golang.org/grpc"
)

// MySQLDataStore ...
type MySQLDataStore struct {
	subscriptionEndpoint string
	cli                  mysql.PublishUpdatesClient
	schema               libqpu.Schema
	conn                 *grpc.ClientConn
	db                   *sql.DB
}

// MySQLUpdate ...
type MySQLUpdate struct {
	RecordID   string
	Table      string
	Attributes []struct {
		Key      string
		ValueOld string
		ValueNew string
	}
}

//---------------- API Functions -------------------

// NewDatastore ...
func NewDatastore(conf *libqpu.QPUConfig, schema libqpu.Schema) (MySQLDataStore, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		conf.DatastoreConfig.Credentials.AccessKeyID,
		conf.DatastoreConfig.Credentials.SecretAccessKey,
		conf.DatastoreConfig.Endpoint,
		conf.DatastoreConfig.DBName,
	)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return MySQLDataStore{}, err
	}

	conn, err := grpc.Dial(conf.DatastoreConfig.LogStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		return MySQLDataStore{}, err
	}

	s := MySQLDataStore{
		subscriptionEndpoint: conf.DatastoreConfig.LogStreamEndpoint,
		cli:                  mysql.NewPublishUpdatesClient(conn),
		schema:               schema,
		conn:                 conn,
		db:                   db,
	}

	return s, nil
}

// SubscribeOps ...
func (ds MySQLDataStore) SubscribeOps(table string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := ds.cli.SubscribeToUpdates(ctx)
	if err != nil {
		errCh <- err
		cancel()
		return nil, nil, errCh
	}

	err = stream.Send(
		&mysql.RequestStream{
			Val: &mysql.RequestStream_Request{
				Request: &mysql.SubRequest{
					Timestamp: 0,
					Sync:      false,
					Table:     table,
				},
			},
		},
	)

	go ds.opConsumer(stream, logOpCh, errCh)

	return logOpCh, cancel, errCh
}

// GetSnapshot ...
func (ds MySQLDataStore) GetSnapshot(table string, columns []string) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	projection := ""
	for i, col := range columns {
		projection += col
		if i < len(columns)-1 {
			projection += ", "
		}
	}
	query := fmt.Sprintf("SELECT %s FROM %s", projection, table)

	rows, err := ds.db.Query(query)
	if err != nil {
		errCh <- err
		return logOpCh, errCh
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	go func() {
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				errCh <- err
				break
			}

			var recordID string
			attributes := make(map[string]*qpu.Value, 0)
			for i, col := range values {
				if i == 0 {
					recordID = string(col)
				} else if col != nil {
					value, err := ds.schema.StrToValue(table, columns[i], string(col))
					if err != nil {
						errCh <- err
						break
					}
					attributes[columns[i]] = value
				}
			}

			logOpCh <- libqpu.LogOperationState(
				recordID,
				table,
				libqpu.Vectorclock(map[string]uint64{ds.subscriptionEndpoint: uint64(time.Now().UnixNano())}),
				attributes)

		}
		close(logOpCh)
		close(errCh)
	}()

	return logOpCh, errCh
}

// ---------------- Internal Functions --------------

func (ds MySQLDataStore) opConsumer(stream mysql.PublishUpdates_SubscribeToUpdatesClient, msg chan libqpu.LogOperation, errCh chan error) {
	for {
		op, err := stream.Recv()
		if err == io.EOF {
			errCh <- libqpu.Error("opConsumer received EOF")
			break
		}
		if err != nil {
			errCh <- err
			break
		}
		formattedOp, err := ds.formatLogOpDelta(op)
		if err != nil {
			errCh <- err
			break
		}
		msg <- formattedOp

	}
	close(msg)
	close(errCh)
}

func (ds MySQLDataStore) formatLogOpDelta(notificationMsg *mysql.NotificationStream) (libqpu.LogOperation, error) {
	var update MySQLUpdate
	if err := json.Unmarshal([]byte(notificationMsg.GetPayload()), &update); err != nil {
		panic(err)
	}

	attributesOld := make(map[string]*qpu.Value)
	attributesNew := make(map[string]*qpu.Value)
	for _, attribute := range update.Attributes {
		if attribute.ValueOld != "" {
			value, err := ds.schema.StrToValue(update.Table, attribute.Key, attribute.ValueOld)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesOld[attribute.Key] = value
		}
		if attribute.ValueNew != "" {
			value, err := ds.schema.StrToValue(update.Table, attribute.Key, attribute.ValueNew)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesNew[attribute.Key] = value
		}
	}

	return libqpu.LogOperationDelta(
		update.RecordID,
		update.Table,
		libqpu.Vectorclock(map[string]uint64{ds.subscriptionEndpoint: uint64(time.Now().UnixNano())}),
		attributesOld, attributesNew), nil
}
