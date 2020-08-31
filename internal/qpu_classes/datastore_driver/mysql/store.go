package mysqldriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/mysql"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc"

	//
	_ "github.com/go-sql-driver/mysql"
)

// MySQLDataStore ...
type MySQLDataStore struct {
	subscriptionEndpoint string
	cli                  mysql.PublishUpdatesClient
	inputSchema          libqpu.Schema
	conn                 *grpc.ClientConn
	db                   *sql.DB
}

// MySQLUpdate ...
type MySQLUpdate struct {
	RecordID   string
	Table      string
	Timestamp  string
	Attributes []struct {
		Key      string
		ValueOld string
		ValueNew string
	}
}

//---------------- API Functions -------------------

// NewDatastore ...
func NewDatastore(conf *libqpu.QPUConfig, inputSchema libqpu.Schema) (MySQLDataStore, error) {
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
		inputSchema:          inputSchema,
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
		&mysql.Request{
			Val: &mysql.Request_Request{
				Request: &mysql.SubRequest{
					// Timestamp: 0,
					Sync:  false,
					Table: table,
				},
			},
		},
	)
	if err != nil {
		errCh <- err
		cancel()
		return nil, nil, errCh
	}

	go ds.opConsumer(stream, logOpCh, errCh)

	return logOpCh, cancel, errCh
}

// GetSnapshot ...
func (ds MySQLDataStore) GetSnapshot(table string, projection, isNull, isNotNull []string) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	projection = append(projection, "ts")
	projectionStmt := ""
	for i, attr := range projection {
		projectionStmt += attr
		if i < len(projection)-1 {
			projectionStmt += ", "
		}
	}

	whereStmt := ""
	if len(isNull) > 0 || len(isNotNull) > 0 {
		whereStmt += "WHERE "
	}
	for _, attr := range isNull {
		whereStmt += attr + " IS NULL"
	}

	for _, attr := range isNotNull {
		whereStmt += attr + " IS NOT NULL"
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s", projectionStmt, table, whereStmt)

	rows, err := ds.db.Query(query)
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	go func() {
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				errCh <- err
				break
			}

			var recordID string
			attributes := make(map[string]*qpu.Value)
			for i, col := range values {
				if i == 0 {
					recordID = string(col)
				}
				if col != nil {
					value, err := ds.inputSchema.StrToValue(table, projection[i], string(col))
					if err != nil {
						errCh <- err
						break
					}
					attributes[projection[i]] = value
				}
			}

			var ts time.Time
			ts, err = time.Parse("2006-01-02 15:04:05.000000", attributes["ts"].GetStr())
			if err != nil {
				ts, err = time.Parse("2006-01-02 15:04:05", attributes["ts"].GetStr())
				if err != nil {
					errCh <- utils.Error(err)
					break
				}
			}
			timestamp, err := ptypes.TimestampProto(ts)
			if err != nil {
				errCh <- err
				break
			}

			delete(attributes, "ts")

			logOpCh <- libqpu.LogOperationState(
				recordID,
				table,
				libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
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
			errCh <- utils.Error(errors.New("opConsumer received EOF"))
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

func (ds MySQLDataStore) formatLogOpDelta(notificationMsg *mysql.UpdateRecord) (libqpu.LogOperation, error) {
	utils.Trace("store received", map[string]interface{}{"notificationMsg": notificationMsg})

	attributesOld := make(map[string]*qpu.Value)
	attributesNew := make(map[string]*qpu.Value)
	for _, attribute := range notificationMsg.Attributes {
		if attribute.ValueOld != "" {
			value, err := ds.inputSchema.StrToValue(notificationMsg.Table, attribute.Key, attribute.ValueOld)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesOld[attribute.Key] = value
		}
		if attribute.ValueNew != "" {
			value, err := ds.inputSchema.StrToValue(notificationMsg.Table, attribute.Key, attribute.ValueNew)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesNew[attribute.Key] = value
		}
	}

	return libqpu.LogOperationDelta(
		notificationMsg.RecordID,
		notificationMsg.Table,
		libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: notificationMsg.Timestamp}),
		attributesOld,
		attributesNew,
	), nil
}
