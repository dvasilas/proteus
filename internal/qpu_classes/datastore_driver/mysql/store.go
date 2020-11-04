package mysqldriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
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

	for {
		c, err := net.DialTimeout("tcp", conf.DatastoreConfig.Endpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retying connecting to: ", conf.DatastoreConfig.Endpoint)
		} else {
			c.Close()
			break
		}
	}

	for {
		c, err := net.DialTimeout("tcp", conf.DatastoreConfig.LogStreamEndpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retying connecting to: ", conf.DatastoreConfig.LogStreamEndpoint)
		} else {
			c.Close()
			break
		}
	}

	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
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

	err = errors.New("not tried yet")
	for err != nil {
		ctx, cancel := context.WithCancel(context.Background())
		_, err = s.cli.SubscribeToUpdates(ctx)
		time.Sleep(2 * time.Second)
		cancel()
		fmt.Println("retrying a test query", err)
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
		errCh <- utils.Error(err)
		cancel()
		return logOpCh, nil, errCh
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
		errCh <- utils.Error(err)
		cancel()
		return logOpCh, nil, errCh
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

	stmt, err := ds.db.Prepare(query)
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	columns, _ := rows.Columns()

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var ts time.Time

	go func() {
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}
			var recordID string
			attributes := make(map[string]*qpu.Value)

			for i, col := range values {
				if col != nil {
					if columns[i] == "ts" {
						ts = col.(time.Time)
					} else {
						value, err := ds.inputSchema.InterfaceToValue(table, columns[i], col)
						if err != nil {
							errCh <- err
							break
						}
						attributes[projection[i]] = value
					}
				}
			}

			timestamp, err := ptypes.TimestampProto(ts)
			if err != nil {
				errCh <- err
				break
			}

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
