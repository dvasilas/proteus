package joinqpu

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"

	//
	_ "github.com/go-sql-driver/mysql"
)

const stateDatabase = "stateDB"

// JoinQPU ...
type JoinQPU struct {
	state                      libqpu.QPUState
	inputSchema                libqpu.Schema
	outputSchema               libqpu.Schema
	stateTable                 string
	joinAttributeKey           string
	inMemState                 *inMemState
	joinAttributes             map[string]string
	endOfStreamCnt             int
	catchUpDoneCh              chan int
	port                       string
	logTimestamps              bool
	catchUpDone                bool
	measureNotificationLatency bool
	notificationLatencyM       metrics.NotificationLatencyM
}

type stateEntry struct {
	attributes map[string]*qpu.Value
	ts         *qpu.Vectorclock
	mutex      sync.RWMutex
}

type inMemState struct {
	entries map[int64]*stateEntry
	mutex   sync.RWMutex
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*JoinQPU, error) {
	jqpu := &JoinQPU{
		state:                      qpu.State,
		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
		joinAttributes:             qpu.Config.JoinConfig.JoinAttribute,
		inMemState:                 &inMemState{entries: make(map[int64]*stateEntry)},
		stateTable:                 qpu.Config.JoinConfig.OutputTableAlias,
		joinAttributeKey:           qpu.Config.JoinConfig.JoinedAttributeAlias,
		catchUpDoneCh:              catchUpDoneCh,
		port:                       qpu.Config.Port,
		logTimestamps:              qpu.Config.Evaluation.LogTimestamps,
		measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		catchUpDone:                false,
	}

	if jqpu.measureNotificationLatency {
		jqpu.notificationLatencyM = metrics.NewNotificationLatencyM()
	}

	err := jqpu.initializeState()
	if err != nil {
		return nil, err
	}

	for tableName, table := range jqpu.inputSchema {
		for i := 0; i < len(qpu.AdjacentQPUs); i++ {
			for _, t := range qpu.AdjacentQPUs[i].OutputSchema {
				if t == tableName {
					projection := make([]string, len(table.Attributes))
					j := 0
					for attr := range table.Attributes {
						projection[j] = attr
						j++
					}
					querySnapshot := queries.NewQuerySnapshotAndSubscribe(
						tableName,
						projection,
						[]string{},
						[]string{},
						nil,
					)
					responseStreamStories, err := qpugraph.SendQuery(libqpu.NewQuery(nil, querySnapshot.Q), qpu.AdjacentQPUs[i])
					if err != nil {
						return nil, err
					}
					go func() {
						if err = responsestream.StreamConsumer(responseStreamStories, jqpu.processRespRecord, nil, nil); err != nil {
							panic(err)
						}
					}()
					break
				}
			}
		}
	}

	return jqpu, nil
}

func (q *JoinQPU) initializeState() error {
	utils.Assert(len(q.inputSchema) == 2, "input schema should definer 2 tables")

	q.outputSchema[q.stateTable] = libqpu.SchemaTable{
		Attributes: make(map[string]libqpu.DatastoreAttributeType),
	}
	q.outputSchema[q.stateTable].Attributes[q.joinAttributeKey] = libqpu.INT

	createSchemaStmt := ""
	for k, table := range q.inputSchema {
		for attr, attrType := range table.Attributes {
			if attr != q.joinAttributes[k] {
				q.outputSchema[q.stateTable].Attributes[attr] = attrType
				createSchemaStmt += attr
				switch attrType {
				case libqpu.INT:
					createSchemaStmt += " INT, "
				case libqpu.STR:
					createSchemaStmt += " TEXT, "
				default:
					return utils.Error(errors.New("unknown attribute type"))
				}
			}
		}
	}

	if err := q.state.Init(
		stateDatabase,
		q.stateTable+q.port,
		fmt.Sprintf(
			"CREATE TABLE %s (%s %s int NOT NULL, ts_key varchar(30), ts datetime(6), PRIMARY KEY (%s), INDEX i (vote_sum) )",
			q.stateTable+q.port,
			createSchemaStmt,
			q.joinAttributeKey,
			q.joinAttributeKey,
		),
	); err != nil {
		return err
	}

	return nil
}

// ProcessQuerySnapshot ...
func (q *JoinQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {

	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	errCh <- errors.New("not implemented")

	return logOpCh, errCh
}

// ClientQuery ...
func (q *JoinQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*pb.QueryResp, error) {
	var respRecords []*pb.QueryRespRecord

	// prepare the state.Get predicate
	var predicate []string
	predicate = nil
	if query.GetPredicate() != nil {
		predicate = []string{fmt.Sprintf("%s = %s", query.GetPredicate()[0].GetAttr().GetAttrKey(), query.GetPredicate()[0].GetLbound().GetStr())}
	}

	// prepare the state.Get orderBy clause
	orderBy := ""
	if query.GetOrderBy() != nil {
		orderBy += query.GetOrderBy().GetAttributeName() + " " + query.GetOrderBy().GetDirection().String()
	}

	snapshotTs := time.Now()
	stateCh, err := q.state.Get(
		q.stateTable+q.port,
		append(query.GetProjection(), q.joinAttributeKey, "ts_key", "ts"),
		predicate,
		orderBy,
		query.GetLimit(),
		nil)
	if err != nil {
		return nil, err
	}

	respRecords = make([]*pb.QueryRespRecord, 0)
	for record := range stateCh {
		// process timestamp
		vectorClockKey := record["ts_key"].(string)
		ts := record["ts"].(time.Time)
		if err != nil {
			return nil, utils.Error(err)
		}
		timestamp, err := ptypes.TimestampProto(ts)
		if err != nil {
			return nil, err
		}
		delete(record, "ts")
		delete(record, "ts_key")

		// process the rest of the attributes
		attribs := make(map[string]string)
		for k, v := range record {
			attribs[k] = v.(string)
		}

		respRecords = append(respRecords, &pb.QueryRespRecord{
			RecordId:   string(attribs[q.joinAttributeKey]),
			Attributes: attribs,
			Timestamp:  map[string]*tspb.Timestamp{vectorClockKey: timestamp},
		})
	}

	// log the query
	if q.logTimestamps {
		err = q.state.LogQuery(q.stateTable+q.port, snapshotTs, respRecords)
		if err != nil {
			return nil, err
		}
	}

	return &pb.QueryResp{
		RespRecord: respRecords,
	}, nil
}

// ProcessQuerySubscribe ...
func (q *JoinQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *JoinQPU) RemovePersistentQuery(table string, queryID int) {
}

// GetConfig ...
func (q JoinQPU) GetConfig() *qpu_api.ConfigResponse {
	return &qpu_api.ConfigResponse{
		Schema: []string{q.stateTable},
	}
}

// GetMetrics ...
func (q *JoinQPU) GetMetrics(*pb.MetricsRequest) (*pb.MetricsResponse, error) {
	p50, p90, p95, p99 := q.notificationLatencyM.GetMetrics()
	return &pb.MetricsResponse{
		NotificationLatencyP50: p50,
		NotificationLatencyP90: p90,
		NotificationLatencyP95: p95,
		NotificationLatencyP99: p99,
	}, nil
}

// ---------------- Internal Functions --------------

func (q *JoinQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	if q.catchUpDone && q.measureNotificationLatency {
		if err := q.notificationLatencyM.Add(respRecord.GetLogOp()); err != nil {
			return err
		}
	}

	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}

	if respRecordType == libqpu.EndOfStream {
		q.endOfStreamCnt++
		if q.endOfStreamCnt == len(q.joinAttributes) {
			q.catchUpDone = true

			err := q.flushState()
			if err != nil {
				return err
			}

			go func() {
				q.catchUpDoneCh <- 0
			}()
		}
	} else if respRecordType == libqpu.State {
		if err := q.processRespRecordInMem(respRecord, data, recordCh); err != nil {
			return err
		}
	} else if respRecordType == libqpu.Delta {
		attributes := respRecord.GetAttributes()
		joinAttribute := q.joinAttributes[respRecord.GetLogOp().GetTable()]
		joinAttributeValue := attributes[joinAttribute].GetInt()
		delete(attributes, joinAttribute)
		_, err := q.updateState(joinAttributeValue, attributes, respRecord.GetLogOp().GetTimestamp().GetVc())
		if err != nil {
			return err
		}
	}

	return nil
}

func (q JoinQPU) processRespRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	attributes := respRecord.GetAttributes()

	joinAttribute := q.joinAttributes[respRecord.GetLogOp().GetTable()]
	joinAttributeValue := attributes[joinAttribute].GetInt()
	delete(attributes, joinAttribute)

	q.inMemState.mutex.RLock()
	if entry, found := q.inMemState.entries[joinAttributeValue]; found {
		q.inMemState.mutex.RUnlock()
		entry.mutex.Lock()
		for attr, val := range attributes {
			entry.attributes[attr] = val
			entry.ts = respRecord.GetLogOp().GetTimestamp()
		}
		entry.mutex.Unlock()
	} else {
		q.inMemState.mutex.RUnlock()
		q.inMemState.mutex.Lock()
		q.inMemState.entries[joinAttributeValue] = &stateEntry{
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}
		q.inMemState.mutex.Unlock()
	}

	return nil
}

func (q *JoinQPU) flushState() error {
	for stateRecordID, entry := range q.inMemState.entries {
		entry.mutex.RLock()
		_, err := q.updateState(stateRecordID, entry.attributes, entry.ts.GetVc())
		entry.mutex.RUnlock()
		if err != nil {
			return err
		}
	}
	return q.state.SeparateTS(q.stateTable + q.port)
}

func (q JoinQPU) updateState(joinID int64, values map[string]*qpu.Value, vc map[string]*tspb.Timestamp) (map[string]*qpu.Value, error) {
	row := make(map[string]interface{})
	for attributeKey := range values {
		val, err := q.outputSchema.GetValue(values, q.stateTable, attributeKey)
		if err != nil {
			return nil, err
		}
		row[attributeKey] = val
	}

	res := q.state.GetRow(
		q.stateTable+q.port,
		[]string{q.joinAttributeKey},
		[]string{fmt.Sprintf("%s = %s", q.joinAttributeKey, strconv.FormatInt(joinID, 10))},
		nil,
	)

	var t int64
	errScan := res.Scan(&t)
	var err error

	if errScan == sql.ErrNoRows {
		row[q.joinAttributeKey] = joinID
		for k, v := range q.outputSchema[q.stateTable].Attributes {
			if _, ok := row[k]; !ok {
				switch v {
				case libqpu.INT:
					row[k] = 0
				}
			}
		}

		err = q.state.Insert(q.stateTable+q.port, row, vc, joinID)
	} else {
		err = q.state.Update(q.stateTable+q.port,
			map[string]interface{}{q.joinAttributeKey: joinID},
			row, vc, joinID)
	}
	if err != nil {
		return nil, err
	}
	return values, nil
}
