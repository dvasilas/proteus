package joinqpu

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/golang/protobuf/ptypes"

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
	notificationLatencyM       metrics.LatencyM
	stateUpdateM               metrics.LatencyM
	writeLog                   writeLog
	queryLog                   queryLog
}

type stateEntry struct {
	attributes map[string]*qpu.Value
	ts         *qpu.Vectorclock
	mutex      sync.RWMutex
}

type inMemState struct {
	entries map[int32]*stateEntry
	mutex   sync.RWMutex
}

type writeLog struct {
	sync.Mutex
	entries []libqpu.WriteLogEntry
}

type queryLog struct {
	sync.Mutex
	entries []libqpu.QueryLogEntry
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*JoinQPU, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	jqpu := &JoinQPU{
		state:                      qpu.State,
		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
		joinAttributes:             qpu.Config.JoinConfig.JoinAttribute,
		inMemState:                 &inMemState{entries: make(map[int32]*stateEntry)},
		stateTable:                 qpu.Config.JoinConfig.OutputTableAlias,
		joinAttributeKey:           qpu.Config.JoinConfig.JoinedAttributeAlias,
		catchUpDoneCh:              catchUpDoneCh,
		port:                       qpu.Config.Port,
		logTimestamps:              qpu.Config.Evaluation.LogTimestamps,
		measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		catchUpDone:                false,
		writeLog: writeLog{
			entries: make([]libqpu.WriteLogEntry, 0),
		},
		queryLog: queryLog{
			entries: make([]libqpu.QueryLogEntry, 0),
		},
	}

	if jqpu.measureNotificationLatency {
		jqpu.notificationLatencyM = metrics.NewLatencyM()
		jqpu.stateUpdateM = metrics.NewLatencyM()
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
						nil,
					)
					responseStreamStories, err := qpugraph.SendQuery(libqpu.NewQuery(nil, querySnapshot.Q), qpu.AdjacentQPUs[i])
					if err != nil {
						return nil, err
					}
					queryID := rand.Int()
					go func() {
						if err = responsestream.StreamConsumer(responseStreamStories, qpu.Config.ProcessingConfig.Input.MaxWorkers, qpu.Config.ProcessingConfig.Input.MaxJobQueue, jqpu.processRespRecord, nil, nil, queryID); err != nil {
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
func (q *JoinQPU) ClientQuery(query libqpu.ASTQuery, queryStr string, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	var respRecords []*qpuextapi.QueryRespRecord

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

	respRecords = make([]*qpuextapi.QueryRespRecord, query.GetLimit())
	i := 0
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

		respRecords[i] = &qpuextapi.QueryRespRecord{
			RecordId:   string(attribs[q.joinAttributeKey]),
			Attributes: attribs,
			Timestamp:  map[string]*tspb.Timestamp{vectorClockKey: timestamp},
		}
		i++
	}

	// log the query
	if q.logTimestamps {
		qLogEntry := libqpu.QueryLogEntry{
			RowIDs: make([]string, 0),
			Ts:     snapshotTs,
		}
		for _, rec := range respRecords {
			qLogEntry.RowIDs = append(qLogEntry.RowIDs, rec.RecordId)
		}

		q.queryLog.entries = append(q.queryLog.entries, qLogEntry)
	}

	return &qpuextapi.QueryResp{
		RespRecord: respRecords,
	}, nil
}

// ClientQuery1 ...
func (q *JoinQPU) ClientQuery1(query libqpu.ASTQuery, queryStr string) (*qpuextapi.QueryResp1, error) {
	return nil, nil
}

// ProcessQuerySubscribe ...
func (q *JoinQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *JoinQPU) RemovePersistentQuery(table string, queryID int) {
}

// QuerySubscribe  ...
func (q *JoinQPU) QuerySubscribe(libqpu.ASTQuery, *qpuextapi.QueryReq) (chan libqpu.LogOperation, chan bool, chan error) {
	return nil, nil, nil
}

// GetConfig ...
func (q *JoinQPU) GetConfig() *qpuapi.ConfigResponse {
	return &qpuapi.ConfigResponse{
		Schema: []string{q.stateTable},
	}
}

// ---------------- Internal Functions --------------

func (q *JoinQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) error {
	if q.catchUpDone && q.measureNotificationLatency {
		if err := q.notificationLatencyM.AddFromOp(respRecord.GetLogOp()); err != nil {
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
		t0 := time.Now()
		attributes := respRecord.GetAttributes()
		joinAttribute := q.joinAttributes[respRecord.GetLogOp().GetTable()]
		joinAttributeValue := attributes[joinAttribute].GetInt()
		delete(attributes, joinAttribute)
		_, err := q.updateState(joinAttributeValue, attributes, respRecord.GetLogOp().GetTimestamp().GetVc())
		if err != nil {
			return err
		}
		if q.measureNotificationLatency {
			if err := q.stateUpdateM.AddFromTs(t0); err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *JoinQPU) processRespRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
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
	return nil
}

func (q *JoinQPU) updateState(joinID int32, values map[string]*qpu.Value, vc map[string]*tspb.Timestamp) (map[string]*qpu.Value, error) {
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
		[]string{fmt.Sprintf("%s = %s", q.joinAttributeKey, strconv.FormatInt(int64(joinID), 10))},
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

		err = q.state.Insert(q.stateTable+q.port, row, vc)
	} else {
		err = q.state.Update(q.stateTable+q.port,
			map[string]interface{}{q.joinAttributeKey: joinID},
			row, vc)

		// if q.logTimestamps {
		// 	var t0, t1 time.Time
		// 	t1 = time.Now()

		// 	q.writeLog.Lock()

		// 	for _, v := range vc {
		// 		t0, err = ptypes.Timestamp(v)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 	}
		// 	q.writeLog.entries = append(q.writeLog.entries, libqpu.WriteLogEntry{
		// 		RowID: joinID,
		// 		T0:    t0,
		// 		T1:    t1,
		// 	})
		// 	q.writeLog.Unlock()
		// }
	}
	if err != nil {
		return nil, err
	}

	return values, nil
}
