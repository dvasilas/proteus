package indexqpu

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/golang/protobuf/ptypes/timestamp"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/golang/protobuf/ptypes"

	//
	_ "github.com/go-sql-driver/mysql"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const stateDatabase = "stateDB"

// DataItem ...
type DataItem struct {
	ID         string
	Table      string
	Attributes map[string]Value
	Ts         Timestamp
}

// Timestamp ..
type Timestamp struct {
	Key string
	Ts  time.Time
}

// Value ...
type Value struct {
	ValType int
	StrVal  string
	IntVal  int64
}

// IndexQPU ...
type IndexQPU struct {
	state                      libqpu.QPUState
	inputSchema                libqpu.Schema
	outputSchema               libqpu.Schema
	stateTable                 string
	inMemState                 *inMemState
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
	collections                map[string]*mongo.Collection
	database                   *mongo.Database
}

type inMemState struct {
	entries map[string][]interface{}
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
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*IndexQPU, error) {
	jqpu := &IndexQPU{
		state:                      qpu.State,
		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
		inMemState:                 &inMemState{entries: make(map[string][]interface{})},
		stateTable:                 qpu.Config.JoinConfig.OutputTableAlias,
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
		collections: make(map[string]*mongo.Collection),
	}

	if jqpu.measureNotificationLatency {
		jqpu.notificationLatencyM = metrics.NewLatencyM()
		jqpu.stateUpdateM = metrics.NewLatencyM()
	}

	err := jqpu.initializeState(qpu.Config)
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
						if err = responsestream.StreamConsumer(responseStreamStories, qpu.Config.ProcessingConfig.Input.MaxWorkers, qpu.Config.ProcessingConfig.Input.MaxJobQueue, jqpu.processRespRecord, nil, nil); err != nil {
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

func (q *IndexQPU) initializeState(config *libqpu.QPUConfig) error {
	clientOptions := options.MergeClientOptions(
		&options.ClientOptions{
			Auth: &options.Credential{
				Username: config.StateBackend.Credentials.AccessKeyID,
				Password: config.StateBackend.Credentials.SecretAccessKey,
			}}).ApplyURI(config.StateBackend.Endpoint)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return err
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return err
	}

	q.database = client.Database(stateDatabase)

	return nil
}

func (q *IndexQPU) collection(table string) (*mongo.Collection, error) {
	if col, ok := q.collections[table]; ok {
		return col, nil
	}

	col := q.database.Collection(table)

	_, err := col.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bson.M{"id": 1},
			Options: options.Index().SetUnique(true),
		},
	)
	if err != nil {
		return nil, err
	}

	_, err = col.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys: bson.M{"attribute0": 1},
		},
	)
	if err != nil {
		return nil, err
	}

	_, err = col.DeleteMany(context.TODO(), bson.D{{}})
	if err != nil {
		return nil, err
	}

	q.collections[table] = col

	return col, nil
}

// ProcessQuerySnapshot ...
func (q *IndexQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	errCh <- errors.New("not implemented")

	return logOpCh, errCh
}

// ClientQuery ...
func (q *IndexQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	col, err := q.collection(query.GetTable())
	if err != nil {
		return nil, utils.Error(err)
	}

	var results []*DataItem
	findOptions := options.Find()

	query.GetPredicate()[0].GetAttr().GetAttrKey()

	queryStr := fmt.Sprintf("attributes.%s.intval", query.GetPredicate()[0].GetAttr().GetAttrKey())
	filter := bson.M{queryStr: query.GetPredicate()[0].GetLbound().GetInt()}

	cur, err := col.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, utils.Error(err)
	}
	if err = cur.All(context.Background(), &results); err != nil {
		return nil, utils.Error(err)
	}

	respRecords := make([]*qpuextapi.QueryRespRecord, len(results))
	for i, result := range results {
		attributes := make(map[string]string)
		for k, v := range result.Attributes {
			switch v.ValType {
			case 0:
				attributes[k] = v.StrVal
			case 1:
				attributes[k] = strconv.Itoa(int(v.IntVal))
			}
		}

		timestamp, err := ptypes.TimestampProto(result.Ts.Ts)
		if err != nil {
			return nil, utils.Error(err)
		}

		respRecords[i] = &qpuextapi.QueryRespRecord{
			RecordId:   result.ID,
			Attributes: attributes,
			Timestamp:  map[string]*tspb.Timestamp{result.Ts.Key: timestamp},
		}
	}

	return &qpuextapi.QueryResp{
		RespRecord: respRecords,
	}, nil
}

// ProcessQuerySubscribe ...
func (q *IndexQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *IndexQPU) RemovePersistentQuery(table string, queryID int) {
}

// GetConfig ...
func (q *IndexQPU) GetConfig() *qpuapi.ConfigResponse {
	return &qpuapi.ConfigResponse{
		Schema: []string{q.stateTable},
	}
}

// GetMetrics ...
func (q *IndexQPU) GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	var err error
	var FL50, FL90, FL95, FL99 float64
	var FV0, FV1, FV2, FV4 float64
	var NL50, NL90, NL95, NL99 float64
	var UL50, UL90, UL95, UL99 float64

	FL50, FL90, FL95, FL99 = -1, -1, -1, -1
	FV0, FV1, FV2, FV4 = -1, -1, -1, -1
	NL50, NL90, NL95, NL99 = -1, -1, -1, -1
	UL50, UL90, UL95, UL99 = -1, -1, -1, -1

	if q.logTimestamps {
		FL50, FL90, FL95, FL99 = metrics.FreshnessLatency(q.writeLog.entries)

		FV0, FV1, FV2, FV4, err = metrics.FreshnessVersions(q.queryLog.entries, q.writeLog.entries)
		if err != nil {
			return nil, err
		}
	}

	if q.measureNotificationLatency {
		NL50, NL90, NL95, NL99 = q.notificationLatencyM.GetMetrics()
		UL50, UL90, UL95, UL99 = q.stateUpdateM.GetMetrics()
	}

	return &qpuextapi.MetricsResponse{
		NotificationLatencyP50: NL50,
		NotificationLatencyP90: NL90,
		NotificationLatencyP95: NL95,
		NotificationLatencyP99: NL99,
		ProcessingLatencyP50:   -1.0,
		ProcessingLatencyP90:   -1.0,
		ProcessingLatencyP95:   -1.0,
		ProcessingLatencyP99:   -1.0,
		FreshnessLatencyP50:    FL50,
		FreshnessLatencyP90:    FL90,
		FreshnessLatencyP95:    FL95,
		FreshnessLatencyP99:    FL99,
		FreshnessVersions0:     FV0,
		FreshnessVersions1:     FV1,
		FreshnessVersions2:     FV2,
		FreshnessVersions4:     FV4,
		StateUpdateLatencyP50:  UL50,
		StateUpdateLatencyP90:  UL90,
		StateUpdateLatencyP95:  UL95,
		StateUpdateLatencyP99:  UL99,
	}, nil
}

// ---------------- Internal Functions --------------

func (q *IndexQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
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
		q.catchUpDone = true

		err := q.flushState()
		if err != nil {
			return err
		}

		go func() {
			q.catchUpDoneCh <- 0
		}()
	} else if respRecordType == libqpu.State {
		if err := q.processRespRecordInMem(respRecord, data, recordCh); err != nil {
			return err
		}
	} else if respRecordType == libqpu.Delta {
		if respRecord.GetLogOp().HasOldState() {
			attributes := make(map[string]Value)
			for k, v := range respRecord.GetAttributes() {
				switch v.GetVal().(type) {
				case *qpu.Value_Str:
					attributes[k] = Value{
						ValType: 0,
						StrVal:  v.GetStr(),
					}
				case *qpu.Value_Int:
					attributes[k] = Value{
						ValType: 1,
						IntVal:  v.GetInt(),
					}
				case *qpu.Value_Flt:
					return utils.Error(errors.New("float value not implemented"))
				default:
					return utils.Error(errors.New("unknown value type"))
				}
			}

			col, err := q.collection(respRecord.GetLogOp().GetTable())
			if err != nil {
				return err
			}

			_, err = col.UpdateOne(context.Background(),
				bson.D{{"id", respRecord.GetLogOp().GetObjectId()}},
				bson.D{{"$set",
					bson.M{"attributes": attributes},
				}},
			)
			if err != nil {
				return utils.Error(err)
			}

		} else {
			di, err := q.encodeDataItem(respRecord)
			if err != nil {
				return utils.Error(err)
			}

			col, err := q.collection(respRecord.GetLogOp().GetTable())
			if err != nil {
				return utils.Error(err)
			}

			_, err = col.InsertOne(context.Background(), di)
			if err != nil {
				return utils.Error(err)
			}
		}
	}

	return nil
}

func (q *IndexQPU) processRespRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	di, err := q.encodeDataItem(respRecord)
	if err != nil {
		return err
	}

	if _, ok := q.inMemState.entries[respRecord.GetLogOp().GetTable()]; !ok {
		q.inMemState.entries[respRecord.GetLogOp().GetTable()] = make([]interface{}, 0)
	}
	q.inMemState.entries[respRecord.GetLogOp().GetTable()] = append(q.inMemState.entries[respRecord.GetLogOp().GetTable()], di)

	return nil
}

func (q *IndexQPU) flushState() error {
	for table := range q.inMemState.entries {
		col, err := q.collection(table)
		if err != nil {
			return err
		}
		_, err = col.InsertMany(context.TODO(), q.inMemState.entries[table])
		if err != nil {
			return err
		}

	}

	return nil
}

func (q *IndexQPU) encodeDataItem(respRecord libqpu.ResponseRecord) (DataItem, error) {
	var timestamp *timestamp.Timestamp
	var timestampKey string
	for k, t := range respRecord.GetLogOp().GetTimestamp().GetVc() {
		timestampKey = k
		timestamp = t
	}

	ts, err := ptypes.Timestamp(timestamp)
	if err != nil {
		return DataItem{}, err
	}

	di := DataItem{
		ID:         respRecord.GetLogOp().GetObjectId(),
		Table:      respRecord.GetLogOp().GetTable(),
		Attributes: make(map[string]Value),
		Ts: Timestamp{
			Key: timestampKey,
			Ts:  ts,
		},
	}

	for k, v := range respRecord.GetAttributes() {
		switch v.GetVal().(type) {
		case *qpu.Value_Str:
			val := Value{
				ValType: 0,
				StrVal:  v.GetStr(),
			}
			di.Attributes[k] = val
		case *qpu.Value_Int:
			val := Value{
				ValType: 1,
				IntVal:  v.GetInt(),
			}
			di.Attributes[k] = val
		case *qpu.Value_Flt:
			return DataItem{}, utils.Error(errors.New("float value not implemented"))
		default:
			return DataItem{}, utils.Error(errors.New("unknown value type"))
		}
	}

	return di, nil
}
