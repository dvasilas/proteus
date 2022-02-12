package indexqpu

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	"github.com/golang/protobuf/ptypes/timestamp"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/golang/protobuf/ptypes"

	//
	_ "github.com/go-sql-driver/mysql"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const stateDatabase = "stateDB"

type catchUp struct {
	sync.Mutex
	catchUpDoneCh  chan int
	catchupQueries map[int]*catchupQuery
	catchUpDone    bool
}

type catchupQuery struct {
	endOfStreamSeqID int64
	catchupDone      bool
	appliedSeqID     int64
}

// IndexQPU ...
type IndexQPU struct {
	state                      libqpu.QPUState
	inputSchema                libqpu.Schema
	outputSchema               libqpu.Schema
	stateTable                 string
	inMemState                 *inMemState
	dbCollection               string
	port                       string
	logTimestamps              bool
	measureNotificationLatency bool
	downstreamQPUs             int
	notificationLatencyM       metrics.LatencyM
	stateUpdateM               metrics.LatencyM
	writeLog                   writeLog
	queryLog                   queryLog
	collections                map[string]*mongo.Collection
	database                   *mongo.Database
	catchUp                    catchUp
	findOptions                *options.FindOptions
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
	rand.Seed(time.Now().UTC().UnixNano())

	jqpu := &IndexQPU{
		state:                      qpu.State,
		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
		inMemState:                 &inMemState{entries: make(map[string][]interface{})},
		dbCollection:               stateDatabase + qpu.Config.Port,
		port:                       qpu.Config.Port,
		logTimestamps:              qpu.Config.Evaluation.LogTimestamps,
		measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		downstreamQPUs:             len(qpu.AdjacentQPUs),
		writeLog: writeLog{
			entries: make([]libqpu.WriteLogEntry, 0),
		},
		queryLog: queryLog{
			entries: make([]libqpu.QueryLogEntry, 0),
		},
		collections: make(map[string]*mongo.Collection),
		catchUp: catchUp{
			catchupQueries: make(map[int]*catchupQuery),
			catchUpDoneCh:  catchUpDoneCh,
		},
		findOptions: options.Find(),
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
						nil,
					)
					responseStreamStories, err := qpugraph.SendQuery(libqpu.NewQuery(nil, querySnapshot.Q), qpu.AdjacentQPUs[i])
					if err != nil {
						return nil, err
					}
					queryID := rand.Int()
					jqpu.catchUp.catchupQueries[queryID] = &catchupQuery{
						appliedSeqID:     -1,
						endOfStreamSeqID: -1,
					}
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

	go func() {
		for {
			time.Sleep(time.Second)

			jqpu.catchUp.Lock()
			if jqpu.catchUp.catchUpDone {
				break
			}

			done := true
			for _, q := range jqpu.catchUp.catchupQueries {
				if q.appliedSeqID < 0 || q.endOfStreamSeqID < 0 || q.appliedSeqID+1 != q.endOfStreamSeqID {
					done = false
					break
				}
			}

			if done {
				jqpu.catchUp.catchUpDone = true

				err := jqpu.flushState()

				if err != nil {
					log.Fatal(err)
				}

				jqpu.catchUp.catchUpDoneCh <- 0
				jqpu.catchUp.Unlock()
				return
			}
			jqpu.catchUp.Unlock()
		}
	}()

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

	for {
		err = client.Ping(context.Background(), nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting to: ", config.StateBackend.Endpoint)
		} else {
			break
		}
	}

	q.database = client.Database(q.dbCollection)

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
func (q *IndexQPU) ClientQuery(query libqpu.ASTQuery, queryStr string, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	col, err := q.collection(query.GetTable())
	if err != nil {
		return nil, utils.Error(err)
	}

	var results []*map[string]interface{}

	filter := bson.M{query.GetPredicate()[0].GetAttr().GetAttrKey(): query.GetPredicate()[0].GetLbound().GetInt()}

	cur, err := col.Find(context.Background(), filter, q.findOptions)
	if err != nil {
		return nil, utils.Error(err)
	}
	if err = cur.All(context.Background(), &results); err != nil {
		return nil, utils.Error(err)
	}

	respRecords := make([]*qpuextapi.QueryRespRecord, len(results))
	for i, result := range results {

		id := (*result)["id"].(string)
		vc := (*result)["ts"].(map[string]interface{})
		ts := vc["ts"].(primitive.DateTime).Time()

		delete(*result, "_id")
		delete(*result, "id")
		delete(*result, "ts")

		timestamp, err := ptypes.TimestampProto(ts)
		if err != nil {
			return nil, utils.Error(err)
		}

		attributes := make(map[string]string)
		for k, v := range *result {
			if k != "_id" && k != "id" && k != "ts" {
				switch v.(type) {
				case string:
					attributes[k] = v.(string)
				case int:
					attributes[k] = strconv.Itoa(v.(int))
				case int64:
					attributes[k] = strconv.FormatInt(v.(int64), 10)
				}
			}
		}

		respRecords[i] = &qpuextapi.QueryRespRecord{
			RecordId:   id,
			Attributes: attributes,
			Timestamp:  map[string]*tspb.Timestamp{vc["key"].(string): timestamp},
		}
	}

	return &qpuextapi.QueryResp{
		RespRecord: respRecords,
	}, nil
}

// QuerySubscribe  ...
func (q *IndexQPU) QuerySubscribe(query libqpu.ASTQuery, req *qpuextapi.QueryReq) (chan libqpu.LogOperation, chan bool, chan error) {
	return nil, nil, nil
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

// ---------------- Internal Functions --------------

func (q *IndexQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) error {
	if q.catchUp.catchUpDone && q.measureNotificationLatency {
		if err := q.notificationLatencyM.AddFromOp(respRecord.GetLogOp()); err != nil {
			return err
		}
	}

	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}

	if respRecordType == libqpu.EndOfStream {
		q.catchUp.Lock()
		q.catchUp.catchupQueries[queryID].endOfStreamSeqID = respRecord.GetSequenceID()
		q.catchUp.Unlock()
	} else if respRecordType == libqpu.State {
		if err := q.processRespRecordInMem(respRecord, data, recordCh); err != nil {
			return err
		}
		q.catchUp.Lock()
		q.catchUp.catchupQueries[queryID].appliedSeqID = respRecord.GetSequenceID()
		q.catchUp.Unlock()

	} else if respRecordType == libqpu.Delta {
		if respRecord.GetLogOp().HasOldState() {
			update := bson.M{}
			for k, v := range respRecord.GetAttributes() {
				switch v.GetVal().(type) {
				case *qpu.Value_Str:
					update[k] = v.GetStr()
				case *qpu.Value_Int:
					update[k] = v.GetInt()
				case *qpu.Value_Flt:
					update[k] = v.GetFlt()
				default:
					return utils.Error(errors.New("unknown value type"))
				}
			}

			col, err := q.collection(respRecord.GetLogOp().GetTable())
			if err != nil {
				return err
			}

			_, err = col.UpdateOne(context.Background(),
				bson.D{{"id", respRecord.GetLogOp().GetObjectID()}},
				bson.M{"$set": update},
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

func (q *IndexQPU) encodeDataItem(respRecord libqpu.ResponseRecord) (map[string]interface{}, error) {
	var timestamp *timestamp.Timestamp
	var timestampKey string
	for k, t := range respRecord.GetLogOp().GetTimestamp().GetVc() {
		timestampKey = k
		timestamp = t
	}

	ts, err := ptypes.Timestamp(timestamp)
	if err != nil {
		return nil, err
	}

	di := map[string]interface{}{
		"id": respRecord.GetLogOp().GetObjectID(),
		"ts": map[string]interface{}{
			"key": timestampKey,
			"ts":  ts,
		},
	}

	for k, v := range respRecord.GetAttributes() {
		switch v.GetVal().(type) {
		case *qpu.Value_Str:
			di[k] = v.GetStr()
		case *qpu.Value_Int:
			di[k] = v.GetInt()
		case *qpu.Value_Flt:
			di[k] = v.GetFlt()
		default:
			return nil, utils.Error(errors.New("unknown value type"))
		}
	}

	return di, nil
}
