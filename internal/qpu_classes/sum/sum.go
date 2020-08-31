package sumqpu

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"
)

// assumptions
// "id" and "sum" attributes are of type Value_Int
// the state only stores those two attributes (but "id" can be compose of
// multiple attributes)

const stateDatabase = "stateDB"

// SumQPU ...
type SumQPU struct {
	state                libqpu.QPUState
	inputSchema          libqpu.Schema
	subscribeQueries     []chan libqpu.LogOperation
	aggregationAttribute string
	groupBy              string
	schemaTable          string
	inMemState           *inMemState
	port                 string
	catchUpDoneCh        chan int
}

type inMemState struct {
	entries map[int64]*stateEntry
}

type stateEntry struct {
	val        int64
	attributes map[string]*qpu.Value
	ts         *qpu.Vectorclock
	mutex      sync.RWMutex
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(q *libqpu.QPU, catchUpDoneCh chan int) (*SumQPU, error) {
	sqpu := &SumQPU{
		state:                q.State,
		inputSchema:          q.InputSchema,
		subscribeQueries:     make([]chan libqpu.LogOperation, 0),
		aggregationAttribute: q.Config.AggregationConfig.AggregationAttribute,
		groupBy:              q.Config.AggregationConfig.GroupBy,
		inMemState:           &inMemState{entries: make(map[int64]*stateEntry)},
		port:                 q.Config.Port,
		catchUpDoneCh:        catchUpDoneCh,
	}

	err := sqpu.initializeState()
	if err != nil {
		return &SumQPU{}, err
	}

	query := sqpu.prepareDownstreamQuery()

	for _, adjQPU := range q.AdjacentQPUs {
		responseStream, err := qpugraph.SendQuery(libqpu.NewQuery(nil, query.Q), adjQPU)
		if err != nil {
			return &SumQPU{}, err
		}
		go func() {
			if err = responsestream.StreamConsumer(responseStream, sqpu.processRespRecord, nil, nil); err != nil {
				panic(err)
			}
		}()
	}

	return sqpu, nil
}

func (q *SumQPU) initializeState() error {
	utils.Assert(len(q.inputSchema) == 1, "input schema should define a single table")
	// input schema has one table
	// get a ref to it
	var outputSchemaTable libqpu.SchemaTable
	for k, v := range q.inputSchema {
		q.schemaTable = k + "_sum"
		outputSchemaTable = v
	}

	createSchemaStmt := ""
	for attr, attrType := range outputSchemaTable.Attributes {
		if attr == q.aggregationAttribute {
			createSchemaStmt += q.aggregationAttribute + "_sum"
		} else {
			createSchemaStmt += attr
		}
		switch attrType {
		case libqpu.INT:
			createSchemaStmt += " bigint, "
		default:
			return utils.Error(errors.New("unknown attribute type"))
		}
	}

	if err := q.state.Init(
		stateDatabase,
		q.schemaTable+q.port,
		fmt.Sprintf(
			"CREATE TABLE %s (%s ts_key varchar(30), ts datetime(6), PRIMARY KEY ( %s ) )",
			q.schemaTable+q.port,
			createSchemaStmt,
			q.groupBy,
		),
	); err != nil {
		return err
	}

	return nil
}

func (q *SumQPU) prepareDownstreamQuery() libqpu.ASTQuery {
	// inputSchema has one table
	// get a ref to it
	var inputSchemaTableName string
	var inputSchemaTable libqpu.SchemaTable
	for k, v := range q.inputSchema {
		inputSchemaTableName = k
		inputSchemaTable = v
	}
	projection := make([]string, len(inputSchemaTable.Attributes))
	i := 0
	for k := range inputSchemaTable.Attributes {
		projection[i] = k
		i++
	}
	return queries.NewQuerySnapshotAndSubscribe(
		inputSchemaTableName,
		projection,
		inputSchemaTable.DownstreamQuery.IsNull,
		inputSchemaTable.DownstreamQuery.IsNotNull,
		nil,
	)
}

// ProcessQuerySnapshot ...
func (q *SumQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	stateCh, err := q.state.Get(
		q.schemaTable+q.port,
		[]string{"*"},
		nil, "",
		query.GetLimit(),
		false,
		nil,
	)
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	go func() {
		for record := range stateCh {
			// prepare timestamp
			vectorClockKey := string(record["ts_key"].([]byte))
			ts := record["ts"].(time.Time)
			// ts, err = time.Parse("2006-01-02 15:04:05.000000", string(record["ts"].([]byte)))
			// if err != nil {
			// 	ts, err = time.Parse("2006-01-02 15:04:05", string(record["ts"].([]byte)))
			// 	if err != nil {
			// 		errCh <- utils.Error(err)
			// 		break
			// 	}
			// }
			timestamp, err := ptypes.TimestampProto(ts)
			if err != nil {
				errCh <- err
				break
			}
			delete(record, "ts")
			delete(record, "ts_key")

			// prepare attributes
			attributes := make(map[string]*qpu.Value)
			attributes[q.groupBy] = libqpu.ValueInt(record[q.groupBy].(int64))
			attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(record[q.aggregationAttribute+"_sum"].(int64))

			// prepare record ID
			recordID := strconv.FormatInt(record[q.groupBy].(int64), 10)

			// convert any remaining attributes and and add to 'attributes' mup
			delete(record, q.groupBy)
			delete(record, q.aggregationAttribute+"_sum")
			attrs, err := q.inputSchema.InterfaceToAttributes(q.schemaTable, record)
			if err != nil {
				errCh <- err
				break
			}
			for k, v := range attrs {
				attributes[k] = v
			}

			logOpCh <- libqpu.LogOperationState(
				recordID,
				q.schemaTable,
				libqpu.Vectorclock(map[string]*tspb.Timestamp{vectorClockKey: timestamp}),
				attributes,
			)

		}
		close(logOpCh)
		close(errCh)
	}()

	return logOpCh, errCh
}

// ProcessQuerySubscribe ...
func (q *SumQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	q.subscribeQueries = append(q.subscribeQueries, logOpCh)

	return -1, logOpCh, errCh
}

// ClientQuery ...
func (q *SumQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*qpu_api.QueryResp, error) {
	return nil, errors.New("not implemented")
}

// RemovePersistentQuery ...
func (q *SumQPU) RemovePersistentQuery(table string, queryID int) {}

// ---------------- Internal Functions --------------

func (q *SumQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}

	if respRecordType == libqpu.EndOfStream {
		err := q.flushState()
		if err != nil {
			return err
		}
		go func() {
			q.catchUpDoneCh <- 0
		}()
		return nil
	} else if respRecordType == libqpu.State {
		if err := q.processRespRecordInMem(respRecord, data, recordCh); err != nil {
			return err
		}
	} else {
		attributes := respRecord.GetAttributes()

		groupByValue := attributes[q.groupBy].GetInt()
		sumValue := attributes[q.aggregationAttribute].GetInt()
		delete(attributes, q.groupBy)
		delete(attributes, q.aggregationAttribute)

		newSumValue, err := q.updateState(groupByValue, sumValue, map[string]*qpu.Value{}, respRecord.GetLogOp().GetTimestamp().GetVc())
		if err != nil {
			return err
		}

		attributes[q.groupBy] = libqpu.ValueInt(groupByValue)
		attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(newSumValue)

		logOp := libqpu.LogOperationDelta(
			respRecord.GetRecordID(),
			q.schemaTable,
			respRecord.GetLogOp().GetTimestamp(),
			nil,
			attributes,
		)

		for _, ch := range q.subscribeQueries {
			ch <- logOp
		}
	}

	return nil
}

func (q *SumQPU) processRespRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	attributes := respRecord.GetAttributes()

	sumValue := attributes[q.aggregationAttribute].GetInt()
	groupByValue := attributes[q.groupBy].GetInt()
	delete(attributes, q.aggregationAttribute)
	delete(attributes, q.groupBy)

	_, found := q.inMemState.entries[groupByValue]
	if found {
		// sequential processing for now
		// value.mutex.Lock()
		q.inMemState.entries[groupByValue].val += sumValue
		q.inMemState.entries[groupByValue].attributes = attributes
		q.inMemState.entries[groupByValue].ts = respRecord.GetLogOp().GetTimestamp()
		// value.mutex.Unlock()
	} else {
		q.inMemState.entries[groupByValue] = &stateEntry{
			val:        sumValue,
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}
	}

	return nil
}

func (q *SumQPU) flushState() error {
	for stateMapKey, stateEntry := range q.inMemState.entries {
		stateEntry.mutex.RLock()
		_, err := q.updateState(stateMapKey, stateEntry.val, stateEntry.attributes, stateEntry.ts.GetVc())
		stateEntry.mutex.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// updateState first does a state.Get to see if the groupByVal exists already
// if no, it inserts an entry for the new groupByVal
// if yes, it updates the entry with the new sumVal
// it returns the new sumVal
func (q *SumQPU) updateState(groupByVal, sumVal int64, attributes map[string]*qpu.Value, vc map[string]*tspb.Timestamp) (int64, error) {
	stateCh, err := q.state.Get(q.schemaTable+q.port,
		[]string{q.aggregationAttribute + "_sum"},
		map[string]interface{}{q.groupBy: groupByVal},
		"", 0, false, nil,
	)
	if err != nil {
		return -1, err
	}

	var storedSumValue int64
	found := false
	for record := range stateCh {
		found = true
		storedSumValue = record[q.aggregationAttribute+"_sum"].(int64)
	}

	// prepare the row to be inserted / updated
	row := make(map[string]interface{})
	// first, add the "groupBy" attribute (the primary key)
	row[q.groupBy] = groupByVal
	// then, add the attributes, if any
	for k, v := range attributes {
		val, err := utils.GetValue(v)
		if err != nil {
			return -1, err
		}
		row[k] = val
	}

	var newSumValue int64
	if !found {
		// insert the new value
		row[q.aggregationAttribute+"_sum"] = sumVal
		err = q.state.Insert(q.schemaTable+q.port, row, vc, sumVal)
		newSumValue = sumVal
	} else {
		newSumValue = storedSumValue + sumVal
		err = q.state.Update(q.schemaTable+q.port, row, map[string]interface{}{q.aggregationAttribute + "_sum": newSumValue}, vc, sumVal)
	}

	if err != nil {
		return -1, err
	}

	return newSumValue, nil
}

// GetConfig ...
func (q *SumQPU) GetConfig() *qpu_api.ConfigResponse {
	return &qpu_api.ConfigResponse{
		Schema: []string{q.schemaTable},
	}
}
