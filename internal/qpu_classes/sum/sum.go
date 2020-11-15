package sumqpu

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
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
	state                      libqpu.QPUState
	inputSchema                libqpu.Schema
	outputSchema               libqpu.Schema
	subscribeQueries           []chan libqpu.LogOperation
	aggregationAttribute       string
	groupBy                    string
	schemaTable                string
	inMemState                 *inMemState
	port                       string
	catchUpDoneCh              chan int
	catchUpDone                bool
	measureNotificationLatency bool
	notificationLatencyM       metrics.LatencyM
}

type inMemState struct {
	sync.RWMutex
	entries map[int64]*stateEntry
}

type stateEntry struct {
	sync.RWMutex
	val        int64
	attributes map[string]*qpu.Value
	ts         *qpu.Vectorclock
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*SumQPU, error) {
	sqpu := &SumQPU{
		// state:                      qpu.State,
		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
		subscribeQueries:           make([]chan libqpu.LogOperation, 0),
		aggregationAttribute:       qpu.Config.AggregationConfig.AggregationAttribute,
		groupBy:                    qpu.Config.AggregationConfig.GroupBy,
		inMemState:                 &inMemState{entries: make(map[int64]*stateEntry)},
		port:                       qpu.Config.Port,
		catchUpDoneCh:              catchUpDoneCh,
		measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		catchUpDone:                false,
	}

	err := sqpu.initializeState()
	if err != nil {
		return &SumQPU{}, err
	}

	if sqpu.measureNotificationLatency {
		sqpu.notificationLatencyM = metrics.NewLatencyM()
	}

	query := sqpu.prepareDownstreamQuery()

	for _, adjQPU := range qpu.AdjacentQPUs {
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

	q.outputSchema[q.schemaTable] = outputSchemaTable

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

	go func() {
		q.inMemState.Lock()
		for k, e := range q.inMemState.entries {
			recordID := strconv.FormatInt(k, 10)

			attributes := make(map[string]*qpu.Value)
			attributes[q.groupBy] = libqpu.ValueInt(k)
			attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(e.val)

			for attrN, attrV := range e.attributes {
				attributes[attrN] = attrV
			}
			logOp := libqpu.LogOperationState(
				recordID,
				q.schemaTable,
				e.ts,
				attributes,
			)
			logOpCh <- logOp
		}
		q.inMemState.Unlock()

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
func (q *SumQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*pb.QueryResp, error) {
	return nil, errors.New("not implemented")
}

// GetConfig ...
func (q *SumQPU) GetConfig() *qpu_api.ConfigResponse {
	return &qpu_api.ConfigResponse{
		Schema: []string{q.schemaTable},
	}
}

// RemovePersistentQuery ...
func (q *SumQPU) RemovePersistentQuery(table string, queryID int) {}

// GetMetrics ...
func (q *SumQPU) GetMetrics(*pb.MetricsRequest) (*pb.MetricsResponse, error) {
	p50, p90, p95, p99 := q.notificationLatencyM.GetMetrics()
	return &pb.MetricsResponse{
		NotificationLatencyP50: p50,
		NotificationLatencyP90: p90,
		NotificationLatencyP95: p95,
		NotificationLatencyP99: p99,
	}, nil
}

// ---------------- Internal Functions --------------

func (q *SumQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
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

		go func() {
			q.catchUpDoneCh <- 0
		}()

	} else if respRecordType == libqpu.State {
		q.processStateRecordInMem(respRecord, data, recordCh)
	} else {

		logOp := q.processUpdateRecordInMem(respRecord, data, recordCh)

		logOp.InTs = respRecord.InTs

		for _, ch := range q.subscribeQueries {
			ch <- logOp
		}
	}

	return nil
}

func (q *SumQPU) processStateRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) {
	attributes := respRecord.GetAttributes()

	sumValue := attributes[q.aggregationAttribute].GetInt()
	groupByValue := attributes[q.groupBy].GetInt()
	delete(attributes, q.aggregationAttribute)
	delete(attributes, q.groupBy)

	_, found := q.inMemState.entries[groupByValue]
	if found {
		// initial snapshot is processed sequentially for now, no need to lock
		q.inMemState.entries[groupByValue].val += sumValue
		q.inMemState.entries[groupByValue].attributes = attributes
		q.inMemState.entries[groupByValue].ts = respRecord.GetLogOp().GetTimestamp()
	} else {
		q.inMemState.entries[groupByValue] = &stateEntry{
			val:        sumValue,
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}
	}
}

func (q *SumQPU) processUpdateRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) libqpu.LogOperation {
	attributes := respRecord.GetAttributes()

	groupByValue := attributes[q.groupBy].GetInt()
	sumValue := attributes[q.aggregationAttribute].GetInt()
	delete(attributes, q.aggregationAttribute)

	_, found := q.inMemState.entries[groupByValue]
	if found {
		q.inMemState.entries[groupByValue].Lock()

		q.inMemState.entries[groupByValue].val += sumValue
		q.inMemState.entries[groupByValue].attributes = attributes
		q.inMemState.entries[groupByValue].ts = respRecord.GetLogOp().GetTimestamp()

		attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(q.inMemState.entries[groupByValue].val)

		q.inMemState.entries[groupByValue].Unlock()

	} else {
		q.inMemState.Lock()

		q.inMemState.entries[groupByValue] = &stateEntry{
			val:        sumValue,
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}

		attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(sumValue)

		q.inMemState.Unlock()
	}

	return libqpu.LogOperationDelta(
		respRecord.GetRecordID(),
		q.schemaTable,
		respRecord.GetLogOp().GetTimestamp(),
		nil,
		attributes,
	)
}

func (q *SumQPU) flushState() error {
	for stateMapKey, stateEntry := range q.inMemState.entries {
		stateEntry.RLock()
		_, err := q.updateState(stateMapKey, stateEntry.val, stateEntry.attributes, stateEntry.ts.GetVc())
		stateEntry.RUnlock()
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
	res := q.state.GetRow(q.schemaTable+q.port,
		[]string{q.aggregationAttribute + "_sum"},
		[]string{fmt.Sprintf("%s = %s", q.groupBy, strconv.FormatInt(groupByVal, 10))},
		nil,
	)

	var storedSumValue int64
	errScan := res.Scan(&storedSumValue)
	if errScan != nil && errScan != sql.ErrNoRows {
		return -1, errScan
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
	var err error
	if errScan == sql.ErrNoRows {
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
