package sumqpu

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
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
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
	subscribeQueries           map[int]chan libqpu.LogOperation
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
	entries map[int32]*stateEntry
}

type stateEntry struct {
	sync.RWMutex
	val        int32
	attributes map[string]*qpu.Value
	ts         *qpu.Vectorclock
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*SumQPU, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	sqpu := &SumQPU{
		// state:                      qpu.State,
		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
		subscribeQueries:           make(map[int]chan libqpu.LogOperation),
		aggregationAttribute:       qpu.Config.AggregationConfig.AggregationAttribute,
		groupBy:                    qpu.Config.AggregationConfig.GroupBy,
		inMemState:                 &inMemState{entries: make(map[int32]*stateEntry)},
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
		queryID := rand.Int()
		go func() {
			if err = responsestream.StreamConsumer(responseStream, qpu.Config.ProcessingConfig.Input.MaxWorkers, qpu.Config.ProcessingConfig.Input.MaxJobQueue, sqpu.processRespRecord, nil, nil, queryID); err != nil {
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
			recordID := strconv.FormatInt(int64(k), 10)

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

	queryID := rand.Int()
	q.subscribeQueries[queryID] = logOpCh

	return queryID, logOpCh, errCh
}

// ClientQuery ...
func (q *SumQPU) ClientQuery(query libqpu.ASTQuery, queryStr string, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	return nil, errors.New("not implemented")
}

// ClientQuery1 ...
func (q *SumQPU) ClientQuery1(query libqpu.ASTQuery, queryStr string) (*qpuextapi.QueryResp1, error) {
	return nil, nil
}

// QuerySubscribe  ...
func (q *SumQPU) QuerySubscribe(libqpu.ASTQuery, *qpuextapi.QueryReq) (chan libqpu.LogOperation, chan bool, chan error) {
	return nil, nil, nil
}

// GetConfig ...
func (q *SumQPU) GetConfig() *qpuapi.ConfigResponse {
	return &qpuapi.ConfigResponse{
		Schema: []string{q.schemaTable},
	}
}

// RemovePersistentQuery ...
func (q *SumQPU) RemovePersistentQuery(table string, queryID int) {
	delete(q.subscribeQueries, queryID)
}

// GetMetrics ...
func (q *SumQPU) GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	var p50, p90, p95, p99 float64
	p50, p90, p95, p99 = -1, -1, -1, -1

	if q.measureNotificationLatency {
		p50, p90, p95, p99 = q.notificationLatencyM.GetMetrics()
	}
	return &qpuextapi.MetricsResponse{
		NotificationLatencyP50: p50,
		NotificationLatencyP90: p90,
		NotificationLatencyP95: p95,
		NotificationLatencyP99: p99,
	}, nil
}

// GetWriteLog ...
func (q *SumQPU) GetWriteLog(req *qpuextapi.GetWriteLogReq, stream qpuapi.QPUAPI_GetWriteLogServer) error {
	return nil
}

// ---------------- Internal Functions --------------

func (q *SumQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) error {
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

	} else {
		logOp := q.processRecordInMem(respRecord, data, recordCh)
		if respRecordType == libqpu.Delta {
			logOp.InTs = respRecord.InTs

			for _, ch := range q.subscribeQueries {
				ch <- logOp
			}
		}
	}

	return nil
}

func (q *SumQPU) processRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) libqpu.LogOperation {
	attributes := respRecord.GetAttributes()

	groupByValue := attributes[q.groupBy].GetInt()
	sumValue := attributes[q.aggregationAttribute].GetInt()
	delete(attributes, q.aggregationAttribute)

	q.inMemState.Lock()
	_, found := q.inMemState.entries[groupByValue]
	if found {
		q.inMemState.entries[groupByValue].val += sumValue
		q.inMemState.entries[groupByValue].attributes = attributes
		q.inMemState.entries[groupByValue].ts = respRecord.GetLogOp().GetTimestamp()

		attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(q.inMemState.entries[groupByValue].val)
	} else {
		q.inMemState.entries[groupByValue] = &stateEntry{
			val:        sumValue,
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}

		attributes[q.aggregationAttribute+"_sum"] = libqpu.ValueInt(sumValue)

	}

	q.inMemState.Unlock()

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
func (q *SumQPU) updateState(groupByVal, sumVal int32, attributes map[string]*qpu.Value, vc map[string]*tspb.Timestamp) (int32, error) {
	res := q.state.GetRow(q.schemaTable+q.port,
		[]string{q.aggregationAttribute + "_sum"},
		[]string{fmt.Sprintf("%s = %s", q.groupBy, strconv.FormatInt(int64(groupByVal), 10))},
		nil,
	)

	var storedSumValue int32
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

	var newSumValue int32
	var err error
	if errScan == sql.ErrNoRows {
		// insert the new value
		row[q.aggregationAttribute+"_sum"] = sumVal
		err = q.state.Insert(q.schemaTable+q.port, row, vc)
		newSumValue = sumVal
	} else {
		newSumValue = storedSumValue + sumVal
		err = q.state.Update(q.schemaTable+q.port, row, map[string]interface{}{q.aggregationAttribute + "_sum": newSumValue}, vc)
	}

	if err != nil {
		return -1, err
	}

	return newSumValue, nil
}
