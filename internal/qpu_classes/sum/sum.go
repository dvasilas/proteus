package sumqpu

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
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
const stateTable = "stateTableSum"

// SumQPU ...
type SumQPU struct {
	state             libqpu.QPUState
	schema            libqpu.Schema
	subscribeQueries  []chan libqpu.LogOperation
	attributeToSum    string
	idAttributes      []string
	stateSumAttribute string
	sourceTable       string
	inMemState        *inMemState
	port              string
	catchUpDoneCh     chan int
}

type subscribeQuery struct {
	query  libqpu.InternalQuery
	stream libqpu.RequestStream
	seqID  int64
}

type inMemState struct {
	entries map[string]*stateEntry
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
		state:             q.State,
		schema:            q.Schema,
		subscribeQueries:  make([]chan libqpu.LogOperation, 0),
		attributeToSum:    q.Config.SumConfig.AttributeToSum,
		idAttributes:      q.Config.SumConfig.RecordIDAttribute,
		stateSumAttribute: q.Config.SumConfig.AttributeToSum + "_sum",
		sourceTable:       q.Config.SumConfig.SourceTable,
		inMemState:        &inMemState{entries: make(map[string]*stateEntry)},
		port:              q.Config.Port,
		catchUpDoneCh:     catchUpDoneCh,
	}

	sqpu.schema[stateTable] = make(map[string]libqpu.DatastoreAttributeType)
	for _, attr := range sqpu.idAttributes {
		sqpu.schema[stateTable][attr] = libqpu.INT
	}
	sqpu.schema[stateTable][sqpu.stateSumAttribute] = libqpu.INT

	idAttributesColumns := ""
	idAttributesUniqueKey := "("
	for i, attr := range sqpu.idAttributes {
		idAttributesColumns += attr + " bigint unsigned NOT NULL, "
		idAttributesUniqueKey += attr
		if len(sqpu.idAttributes) > 1 && i < len(sqpu.idAttributes)-1 {
			idAttributesUniqueKey += ", "
		}
	}
	idAttributesUniqueKey += ")"

	if err := sqpu.state.Init(
		stateDatabase,
		stateTable+sqpu.port,
		fmt.Sprintf(
			// vote_count int
			"CREATE TABLE %s (%s %s int NOT NULL, ts_key varchar(30), ts TIMESTAMP, UNIQUE KEY %s )",
			stateTable+sqpu.port,
			idAttributesColumns,
			sqpu.stateSumAttribute,
			idAttributesUniqueKey,
		),
	); err != nil {
		return &SumQPU{}, err
	}

	query := queries.NewQuerySnapshotAndSubscribe(
		sqpu.sourceTable,
		nil,
		q.Config.SumConfig.Query.Projection,
		q.Config.SumConfig.Query.IsNull,
		q.Config.SumConfig.Query.IsNotNull,
		nil,
	)

	for _, adjQPU := range q.AdjacentQPUs {
		responseStream, err := qpugraph.SendQueryI(query, adjQPU)
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

// ProcessQuerySnapshot ...
func (q *SumQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	stateCh, err := q.state.Scan(
		stateTable+q.port,
		append(q.idAttributes, q.stateSumAttribute),
		query.GetLimit(),
		nil,
	)
	if err != nil {
		errCh <- err
		return logOpCh, errCh
	}

	go func() {
		for record := range stateCh {
			recordID := ""
			for _, attr := range q.idAttributes {
				recordID += record[attr]
			}

			vectorClockKey := record["ts_key"]
			vectorClockVal, err := strconv.ParseInt(record["unix_timestamp(ts)"], 10, 64)
			if err != nil {
				errCh <- err
				break
			}
			timestamp, err := ptypes.TimestampProto(time.Unix(vectorClockVal, 0))
			if err != nil {
				errCh <- err
				break
			}

			delete(record, "unix_timestamp(ts)")
			delete(record, "ts_key")

			attributes, err := q.schema.StrToAttributes(stateTable, record)
			if err != nil {
				errCh <- err
				break
			}

			logOpCh <- libqpu.LogOperationState(
				recordID,
				stateTable,
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
func (q *SumQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	q.subscribeQueries = append(q.subscribeQueries, logOpCh)

	return -1, logOpCh, errCh
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
	}

	if err := q.processRespRecordInMem(respRecord, data, recordCh); err != nil {
		return err
	}

	if respRecord.GetLogOp().IsDelta() {
		recordID := make(map[string]*qpu.Value)

		attributes := respRecord.GetAttributes()
		var err error
		for _, idAttr := range q.idAttributes {
			recordID[idAttr] = attributes[idAttr]
		}

		sumValue := attributes[q.attributeToSum].GetInt()

		attributesNew, err := q.updateState(recordID, sumValue, respRecord.GetLogOp().GetTimestamp().GetVc())
		if err != nil {
			return err
		}

		logOp := libqpu.LogOperationDelta(
			respRecord.GetRecordID(),
			stateTable,
			respRecord.GetLogOp().GetTimestamp(),
			nil,
			attributesNew,
		)

		for _, ch := range q.subscribeQueries {
			ch <- logOp
		}
	}

	return nil
}

func (q *SumQPU) processRespRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	attributes := respRecord.GetAttributes()
	sumValue := attributes[q.attributeToSum].GetInt()

	stateMapKey := ""
	for i, idAttr := range q.idAttributes {
		stateMapKey += idAttr + ":" + strconv.Itoa(int(attributes[idAttr].GetInt()))
		if i < len(q.idAttributes)-1 {
			stateMapKey += "__"
		}
	}

	value, found := q.inMemState.entries[stateMapKey]
	if found {
		value.mutex.Lock()
		q.inMemState.entries[stateMapKey].val += sumValue
		q.inMemState.entries[stateMapKey].attributes = attributes
		q.inMemState.entries[stateMapKey].ts = respRecord.GetLogOp().GetTimestamp()
		value.mutex.Unlock()
	} else {
		q.inMemState.entries[stateMapKey] = &stateEntry{
			val:        sumValue,
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}
	}

	return nil
}

func (q *SumQPU) updateState(recordID map[string]*qpu.Value, sumVal int64, vc map[string]*tspb.Timestamp) (map[string]*qpu.Value, error) {
	var newSumValue int64

	currentSumValue, err := q.state.Get(stateTable+q.port, q.stateSumAttribute, recordID)
	row := make(map[string]interface{})
	for k, v := range recordID {
		row[k] = v.GetInt()
	}

	if err != nil && err.Error() == "sql: no rows in result set" {
		row[q.stateSumAttribute] = sumVal
		err = q.state.Insert(stateTable+q.port, row, vc)
		newSumValue = sumVal
	} else if err != nil {
		return nil, err
	} else {
		newSumValue = currentSumValue.(int64) + sumVal
		err = q.state.Update(stateTable+q.port, row, map[string]interface{}{q.stateSumAttribute: newSumValue}, vc)
	}

	if err != nil {
		return nil, err
	}

	recordID[q.stateSumAttribute] = libqpu.ValueInt(newSumValue)

	return recordID, nil
}

func (q *SumQPU) flushState() error {
	for stateMapKey, stateEntry := range q.inMemState.entries {
		stateRecordID := make(map[string]*qpu.Value)
		keyComposites := strings.Split(stateMapKey, "__")
		for _, attr := range keyComposites {
			recordIDAttr := strings.Split(attr, ":")
			val, err := strconv.ParseInt(recordIDAttr[1], 10, 64)
			if err != nil {
				return err
			}
			stateRecordID[recordIDAttr[0]] = libqpu.ValueInt(val)
		}
		stateEntry.mutex.RLock()
		_, err := q.updateState(stateRecordID, stateEntry.val, stateEntry.ts.GetVc())
		stateEntry.mutex.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}
