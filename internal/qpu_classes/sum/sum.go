package sumqpu

import (
	"fmt"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	mysqlbackend "github.com/dvasilas/proteus/internal/qpustate/mysql_backend"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"

	//
	_ "github.com/go-sql-driver/mysql"
)

// assumptions
// "id" and "sum" attributes are of type Value_Int
// the state only stores those two attributes (but "id" can be compose of
// multiple attributes)

const stateDatabase = "stateDB"
const stateTable = "stateTable"

// SumQPU ...
type SumQPU struct {
	state             libqpu.QPUState
	schema            libqpu.Schema
	subscribeQueries  []chan libqpu.LogOperation
	attributeToSum    string
	idAttributes      []string
	stateSumAttribute string
	sourceTable       string
}

type subscribeQuery struct {
	query  libqpu.InternalQuery
	stream libqpu.RequestStream
	seqID  int64
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU) (*SumQPU, error) {
	sqpu := &SumQPU{
		state:             qpu.State,
		schema:            qpu.Schema,
		subscribeQueries:  make([]chan libqpu.LogOperation, 0),
		attributeToSum:    qpu.Config.SumConfig.AttributeToSum,
		idAttributes:      qpu.Config.SumConfig.RecordIDAttribute,
		stateSumAttribute: qpu.Config.SumConfig.AttributeToSum + "_sum",
		sourceTable:       qpu.Config.SumConfig.SourceTable,
	}

	sqpu.schema[stateTable] = make(map[string]libqpu.DatastoreAttributeType, len(sqpu.idAttributes)+1)
	for _, attr := range sqpu.idAttributes {
		sqpu.schema[stateTable][attr] = libqpu.INT
	}
	sqpu.schema[stateTable][sqpu.stateSumAttribute] = libqpu.INT

	idAttributesColumns := ""
	idAttributesUniqueKey := "("
	for i, attr := range sqpu.idAttributes {
		idAttributesColumns += attr + " bigint unsigned NOT NULL,"
		idAttributesUniqueKey += attr
		if i > 0 {
			idAttributesUniqueKey += ", "
		}
	}
	idAttributesUniqueKey += ")"

	if err := sqpu.state.Init(
		stateDatabase,
		stateTable,

		fmt.Sprintf(
			// vote_count int
			"CREATE TABLE %s (%s %s int NOT NULL, UNIQUE KEY %s )",
			stateTable,
			idAttributesColumns,
			sqpu.stateSumAttribute,
			idAttributesUniqueKey,
		),
	); err != nil {
		return &SumQPU{}, err
	}

	querySnapshot := queries.GetSnapshot(
		sqpu.sourceTable,
		qpu.Config.SumConfig.Query.Projection,
		qpu.Config.SumConfig.Query.IsNull,
		[]string{},
	)
	querySubscribe := queries.SubscribeToAllUpdates(
		sqpu.sourceTable,
		qpu.Config.SumConfig.Query.Projection,
		qpu.Config.SumConfig.Query.IsNull,
		[]string{},
	)
	// sqpu.sourceTable, []string{"id", "story_id", "vote"}, []string{"comment_id"}, []string{})
	for _, adjQPU := range qpu.AdjacentQPUs {
		responseStream, err := qpugraph.SendQueryI(querySnapshot, adjQPU)
		if err != nil {
			return &SumQPU{}, err
		}

		if err = responsestream.StreamConsumer(responseStream, sqpu.processRespRecord, nil, nil); err != nil {
			return &SumQPU{}, err
		}

		responseStream, err = qpugraph.SendQueryI(querySubscribe, adjQPU)
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
func (q *SumQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	stateCh, err := q.state.Scan(
		stateTable,
		append(q.idAttributes, q.stateSumAttribute),
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

			attributes, err := q.schema.StrToAttributes(stateTable, record)
			if err != nil {
				errCh <- err
			}

			logOpCh <- libqpu.LogOperationState(
				recordID,
				stateTable,
				libqpu.Vectorclock(map[string]uint64{"tmp": uint64(time.Now().UnixNano())}),
				attributes,
			)

		}
		close(logOpCh)
		close(errCh)
	}()

	return logOpCh, errCh
}

// ProcessQuerySubscribe ...
func (q *SumQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	// q.snapshotConsumer(query, stream)
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	q.subscribeQueries = append(q.subscribeQueries, logOpCh)

	return logOpCh, errCh
}

// ---------------- Internal Functions --------------

func (q *SumQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	libqpu.Trace("received record", map[string]interface{}{"record": respRecord})

	if recordCh != nil {
		recordCh <- respRecord
	}

	recordID := make(map[string]*qpu.Value)

	attributes := respRecord.GetAttributes()
	var err error
	for _, idAttr := range q.idAttributes {
		recordID[idAttr] = attributes[idAttr]
	}

	sumValue := attributes[q.attributeToSum].GetInt()

	attributesNew, err := q.updateState(recordID, sumValue)
	if err != nil {
		return err
	}

	logOp := libqpu.LogOperationDelta(
		respRecord.GetRecordID(),
		stateTable,
		libqpu.Vectorclock(map[string]uint64{"TODO": uint64(time.Now().UnixNano())}),
		nil,
		attributesNew,
	)

	for _, ch := range q.subscribeQueries {
		ch <- logOp
	}

	return nil
}

func (q *SumQPU) updateState(recordID map[string]*qpu.Value, sumVal int64) (map[string]*qpu.Value, error) {
	var newSumValue int64

	selectStmt, selectValues := mysqlbackend.ConstructSelect(recordID)
	currentSumValue, err := q.state.Get(q.stateSumAttribute, stateTable, selectStmt, selectValues...)
	if err != nil && err.Error() == "sql: no rows in result set" {
		insertStmt, insertValStmt, insertValues := mysqlbackend.ConstructInsert(q.stateSumAttribute, sumVal, recordID)
		err = q.state.Insert(stateTable, insertStmt, insertValStmt, insertValues...)

		newSumValue = sumVal
	} else if err != nil {
		return nil, err
	} else {
		newSumValue = currentSumValue.(int64) + sumVal
		setStmt, updateValues := mysqlbackend.ConstructUpdate(q.stateSumAttribute, newSumValue, recordID)
		err = q.state.Update(stateTable, setStmt, selectStmt, updateValues...)

	}

	if err != nil {
		return nil, err
	}

	recordID[q.stateSumAttribute] = libqpu.ValueInt(newSumValue)

	return recordID, nil
}
