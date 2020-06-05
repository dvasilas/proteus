package joinqpu

import (
	"fmt"
	"sync"

	"github.com/dvasilas/proteus/internal/libqpu"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	tspb "github.com/golang/protobuf/ptypes/timestamp"

	//
	_ "github.com/go-sql-driver/mysql"
)

const stateDatabase = "stateDB"
const stateTable = "stateTableJoin"
const joinAttributeKey = "joinID"

// JoinQPU ...
type JoinQPU struct {
	state          libqpu.QPUState
	schema         libqpu.Schema
	joinAttributes map[string]string
	inMemState     *inMemState
}

type stateEntry struct {
	attributes map[string]*qpu.Value
	ts         *qpu.Vectorclock
	mutex      sync.RWMutex
}

type inMemState struct {
	entries map[int64]*stateEntry
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU) (*JoinQPU, error) {
	jqpu := &JoinQPU{
		state:          qpu.State,
		schema:         qpu.Schema,
		joinAttributes: make(map[string]string),
		inMemState:     &inMemState{entries: make(map[int64]*stateEntry)},
	}

	jqpu.schema[stateTable] = make(map[string]libqpu.DatastoreAttributeType)
	for _, src := range qpu.Config.JoinConfig.Source {
		if _, found := jqpu.schema[src.Table]; !found {
			jqpu.schema[src.Table] = make(map[string]libqpu.DatastoreAttributeType)
		}
		for i, attr := range src.Projection {
			if i == 0 {
				jqpu.joinAttributes[src.Table] = attr
			}
			if _, found := jqpu.schema[src.Table][attr]; !found {
				if i == 0 {
					jqpu.schema[src.Table][attr] = libqpu.INT
					jqpu.joinAttributes[src.Table] = attr
				} else {
					jqpu.schema[src.Table][attr] = libqpu.STR
				}
			}
			if i != 0 {
				jqpu.schema[stateTable][attr] = jqpu.schema[src.Table][attr]
			}
		}
		jqpu.schema[stateTable][joinAttributeKey] = libqpu.INT
	}

	idAttributesColumns := ""
	for attr, attrType := range jqpu.schema[stateTable] {
		if attr != joinAttributeKey {
			idAttributesColumns += attr
			switch attrType {
			case libqpu.INT:
				idAttributesColumns += " INT, "
			case libqpu.STR:
				idAttributesColumns += " TEXT, "
			default:
				return &JoinQPU{}, libqpu.Error("unknown attribute type")
			}
		}
	}

	if err := jqpu.state.Init(
		stateDatabase,
		stateTable,
		fmt.Sprintf(
			// vote_count int
			"CREATE TABLE %s (%s %s int NOT NULL, ts_key varchar(20), ts TIMESTAMP, PRIMARY KEY (%s) )",
			stateTable,
			idAttributesColumns,
			joinAttributeKey,
			joinAttributeKey,
		),
	); err != nil {
		return &JoinQPU{}, err
	}

	// libqpu.Assert(len(qpu.AdjacentQPUs) == 2, "Join QPU should have two adjacent QPUs")

	for i := 0; i < len(qpu.AdjacentQPUs); i++ {
		querySnapshot := queries.GetSnapshot(
			qpu.Config.JoinConfig.Source[i].Table,
			qpu.Config.JoinConfig.Source[i].Projection,
			[]string{}, []string{})
		responseStreamStories, err := qpugraph.SendQueryI(querySnapshot, qpu.AdjacentQPUs[i])
		if err != nil {
			return &JoinQPU{}, err
		}
		if err = responsestream.StreamConsumer(responseStreamStories, jqpu.processRecordInMem, nil, nil); err != nil {
			return &JoinQPU{}, err
		}
	}
	if err := jqpu.flushState(); err != nil {
		return &JoinQPU{}, err
	}

	for i := 0; i < len(qpu.AdjacentQPUs); i++ {
		querySnapshot := queries.SubscribeToAllUpdates(
			qpu.Config.JoinConfig.Source[i].Table,
			qpu.Config.JoinConfig.Source[i].Projection,
			[]string{}, []string{})
		responseStreamStories, err := qpugraph.SendQueryI(querySnapshot, qpu.AdjacentQPUs[i])
		if err != nil {
			return &JoinQPU{}, err
		}
		go func() {
			if err = responsestream.StreamConsumer(responseStreamStories, jqpu.processRecord, nil, nil); err != nil {
				panic(err)
			}
		}()
	}

	return jqpu, nil
}

// ProcessQuery ...
func (q *JoinQPU) ProcessQuery(libqpu.InternalQuery, libqpu.RequestStream, map[string]string, bool) error {
	return nil
}

// ProcessQuerySnapshot ...
func (q *JoinQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	// q.opConsumer(query, stream)
	return nil, nil
}

// ProcessQuerySubscribe ...
func (q *JoinQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	// q.snapshotConsumer(query, stream)
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *JoinQPU) RemovePersistentQuery(table string, queryID int) {
}

// ---------------- Internal Functions --------------

func (q JoinQPU) processRecordInMem(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	libqpu.Trace("received record", map[string]interface{}{"record": respRecord})

	attributes := respRecord.GetAttributes()
	joinAttribute := q.joinAttributes[respRecord.GetLogOp().GetTable()]
	joinAttributeValue := attributes[joinAttribute].GetInt()

	delete(attributes, joinAttribute)

	if entry, found := q.inMemState.entries[joinAttributeValue]; found {
		entry.mutex.Lock()
		for attr, val := range attributes {
			entry.attributes[attr] = val
		}
		entry.mutex.Unlock()
	} else {
		q.inMemState.entries[joinAttributeValue] = &stateEntry{
			attributes: attributes,
			ts:         respRecord.GetLogOp().GetTimestamp(),
		}
	}

	attributes[joinAttribute] = libqpu.ValueInt(joinAttributeValue)

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

func (q *JoinQPU) flushStateEntry(entryID int64) error {
	stateEntry := q.inMemState.entries[entryID]
	stateEntry.mutex.RLock()
	_, err := q.updateState(entryID, stateEntry.attributes, stateEntry.ts.GetVc())
	stateEntry.mutex.RUnlock()
	if err != nil {
		return err
	}

	return nil
}

func (q JoinQPU) updateState(joinID int64, values map[string]*qpu.Value, vc map[string]*tspb.Timestamp) (map[string]*qpu.Value, error) {
	for _, joinAttribute := range q.joinAttributes {
		delete(values, joinAttribute)
	}
	row := make(map[string]interface{})
	for attributeKey := range values {
		val, err := q.schema.GetValue(values, stateTable, attributeKey)
		if err != nil {
			return nil, err
		}
		row[attributeKey] = val
	}

	_, err := q.state.Get(stateTable, joinAttributeKey, map[string]*qpu.Value{joinAttributeKey: libqpu.ValueInt(joinID)})
	if err != nil && err.Error() == "sql: no rows in result set" {
		row[joinAttributeKey] = joinID
		err = q.state.Insert(stateTable, row, vc)
	} else if err != nil {
		return nil, err
	} else {
		err = q.state.Update(stateTable,
			map[string]interface{}{joinAttributeKey: joinID},
			row, vc)
	}

	if err != nil {
		return nil, err
	}

	return values, nil
}

func (q JoinQPU) processRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	libqpu.Trace("received record", map[string]interface{}{"record": respRecord})
	if err := q.processRecordInMem(respRecord, data, recordCh); err != nil {
		return err
	}

	attributes := respRecord.GetAttributes()
	joinAttribute := q.joinAttributes[respRecord.GetLogOp().GetTable()]
	joinAttributeValue := attributes[joinAttribute].GetInt()

	if err := q.flushStateEntry(joinAttributeValue); err != nil {
		return err
	}

	return nil
}
