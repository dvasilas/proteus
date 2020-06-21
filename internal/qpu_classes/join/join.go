package joinqpu

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"

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
	endOfStreamCnt int
	catchUpDoneCh  chan int
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
		state:          qpu.State,
		schema:         qpu.Schema,
		joinAttributes: make(map[string]string),
		inMemState:     &inMemState{entries: make(map[int64]*stateEntry)},
		catchUpDoneCh:  catchUpDoneCh,
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
				return &JoinQPU{}, libqpu.Error(errors.New("unknown attribute type"))
			}
		}
	}

	if err := jqpu.state.Init(
		stateDatabase,
		stateTable,
		fmt.Sprintf(
			"CREATE TABLE %s (%s %s int NOT NULL, ts_key varchar(30), ts TIMESTAMP, PRIMARY KEY (%s) )",
			stateTable,
			idAttributesColumns,
			joinAttributeKey,
			joinAttributeKey,
		),
	); err != nil {
		return &JoinQPU{}, err
	}

	for i := 0; i < len(qpu.AdjacentQPUs); i++ {
		querySnapshot := queries.NewQuerySnapshotAndSubscribe(
			qpu.Config.JoinConfig.Source[i].Table,
			qpu.Config.JoinConfig.Source[i].Projection,
			[]string{}, []string{})
		responseStreamStories, err := qpugraph.SendQueryI(querySnapshot, qpu.AdjacentQPUs[i])
		if err != nil {
			return &JoinQPU{}, err
		}
		go func() {
			if err = responsestream.StreamConsumer(responseStreamStories, jqpu.processRespRecord, nil, nil); err != nil {
				panic(err)
			}
		}()
	}

	return jqpu, nil
}

// ProcessQuerySnapshot ...
func (q *JoinQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	libqpu.Trace("Join QPU ProcessQuerySnapshot", map[string]interface{}{"query": query})

	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	projection := make([]string, len(q.schema[query.GetTable()]))
	i := 0
	for attr := range q.schema[query.GetTable()] {
		projection[i] = attr
		i++
	}

	stateCh, err := q.state.Scan(stateTable, projection, query.GetLimit())
	if err != nil {
		errCh <- err
		return logOpCh, errCh
	}

	go func() {
		for record := range stateCh {
			recordID := record[joinAttributeKey]

			vectorClockKey := record["ts_key"]
			vectorClockVal, err := strconv.ParseInt(record["unix_timestamp(ts)"], 10, 64)
			if err != nil {
				libqpu.Error(err)
				errCh <- err
				break
			}
			timestamp, err := ptypes.TimestampProto(time.Unix(vectorClockVal, 0))
			if err != nil {
				libqpu.Error(err)
				errCh <- err
				break
			}

			delete(record, "unix_timestamp(ts)")
			delete(record, "ts_key")

			attributes, err := q.schema.StrToAttributes(stateTable, record)
			if err != nil {
				libqpu.Error(err)
				errCh <- err
				break
			}

			if _, found := attributes["vote_sum"]; !found {
				attributes["vote_sum"] = libqpu.ValueInt(0)
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
func (q *JoinQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	// q.snapshotConsumer(query, stream)
	return -1, nil, nil
}

// RemovePersistentQuery ...
func (q *JoinQPU) RemovePersistentQuery(table string, queryID int) {
}

// ---------------- Internal Functions --------------

func (q *JoinQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}
	if respRecordType == libqpu.EndOfStream {
		q.endOfStreamCnt++
		if q.endOfStreamCnt == len(q.joinAttributes) {
			err := q.flushState()
			if err != nil {
				return err
			}
			go func() {
				q.catchUpDoneCh <- 0
			}()
			return nil
		}
		return nil
	}

	if err := q.processRespRecordInMem(respRecord, data, recordCh); err != nil {
		return err
	}

	if respRecord.GetLogOp().IsDelta() {
		attributes := respRecord.GetAttributes()
		joinAttribute := q.joinAttributes[respRecord.GetLogOp().GetTable()]
		joinAttributeValue := attributes[joinAttribute].GetInt()

		stateEntry := q.inMemState.entries[joinAttributeValue]
		stateEntry.mutex.RLock()
		_, err := q.updateState(joinAttributeValue, stateEntry.attributes, stateEntry.ts.GetVc())
		stateEntry.mutex.RUnlock()
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

	attributes[joinAttribute] = libqpu.ValueInt(joinAttributeValue)

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
