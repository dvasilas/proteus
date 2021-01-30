package inmemindexqpu

import (
	"container/list"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"

	//
	_ "github.com/go-sql-driver/mysql"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type catchUp struct {
	sync.Mutex
	catchUpDoneCh  chan int
	catchupQueries map[int]*catchupQuery
	catchUpDone    bool
}

type catchupQuery struct {
	endOfStreamSeqID int64
	catchupDone      bool
	catchUpSeqID     map[int64]bool
}

// InMemIndex represents a generic B-Tree index.
// It can be used for indexing different types of attributes
// by using different implementation of the indexImplementation interface.
type InMemIndex struct {
	index         indexImplementation
	attributeName string
}

// indexImplementation represents a B-Tree index implementation for a specific attribute type.
type indexImplementation interface {
	update(*qpu.Value, *qpu.Value, libqpu.LogOperation) error
	updateCatchUp(*qpu.Value, libqpu.LogOperation) error
	lookup(libqpu.ASTQuery) []*qpuextapi.QueryRespRecord
	print()
}

// IndexQPU ...
type IndexQPU struct {
	index                      *InMemIndex
	inputSchema                libqpu.Schema
	outputSchema               libqpu.Schema
	stateTable                 string
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
		index: &InMemIndex{
			attributeName: "attribute0",
		},

		inputSchema:                qpu.InputSchema,
		outputSchema:               make(map[string]libqpu.SchemaTable),
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

	jqpu.index.index = newBTreeIndex()

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
					queryID := rand.Int()
					jqpu.catchUp.catchupQueries[queryID] = &catchupQuery{
						catchUpSeqID:     make(map[int64]bool),
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
				if q.endOfStreamSeqID < 0 {
					done = false
					break
				} else if len(q.catchUpSeqID) < int(q.endOfStreamSeqID) {
					done = false

				} else {
					for _, v := range q.catchUpSeqID {
						if !v {
							done = false
							break
						}
					}
				}
			}

			if done {
				jqpu.catchUp.catchUpDone = true

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
	return nil
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
	// return &qpuextapi.QueryResp{}, nil
	result := q.index.index.lookup(query)

	return &qpuextapi.QueryResp{
		RespRecord: result,
	}, nil
}

// ClientQuery1 ...
func (q *IndexQPU) ClientQuery1(query libqpu.ASTQuery, queryStr string) (*qpuextapi.QueryResp1, error) {
	return &qpuextapi.QueryResp1{}, nil
	// result := q.index.index.lookup(query)

	// return &qpuextapi.QueryResp1{
	// 	RespRecord: result,
	// }, nil
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

func (q *IndexQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) error {
	q.catchUp.Lock()
	q.catchUp.catchupQueries[queryID].catchUpSeqID[respRecord.GetSequenceID()] = false
	q.catchUp.Unlock()

	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}

	if respRecordType == libqpu.EndOfStream {
		q.catchUp.Lock()
		q.catchUp.catchupQueries[queryID].endOfStreamSeqID = respRecord.GetSequenceID()
		q.catchUp.catchupQueries[queryID].catchUpSeqID[respRecord.GetSequenceID()] = true
		q.catchUp.Unlock()
	} else {

		inSchema := false
		for tableName := range q.inputSchema {
			if respRecord.GetLogOp().GetTable() == tableName {
				inSchema = true
			}
		}
		if !inSchema {
			return nil
		}

		if q.catchUp.catchUpDone && q.measureNotificationLatency {
			if err := q.notificationLatencyM.AddFromOp(respRecord.GetLogOp()); err != nil {
				return err
			}
		}

		if respRecordType == libqpu.State {
			if err := q.index.index.updateCatchUp(respRecord.GetLogOp().GetAttributes()["attribute0"], respRecord.GetLogOp()); err != nil {
				return err
			}

			q.catchUp.Lock()
			q.catchUp.catchupQueries[queryID].catchUpSeqID[respRecord.GetSequenceID()] = true
			q.catchUp.Unlock()

		} else if respRecordType == libqpu.Delta {
			if respRecord.GetLogOp().HasOldState() {

				if err := q.index.index.update(respRecord.GetAttributesOld()["attribute0"], respRecord.GetLogOp().GetAttributes()["attribute0"], respRecord.GetLogOp()); err != nil {
					return err
				}

			} else {
				if err := q.index.index.update(nil, respRecord.GetLogOp().GetAttributes()["attribute0"], respRecord.GetLogOp()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func encodeDataItem(dataItemID string, attributes map[string]*qpu.Value) (string, *qpuextapi.QueryRespRecord, error) {
	// di := make(map[string]*qpuextapi.Payload)
	// di["id"] = &qpuextapi.Payload{
	// 	Value: []byte(dataItemID),
	// }

	attrs := make(map[string]string)
	for k, v := range attributes {
		switch v.GetVal().(type) {

		case *qpu.Value_Str:
			attrs[k] = v.GetStr()

		case *qpu.Value_Int:
			attrs[k] = strconv.Itoa(int(v.GetInt()))
			// b := make([]byte, 8)
			// binary.LittleEndian.PutUint64(b, uint64())
			// di[k] = &qpuextapi.Payload{
			// 	Value: b,
			// }
		default:
			return "", nil, utils.Error(errors.New("unknown value type"))
		}
	}

	return dataItemID,
		&qpuextapi.QueryRespRecord{
			RecordId:   dataItemID,
			Attributes: attrs,
		}, nil
}

// //------- indexImplementation interface ------------

// bTreeIndex implements indexImplementation
type bTreeIndex struct {
	tree  *btree.BTree
	mutex sync.RWMutex
	entry indexEntry
}

func newBTreeIndex() *bTreeIndex {
	index := &bTreeIndex{tree: btree.New(2)}
	index.entry = newIndexInt()
	return index
}

func (i *bTreeIndex) update(valueOld, valueNew *qpu.Value, logOp libqpu.LogOperation) error {
	dataItemID, dataItem, err := encodeDataItem(logOp.GetObjectID(), logOp.GetAttributes())
	if err != nil {
		return err
	}

	i.mutex.Lock()
	if valueOld == nil {
		if indexEntry, found := i.getIndexEntry(valueNew); found {
			indexEntry.addToPosting(dataItemID, dataItem)
			i.updateIndexEntry(indexEntry)
		} else {
			indexEntry := i.newIndexEntry(valueNew, dataItemID, dataItem).(treeNode)
			i.updateIndexEntry(indexEntry)
		}
	} else if valueNew != nil {
		if indexEntry, found := i.getIndexEntry(valueOld); found {
			indexEntry.removeObjFromEntry(dataItemID)
		} else {
			return errors.New("index entry for old value not found")
		}
		if indexEntry, found := i.getIndexEntry(valueNew); found {
			indexEntry.addToPosting(dataItemID, dataItem)
			i.updateIndexEntry(indexEntry)
		} else {
			return errors.New("index entry for new value not found")
		}
	}
	// i.print()
	i.mutex.Unlock()
	return nil
}

func (i *bTreeIndex) updateCatchUp(value *qpu.Value, logOp libqpu.LogOperation) error {
	dataItemID, dataItem, err := encodeDataItem(logOp.GetObjectID(), logOp.GetAttributes())
	if err != nil {
		return err
	}

	i.mutex.Lock()
	if indexEntry, found := i.getIndexEntry(value); found {
		indexEntry.addToPosting(dataItemID, dataItem)
		i.updateIndexEntry(indexEntry)
	} else {
		indexEntry := i.newIndexEntry(value, dataItemID, dataItem).(treeNode)
		i.updateIndexEntry(indexEntry)
	}
	i.mutex.Unlock()
	// i.print()

	return nil
}

func (i *bTreeIndex) lookup(query libqpu.ASTQuery) []*qpuextapi.QueryRespRecord {
	res := make([]*qpuextapi.QueryRespRecord, 0)

	it := func(node btree.Item) bool {
		postings := node.(treeNode).getLatestVersion()
		for _, v := range postings.DataItems {
			res = append(res, v)
		}
		return true
	}
	lb := query.GetPredicate()[0].GetLbound()
	ub := query.GetPredicate()[0].GetUbound()

	if comp, _ := utils.Compare(lb, ub); comp == 0 {
		ub = libqpu.ValueInt(ub.GetInt() + 1)
	}
	lbEntry, ubEntry := i.entry.predicateToIndexEntries(lb, ub)

	i.mutex.RLock()
	i.tree.AscendRange(lbEntry, ubEntry, it)
	i.mutex.RUnlock()

	return res
}

func (i *bTreeIndex) newIndexEntry(value *qpu.Value, dataItemID string, dataItem *qpuextapi.QueryRespRecord) btree.Item {
	item := i.entry.newIndexEntry(value)

	posting := Posting{
		DataItems: map[string]*qpuextapi.QueryRespRecord{
			dataItemID: dataItem,
		},
	}
	item.createNewVersion(posting)
	return item
}

func (i *bTreeIndex) getIndexEntry(value *qpu.Value) (treeNode, bool) {
	indexEntry := i.entry.attrToIndexEntry(value)
	if i.tree.Has(indexEntry) {
		return i.tree.Get(indexEntry).(treeNode), true
	}
	return treeNode{}, false
}

func (i *bTreeIndex) updateIndexEntry(e btree.Item) {
	i.tree.ReplaceOrInsert(e)
}

func (i *bTreeIndex) print() {
	log.Debug("Printing index")
	it := func(item btree.Item) bool {
		if item != nil {
			log.WithFields(log.Fields{"val": item.(treeNode).Value}).Debug("value")
			for e := item.(treeNode).Postings.Front(); e != nil; e = e.Next() {
				// log.WithFields(log.Fields{.Debug("posting list version")
				for o := range e.Value.(Posting).DataItems {
					log.Debug("- ", o)
				}
			}
		}
		return true
	}
	i.tree.Ascend(it)
	log.Debug()
}

type indexEntry interface {
	newIndexEntry(*qpu.Value) treeNode
	attrToIndexEntry(*qpu.Value) btree.Item
	predicateToIndexEntries(lb, ub *qpu.Value) (btree.Item, btree.Item)
}

// indexInt implements indexEntry
type indexInt struct {
}

func newIndexInt() indexInt {
	return indexInt{}
}

func (i indexInt) newIndexEntry(value *qpu.Value) treeNode {
	return treeNode{Value: valueInt{Val: value.GetInt()}, Postings: list.New()}
}
func (i indexInt) attrToIndexEntry(value *qpu.Value) btree.Item {
	return treeNode{Value: valueInt{Val: value.GetInt()}}
}
func (i indexInt) predicateToIndexEntries(lb, ub *qpu.Value) (btree.Item, btree.Item) {
	return treeNode{Value: valueInt{Val: lb.GetInt()}}, treeNode{Value: valueInt{Val: ub.GetInt()}}
	// return treeNode{}, treeNode{}
}

//------------ btree.Item interface ----------------

// treeNode implements btree.Item (need to implement Less)
type treeNode struct {
	Value    comparable
	Postings *list.List
}

// Posting ...
type Posting struct {
	DataItems map[string]*qpuextapi.QueryRespRecord
}

func (n treeNode) Less(than btree.Item) bool {
	return n.Value.less(than.(treeNode).Value)
}

func (n treeNode) createNewVersion(p Posting) {
	n.Postings.PushBack(p)
}

func (n treeNode) getLatestVersion() Posting {
	return n.Postings.Back().Value.(Posting)
}

func (n treeNode) removeObjFromEntry(dataItemID string) {
	delete(n.getLatestVersion().DataItems, dataItemID)
}

func (n treeNode) addToPosting(dataItemID string, dataItem *qpuextapi.QueryRespRecord) {
	n.Postings.Front().Value.(Posting).DataItems[dataItemID] = dataItem
}

// ------------ comparable interface ----------------

type comparable interface {
	less(comparable) bool
}

// valueInt implements comparable
type valueInt struct {
	Val int32
}

func (x valueInt) less(than comparable) bool {
	return x.Val < than.(valueInt).Val
}
