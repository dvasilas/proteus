package index

import (
	"context"
	"errors"
	"io"
	"time"

	utils "github.com/dimitriosvasilas/proteus"
	attribute "github.com/dimitriosvasilas/proteus/attributes"
	pbDsQPU "github.com/dimitriosvasilas/proteus/protos/datastore"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	indexMapCrdtKeys "github.com/dimitriosvasilas/proteus/qpu/index/indexMapCrdt/indexKeys"
	indexSize "github.com/dimitriosvasilas/proteus/qpu/index/indexSize"
	indexTagF "github.com/dimitriosvasilas/proteus/qpu/index/indexTagFloat"
	indexTagStr "github.com/dimitriosvasilas/proteus/qpu/index/indexTagStr"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

//IQPU implements an index QPU
type IQPU struct {
	index *Index
}

//Impl specifies the API of an index structure
type Impl interface {
	FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string)
	OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item
	GetPostings(entry btree.Item) map[string]utils.Posting
	AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item
	BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item
	ReducedKeyToTagKey(k string) string
}

//Index implements a generic index structure
type Index struct {
	Index     Impl
	attribute string
	lbound    *pbQPU.Value
	ubound    *pbQPU.Value
	entries   *btree.BTree
	state     map[string]pbQPU.Object
}

//---------------- API Functions -------------------

//QPU creates an index QPU
func QPU(conf utils.QPUConfig, conns utils.DownwardConns) (*IQPU, error) {
	index, err := new(conf.IndexConfig.Attribute, conf.IndexConfig.IndexType, conf.IndexConfig.LBound, conf.IndexConfig.UBound)
	if err != nil {
		return nil, err
	}

	qpu := &IQPU{
		index: index,
	}

	for _, c := range conns.DsConn {
		if conf.IndexConfig.ConsLevel == "async" {
			streamIn, cancel, err := c.SubscribeOpsAsync(time.Now().UnixNano())
			if err != nil {
				cancel()
				return nil, err
			}
			go qpu.opConsumerAsync(streamIn, cancel)
		} else if conf.IndexConfig.ConsLevel == "sync" {
			streamIn, cancel, err := c.SubscribeOpsSync(time.Now().UnixNano())
			if err != nil {
				cancel()
				return nil, err
			}
			go qpu.opConsumerSync(streamIn, cancel)
		} else {
			return nil, errors.New("ConsLevel in IndexConfig can be sync/async")
		}
	}

	if err := qpu.indexCatchUp(conns); err != nil {
		return nil, err
	}

	return qpu, nil
}

//Find implements the Find API for the index QPU
func (q *IQPU) Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error {
	log.WithFields(log.Fields{
		"query": in.Predicate,
	}).Debug("Index lookup")
	indexResult, found, err := q.index.get(in.Predicate)
	if err != nil {
		return err
	}
	if found {
		for _, item := range indexResult {
			if err := streamOut.Send(&pb.QueryResultStream{Object: &item.Object, Dataset: &item.Dataset}); err != nil {
				return err
			}
		}
	}
	return nil
}

//----------- Stream Consumer Functions ------------

//Receives an asynchronous stream of write operations
//Updates the index for each operation
//TODO: Find a way to handle an error here
func (q *IQPU) opConsumerAsync(streamIn pbDsQPU.DataStore_SubscribeOpsAsyncClient, cancel context.CancelFunc) {
	for {
		streamMsg, err := streamIn.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("opConsumer: stream.Recv() error")
			return
		} else {
			if streamMsg.Operation.OpId == "no_op" {
				continue
			}
			log.WithFields(log.Fields{
				"operation": streamMsg.GetOperation(),
			}).Debug("index QPU received operation")

			if err := q.update(streamMsg.GetOperation()); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"op":    streamMsg.Operation,
				}).Fatal("opConsumer: index Update failed")
				return
			}
		}
	}
}

//Receives a synchronous stream of write operations
//Updates the index for each operation and sends an ACK through the stream
//TODO: Find a way to handle an error here
func (q *IQPU) opConsumerSync(streamInOut pbDsQPU.DataStore_SubscribeOpsSyncClient, cancel context.CancelFunc) {
	for {
		streamMsg, err := streamInOut.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("opConsumer: stream.Recv() error")
			return
		} else {
			if streamMsg.Operation.OpId == "no_op" {
				continue
			}
			log.WithFields(log.Fields{
				"op": streamMsg.GetOperation(),
			}).Debug("QPUServer:opConsumerSync:  received op")
			if err := q.update(streamMsg.GetOperation()); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"op":    streamMsg.Operation,
				}).Fatal("opConsumer: index Update failed")
				return
			}
			log.Debug("QPUServer:index updated, sending ACK")
			if err := streamInOut.Send(&pbDsQPU.OpAckStream{Msg: "ack", OpId: streamMsg.GetOperation().GetOpId()}); err != nil {
				log.Fatal("opConsumerSync stream.Send failed")
				return
			}
		}
	}
}

//Receives a stream of objects stored in the data store, as a result of a catch-up operation
//Updates the index for each object
func (q *IQPU) catchUpConsumer(streamIn pbDsQPU.DataStore_GetSnapshotClient, errs chan error) {
	for {
		streamMsg, err := streamIn.Recv()
		if err == io.EOF {
			errs <- nil
			return
		} else if err != nil {
			errs <- err
			return
		}
		op := &pbQPU.Operation{
			OpId:      "catchUp",
			OpPayload: &pbQPU.OperationPayload{Payload: &pbQPU.OperationPayload_State{State: streamMsg.GetObject()}},
			DataSet:   streamMsg.GetDataset(),
		}
		if err := q.update(op); err != nil {
			errs <- err
			return
		}
	}
}

//---------------- Internal Functions --------------

//Creates a new index structure
func new(attrK string, indexType string, lb string, ub string) (*Index, error) {
	i := &Index{
		attribute: attrK,
		entries:   btree.New(2),
		state:     make(map[string]pbQPU.Object),
	}

	attr, err := attribute.Attr(attrK, nil)
	if err != nil {
		return nil, err
	}
	lbound, ubound, err := attr.BoundStrToVal(lb, ub)
	if err != nil {
		return &Index{}, errors.New("Bounds in index configuration is not the right datatype")
	}
	i.lbound = lbound
	i.ubound = ubound
	switch indexType {
	case "tagF":
		i.Index = indexTagF.New()
	case "size":
		i.Index = indexSize.New()
	case "tagStr":
		i.Index = indexTagStr.New()
	case "mapCrdtKeys":
		i.Index = indexMapCrdtKeys.New()
	default:
		return &Index{}, errors.New("index datatype not implemented")
	}
	return i, nil
}

//Performs an index catch-up  operation, initiating an object stream from the data store
func (q *IQPU) indexCatchUp(conns utils.DownwardConns) error {
	errs := make([]chan error, len(conns.DsConn))
	for i := range conns.DsConn {
		errs[i] = make(chan error)
	}

	for i, c := range conns.DsConn {
		streamIn, cancel, err := c.GetSnapshot(time.Now().UnixNano())
		defer cancel()
		if err != nil {
			return err
		}
		go q.catchUpConsumer(streamIn, errs[i])
	}
	for i := range conns.DsConn {
		err := <-errs[i]
		if err != nil {
			return err
		}
	}
	return nil
}

//Updates the index, given an operation
func (q *IQPU) update(op *pbQPU.Operation) error {
	log.WithFields(log.Fields{
		"operation": op,
	}).Debug("index:Update")

	switch op.GetOpPayload().Payload.(type) {
	case *pbQPU.OperationPayload_State:
		for k, v := range op.GetOpPayload().GetState().GetAttributes() {
			if indexable, k := q.index.Index.FilterIndexable(k, v, q.index.attribute, q.index.lbound, q.index.ubound); indexable {
				if op.GetOpId() != "catchUp" {
					removeOldEntry(k, v, op.GetOpPayload().GetState(), q.index)
				}
				if err := put(k, v, op.GetOpPayload().GetState(), op.GetDataSet(), q.index); err != nil {
					return err
				}
			}
		}
	case *pbQPU.OperationPayload_Op:
		log.Debug("index:Update: OperationPayload_Op")
	}

	return nil
}

//Updates the index, given an object that has been updated
func put(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet, i *Index) error {
	entry := i.Index.OpToEntry(attrKey, attrVal)
	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		item = i.Index.AppendPostings(item, obj.Key, utils.Posting{Object: *obj, Dataset: *ds})
		i.entries.ReplaceOrInsert(item)
	} else {
		entry = i.Index.AppendPostings(entry, obj.Key, utils.Posting{Object: *obj, Dataset: *ds})
		i.entries.ReplaceOrInsert(entry)
	}
	i.state[obj.Key] = *obj
	return nil
}

//Performs an index lookup given a predicate
//Returns aposting list containing the objects that match the given predicate
func (i *Index) get(p []*pbQPU.Predicate) (map[string]utils.Posting, bool, error) {
	res := make(map[string]utils.Posting)
	it := func(item btree.Item) bool {
		for _, p := range i.Index.GetPostings(item) {
			res[p.Object.GetKey()] = p
		}
		return true
	}
	i.entries.AscendRange(i.Index.BoundToEntry(p[0].GetAttribute(), p[0].Lbound), i.Index.BoundToEntry(p[0].GetAttribute(), p[0].Ubound), it)
	return res, true, nil
}

//Removes an old index entry, given an object that has been updated
func removeOldEntry(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, i *Index) {
	if s, ok := i.state[obj.Key]; ok {
		entry := i.Index.OpToEntry(attrKey, s.Attributes[i.Index.ReducedKeyToTagKey(attrKey)])
		item := i.entries.Get(entry)
		delete(i.Index.GetPostings(item), s.Key)
	}
}
