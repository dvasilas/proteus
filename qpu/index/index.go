package index

import (
	"context"
	"errors"
	"io"
	"time"

	utils "github.com/dimitriosvasilas/proteus"
	attribute "github.com/dimitriosvasilas/proteus/attributes"
	"github.com/dimitriosvasilas/proteus/config"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	antidoteIndex "github.com/dimitriosvasilas/proteus/qpu/index/antidote"
	inMemIndex "github.com/dimitriosvasilas/proteus/qpu/index/inMem"
	log "github.com/sirupsen/logrus"
)

//IQPU implements an index QPU
type IQPU struct {
	index  *Index
	cancel context.CancelFunc
}

//Store specifies the interface of an index store
type Store interface {
	Update(op *pbQPU.Operation, attribute string, lbound *pbQPU.Value, ubound *pbQPU.Value) error
	Lookup(p []*pbQPU.Predicate) (map[string]utils.Posting, bool, error)
}

//Index implements a generic index structure
type Index struct {
	indexstore Store
	attribute  string
	lbound     *pbQPU.Value
	ubound     *pbQPU.Value
}

//---------------- API Functions -------------------

//QPU creates an index QPU
func QPU(conf config.IndexConfig, conns utils.DownwardConns) (*IQPU, error) {
	index, err := new(conf)
	if err != nil {
		return nil, err
	}

	qpu := &IQPU{
		index: index,
	}

	for _, db := range conns.DBs {
		for _, r := range db.DCs {
			for _, sh := range r.Shards {
				for _, q := range sh.QPUs {
					if conf.ConsLevel == "async" {
						streamIn, cancel, err := q.Client.SubscribeOpsAsync(time.Now().UnixNano())
						if err != nil {
							cancel()
							return nil, err
						}
						qpu.cancel = cancel
						go qpu.opConsumerAsync(streamIn, cancel)
					} else if conf.ConsLevel == "sync" {
						streamIn, cancel, err := q.Client.SubscribeOpsSync(time.Now().UnixNano())
						if err != nil {
							cancel()
							return nil, err
						}
						go qpu.opConsumerSync(streamIn, cancel)
					} else {
						return nil, errors.New("unknown consLevel in index QPU configuration")
					}
				}
			}
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
	indexResult, found, err := q.index.indexstore.Lookup(in.Predicate)
	if !found {
		if err := streamOut.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: "noResults"}}); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	for _, item := range indexResult {
		if err := streamOut.Send(&pb.QueryResultStream{Object: &item.Object, Dataset: &item.Dataset}); err != nil {
			return err
		}
	}
	return nil
}

//GetSnapshot ...
func (q *IQPU) GetSnapshot(in *pb.SubRequest, stream pb.QPU_GetSnapshotServer) error {
	return errors.New("index QPU does not support GetSnapshot()")
}

//SubscribeOpsAsync ...
func (q *IQPU) SubscribeOpsAsync(in *pb.SubRequest, stream pb.QPU_SubscribeOpsAsyncServer) error {
	return errors.New("index QPU does not support SubscribeOpsAsync()")
}

//SubscribeOpsSync ...
func (q *IQPU) SubscribeOpsSync(stream pb.QPU_SubscribeOpsSyncServer) error {
	return errors.New("index QPU does not support SubscribeOpsSync()")
}

//Cleanup ...
func (q *IQPU) Cleanup() {
	log.Info("index QPU cleanup")
	q.cancel()
}

//----------- Stream Consumer Functions ------------

//Receives an asynchronous stream of write operations
//Updates the index for each operation
//TODO: Find a way to handle an error here
func (q *IQPU) opConsumerAsync(streamIn pb.QPU_SubscribeOpsAsyncClient, cancel context.CancelFunc) {
	for {
		streamMsg, err := streamIn.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			return
		} else {
			if streamMsg.Operation.OpId == "no_op" {
				continue
			}
			log.WithFields(log.Fields{
				"operation": streamMsg.GetOperation(),
			}).Debug("index QPU received operation")

			if err := q.updateIndex(streamMsg.GetOperation()); err != nil {
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
func (q *IQPU) opConsumerSync(streamInOut pb.QPU_SubscribeOpsSyncClient, cancel context.CancelFunc) {
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
			if err := q.updateIndex(streamMsg.GetOperation()); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"op":    streamMsg.Operation,
				}).Fatal("opConsumer: index Update failed")
				return
			}
			log.Debug("QPUServer:index updated, sending ACK")
			if err := streamInOut.Send(&pb.OpAckStream{Msg: "ack", OpId: streamMsg.GetOperation().GetOpId()}); err != nil {
				log.Fatal("opConsumerSync stream.Send failed")
				return
			}
		}
	}
}

//Receives a stream of objects stored in the data store, as a result of a catch-up operation
//Updates the index for each object
func (q *IQPU) catchUpConsumer(streamIn pb.QPU_GetSnapshotClient, errs chan error) {
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
		if err := q.updateIndex(op); err != nil {
			errs <- err
			return
		}
	}
}

//---------------- Internal Functions --------------

//Creates a new index structure
func new(conf config.IndexConfig) (*Index, error) {
	i := &Index{
		attribute: conf.Attribute,
	}

	attr, _, err := attribute.Attr(conf.Attribute, nil)
	if err != nil {
		return nil, err
	}
	lbound, ubound, err := attr.BoundStrToVal(conf.LBound, conf.UBound)
	if err != nil {
		return &Index{}, errors.New("Bounds in index configuration is not the right datatype")
	}
	i.lbound = lbound
	i.ubound = ubound

	var indexStore Store
	switch conf.IndexStore.Store {
	case "antidote":
		indexStore, err = antidoteIndex.New(conf.IndexType, conf.IndexStore.Host, conf.IndexStore.Port, conf.IndexStore.Bucket)
		if err != nil {
			return nil, err
		}
	case "inMem":
		indexStore, err = inMemIndex.New(conf.IndexType)
		if err != nil {
			return nil, err
		}
	}
	i.indexstore = indexStore
	return i, nil
}

//Given an operation sent from the data store, updates the index
func (q *IQPU) updateIndex(op *pbQPU.Operation) error {
	log.WithFields(log.Fields{
		"operation": op,
	}).Debug("index: update")

	return q.index.indexstore.Update(op, q.index.attribute, q.index.lbound, q.index.ubound)
}

//Performs an index catch-up  operation, initiating an object stream from the data store
func (q *IQPU) indexCatchUp(conns utils.DownwardConns) error {
	for _, db := range conns.DBs {
		for _, r := range db.DCs {
			for _, sh := range r.Shards {
				errs := make([]chan error, len(sh.QPUs))
				for i := range sh.QPUs {
					errs[i] = make(chan error)
				}
				for i, c := range sh.QPUs {
					streamIn, cancel, err := c.Client.GetSnapshot(time.Now().UnixNano())
					defer cancel()
					if err != nil {
						return err
					}
					go q.catchUpConsumer(streamIn, errs[i])
				}
				for i := range sh.QPUs {
					err := <-errs[i]
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
