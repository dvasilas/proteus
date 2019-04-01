package index

import (
	"context"
	"errors"
	"io"
	"time"

	utils "github.com/dvasilas/proteus"
	attribute "github.com/dvasilas/proteus/attributes"
	"github.com/dvasilas/proteus/config"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	antidoteIndex "github.com/dvasilas/proteus/qpu/index/antidote"
	inMemIndex "github.com/dvasilas/proteus/qpu/index/inMem"
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
	Lookup(p []*pbQPU.AttributePredicate) (map[string]utils.Posting, bool, error)
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
					sync := false
					if conf.ConsLevel == "sync" {
						sync = true
					}
					streamIn, cancel, err := q.Client.SubscribeOps(&pbQPU.TimestampPredicate{Lbound: &pbQPU.Timestamp{Ts: time.Now().UnixNano()}}, sync)
					if err != nil {
						cancel()
						return nil, err
					}
					qpu.cancel = cancel
					go qpu.opConsumer(streamIn, cancel, sync)
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
		if err := streamOut.Send(&pb.FindResponseStream{Object: &pbQPU.Object{Key: "noResults"}}); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	for _, item := range indexResult {
		if err := streamOut.Send(&pb.FindResponseStream{Object: &item.Object, Dataset: &item.Dataset}); err != nil {
			return err
		}
	}
	return nil
}

//SubscribeOps ...
func (q *IQPU) SubscribeOps(stream pb.QPU_SubscribeOpsServer) error {
	return errors.New("index QPU does not support SubscribeOps()")
}

//Cleanup ...
func (q *IQPU) Cleanup() {
	log.Info("index QPU cleanup")
	q.cancel()
}

//----------- Stream Consumer Functions ------------

//Receives an stream of write operations
//Updates the index for each operation
//TODO: Find a way to handle an error here
func (q *IQPU) opConsumer(stream pb.QPU_SubscribeOpsClient, cancel context.CancelFunc, sync bool) {
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.Fatal("opConsumer err", err)
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
			if sync {
				log.Debug("QPUServer:index updated, sending ACK")
				if err := stream.Send(&pb.ReqStream{Payload: &pb.ReqStream_Ack{Ack: &pb.AckMsg{Msg: "ack", OpId: streamMsg.GetOperation().GetOpId()}}}); err != nil {
					log.Fatal("opConsumer stream.Send failed")
					return
				}
			}
		}
	}
}

func (q *IQPU) stateToOp(pred []*pbQPU.AttributePredicate, streamMsg *pb.FindResponseStream, stream pb.QPU_FindServer) error {
	op := &pbQPU.Operation{
		OpId:      "catchUp",
		OpPayload: &pbQPU.OperationPayload{Payload: &pbQPU.OperationPayload_State{State: streamMsg.GetObject()}},
		DataSet:   streamMsg.GetDataset(),
	}
	return q.updateIndex(op)
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
					pred := make([]*pbQPU.AttributePredicate, 0)
					streamIn, cancel, err := c.Client.Find(&pbQPU.TimestampPredicate{Lbound: &pbQPU.Timestamp{Ts: time.Now().UnixNano()}}, pred)
					defer cancel()
					if err != nil {
						return err
					}
					go utils.FindResponseConsumer(pred, streamIn, nil, errs[i], q.stateToOp)
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
