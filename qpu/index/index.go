package index

import (
	"context"
	"errors"
	"io"
	"time"

	utils "github.com/dvasilas/proteus"
	attribute "github.com/dvasilas/proteus/attributes"
	"github.com/dvasilas/proteus/config"
	"github.com/dvasilas/proteus/protos"
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
					emptyPred := make([]*pbQPU.AttributePredicate, 0)
					streamIn, cancel, err := q.Client.Query(emptyPred, protoutils.TimestampPredicate(time.Now().UnixNano(), time.Now().UnixNano()), true, sync)
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

//Query implements the Query API for the index QPU
func (q *IQPU) Query(streamOut pb.QPU_QueryServer, conns utils.DownwardConns) error {
	msg, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}
	req := msg.GetRequest()

	if req.GetOps() {
		return errors.New("not supported")
	}

	log.WithFields(log.Fields{
		"query": req.GetPredicate(),
	}).Debug("Index lookup")
	indexResult, found, err := q.index.indexstore.Lookup(req.GetPredicate())
	if !found {
		if err := streamOut.Send(protoutils.QueryResponseStreamState(&pbQPU.Object{Key: "noResults"}, nil)); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	for _, item := range indexResult {
		if err := streamOut.Send(protoutils.QueryResponseStreamState(&item.Object, &item.Dataset)); err != nil {
			return err
		}
	}
	return nil
}

//Cleanup ...
func (q *IQPU) Cleanup() {
	log.Info("index QPU cleanup")
	q.cancel()
}

//----------- Stream Consumer Functions ------------

//Receives an stream of write operations
//Updates the index for each operation
//TODO: Query a way to handle an error here
func (q *IQPU) opConsumer(stream pb.QPU_QueryClient, cancel context.CancelFunc, sync bool) {
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.Fatal("opConsumer err", err)
			return
		} else {
			if streamMsg.GetOperation().GetOpId() == "no_op" {
				continue
			}
			log.WithFields(log.Fields{
				"operation": streamMsg.GetOperation(),
			}).Debug("index QPU received operation")

			if err := q.updateIndex(streamMsg.GetOperation()); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"op":    streamMsg.GetOperation(),
				}).Fatal("opConsumer: index Update failed")
				return
			}
			if sync {
				log.Debug("QPUServer:index updated, sending ACK")
				if err := stream.Send(protoutils.RequestStreamAck("ack", streamMsg.GetOperation().GetOpId())); err != nil {
					log.Fatal("opConsumer stream.Send failed")
					return
				}
			}
		}
	}
}

func (q *IQPU) stateToOp(pred []*pbQPU.AttributePredicate, streamMsg *pb.QueryResponseStream, stream pb.QPU_QueryServer) error {
	op := protoutils.OperationState(
		"catchUp",
		streamMsg.GetState().GetObject(),
		streamMsg.GetState().GetDataset(),
	)
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
					emptyPred := make([]*pbQPU.AttributePredicate, 0)
					streamIn, cancel, err := c.Client.Query(emptyPred, protoutils.TimestampPredicate(0, time.Now().UnixNano()), false, false)
					defer cancel()
					if err != nil {
						return err
					}
					go utils.QueryResponseConsumer(emptyPred, streamIn, nil, errs[i], q.stateToOp)
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
