package partitionΜanager

import (
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	cli "github.com/dvasilas/proteus/qpu/client"
	log "github.com/sirupsen/logrus"
)

//PmQPU implements a partition manager QPU
type PmQPU struct {
	conns map[string]*utils.Shard
}

//---------------- API Functions -------------------

//QPU creates a partition manager QPU
func QPU(downwardsConns utils.DownwardConns) (*PmQPU, error) {
	if len(downwardsConns.DBs) > 1 {
		return &PmQPU{}, errors.New("a partition_manager QPU should be connected only to one DB")
	}
	for _, db := range downwardsConns.DBs {
		if len(db.DCs) > 1 {
			return &PmQPU{}, errors.New("a partition_manager QPU should be connected only to one DC")
		}
		for _, r := range db.DCs {
			return &PmQPU{
				conns: r.Shards,
			}, nil
		}
	}
	return &PmQPU{}, errors.New("error in DB/DC/Shard configuration")
}

//Find implements the Find API for the partition manager QPU
func (q *PmQPU) Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error {
	clients, err := q.forwardQuery(in.Predicate)
	if err != nil {
		return err
	}
	errs := make([]chan error, len(clients))
	for i := range clients {
		errs[i] = make(chan error)
	}
	for i, c := range clients {

		streamIn, _, err := c.Find(in.Timestamp, in.Predicate)
		if err != nil {
			return err
		}
		go q.findResultConsumer(in.Predicate, streamIn, streamOut, errs[i], forwardResponse)

		time.Sleep(time.Millisecond * 100)
	}
	for _, e := range errs {
		err = <-e
		if err != nil {
			return err
		}
	}
	return nil
}

//SubscribeOps ...
func (q *PmQPU) SubscribeOps(stream pb.QPU_SubscribeOpsServer) error {
	return errors.New("partition_manager QPU does not support SubscribeOps()")
}

//Cleanup ...
func (q *PmQPU) Cleanup() {
	log.Info("partition_manager QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//Receives stream of query results and forwards upwards
func (q *PmQPU) findResultConsumer(pred []*pbQPU.AttributePredicate, streamIn pb.QPU_FindClient, streamOut pb.QPU_FindServer, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.AttributePredicate, pb.QPU_FindServer) error) {
	for {
		streamMsg, err := streamIn.Recv()
		if err == io.EOF {
			errs <- nil
			return
		} else if err != nil {
			errs <- err
			return
		}
		if err = process(streamMsg.GetObject(), streamMsg.GetDataset(), pred, streamOut); err != nil {
			errs <- err
			return
		}
	}
}

//---------------- Internal Functions --------------

//Sends an object received from the input stream as part of query results to the output stream corresponding to this query.
func forwardResponse(obj *pbQPU.Object, ds *pbQPU.DataSet, pred []*pbQPU.AttributePredicate, stream pb.QPU_FindServer) error {
	return stream.Send(&pb.FindResponseStream{
		Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
		Dataset: ds,
	})
}

//forwardQuery selects a set of downward connections for forwarding a query, based on the available QPUs and their configuration.
//Returns an array connections for initiating Find queries, and any error encountered.
func (q *PmQPU) forwardQuery(query []*pbQPU.AttributePredicate) ([]cli.Client, error) {
	forwardTo := make([]cli.Client, 0)
	for _, sh := range q.conns {
		for _, q := range sh.QPUs {
			if (q.QpuType == "index" || q.QpuType == "cache") && utils.CanProcessQuery(q, query) {
				if utils.QueryInAttrRange(q, query) {
					forwardTo = append(forwardTo, q.Client)
				}
			}
		}
		for _, q := range sh.QPUs {
			if q.QpuType == "filter" {
				forwardTo = append(forwardTo, q.Client)
			}
		}
	}
	if len(forwardTo) == 0 {
		return forwardTo, errors.New("partition_manager found no QPU to forward query")
	}
	return forwardTo, nil
}
