package partitionΜanager

import (
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus"
	"github.com/dvasilas/proteus/protos"
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

//Query implements the Query API for the partition manager QPU
func (q *PmQPU) Query(streamOut pb.QPU_QueryServer, conns utils.DownwardConns) error {
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

	clients, err := q.forwardQuery(req.GetPredicate())
	if err != nil {
		return err
	}
	errs := make([]chan error, len(clients))
	for i := range clients {
		errs[i] = make(chan error)
	}
	for i, c := range clients {

		streamIn, _, err := c.Query(req.GetPredicate(), req.GetClock(), false, false)
		if err != nil {
			return err
		}
		go utils.QueryResponseConsumer(req.GetPredicate(), streamIn, streamOut, errs[i], forwardResponse)

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

//Cleanup ...
func (q *PmQPU) Cleanup() {
	log.Info("partition_manager QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//---------------- Internal Functions --------------

//Sends an object received from the input stream as part of query results to the output stream corresponding to this query.
func forwardResponse(pred []*pbQPU.AttributePredicate, streamMsg *pb.QueryResponseStream, stream pb.QPU_QueryServer) error {
	return stream.Send(protoutils.QueryResponseStreamState(streamMsg.GetState().GetObject(), streamMsg.GetState().GetDataset()))
}

//forwardQuery selects a set of downward connections for forwarding a query, based on the available QPUs and their configuration.
//Returns an array connections for initiating Query queries, and any error encountered.
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
