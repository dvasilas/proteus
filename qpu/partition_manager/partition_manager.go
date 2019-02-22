package partitionΜanager

import (
	"errors"
	"time"

	"github.com/dimitriosvasilas/proteus"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	cli "github.com/dimitriosvasilas/proteus/qpu/client"
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
	pred := make([]cli.Predicate, 0)
	pred = append(pred, cli.Predicate{
		Attribute: in.Predicate[0].Attribute,
		Datatype:  in.Predicate[0].Datatype,
		LBound:    in.Predicate[0].Lbound,
		UBound:    in.Predicate[0].Ubound,
	})

	msg := make(chan *pb.QueryResultStream)
	done := make([]chan bool, len(clients))
	errs := make([]chan error, len(clients))
	errsFind := make([]chan error, len(clients))
	for i := range clients {
		done[i] = make(chan bool)
		errs[i] = make(chan error)
		errsFind[i] = make(chan error)
	}
	for i, c := range clients {
		go q.findResultConsumer(in.Predicate, streamOut, msg, done[i], errsFind[i], errs[i], forwardResponse)
		go c.Find(in.Timestamp, pred, msg, done[i], errsFind[i])
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

//GetSnapshot ...
func (q *PmQPU) GetSnapshot(in *pb.SubRequest, stream pb.QPU_GetSnapshotServer) error {
	return errors.New("partition_manager QPU does not support GetSnapshot()")
}

//SubscribeOpsAsync ...
func (q *PmQPU) SubscribeOpsAsync(in *pb.SubRequest, stream pb.QPU_SubscribeOpsAsyncServer) error {
	return errors.New("partition_manager QPU does not support SubscribeOpsAsync()")
}

//SubscribeOpsSync ...
func (q *PmQPU) SubscribeOpsSync(stream pb.QPU_SubscribeOpsSyncServer) error {
	return errors.New("partition_manager QPU does not support SubscribeOpsSync()")
}

//Cleanup ...
func (q *PmQPU) Cleanup() {
	log.Info("partition_manager QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//Receives stream of query results and forwards upwards
func (q *PmQPU) findResultConsumer(pred []*pbQPU.Predicate, streamOut pb.QPU_FindServer, msg chan *pb.QueryResultStream, done chan bool, errFind chan error, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errFind
			errs <- err
		}
		streamMsg := <-msg
		if err := process(streamMsg.GetObject(), streamMsg.GetDataset(), pred, streamOut); err != nil {
			errs <- err
		}
	}
}

//---------------- Internal Functions --------------

//Sends an object received from the input stream as part of query results to the output stream corresponding to this query.
func forwardResponse(obj *pbQPU.Object, ds *pbQPU.DataSet, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	return stream.Send(&pb.QueryResultStream{
		Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
		Dataset: ds,
	})
}

//forwardQuery selects a set of downward connections for forwarding a query, based on the available QPUs and their configuration.
//Returns an array connections for initiating Find queries, and any error encountered.
func (q *PmQPU) forwardQuery(query []*pbQPU.Predicate) ([]cli.Client, error) {
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
