package partitionΜanager

import (
	"errors"
	"time"

	utils "github.com/dimitriosvasilas/proteus"
	attribute "github.com/dimitriosvasilas/proteus/attributes"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	cli "github.com/dimitriosvasilas/proteus/qpu/client"
)

//PmQPU implements a partition manager QPU
type PmQPU struct {
}

//---------------- API Functions -------------------

//QPU creates a partition manager QPU
func QPU() (*PmQPU, error) {
	return &PmQPU{}, nil
}

//Find implements the Find API for the partition manager QPU
func (q *PmQPU) Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error {
	clients, err := ForwardQuery(conns, in.Predicate)
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

//ForwardQuery selects a set of downward connections for forwarding a query, based on the available QPUs and their configuration.
//Returns an array connections for initiating Find queries, and any error encountered.
func ForwardQuery(conns utils.DownwardConns, query []*pbQPU.Predicate) ([]cli.Client, error) {
	forwardTo := make([]cli.Client, 0)
	for _, db := range conns.DBs {
		for _, r := range db.DCs {
			for _, sh := range r.Shards {
				for _, q := range sh.QPUs {
					if (q.QpuType == "index" || q.QpuType == "cache") && canProcessQuery(q, query) {
						if queryInAttrRange(q, query) {
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
		}
	}
	if len(forwardTo) == 0 {
		return forwardTo, errors.New("dispatch found no QPU to forward query")
	}
	return forwardTo, nil

}

//---------------- Auxiliary Functions -------------

//Checks if given predicate can be satisfied by a QPU based on its querable attribute value bounds
func queryInAttrRange(conn utils.QPUConn, query []*pbQPU.Predicate) bool {
	for _, p := range query {
		switch conn.Lbound.Val.(type) {
		case *pbQPU.Value_Int:
			if p.Lbound.GetInt() > conn.Ubound.GetInt() || p.Ubound.GetInt() < conn.Lbound.GetInt() {
				return false
			}
		case *pbQPU.Value_Str:
			if conn.Ubound.GetStr() != "any" && (p.Lbound.GetStr() > conn.Ubound.GetStr() || p.Ubound.GetStr() < conn.Lbound.GetStr()) {
				return false
			}
		case *pbQPU.Value_Flt:
			if p.Lbound.GetFlt() > conn.Ubound.GetFlt() || p.Ubound.GetFlt() < conn.Lbound.GetFlt() {
				return false
			}
		default:
			return false
		}
	}
	return true
}

//Checks if given predicate can be satisfied by a QPU based on its querable attribute
func canProcessQuery(conn utils.QPUConn, query []*pbQPU.Predicate) bool {
	if conn.Attribute == "any" {
		return true
	}
	for _, p := range query {
		attr, err := attribute.Attr(conn.Attribute, nil)
		if err != nil {
			return false
		}
		if p.Datatype != attr.GetDatatype() {
			return false
		}
	}
	return true
}
