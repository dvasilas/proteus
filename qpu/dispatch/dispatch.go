package dispatch

import (
	"errors"

	utils "github.com/dimitriosvasilas/modqp"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

//ForwardResponse ...
func ForwardResponse(obj *pbQPU.Object, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp}})
	return nil
}

//ForwardQuery ...
func ForwardQuery(conns []utils.DownwardConn, pred pbQPU.Predicate) (cli.Client, error) {
	for _, c := range conns {
		if c.QpuType == "index" && canProcessQuery(c, pred) {
			if predicateInAttrRange(c, pred) {
				return c.Client, nil
			}
		}
	}
	for _, c := range conns {
		if c.QpuType == "scan" {
			return c.Client, nil
		}
	}
	return cli.Client{}, errors.New("dispatchQPU found no QPU to forward query")
}

func predicateInAttrRange(conn utils.DownwardConn, pred pbQPU.Predicate) bool {
	switch conn.Lbound.Val.(type) {
	case *pbQPU.Value_Int:
		if pred.Lbound.GetInt() >= conn.Lbound.GetInt() && pred.Ubound.GetInt() <= conn.Ubound.GetInt() {
			return true
		}
	case *pbQPU.Value_Name:
	}
	return false
}

func canProcessQuery(conn utils.DownwardConn, pred pbQPU.Predicate) bool {
	if conn.Attribute == pred.Attribute || pred.Attribute == "any" {
		return true
	}
	return false
}
