package dispatch

import (
	"errors"

	utils "github.com/dimitriosvasilas/modqp"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

//ForwardResponse sends an object received from downstream as part of query results to an upward stream corresponding to this query.
//It returns any error encountered.
func ForwardResponse(obj *pbQPU.Object, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	return stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp}})
}

//ForwardQuery selects an appropriate downstream for forwarding a query, based on the available QPUs and their configuration.
//It returns a client object of the selected downstream QPU, and any error encountered.
func ForwardQuery(conns []utils.DownwardConn, pred pbQPU.Predicate) (cli.Client, error) {
	for _, c := range conns {
		if (c.QpuType == "index" || c.QpuType == "cache") && canProcessQuery(c, pred) {
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
	return cli.Client{}, errors.New("dispatch found no QPU to forward query")
}

func predicateInAttrRange(conn utils.DownwardConn, pred pbQPU.Predicate) bool {
	switch conn.Lbound.Val.(type) {
	case *pbQPU.Value_Int:
		if pred.Lbound.GetInt() >= conn.Lbound.GetInt() && pred.Ubound.GetInt() <= conn.Ubound.GetInt() {
			return true
		}
	case *pbQPU.Value_Name:
		if conn.Ubound.GetName() == "any" {
			return true
		}
	}
	return false
}

func canProcessQuery(conn utils.DownwardConn, pred pbQPU.Predicate) bool {
	if conn.Attribute == pred.Attribute || conn.Attribute == "any" {
		return true
	}
	return false
}
