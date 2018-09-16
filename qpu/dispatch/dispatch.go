package dispatch

import (
	"errors"
	"fmt"

	utils "github.com/dimitriosvasilas/modqp"
	pb "github.com/dimitriosvasilas/modqp/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
)

//ForwardResponse sends an object received from downstream as part of query results to an upward stream corresponding to this query.
//It returns any error encountered.
func ForwardResponse(obj *pbQPU.Object, ds *pbQPU.DataSet, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	return stream.Send(&pb.QueryResultStream{
		Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
		Dataset: ds,
	})
}

//ForwardQuery selects an appropriate downstream for forwarding a query, based on the available QPUs and their configuration.
//It returns a client object of the selected downstream QPU, and any error encountered.
func ForwardQuery(conns utils.DownwardConns, pred pbQPU.Predicate) ([]cli.Client, error) {
	forwardTo := make([]cli.Client, 0)
	for _, db := range conns.DBs {
		for _, r := range db.Replicas {
			for _, sh := range r.Shards {
				for _, q := range sh.QPUs {
					fmt.Println(q)
					if (q.QpuType == "index" || q.QpuType == "cache") && canProcessQuery(q, pred) {
						if predicateInAttrRange(q, pred) {
							forwardTo = append(forwardTo, q.Client)
						}
					}
				}
				for _, q := range sh.QPUs {
					if q.QpuType == "scan" {
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

func predicateInAttrRange(conn utils.QPUConn, pred pbQPU.Predicate) bool {
	switch conn.Lbound.Val.(type) {
	case *pbQPU.Value_Int:
		if pred.Lbound.GetInt() > conn.Ubound.GetInt() || pred.Ubound.GetInt() < conn.Lbound.GetInt() {
			fmt.Println("predicateInAttrRange Not OK")
			return false
		}
		return true
	case *pbQPU.Value_Name:
		if conn.Ubound.GetName() == "any" {
			return true
		}
	}
	return false
}

func canProcessQuery(conn utils.QPUConn, pred pbQPU.Predicate) bool {
	if conn.Attribute == pred.Attribute || conn.Attribute == "any" {
		return true
	}
	return false
}
