package filter

import (
	"errors"
	"io"

	"github.com/dvasilas/proteus"
	"github.com/dvasilas/proteus/protos"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
)

//FQPU implements a filter QPU
type FQPU struct{}

//---------------- API Functions -------------------

//QPU creates a filter QPU
func QPU() (*FQPU, error) {
	return &FQPU{}, nil
}

//Query implements the Query API for the filter QPU
func (q *FQPU) Query(streamOut pb.QPU_QueryServer, conns utils.DownwardConns) error {
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
	if req.GetClock().GetLbound().GetType() != pbQPU.SnapshotTime_ZERO || req.GetClock().GetUbound().GetType() != pbQPU.SnapshotTime_LATEST {
		return errors.New("not supported")
	}

	for _, db := range conns.DBs {
		for _, r := range db.DCs {
			for _, sh := range r.Shards {
				for _, c := range sh.QPUs {
					errs := make(chan error)
					emptyPred := make([]*pbQPU.AttributePredicate, 0)
					streamIn, _, err := c.Client.Query(emptyPred, req.GetClock(), false, false)
					if err != nil {
						return err
					}
					go utils.QueryResponseConsumer(req.GetPredicate(), streamIn, streamOut, errs, forward)
					err = <-errs
					return err
				}
			}
		}
	}
	return errors.New("filter QPU: Query : should not have reached here")
}

//Cleanup ...
func (q *FQPU) Cleanup() {
	log.Info("filter QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//---------------- Internal Functions --------------

//Examines whether an object matches a given predicate,
//Returns a boolean accordingly
func filter(obj *pbQPU.Object, query []*pbQPU.AttributePredicate) bool {
	for _, q := range query {
		switch q.Lbound.Val.(type) {
		case *pbQPU.Value_Int:
			attrK := "size"
			if q.Attribute != "size" {
				attrK = "x-amz-meta-f-" + q.Attribute
			}
			if attrVal, ok := obj.GetAttributes()[attrK]; ok {
				if attrVal.GetInt() < q.Lbound.GetInt() || attrVal.GetInt() > q.Ubound.GetInt() {
					return noMatch(obj)
				}
			} else {
				return noMatch(obj)
			}
		case *pbQPU.Value_Str:
			if q.Attribute == "key" {
				if obj.GetKey() < q.Lbound.GetStr() || obj.GetKey() > q.Ubound.GetStr() {
					return noMatch(obj)
				}
			} else {
				attrK := "x-amz-meta-f-" + q.Attribute
				if attrVal, ok := obj.GetAttributes()[attrK]; ok {
					if attrVal.GetStr() < q.Lbound.GetStr() || attrVal.GetStr() > q.Ubound.GetStr() {
						return noMatch(obj)
					}
				} else {
					return noMatch(obj)
				}
			}
		case *pbQPU.Value_Flt:
			attrK := "x-amz-meta-f-" + q.Attribute
			if attrVal, ok := obj.GetAttributes()[attrK]; ok {
				if attrVal.GetFlt() < q.Lbound.GetFlt() || attrVal.GetFlt() > q.Ubound.GetFlt() {
					return noMatch(obj)
				}
			} else {
				return noMatch(obj)
			}
		default:
			return noMatch(obj)
		}
	}
	return match(obj)
}

//Sends an object through an upward stream, if the object matches the given predicate
func forward(pred []*pbQPU.AttributePredicate, streamMsg *pb.QueryResponseStream, streamOut pb.QPU_QueryServer) error {
	log.WithFields(log.Fields{
		"elemnt": streamMsg,
	}).Debug("filterQPU: received input stream element")

	if filter(streamMsg.GetState().GetObject(), pred) {
		return streamOut.Send(protoutils.QueryResponseStreamState(streamMsg.GetState().GetObject(), streamMsg.GetState().GetDataset()))
	}
	return nil
}

//---------------- Auxiliary Functions -------------

//Performs an action when an object matches a predicate
func match(obj *pbQPU.Object) bool {
	log.WithFields(log.Fields{
		"Object": obj,
	}).Debug("Object matches query")
	return true
}

//Performs an action when an object does not match a predicate
func noMatch(obj *pbQPU.Object) bool {
	log.WithFields(log.Fields{
		"Object": obj,
	}).Debug("Object does not match query")
	return false
}
