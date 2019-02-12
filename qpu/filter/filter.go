package filter

import (
	"io"

	utils "github.com/dimitriosvasilas/proteus"
	pbDsQPU "github.com/dimitriosvasilas/proteus/protos/datastore"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
)

//FQPU implements a filter QPU
type FQPU struct{}

//---------------- API Functions -------------------

//QPU creates a filter QPU
func QPU() (*FQPU, error) {
	return &FQPU{}, nil
}

//Find implements the Find API for the filter QPU
func (q *FQPU) Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error {
	errs := make(chan error)
	streamIn, cancel, err := conns.DsConn[0].GetSnapshot(in.Timestamp)
	defer cancel()
	if err != nil {
		return err
	}
	go q.snapshotConsumer(in.Predicate, streamIn, streamOut, errs, forward)
	err = <-errs
	return err
}

//----------- Stream Consumer Functions ------------

//Receives and processes an input stream of objects
func (q *FQPU) snapshotConsumer(pred []*pbQPU.Predicate, streamIn pbDsQPU.DataStore_GetSnapshotClient, streamOut pb.QPU_FindServer, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
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

//Examines whether an object matches a given predicate,
//Returns a boolean accordingly
func filter(obj *pbQPU.Object, query []*pbQPU.Predicate) bool {
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
func forward(obj *pbQPU.Object, ds *pbQPU.DataSet, pred []*pbQPU.Predicate, streamOut pb.QPU_FindServer) error {
	if filter(obj, pred) {
		return streamOut.Send(&pb.QueryResultStream{
			Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
			Dataset: ds,
		})
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
