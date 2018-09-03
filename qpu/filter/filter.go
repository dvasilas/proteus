package filter

import (
	pb "github.com/dimitriosvasilas/modqp/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	log "github.com/sirupsen/logrus"
)

func match(obj *pbQPU.Object) bool {
	log.WithFields(log.Fields{
		"Object": obj,
	}).Debug("Object matches query")
	return true
}

func noMatch(obj *pbQPU.Object) bool {
	log.WithFields(log.Fields{
		"Object": obj,
	}).Debug("Object does not match query")
	return false
}

//Forward sends an object through an upstream stream if it matches the given predicate.
//It returns any error encountered.
func Forward(obj *pbQPU.Object, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	if Filter(obj, pred) {
		return stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp}})
	}
	return nil
}

//Filter examines whether an object satisfies a given predicate.
//It returns a boolean indicated whether the predicate is satisfied.
func Filter(obj *pbQPU.Object, predicate []*pbQPU.Predicate) bool {
	log.SetLevel(log.DebugLevel)

	for _, pred := range predicate {
		switch pred.Lbound.Val.(type) {
		case *pbQPU.Value_Int:
			if obj.Attributes[pred.Attribute].GetInt() < pred.Lbound.GetInt() || obj.Attributes[pred.Attribute].GetInt() > pred.Ubound.GetInt() {
				return noMatch(obj)
			}
		case *pbQPU.Value_Name:
			if pred.Attribute == "key" {
				if obj.Key != pred.Lbound.GetName() {
					return noMatch(obj)
				}
			}
		default:
			return noMatch(obj)
		}
	}
	return match(obj)
}
