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
func Forward(obj *pbQPU.Object, ds *pbQPU.DataSet, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	if Filter(obj, pred) {
		return stream.Send(&pb.QueryResultStream{
			Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
			Dataset: ds,
		})
	}
	return nil
}

//Filter examines whether an object satisfies a given query.
//It returns a boolean indicated whether the query is satisfied.
func Filter(obj *pbQPU.Object, query []*pbQPU.Predicate) bool {
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
