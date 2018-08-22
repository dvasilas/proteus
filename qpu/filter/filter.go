package filter

import (
	"fmt"

	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/fatih/color"
)

func match(obj *pbQPU.Object) (bool, error) {
	//demo
	color.Set(color.FgGreen)
	defer color.Unset()
	fmt.Println(obj)
	//
	return true, nil
}

func noMatch(obj *pbQPU.Object) (bool, error) {
	//demo
	color.Set(color.FgRed)
	defer color.Unset()
	fmt.Println(obj)
	//
	return false, nil
}

//Forward ...
func Forward(obj *pbQPU.Object, pred []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	f, err := Filter(obj, pred)
	if err != nil {
		return nil
	}
	if f {
		stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp}})
	}
	return nil
}

//Filter ...
func Filter(obj *pbQPU.Object, predicate []*pbQPU.Predicate) (bool, error) {
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
