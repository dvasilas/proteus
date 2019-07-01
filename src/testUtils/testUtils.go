package testutils

import (
	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/protos"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
)

// ObjectLogOp ...
func ObjectLogOp(key, buck, attrK string, attrT pbUtils.Attribute_AttributeType, attrV *pbUtils.Value) *pbUtils.LogOperation {
	return &pbUtils.LogOperation{
		ObjectId: key,
		Bucket:   buck,
		Payload: protoutils.PayloadState(
			protoutils.ObjectState([]*pbUtils.Attribute{
				protoutils.Attribute(attrK, attrT, attrV),
			})),
	}
}

// ObjectState ...
func ObjectState(key, buck, attrK string, attrT pbUtils.Attribute_AttributeType, attrV *pbUtils.Value) utils.ObjectState {
	return utils.ObjectState{
		ObjectID: key,
		Bucket:   buck,
		State: *protoutils.ObjectState([]*pbUtils.Attribute{
			protoutils.Attribute(attrK, attrT, attrV),
		}),
		Timestamp: *protoutils.Vectorclock(map[string]uint64{"1": 10, "2": 20}),
	}
}
