package testutils

import (
	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
)

// ObjectLogOp ...
func ObjectLogOp(key, buck, attrK string, attrV *qpu.Value) *qpu.LogOperation {
	return &qpu.LogOperation{
		ObjectId: key,
		Bucket:   buck,
		Payload: protoutils.PayloadState(
			protoutils.ObjectState([]*qpu.Attribute{
				protoutils.Attribute(attrK, attrV),
			})),
	}
}

// ObjectState ...
func ObjectState(key, buck, attrK string, attrV *qpu.Value) utils.ObjectState {
	return utils.ObjectState{
		ObjectID: key,
		Bucket:   buck,
		State: *protoutils.ObjectState([]*qpu.Attribute{
			protoutils.Attribute(attrK, attrV),
		}),
		Timestamp: *protoutils.Vectorclock(map[string]uint64{"1": 10, "2": 20}),
	}
}
