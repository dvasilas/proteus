package protoutils

import (
	antidote "github.com/dvasilas/proteus/protos/antidote"
	pbQPU "github.com/dvasilas/proteus/protos/qpu"
	s3 "github.com/dvasilas/proteus/protos/s3"
	pb "github.com/dvasilas/proteus/protos/utils"
)

//QueryResponseStreamState creates a protos/qpu/QueryResponseStream{State} object
func QueryResponseStreamState(obj *pb.Object, ds *pb.DataSet) *pbQPU.QueryResponseStream {
	return &pbQPU.QueryResponseStream{
		Payload: &pbQPU.QueryResponseStream_State{
			State: State(obj, ds),
		},
	}
}

//QueryResponseStreamOperation a protos/qpu/QueryResponseStream{Operation} object
func QueryResponseStreamOperation(op *pb.Operation) *pbQPU.QueryResponseStream {
	return &pbQPU.QueryResponseStream{
		Payload: &pbQPU.QueryResponseStream_Operation{
			Operation: op,
		},
	}
}

//State creates a protos/qpu/State object
func State(obj *pb.Object, ds *pb.DataSet) *pbQPU.State {
	return &pbQPU.State{
		Object:  obj,
		Dataset: ds,
	}
}

//RequestStreamRequest creates a protos/qpu/RequestStream{Request} object
func RequestStreamRequest(ts *pb.TimestampPredicate, predicate []*pb.AttributePredicate, ops bool, sync bool) *pbQPU.RequestStream {
	return &pbQPU.RequestStream{
		Payload: &pbQPU.RequestStream_Request{
			Request: &pbQPU.QueryRequest{
				Timestamp: ts,
				Predicate: predicate,
				Ops:       ops,
				Sync:      sync,
			},
		},
	}
}

//RequestStreamAck creates a protos/qpu/RequestStream{Ack} object
func RequestStreamAck(msg string, opID string) *pbQPU.RequestStream {
	return &pbQPU.RequestStream{
		Payload: &pbQPU.RequestStream_Ack{
			Ack: &pbQPU.AckMsg{
				Msg:  msg,
				OpId: opID,
			},
		},
	}
}

//ConfigRequest creates a protos/qpu/ConfigRequest object
func ConfigRequest() *pbQPU.ConfigRequest {
	return &pbQPU.ConfigRequest{}
}

//Object creates a protos/utils/Object object
func Object(key string, attrs map[string]*pb.Value) *pb.Object {
	return &pb.Object{
		Key:        key,
		Attributes: attrs,
	}
}

//DataSet creates a protos/utils/Dataset object
func DataSet(db string, dc string, shard string) *pb.DataSet {
	return &pb.DataSet{
		Db:    db,
		Dc:    dc,
		Shard: shard,
	}
}

//AttributePredicate create a protos/utils/AttributePredicate object
func AttributePredicate(datatype string, attribute string, lb *pb.Value, ub *pb.Value) *pb.AttributePredicate {
	return &pb.AttributePredicate{
		Datatype:  datatype,
		Attribute: attribute,
		Lbound:    lb,
		Ubound:    ub,
	}
}

//OperationOp creates a protos/utils/Operation{Op} object
func OperationOp(OpID string, key string, bucket string, attrKey string, attrType string, payload string, ds *pb.DataSet) *pb.Operation {
	return &pb.Operation{
		OpId:   OpID,
		Key:    key,
		Bucket: bucket,
		OpPayload: &pb.OperationPayload{
			Payload: &pb.OperationPayload_Op{
				Op: &pb.Op{
					AttrKey:  attrKey,
					AttrType: attrType,
					Payload:  payload,
				},
			},
		},
		DataSet: ds,
	}
}

//OperationState creates a protos/utils/Operation{State} object
func OperationState(OpID string, obj *pb.Object, ds *pb.DataSet) *pb.Operation {
	return &pb.Operation{
		OpId: OpID,
		OpPayload: &pb.OperationPayload{
			Payload: &pb.OperationPayload_State{
				State: obj,
			},
		},
		DataSet: ds,
	}
}

//TimestampPredicate creates a protos/utils/TimestampPredicate object
func TimestampPredicate(lb int64, ub int64) *pb.TimestampPredicate {
	return &pb.TimestampPredicate{
		Lbound: &pb.Timestamp{
			Ts: lb,
		},
		Ubound: &pb.Timestamp{
			Ts: ub,
		},
	}
}

//SubRequestAntidote creates a protos/qpu/antidote/SubRequest object
func SubRequestAntidote(ts int64) *antidote.SubRequest {
	return &antidote.SubRequest{
		Timestamp: ts,
	}
}

//SubRequestS3 creates a protos/qpu/s3/SubRequest object
func SubRequestS3(ts int64) *s3.SubRequest {
	return &s3.SubRequest{
		Timestamp: ts,
	}
}

//OpAckS3 creates a protos/qpu/s3/OpAck object
func OpAckS3(msg string, opID string) *s3.OpAck {
	return &s3.OpAck{
		Msg:  msg,
		OpId: opID,
	}
}

//ValueInt creates a protos/utils/Value{Int} object
func ValueInt(i int64) *pb.Value {
	return &pb.Value{
		Val: &pb.Value_Int{
			Int: i,
		},
	}
}

//ValueStr creates a protos/utils/Value{Str} object
func ValueStr(s string) *pb.Value {
	return &pb.Value{
		Val: &pb.Value_Str{
			Str: s,
		},
	}
}

//ValueFlt creates a protos/utils/Value{Flt} object
func ValueFlt(f float64) *pb.Value {
	return &pb.Value{
		Val: &pb.Value_Flt{
			Flt: f,
		},
	}
}
