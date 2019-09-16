package protoutils

import (
	antidote "github.com/dvasilas/proteus/src/protos/antidote"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
)

// ResponseStreamRecord ...
func ResponseStreamRecord(sID int64, t pbQPU.ResponseStreamRecord_StreamRecordType, logOp *pbUtils.LogOperation) *pbQPU.ResponseStreamRecord {
	return &pbQPU.ResponseStreamRecord{
		SequenceId: sID,
		Type:       t,
		LogOp:      logOp,
	}
}

//LogOperation creates a protos/utils/LogOperation object
func LogOperation(key, buck string, t pbUtils.LogOperation_ObjectType, ts *pbUtils.Vectorclock, payload *pbUtils.Payload) *pbUtils.LogOperation {
	return &pbUtils.LogOperation{
		ObjectId:   key,
		ObjectType: t,
		Bucket:     buck,
		Timestamp:  ts,
		Payload:    payload,
	}
}

// PayloadState ...
func PayloadState(state *pbUtils.ObjectState) *pbUtils.Payload {
	return &pbUtils.Payload{
		Val: &pbUtils.Payload_State{
			State: state,
		},
	}
}

// PayloadOp ...
func PayloadOp(attr []*pbUtils.Attribute, updates []*pbUtils.Operation_Update) *pbUtils.Payload {
	op := &pbUtils.Operation{Op: make([]*pbUtils.Operation_Op, 0)}
	for i := range updates {
		op.Op = append(op.Op, &pbUtils.Operation_Op{
			Attr:   attr[i],
			Update: updates[i],
		})
	}
	return &pbUtils.Payload{
		Val: &pbUtils.Payload_Op{
			Op: op,
		},
	}
}

// PayloadDelta ...
func PayloadDelta(old *pbUtils.ObjectState, new *pbUtils.ObjectState) *pbUtils.Payload {
	return &pbUtils.Payload{
		Val: &pbUtils.Payload_Delta{
			Delta: &pbUtils.Payload_StateDelta{
				Old: old,
				New: new,
			},
		},
	}
}

// Update ...
func Update(opT string, val *pbUtils.Value) *pbUtils.Operation_Update {
	return &pbUtils.Operation_Update{
		OpType: opT,
		Value:  val,
	}
}

//ObjectState creates a protos/qpu/State object
func ObjectState(attrs []*pbUtils.Attribute) *pbUtils.ObjectState {
	objState := &pbUtils.ObjectState{
		Attrs: make([]*pbUtils.Attribute, len(attrs)),
	}
	for i, attr := range attrs {
		objState.Attrs[i] = attr
	}
	return objState
}

//Attribute ...
func Attribute(key string, typ pbUtils.Attribute_AttributeType, val *pbUtils.Value) *pbUtils.Attribute {
	return &pbUtils.Attribute{
		AttrKey:  key,
		AttrType: typ,
		Value:    val,
	}
}

//RequestStreamRequest creates a protos/qpu/RequestStream{Request} object
func RequestStreamRequest(ts *pbUtils.SnapshotTimePredicate, predicate []*pbUtils.AttributePredicate, sync bool) *pbQPU.RequestStream {
	return &pbQPU.RequestStream{
		Payload: &pbQPU.RequestStream_Request{
			Request: QueryRequest(ts, predicate, sync),
		},
	}
}

//RequestStreamAck creates a protos/qpu/RequestStream{Ack} object
func RequestStreamAck(sID int64) *pbQPU.RequestStream {
	return &pbQPU.RequestStream{
		Payload: &pbQPU.RequestStream_Ack{
			Ack: &pbQPU.AckMsg{
				SequenceId: sID,
			},
		},
	}
}

// RequestStreamPing ...
func RequestStreamPing(sID int64) *pbQPU.RequestStream {
	return &pbQPU.RequestStream{
		Payload: &pbQPU.RequestStream_Ping{
			Ping: &pbQPU.PingMsg{
				SeqId: sID,
			},
		},
	}
}

//QueryRequest ...
func QueryRequest(ts *pbUtils.SnapshotTimePredicate, predicate []*pbUtils.AttributePredicate, sync bool) *pbQPU.QueryRequest {
	return &pbQPU.QueryRequest{
		Clock:     ts,
		Predicate: predicate,
		Sync:      sync,
	}
}

//ConfigRequest creates a protos/qpu/ConfigRequest object
func ConfigRequest() *pbQPU.ConfigRequest {
	return &pbQPU.ConfigRequest{}
}

//ConfigRespοnse ...
func ConfigRespοnse(typ pbQPU.ConfigResponse_QPUType, attrs []*pbUtils.AttributePredicate, ds *pbQPU.DataSet) *pbQPU.ConfigResponse {
	return &pbQPU.ConfigResponse{
		QpuType:          typ,
		SupportedQueries: attrs,
		Dataset:          ds,
	}
}

//DataSet creates a protos/utils/Dataset object
func DataSet(dbMap map[string]map[string][]string) *pbQPU.DataSet {
	dbs := make(map[string]*pbQPU.DataSet_DB)
	for dbID, dcMap := range dbMap {
		dcs := make(map[string]*pbQPU.DataSet_DC)
		for dcID, shards := range dcMap {
			dcs[dcID] = &pbQPU.DataSet_DC{Shards: shards}
		}
		dbs[dbID] = &pbQPU.DataSet_DB{Datacenters: dcs}
	}
	return &pbQPU.DataSet{Databases: dbs}
}

//AttributePredicate create a protos/utils/AttributePredicate object
func AttributePredicate(attr *pbUtils.Attribute, lb *pbUtils.Value, ub *pbUtils.Value) *pbUtils.AttributePredicate {
	return &pbUtils.AttributePredicate{
		Attr:   attr,
		Lbound: lb,
		Ubound: ub,
	}
}

//SnapshotTimePredicate create a protos/utils/SnapshotTimePredicate object
func SnapshotTimePredicate(lb *pbUtils.SnapshotTime, ub *pbUtils.SnapshotTime) *pbUtils.SnapshotTimePredicate {
	return &pbUtils.SnapshotTimePredicate{
		Lbound: lb,
		Ubound: ub,
	}
}

//SnapshotTime creates a protos/utils/SnapshotTime object
func SnapshotTime(t pbUtils.SnapshotTime_SnapshotTimeType, vc *pbUtils.Vectorclock) *pbUtils.SnapshotTime {
	return &pbUtils.SnapshotTime{
		Type:  t,
		Value: vc,
	}
}

//Vectorclock creates a protos/utils/Vectorclock object
func Vectorclock(vc map[string]uint64) *pbUtils.Vectorclock {
	return &pbUtils.Vectorclock{
		Vc: vc,
	}
}

//SubRequestAntidote creates a protos/qpu/antidote/SubRequest object
func SubRequestAntidote(ts int64) *antidote.SubRequest {
	return &antidote.SubRequest{
		Timestamp: ts,
	}
}

//ValueInt creates a protos/utils/Value{Int} object
func ValueInt(i int64) *pbUtils.Value {
	return &pbUtils.Value{
		Val: &pbUtils.Value_Int{
			Int: i,
		},
	}
}

//ValueStr creates a protos/utils/Value{Str} object
func ValueStr(s string) *pbUtils.Value {
	return &pbUtils.Value{
		Val: &pbUtils.Value_Str{
			Str: s,
		},
	}
}

//ValueFlt creates a protos/utils/Value{Flt} object
func ValueFlt(f float64) *pbUtils.Value {
	return &pbUtils.Value{
		Val: &pbUtils.Value_Flt{
			Flt: f,
		},
	}
}
