package libqpu

import (
	"errors"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
)

// Assert ...
func Assert(cond bool, msg string) {
	if !cond {
		panic(errors.New(msg))
	}
}

// Error logs the given error message and the stack trace,
// and returns an error.
func Error(msg string) error {
	log.Error(msg)
	debug.PrintStack()
	return errors.New(msg)
}

// Trace is a wrapper for printing trace messaging
// Requires the qpu service to be launched with trace logging level (-l trace)
func Trace(msg string, fields map[string]interface{}) {
	logFields := log.Fields{}
	for k, v := range fields {
		logFields[k] = v
	}
	log.WithFields(logFields).Trace(msg)
}

func LogError(err error) {
	log.Error(err)
}

// import (
// 	"github.com/dvasilas/proteus/internal/proto/antidote"
// 	"github.com/dvasilas/proteus/internal/proto/qpu"
// 	"github.com/dvasilas/proteus/internal/proto/qpu_api"
// )

// // EndOfStream ...
// func EndOfStream(seqID int64) ResponseRecord {
// 	return ResponseRecord{
// 		Rec: &qpu_api.ResponseStreamRecord{
// 			SequenceId: seqID,
// 			Type:       qpu_api.ResponseStreamRecord_END_OF_STREAM,
// 		},
// 	}
// }

// // ResponseStreamRecord ...
// func ResponseStreamRecord(seqID int64, t qpu_api.ResponseStreamRecord_StreamRecordType, logOp *qpu.LogOperation) *qpu_api.ResponseStreamRecord {
// 	return &qpu_api.ResponseStreamRecord{
// 		SequenceId: seqID,
// 		Type:       t,
// 		LogOp:      logOp,
// 	}
// }

// //LogOperation creates a protos/utils/LogOperation object
// func LogOperation(key, buck string, ts *qpu.Vectorclock, payload *qpu.Payload) *qpu.LogOperation {
// 	return &qpu.LogOperation{
// 		ObjectId:  key,
// 		Bucket:    buck,
// 		Timestamp: ts,
// 		Payload:   payload,
// 	}
// }

// // PayloadState ...
// func PayloadState(state *qpu.ObjectState) *qpu.Payload {
// 	return &qpu.Payload{
// 		Val: &qpu.Payload_State{
// 			State: state,
// 		},
// 	}
// }

// // PayloadOp ...
// func PayloadOp(attr []*qpu.Attribute, updates []*qpu.Operation_Update) *qpu.Payload {
// 	op := &qpu.Operation{Op: make([]*qpu.Operation_Op, 0)}
// 	for i := range updates {
// 		op.Op = append(op.Op, &qpu.Operation_Op{
// 			Attr:   attr[i],
// 			Update: updates[i],
// 		})
// 	}
// 	return &qpu.Payload{
// 		Val: &qpu.Payload_Op{
// 			Op: op,
// 		},
// 	}
// }

// // PayloadDelta ...
// func PayloadDelta(old *qpu.ObjectState, new *qpu.ObjectState) *qpu.Payload {
// 	return &qpu.Payload{
// 		Val: &qpu.Payload_Delta{
// 			Delta: &qpu.Payload_StateDelta{
// 				Old: old,
// 				New: new,
// 			},
// 		},
// 	}
// }

// // Update ...
// func Update(opT string, val *qpu.Value) *qpu.Operation_Update {
// 	return &qpu.Operation_Update{
// 		OpType: opT,
// 		Value:  val,
// 	}
// }

// //ObjectState creates a protos/qpu/State object
// func ObjectState(state map[string]*qpu.Value) *qpu.ObjectState {
// 	return &qpu.ObjectState{
// 		Attributes: state,
// 	}
// }

// //Attribute ...
// func Attribute(key string, val *qpu.Value) *qpu.Attribute {
// 	return &qpu.Attribute{
// 		AttrKey: key,
// 		Value:   val,
// 	}
// }

// //RequestStreamRequest creates a protos/qpu/RequestStream{Request} object
// func RequestStreamRequest(query *qpu_api.Query, metadata map[string]string, sync bool) *qpu_api.RequestStreamRecord {
// 	return &qpu_api.RequestStreamRecord{
// 		Request: &qpu_api.RequestStreamRecord_QueryRequest{
// 			QueryRequest: &qpu_api.QueryRequest{
// 				Query:    query,
// 				Metadata: metadata,
// 				Sync:     sync,
// 			},
// 		},
// 	}
// }

// //RequestStreamAck creates a protos/qpu/RequestStream{Ack} object
// func RequestStreamAck(sID int64) *qpu_api.RequestStreamRecord {
// 	return &qpu_api.RequestStreamRecord{
// 		Request: &qpu_api.RequestStreamRecord_Ack{
// 			Ack: &qpu_api.AckMsg{
// 				SequenceId: sID,
// 			},
// 		},
// 	}
// }

// // RequestStreamPing ...
// func RequestStreamPing(sID int64) *qpu_api.RequestStreamRecord {
// 	return &qpu_api.RequestStreamRecord{
// 		Request: &qpu_api.RequestStreamRecord_Ping{
// 			Ping: &qpu_api.PingMsg{
// 				SeqId: sID,
// 			},
// 		},
// 	}
// }

// // Query ...
// func Query(queryI *qpu_api.QueryInternalQuery) *qpu_api.Query {
// 	return &qpu_api.Query{
// 		Val: &qpu_api.Query_QueryI{
// 			QueryI: queryI,
// 		},
// 	}
// }

// // QueryInternal ...
// func QueryInternal(table string, ts *qpu.SnapshotTimePredicate, predicate []*qpu.AttributePredicate, projection []string) *qpu_api.QueryInternalQuery {
// 	return &qpu_api.QueryInternalQuery{
// 		Table:       table,
// 		Projection:  projection,
// 		Predicate:   predicate,
// 		TsPredicate: ts,
// 	}
// }

// // QuerySQL ...
// func QuerySQL(query string) *qpu_api.Query {
// 	return &qpu_api.Query{
// 		Val: &qpu_api.Query_QuerySql{
// 			QuerySql: &qpu_api.Query_SQLQuery{
// 				QueryStr: query,
// 			},
// 		},
// 	}
// }

// //ConfigRequest creates a protos/qpu/ConfigRequest object
// func ConfigRequest() *qpu_api.ConfigRequest {
// 	return &qpu_api.ConfigRequest{}
// }

// //ConfigRespοnse ...
// func ConfigRespοnse(typ qpu_api.ConfigResponse_QPUType, attrs []*qpu.AttributePredicate, ds *qpu_api.DataSet) *qpu_api.ConfigResponse {
// 	return &qpu_api.ConfigResponse{
// 		QpuType:          typ,
// 		SupportedQueries: attrs,
// 		Dataset:          ds,
// 	}
// }

// //DataSet creates a protos/utils/Dataset object
// func DataSet(dbMap map[string]map[string][]string) *qpu_api.DataSet {
// 	dbs := make(map[string]*qpu_api.DataSet_DB)
// 	for dbID, dcMap := range dbMap {
// 		dcs := make(map[string]*qpu_api.DataSet_DC)
// 		for dcID, shards := range dcMap {
// 			dcs[dcID] = &qpu_api.DataSet_DC{Shards: shards}
// 		}
// 		dbs[dbID] = &qpu_api.DataSet_DB{Datacenters: dcs}
// 	}
// 	return &qpu_api.DataSet{Databases: dbs}
// }

// //AttributePredicate create a protos/utils/AttributePredicate object
// func AttributePredicate(attr *qpu.Attribute, lb *qpu.Value, ub *qpu.Value) *qpu.AttributePredicate {
// 	return &qpu.AttributePredicate{
// 		Attr:   attr,
// 		Lbound: lb,
// 		Ubound: ub,
// 	}
// }

// // QueryPredicate ...
// func QueryPredicate() []*qpu.AttributePredicate {
// 	return []*qpu.AttributePredicate{}
// }

// //SnapshotTimePredicate create a protos/utils/SnapshotTimePredicate object
// func SnapshotTimePredicate(lb *qpu.SnapshotTime, ub *qpu.SnapshotTime) *qpu.SnapshotTimePredicate {
// 	return &qpu.SnapshotTimePredicate{
// 		Lbound: lb,
// 		Ubound: ub,
// 	}
// }

// //SnapshotTime creates a protos/utils/SnapshotTime object
// func SnapshotTime(t qpu.SnapshotTime_SnapshotTimeType, vc *qpu.Vectorclock) *qpu.SnapshotTime {
// 	return &qpu.SnapshotTime{
// 		Type:  t,
// 		Value: vc,
// 	}
// }

// //Vectorclock creates a protos/utils/Vectorclock object
// func Vectorclock(vc map[string]uint64) *qpu.Vectorclock {
// 	return &qpu.Vectorclock{
// 		Vc: vc,
// 	}
// }

// //SubRequestAntidote creates a protos/qpu/antidote/SubRequest object
// func SubRequestAntidote(ts int64) *antidote.SubRequest {
// 	return &antidote.SubRequest{
// 		Timestamp: ts,
// 	}
// }

// //ValueInt creates a protos/utils/Value{Int} object
// func ValueInt(i int64) *qpu.Value {
// 	return &qpu.Value{
// 		Val: &qpu.Value_Int{
// 			Int: i,
// 		},
// 	}
// }

// //ValueStr creates a protos/utils/Value{Str} object
// func ValueStr(s string) *qpu.Value {
// 	return &qpu.Value{
// 		Val: &qpu.Value_Str{
// 			Str: s,
// 		},
// 	}
// }

// //ValueFlt creates a protos/utils/Value{Flt} object
// func ValueFlt(f float64) *qpu.Value {
// 	return &qpu.Value{
// 		Val: &qpu.Value_Flt{
// 			Flt: f,
// 		},
// 	}
// }
