package libqpu

import (
	"context"
	"errors"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
)

// ---------------- LogOperation -------------------

// LogOperation ...
type LogOperation struct {
	Op   *qpu.LogOperation
	InTs time.Time
}

// LogOperationDelta ...
func LogOperationDelta(recordID, table string, ts *qpu.Vectorclock,
	attributesOld map[string]*qpu.Value,
	attributesNew map[string]*qpu.Value,
) LogOperation {
	return LogOperation{
		Op: &qpu.LogOperation{
			ObjectId:  recordID,
			Bucket:    table,
			Timestamp: ts,
			Payload: &qpu.Payload{
				Val: &qpu.Payload_Delta{
					Delta: &qpu.Payload_StateDelta{
						Old: &qpu.ObjectState{Attributes: attributesOld},
						New: &qpu.ObjectState{Attributes: attributesNew},
					},
				},
			},
		},
	}
}

// LogOperationState creates a protos/utils/LogOperation object
func LogOperationState(recordID, table string, ts *qpu.Vectorclock,
	atttributes map[string]*qpu.Value) LogOperation {
	return LogOperation{
		Op: &qpu.LogOperation{
			ObjectId:  recordID,
			Bucket:    table,
			Timestamp: ts,
			Payload: &qpu.Payload{
				Val: &qpu.Payload_State{
					State: &qpu.ObjectState{Attributes: atttributes},
				},
			},
		},
	}
}

// GetObjectId ...
func (op LogOperation) GetObjectId() string {
	return op.Op.GetObjectId()
}

// GetTable ...
func (op LogOperation) GetTable() string {
	return op.Op.GetBucket()
}

// HasOldState ...
func (op LogOperation) HasOldState() bool {
	if op.Op.GetPayload().GetDelta().GetOld().GetAttributes() != nil {
		return true
	}
	return false
}

// GetAttributes ...
func (op LogOperation) GetAttributes() map[string]*qpu.Value {
	if op.Op.GetPayload().GetState() != nil {
		return op.Op.GetPayload().GetState().GetAttributes()
	} else if op.Op.GetPayload().GetDelta().GetNew() != nil {
		return op.Op.GetPayload().GetDelta().GetNew().GetAttributes()
	}
	return nil
}

// GetTimestamp ...
func (op LogOperation) GetTimestamp() *qpu.Vectorclock {
	return op.Op.GetTimestamp()
}

// IsDelta ...
func (op LogOperation) IsDelta() bool {
	return op.Op.GetPayload().GetDelta().GetNew() != nil
}

// ---------------- RequestStream -------------------

// ResponseStream ...
type ResponseStream struct {
	Stream     qpuapi.QPUAPI_QueryClient
	CancelFunc context.CancelFunc
}

// Recv ...
func (str ResponseStream) Recv() (ResponseRecord, error) {
	respRecord, err := str.Stream.Recv()
	return ResponseRecord{Rec: respRecord}, err
}

// Cancel ...
func (str ResponseStream) Cancel() {
	if str.CancelFunc != nil {
		str.CancelFunc()
	}
}

// ---------------- RequestStream -------------------

// RequestStream ...
type RequestStream struct {
	Stream qpuapi.QPUAPI_QueryServer
}

// Send ...
func (s RequestStream) Send(seqID int64, recordType ResponseRecordType, logOp LogOperation) error {
	var recType qpuapi.ResponseStreamRecord_StreamRecordType
	switch recordType {
	case Delta:
		recType = qpuapi.ResponseStreamRecord_UPDATEDELTA
	case EndOfStream:
		recType = qpuapi.ResponseStreamRecord_END_OF_STREAM
	case State:
		recType = qpuapi.ResponseStreamRecord_STATE
	default:
		return utils.Error(errors.New("Unknown StreamRecordType"))
	}

	return s.Stream.Send(
		&qpuapi.ResponseStreamRecord{
			SequenceId: seqID,
			Type:       recType,
			LogOp:      logOp.Op,
		},
	)
}

// ResponseRecordType ...
type ResponseRecordType int

const (
	// State ...
	State ResponseRecordType = iota
	// Delta ...
	Delta ResponseRecordType = iota
	// EndOfStream ...
	EndOfStream ResponseRecordType = iota
)

// ---------------- ResponseRecord -------------------

// ResponseRecord ...
type ResponseRecord struct {
	Rec  *qpuapi.ResponseStreamRecord
	InTs time.Time
}

// GetLogOp ...
func (r ResponseRecord) GetLogOp() LogOperation {
	return LogOperation{Op: r.Rec.GetLogOp()}
}

// GetAttributes ...
func (r ResponseRecord) GetAttributes() map[string]*qpu.Value {
	op := LogOperation{Op: r.Rec.GetLogOp()}
	return op.GetAttributes()
}

// GetType ...
func (r ResponseRecord) GetType() (ResponseRecordType, error) {
	switch r.Rec.GetType() {
	case qpuapi.ResponseStreamRecord_STATE:
		return State, nil
	case qpuapi.ResponseStreamRecord_UPDATEDELTA:
		return Delta, nil
	case qpuapi.ResponseStreamRecord_END_OF_STREAM:
		return EndOfStream, nil
	default:
		return 0, utils.Error(errors.New("unknown ResponseStreamRecord type"))
	}
}

// GetRecordID ...
func (r ResponseRecord) GetRecordID() string {
	return r.Rec.GetLogOp().GetObjectId()
}

// ResponseStreamRecord ...
func ResponseStreamRecord(seqID int64, t qpuapi.ResponseStreamRecord_StreamRecordType, logOp *qpu.LogOperation) *qpuapi.ResponseStreamRecord {
	return &qpuapi.ResponseStreamRecord{
		SequenceId: seqID,
		Type:       t,
		LogOp:      logOp,
	}
}

//ObjectState creates a protos/qpu/State object
func ObjectState(state map[string]*qpu.Value) *qpu.ObjectState {
	return &qpu.ObjectState{
		Attributes: state,
	}
}

//Attribute ...
func Attribute(key string, val *qpu.Value) *qpu.Attribute {
	return &qpu.Attribute{
		AttrKey: key,
		Value:   val,
	}
}

//RequestStreamRequest creates a protos/qpu/RequestStream{Request} object
func RequestStreamRequest(query *qpuapi.Query, metadata map[string]string, sync bool) *qpuapi.RequestStreamRecord {
	return &qpuapi.RequestStreamRecord{
		Request: &qpuapi.RequestStreamRecord_QueryRequest{
			QueryRequest: &qpuapi.QueryRequest{
				Query:    query,
				Metadata: metadata,
				Sync:     sync,
			},
		},
	}
}

//RequestStreamAck creates a protos/qpu/RequestStream{Ack} object
func RequestStreamAck(sID int64) *qpuapi.RequestStreamRecord {
	return &qpuapi.RequestStreamRecord{
		Request: &qpuapi.RequestStreamRecord_Ack{
			Ack: &qpuapi.AckMsg{
				SequenceId: sID,
			},
		},
	}
}

//ConfigRequest creates a protos/qpu/ConfigRequest object
func ConfigRequest() *qpuapi.ConfigRequest {
	return &qpuapi.ConfigRequest{}
}

//AttributePredicate create a protos/utils/AttributePredicate object
func AttributePredicate(attr *qpu.Attribute, lb *qpu.Value, ub *qpu.Value) *qpu.AttributePredicate {
	return &qpu.AttributePredicate{
		Attr:   attr,
		Lbound: lb,
		Ubound: ub,
	}
}

// QueryPredicate ...
func QueryPredicate() []*qpu.AttributePredicate {
	return []*qpu.AttributePredicate{}
}

//SnapshotTimePredicate create a protos/utils/SnapshotTimePredicate object
func SnapshotTimePredicate(lb *qpu.SnapshotTime, ub *qpu.SnapshotTime) *qpu.SnapshotTimePredicate {
	return &qpu.SnapshotTimePredicate{
		Lbound: lb,
		Ubound: ub,
	}
}

//SnapshotTime creates a protos/utils/SnapshotTime object
func SnapshotTime(t qpu.SnapshotTime_SnapshotTimeType, vc *qpu.Vectorclock, isClosed bool) *qpu.SnapshotTime {
	return &qpu.SnapshotTime{
		Type:     t,
		Value:    vc,
		IsClosed: isClosed,
	}
}

//Vectorclock creates a protos/utils/Vectorclock object
func Vectorclock(vc map[string]*timestamp.Timestamp) *qpu.Vectorclock {
	return &qpu.Vectorclock{
		Vc: vc,
	}
}

//ValueInt creates a protos/utils/Value{Int} object
func ValueInt(i int64) *qpu.Value {
	return &qpu.Value{
		Val: &qpu.Value_Int{
			Int: i,
		},
	}
}

//ValueStr creates a protos/utils/Value{Str} object
func ValueStr(s string) *qpu.Value {
	return &qpu.Value{
		Val: &qpu.Value_Str{
			Str: s,
		},
	}
}

//ValueFlt creates a protos/utils/Value{Flt} object
func ValueFlt(f float64) *qpu.Value {
	return &qpu.Value{
		Val: &qpu.Value_Flt{
			Flt: f,
		},
	}
}
