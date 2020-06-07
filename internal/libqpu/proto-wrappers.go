package libqpu

import (
	"context"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
)

// ---------------- LogOperation -------------------

// LogOperation ...
type LogOperation struct {
	Op *qpu.LogOperation
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

// GetTable ...
func (op LogOperation) GetTable() string {
	return op.Op.GetBucket()
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
	Stream     qpu_api.QPUAPI_QueryClient
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
	Stream qpu_api.QPUAPI_QueryServer
}

// Send ...
func (s RequestStream) Send(seqID int64, recordType ResponseRecordType, logOp LogOperation) error {
	var recType qpu_api.ResponseStreamRecord_StreamRecordType
	switch recordType {
	case Delta:
		recType = qpu_api.ResponseStreamRecord_UPDATEDELTA
	case EndOfStream:
		recType = qpu_api.ResponseStreamRecord_END_OF_STREAM
	case State:
		recType = qpu_api.ResponseStreamRecord_STATE
	default:
		return Error("Unknown StreamRecordType")
	}

	return s.Stream.Send(
		&qpu_api.ResponseStreamRecord{
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

// ---------------- QueryRequest -------------------

// QueryRequest ...
type QueryRequest struct {
	Req *qpu_api.QueryRequest
}

// NewQueryRequestI ...
func NewQueryRequestI(query InternalQuery, md map[string]string, sync bool) QueryRequest {
	return QueryRequest{
		Req: &qpu_api.QueryRequest{
			Query: &qpu_api.Query{
				Val: &qpu_api.Query_QueryI{
					QueryI: query.Q,
				},
			},
			Metadata: md,
			Sync:     sync,
		},
	}
}

// NewQueryRequestSQL ...
func NewQueryRequestSQL(query string, md map[string]string, sync bool) QueryRequest {
	return QueryRequest{
		Req: &qpu_api.QueryRequest{
			Query: &qpu_api.Query{
				Val: &qpu_api.Query_QuerySql{
					QuerySql: &qpu_api.Query_SQLQuery{
						QueryStr: query,
					},
				},
			},
			Metadata: md,
			Sync:     sync,
		},
	}
}

// ---------------- ResponseRecord -------------------

// ResponseRecord ...
type ResponseRecord struct {
	Rec *qpu_api.ResponseStreamRecord
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
	case qpu_api.ResponseStreamRecord_STATE:
		return State, nil
	case qpu_api.ResponseStreamRecord_UPDATEDELTA:
		return Delta, nil
	case qpu_api.ResponseStreamRecord_END_OF_STREAM:
		return EndOfStream, nil
	default:
		return 0, Error("unknown ResponseStreamRecord type")
	}
}

// GetRecordID ...
func (r ResponseRecord) GetRecordID() string {
	return r.Rec.GetLogOp().GetObjectId()
}

// ---------------- QueryRequest -------------------

// QueryType ...
func (r QueryRequest) QueryType() QueryType {
	switch r.Req.GetQuery().GetVal().(type) {
	case *qpu_api.Query_QueryI:
		return InternalQueryType
	case *qpu_api.Query_QuerySql:
		return SQLQueryType
	default:
		return UnknownType
	}
}

// GetQueryI ...
func (r QueryRequest) GetQueryI() InternalQuery {
	return InternalQuery{Q: r.Req.GetQuery().GetQueryI()}
}

// GetSQLStr ...
func (r QueryRequest) GetSQLStr() string {
	return r.Req.GetQuery().GetQuerySql().GetQueryStr()
}

// GetMetadata ...
func (r QueryRequest) GetMetadata() map[string]string {
	return r.Req.GetMetadata()
}

// GetSync ...
func (r QueryRequest) GetSync() bool {
	return r.Req.GetSync()
}

// ---------------- InternalQuery -------------------

// InternalQuery ...
type InternalQuery struct {
	Q *qpu_api.QueryInternalQuery
}

// GetTable ...
func (q InternalQuery) GetTable() string {
	return q.Q.GetTable()
}

// GetProjection ...
func (q InternalQuery) GetProjection() []string {
	return q.Q.GetProjection()
}

// GetPredicate ...
func (q InternalQuery) GetPredicate() []*qpu.AttributePredicate {
	return q.Q.GetPredicate()
}

// GetPredicateContains ...
func (q InternalQuery) GetPredicateContains() ([]string, []string) {
	isNull := make([]string, 0)
	isNotNull := make([]string, 0)
	for _, pred := range q.Q.GetPredicate() {
		if pred.GetType() == qpu.AttributePredicate_ISNULL {
			isNull = append(isNull, pred.GetAttr().GetAttrKey())
		} else if pred.GetType() == qpu.AttributePredicate_ISNOTNULL {
			isNotNull = append(isNotNull, pred.GetAttr().GetAttrKey())
		}
	}
	return isNull, isNotNull
}

// GetTsPredicate ...
func (q InternalQuery) GetTsPredicate() *qpu.SnapshotTimePredicate {
	return q.Q.GetTsPredicate()
}

// GetLimit ...
func (q InternalQuery) GetLimit() int64 {
	return q.Q.GetLimit()
}

// QueryType ...
type QueryType int

const (
	// InternalQueryType ..
	InternalQueryType QueryType = iota
	// SQLQueryType ..
	SQLQueryType QueryType = iota
	// UnknownType ...
	UnknownType QueryType = iota
)

// ResponseStreamRecord ...
func ResponseStreamRecord(seqID int64, t qpu_api.ResponseStreamRecord_StreamRecordType, logOp *qpu.LogOperation) *qpu_api.ResponseStreamRecord {
	return &qpu_api.ResponseStreamRecord{
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
func RequestStreamRequest(query *qpu_api.Query, metadata map[string]string, sync bool) *qpu_api.RequestStreamRecord {
	return &qpu_api.RequestStreamRecord{
		Request: &qpu_api.RequestStreamRecord_QueryRequest{
			QueryRequest: &qpu_api.QueryRequest{
				Query:    query,
				Metadata: metadata,
				Sync:     sync,
			},
		},
	}
}

//RequestStreamAck creates a protos/qpu/RequestStream{Ack} object
func RequestStreamAck(sID int64) *qpu_api.RequestStreamRecord {
	return &qpu_api.RequestStreamRecord{
		Request: &qpu_api.RequestStreamRecord_Ack{
			Ack: &qpu_api.AckMsg{
				SequenceId: sID,
			},
		},
	}
}

// RequestStreamPing ...
func RequestStreamPing(sID int64) *qpu_api.RequestStreamRecord {
	return &qpu_api.RequestStreamRecord{
		Request: &qpu_api.RequestStreamRecord_Ping{
			Ping: &qpu_api.PingMsg{
				SeqId: sID,
			},
		},
	}
}

// Query ...
func Query(queryI *qpu_api.QueryInternalQuery) *qpu_api.Query {
	return &qpu_api.Query{
		Val: &qpu_api.Query_QueryI{
			QueryI: queryI,
		},
	}
}

// QueryInternal ...
func QueryInternal(table string, ts *qpu.SnapshotTimePredicate, predicate []*qpu.AttributePredicate, projection []string, limit int64) *qpu_api.QueryInternalQuery {
	return &qpu_api.QueryInternalQuery{
		Table:       table,
		Projection:  projection,
		Predicate:   predicate,
		TsPredicate: ts,
		Limit:       limit,
	}
}

// QuerySQL ...
func QuerySQL(query string) *qpu_api.Query {
	return &qpu_api.Query{
		Val: &qpu_api.Query_QuerySql{
			QuerySql: &qpu_api.Query_SQLQuery{
				QueryStr: query,
			},
		},
	}
}

// SQLQUery ...
func SQLQUery(query string) *qpu_api.Query_SQLQuery {
	return &qpu_api.Query_SQLQuery{
		QueryStr: query,
	}
}

//ConfigRequest creates a protos/qpu/ConfigRequest object
func ConfigRequest() *qpu_api.ConfigRequest {
	return &qpu_api.ConfigRequest{}
}

//ConfigRespοnse ...
func ConfigRespοnse(typ qpu_api.ConfigResponse_QPUType, attrs []*qpu.AttributePredicate, ds *qpu_api.DataSet) *qpu_api.ConfigResponse {
	return &qpu_api.ConfigResponse{
		QpuType:          typ,
		SupportedQueries: attrs,
		Dataset:          ds,
	}
}

//DataSet creates a protos/utils/Dataset object
func DataSet(dbMap map[string]map[string][]string) *qpu_api.DataSet {
	dbs := make(map[string]*qpu_api.DataSet_DB)
	for dbID, dcMap := range dbMap {
		dcs := make(map[string]*qpu_api.DataSet_DC)
		for dcID, shards := range dcMap {
			dcs[dcID] = &qpu_api.DataSet_DC{Shards: shards}
		}
		dbs[dbID] = &qpu_api.DataSet_DB{Datacenters: dcs}
	}
	return &qpu_api.DataSet{Databases: dbs}
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
