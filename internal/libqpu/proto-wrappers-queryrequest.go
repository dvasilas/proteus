package libqpu

import (
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
)

// ---------------- QueryReq -------------------

// QueryRequest ...
type QueryRequest struct {
	Req *qpuapi.QueryRequest
}

// QueryType ...
type QueryType int

const (
	// ASTQueryT ..
	ASTQueryT QueryType = iota
	// SQLQueryT ..
	SQLQueryT QueryType = iota
	// UnknownQueryT ...
	UnknownQueryT QueryType = iota
)

// NewQueryRequest ...
func NewQueryRequest(query *qpuapi.Query, md map[string]string, sync, measureDataTransfer bool) QueryRequest {
	return QueryRequest{
		Req: &qpuapi.QueryRequest{
			Query:               query,
			Metadata:            md,
			Sync:                sync,
			MeasureDataTransfer: measureDataTransfer,
		},
	}
}

// QueryType ...
func (r QueryRequest) QueryType() QueryType {
	switch r.Req.GetQuery().GetQuery().(type) {
	case *qpuapi.Query_QueryAst:
		return ASTQueryT
	case *qpuapi.Query_QuerySql:
		return SQLQueryT
	default:
		return UnknownQueryT
	}
}

// GetQueryI ...
func (r QueryRequest) GetQueryI() ASTQuery {
	return ASTQuery{Q: r.Req.GetQuery().GetQueryAst()}
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

// GetMeasureDataTransfer ...
func (r QueryRequest) GetMeasureDataTransfer() bool {
	return r.Req.GetMeasureDataTransfer()
}
