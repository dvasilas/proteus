package libqpu

import (
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
)

// ---------------- ASTQuery -------------------

// ASTQuery ...
type ASTQuery struct {
	Q *qpuapi.ASTQuery
}

// NewQuery ...
func NewQuery(qSQL *qpuapi.SQLQuery, qAST *qpuapi.ASTQuery) *qpuapi.Query {
	if qAST == nil {
		return &qpuapi.Query{
			Query: &qpuapi.Query_QuerySql{
				QuerySql: qSQL,
			},
		}
	}

	return &qpuapi.Query{
		Query: &qpuapi.Query_QueryAst{
			QueryAst: qAST,
		},
	}
}

// NewSQLQuery ...
func NewSQLQuery(query string) *qpuapi.SQLQuery {
	return &qpuapi.SQLQuery{
		QueryStr: query,
	}
}

// NewASTQuery ...
func NewASTQuery(table string, ts *qpu.SnapshotTimePredicate, predicate []*qpu.AttributePredicate, projection []string, limit int64) *qpuapi.ASTQuery {
	return &qpuapi.ASTQuery{
		Table:       table,
		Projection:  projection,
		Predicate:   predicate,
		TsPredicate: ts,
		Limit:       limit,
	}
}

// GetTable ...
func (q ASTQuery) GetTable() string {
	return q.Q.GetTable()
}

// GetProjection ...
func (q ASTQuery) GetProjection() []string {
	return q.Q.GetProjection()
}

// GetPredicate ...
func (q ASTQuery) GetPredicate() []*qpu.AttributePredicate {
	return q.Q.GetPredicate()
}

// GetPredicateContains ...
func (q ASTQuery) GetPredicateContains() ([]string, []string) {
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
func (q ASTQuery) GetTsPredicate() *qpu.SnapshotTimePredicate {
	return q.Q.GetTsPredicate()
}

// GetLimit ...
func (q ASTQuery) GetLimit() int64 {
	return q.Q.GetLimit()
}

// GetOrderBy ...
func (q ASTQuery) GetOrderBy() *qpuapi.OrderBy {
	return q.Q.GetOrderBy()
}
