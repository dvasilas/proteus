package libqpu

import (
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
)

// ---------------- ASTQuery -------------------

// ASTQuery ...
type ASTQuery struct {
	Q *qpu_api.ASTQuery
}

// NewQuery ...
func NewQuery(qSQL *qpu_api.SQLQuery, qAST *qpu_api.ASTQuery) *qpu_api.Query {
	if qAST == nil {
		return &qpu_api.Query{
			Query: &qpu_api.Query_QuerySql{
				QuerySql: qSQL,
			},
		}
	}

	return &qpu_api.Query{
		Query: &qpu_api.Query_QueryAst{
			QueryAst: qAST,
		},
	}
}

// NewSQLQuery ...
func NewSQLQuery(query string) *qpu_api.SQLQuery {
	return &qpu_api.SQLQuery{
		QueryStr: query,
	}
}

// NewASTQuery ...
func NewASTQuery(table string, ts *qpu.SnapshotTimePredicate, predicate []*qpu.AttributePredicate, projection []string, limit int64) *qpu_api.ASTQuery {
	return &qpu_api.ASTQuery{
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
func (q ASTQuery) GetOrderBy() *qpu_api.OrderBy {
	return q.Q.GetOrderBy()
}
