package sqlparser

import (
	"errors"
	"strconv"

	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
)

// Parse ...
func Parse(query string) (*pbQPU.Query, error) {
	log.WithFields(log.Fields{"query": query}).Debug("query received")
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	switch stmt.(type) {
	case *sqlparser.Select:
		return parseSelect(stmt)
	default:
		return nil, errors.New("only select queries are supported")
	}
}

func parseSelect(stmt sqlparser.Statement) (*pbQPU.Query, error) {
	query := parsedQuery{}
	query.parseSelect(stmt)

	return query.toQPUQuery()
}

func (q *parsedQuery) toQPUQuery() (*pbQPU.Query, error) {
	var pred *pbUtils.AttributePredicate

	var selectOp, selectAttr, selectVal expr
	var ok bool
	if selectOp, ok = q.popExpr(); !ok {
		return nil, errors.New("incorrect parsed query stack")
	}
	switch selectOp.(operator) {
	case "=":
		if selectVal, ok = q.popExpr(); !ok {
			return nil, errors.New("incorrect parsed query stack")
		}
		if selectAttr, ok = q.popExpr(); !ok {
			return nil, errors.New("incorrect parsed query stack")
		}
		pred = protoutils.AttributePredicate(
			protoutils.Attribute(selectAttr.(attribute).qpuAttribute.AttrKey, pbUtils.Attribute_S3TAGFLT, nil),
			selectVal.(value).qpuValue,
			selectVal.(value).qpuValue,
		)
	default:
		return nil, errors.New("only = is supported for now")
	}

	return protoutils.QueryInternal(
		q.table,
		protoutils.SnapshotTimePredicate(protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil), protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil)),
		[]*pbUtils.AttributePredicate{pred},
	), nil
}

func (q *parsedQuery) parseSelect(node sqlparser.SQLNode) error {
	visitedSelf := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case sqlparser.Comments:
			return false, nil
		case sqlparser.SelectExprs:
			err = q.parseSelectExprs(node)
			return false, err
		case sqlparser.TableExprs:
			err = q.parseTableExprs(node)
			return false, err
		case *sqlparser.Where:
			if sqlparser.String(node) == "" {
				return false, nil
			}
			err = q.parseExpr(node.(*sqlparser.Where).Expr)
			return false, err
		case sqlparser.GroupBy:
			return false, nil
		case sqlparser.OrderBy:
			return false, nil
		case *sqlparser.Limit:
			return false, nil
		default:
			return false, errors.New("should not have reached here")
		}
	}, node)

	return err
}

func (q *parsedQuery) parseExpr(node sqlparser.SQLNode) error {
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		switch node.(type) {
		case *sqlparser.ComparisonExpr:
			return false, q.ComparisonExpr(node)
		case *sqlparser.ColName:
			return false, q.parseColName(node)
		case *sqlparser.SQLVal:
			return false, q.parseSQLVal(node)
		default:
			return false, errors.New("parseExpr: not supported")
		}
	}, node)

	return err
}

func (q *parsedQuery) parseSelectExprs(node sqlparser.SQLNode) error {
	visitedSelf := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case *sqlparser.StarExpr:
			if q.projection() != nil {
				return false, err
			}
			return false, q.parseStarExpr(node)
		default:
			return false, errors.New("only SELECT '*' is supported for now")
		}
	}, node)

	return err
}

func (q *parsedQuery) parseTableExprs(node sqlparser.SQLNode) error {
	visitedSelf := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case *sqlparser.AliasedTableExpr:
			return false, q.parseAliasedTableExpr(node)
		case *sqlparser.ParenTableExpr:
			return false, errors.New("ParenTableExpr: not supported")
		case *sqlparser.JoinTableExpr:
			return false, errors.New("JoinTableExpr: not supported")
		default:
			return false, errors.New("should not have reached here")
		}
	}, node)

	return err
}

func (q *parsedQuery) ComparisonExpr(node sqlparser.SQLNode) error {
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		left := node.(*sqlparser.ComparisonExpr).Left
		err = q.parseExpr(left)
		if err != nil {
			return false, err
		}

		right := node.(*sqlparser.ComparisonExpr).Right
		err = q.parseExpr(right)
		if err != nil {
			return false, err
		}

		return false, q.pushExpr(operator(node.(*sqlparser.ComparisonExpr).Operator))
	}, node)

	return err
}

func (q *parsedQuery) parseColName(node sqlparser.SQLNode) error {
	return q.pushExpr(
		attribute{
			qpuAttribute: protoutils.Attribute(
				sqlparser.String(node.(*sqlparser.ColName).Name),
				pbUtils.Attribute_S3TAGFLT, nil),
		})
}

func (q *parsedQuery) parseSQLVal(node sqlparser.SQLNode) error {
	switch node.(*sqlparser.SQLVal).Type {
	case sqlparser.IntVal:
		val, err := strconv.ParseInt(string(node.(*sqlparser.SQLVal).Val), 10, 64)
		if err != nil {
			return err
		}
		return q.pushExpr(
			value{
				qpuValue: protoutils.ValueInt(val),
			})
	case sqlparser.FloatVal:
		val, err := strconv.ParseFloat(string(node.(*sqlparser.SQLVal).Val), 64)
		if err != nil {
			return err
		}
		return q.pushExpr(
			value{
				qpuValue: protoutils.ValueFlt(val),
			})
	default:
		return errors.New("parseExpr: not supported")
	}
}

func (q *parsedQuery) parseAliasedTableExpr(node sqlparser.SQLNode) error {
	visitedSelf := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case sqlparser.TableName:
			q.setTable(sqlparser.String(node))
			return false, nil
		case sqlparser.TableIdent:
			return false, nil
		case *sqlparser.IndexHints:
			return false, nil
		default:
			return false, errors.New("should not have reached here")
		}
	}, node)

	return err
}

func (q *parsedQuery) parseStarExpr(node sqlparser.SQLNode) error {
	visitedSelf := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case sqlparser.TableName:
			return false, nil
		default:
			return false, errors.New("parseStarExpr: not supported")
		}
	}, node)

	return err
}

type parsedQuery struct {
	table string
	where []expr
}

func (q *parsedQuery) projection() error {
	return nil
}

func (q *parsedQuery) setTable(tableName string) error {
	q.table = tableName
	return nil
}

type exprType int

const (
	op   = exprType(iota)
	attr = exprType(iota)
	val  = exprType(iota)
)

type expr interface {
	getType() exprType
}

type operator string

func (expr operator) getType() exprType { return op }

type value struct {
	qpuValue *pbUtils.Value
}

func (expr value) getType() exprType { return val }

type attribute struct {
	qpuAttribute *pbUtils.Attribute
}

func (expr attribute) getType() exprType { return attr }

func (q *parsedQuery) popExpr() (expr, bool) {
	if len(q.where) == 0 {
		return operator(""), false
	}
	expr := q.where[len(q.where)-1]
	q.where = q.where[:len(q.where)-1]
	return expr, true
}

func (q *parsedQuery) pushExpr(expr expr) error {
	q.where = append(q.where, expr)
	return nil
}
