package sqlparser

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/xwb1989/sqlparser"
)

type sqlParseCtx struct {
	proteusAST   *qpuapi.ASTQuery
	walkStateStr []string
	walkState    []sqlTreeNodeType
	whereStack   []whereExpr
}

type whereExpr interface {
	getType() whereExprType
}

type whereExprType int

const (
	collName = whereExprType(iota)
	val      = whereExprType(iota)
	op       = whereExprType(iota)
)

type colName string

func (expr colName) getType() whereExprType { return collName }

type value struct {
	v *qpu.Value
}

func (expr value) getType() whereExprType { return val }

type operator string

func (expr operator) getType() whereExprType { return op }

func (ctx *sqlParseCtx) whereExprPush(expr whereExpr) {
	ctx.whereStack = append(ctx.whereStack, expr)
}

func (ctx *sqlParseCtx) whereExprPop() whereExpr {
	expr := ctx.whereStack[len(ctx.whereStack)-1]
	ctx.whereStack = ctx.whereStack[:len(ctx.whereStack)-1]
	return expr
}

type sqlTreeNodeType int

const (
	selectExpr       = sqlTreeNodeType(iota)
	starExpr         = sqlTreeNodeType(iota)
	where            = sqlTreeNodeType(iota)
	aliasedExpr      = sqlTreeNodeType(iota)
	tableExpr        = sqlTreeNodeType(iota)
	aliasedTableExpr = sqlTreeNodeType(iota)
	groupBy          = sqlTreeNodeType(iota)
	orderBy          = sqlTreeNodeType(iota)
	limit            = sqlTreeNodeType(iota)
	comparisonExpr   = sqlTreeNodeType(iota)
)

func createSQLParseCtx() *sqlParseCtx {
	return &sqlParseCtx{
		proteusAST:   &qpuapi.ASTQuery{},
		walkStateStr: make([]string, 0),
		walkState:    make([]sqlTreeNodeType, 0),
	}
}

func (ctx *sqlParseCtx) stateStrPush(str string) {
	ctx.walkStateStr = append(ctx.walkStateStr, str)
}

func (ctx *sqlParseCtx) stateStrPop() {
	ctx.walkStateStr = ctx.walkStateStr[:len(ctx.walkStateStr)-1]
}

func (ctx *sqlParseCtx) stateStrPrint() {
	fmt.Println(ctx.walkStateStr)
}

func (ctx *sqlParseCtx) statePush(node sqlTreeNodeType) {
	ctx.walkState = append(ctx.walkState, node)
}

func (ctx *sqlParseCtx) statePop() {
	ctx.walkState = ctx.walkState[:len(ctx.walkState)-1]
}

// Parse ...
func Parse(query string) (parsedAST libqpu.ASTQuery, err error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return libqpu.ASTQuery{}, err
	}

	switch stmt.(type) {
	case *sqlparser.Select:
		parseCtx := createSQLParseCtx()
		err = parseCtx.parseSelect(stmt)
		if err != nil {
			return
		}
		err = parseCtx.buildProteusAST()
		parsedAST = libqpu.ASTQuery{
			Q: parseCtx.proteusAST,
		}
	default:
		err = utils.Error(errors.New("only select queries are supported"))
	}

	return
}

func (ctx *sqlParseCtx) parseSelect(node sqlparser.SQLNode) error {
	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case sqlparser.Comments:
			return false, nil
		case sqlparser.SelectExprs:
			return false, ctx.parseSelectExprs(node)
		case sqlparser.TableExprs:
			return false, ctx.parseTableExprs(node)
		case *sqlparser.Where:
			return false, ctx.parseWhere(node)
		case sqlparser.GroupBy:
			return false, ctx.parseGroupBy(node)
		case sqlparser.OrderBy:
			return false, ctx.parseOrderBy(node)
		case *sqlparser.Limit:
			return false, ctx.parseLimit(node)
		default:
			return false, utils.Error(errors.New("should not have reached here"))
		}
	}, node)
}

func (ctx *sqlParseCtx) parseSelectExprs(node sqlparser.SQLNode) error {
	ctx.statePush(selectExpr)
	defer ctx.statePop()

	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case *sqlparser.StarExpr:
			return false, ctx.parseStarExpr(node)
		case *sqlparser.AliasedExpr:
			return false, ctx.parseAliasedExpr(node)
		default:
			return false, utils.Error(errors.New("parseSelectExprs: should have reached here"))
		}
	}, node)
}

func (ctx *sqlParseCtx) parseTableExprs(node sqlparser.SQLNode) error {
	ctx.statePush(tableExpr)
	defer ctx.statePop()

	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case *sqlparser.AliasedTableExpr:
			return false, ctx.parseAliasedTableExpr(node)
		case *sqlparser.ParenTableExpr:
			return false, utils.Error(errors.New("ParenTableExpr: not supported"))
		case *sqlparser.JoinTableExpr:
			return false, utils.Error(errors.New("JoinTableExpr: not supported"))
		default:
			return false, utils.Error(errors.New("should not have reached here"))
		}
	}, node)
}

func (ctx *sqlParseCtx) parseWhere(node sqlparser.SQLNode) error {
	ctx.statePush(where)
	defer ctx.statePop()

	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		return false, ctx.parseExpr(node)
	}, node)
}

func (ctx *sqlParseCtx) parseGroupBy(node sqlparser.SQLNode) error {
	ctx.statePush(groupBy)
	defer ctx.statePop()

	return nil
}

func (ctx *sqlParseCtx) parseOrderBy(node sqlparser.SQLNode) error {
	ctx.statePush(orderBy)
	defer ctx.statePop()

	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case *sqlparser.Order:
			ctx.proteusAST.OrderBy = &qpuapi.OrderBy{}
			switch node.(*sqlparser.Order).Direction {
			case "desc":
				ctx.proteusAST.OrderBy.Direction = qpuapi.OrderBy_DESC
			case "asc":
				ctx.proteusAST.OrderBy.Direction = qpuapi.OrderBy_ASC
			default:
				return false, errors.New("unknown value in orderBy:direction")
			}
			return false, ctx.parseExpr(node.(*sqlparser.Order).Expr)
		default:
			return false, utils.Error(errors.New("parseOrderBy: should not have reached here"))
		}
	}, node)
}

func (ctx *sqlParseCtx) parseLimit(node sqlparser.SQLNode) error {
	ctx.statePush(limit)
	defer ctx.statePop()

	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		return false, ctx.parseExpr(node)
	}, node)
}

func (ctx *sqlParseCtx) parseStarExpr(node sqlparser.SQLNode) error {
	ctx.statePush(starExpr)
	defer ctx.statePop()

	ctx.proteusAST.Projection = append(ctx.proteusAST.Projection, "*")

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
			return false, utils.Error(errors.New("parseStarExpr: not supported"))
		}
	}, node)

	return err
}

func (ctx *sqlParseCtx) parseAliasedExpr(node sqlparser.SQLNode) error {
	ctx.statePush(aliasedExpr)
	defer ctx.statePop()

	visitedSelf := false
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case sqlparser.Expr:
			return false, ctx.parseExpr(node)
		case sqlparser.ColIdent:
			return false, nil
		default:
			return false, utils.Error(errors.New("parseAliasedExpr: should not have reached here"))
		}
	}, node)
}

func (ctx *sqlParseCtx) parseAliasedTableExpr(node sqlparser.SQLNode) error {
	ctx.statePush(aliasedTableExpr)
	defer ctx.statePop()

	visitedSelf := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		if !visitedSelf {
			visitedSelf = true
			return true, nil
		}

		switch node.(type) {
		case sqlparser.TableName:
			ctx.proteusAST.Table = sqlparser.String(node)
			return false, nil
		case sqlparser.TableIdent:
			return false, nil
		case *sqlparser.IndexHints:
			return false, nil
		default:
			return false, utils.Error(errors.New("should not have reached here"))
		}
	}, node)

	return err
}

func (ctx *sqlParseCtx) parseExpr(node sqlparser.SQLNode) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
		switch node.(type) {
		case *sqlparser.ComparisonExpr:
			return false, ctx.ComparisonExpr(node)
		case *sqlparser.ColName:
			return false, ctx.parseColName(node)
		case *sqlparser.SQLVal:
			return false, ctx.parseSQLVal(node)
		default:
			return false, utils.Error(errors.New("parseExpr: not supported"))
		}
	}, node)
}

func (ctx *sqlParseCtx) ComparisonExpr(node sqlparser.SQLNode) error {
	ctx.statePush(comparisonExpr)
	defer ctx.statePop()

	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		left := node.(*sqlparser.ComparisonExpr).Left
		err := ctx.parseExpr(left)
		if err != nil {
			return false, err
		}

		right := node.(*sqlparser.ComparisonExpr).Right
		err = ctx.parseExpr(right)
		if err != nil {
			return false, err
		}

		ctx.whereExprPush(operator(node.(*sqlparser.ComparisonExpr).Operator))
		return false, nil
	}, node)
}

func (ctx *sqlParseCtx) parseColName(node sqlparser.SQLNode) error {
	if len(ctx.walkState) >= 2 && ctx.walkState[0] == selectExpr && ctx.walkState[1] == aliasedExpr {
		ctx.proteusAST.Projection = append(ctx.proteusAST.Projection, node.(*sqlparser.ColName).Name.String())
		return nil
	}
	if len(ctx.walkState) >= 1 && ctx.walkState[0] == orderBy {
		ctx.proteusAST.OrderBy.AttributeName = node.(*sqlparser.ColName).Name.String()
		return nil
	}

	if len(ctx.walkState) >= 1 && ctx.walkState[0] == where {
		ctx.whereExprPush(colName(node.(*sqlparser.ColName).Name.String()))
		return nil
	}

	return errors.New("parseColName: should not have reached")
}

func (ctx *sqlParseCtx) parseSQLVal(node sqlparser.SQLNode) (err error) {
	err = nil
	var valInt int64
	var valFlt float64

	switch node.(*sqlparser.SQLVal).Type {
	case sqlparser.StrVal:
		if ctx.walkState[0] == where {
			ctx.whereExprPush(value{v: libqpu.ValueStr(string(node.(*sqlparser.SQLVal).Val))})
		}
	case sqlparser.IntVal:
		valInt, err = strconv.ParseInt(string(node.(*sqlparser.SQLVal).Val), 10, 64)
		if err != nil {
			return err
		}
		if ctx.walkState[0] == where {
			ctx.whereExprPush(value{v: libqpu.ValueInt(int32(valInt))})
		}

	case sqlparser.FloatVal:
		valFlt, err = strconv.ParseFloat(string(node.(*sqlparser.SQLVal).Val), 64)
		if err != nil {
			return err
		}
		if ctx.walkState[0] == where {
			ctx.whereExprPush(value{v: libqpu.ValueFlt(valFlt)})
		}
	default:
		err = utils.Error(errors.New("parseSQLVal: value type not supported"))
	}

	if ctx.walkState[0] == limit {
		ctx.proteusAST.Limit = valInt
	}

	return err
}

func (ctx *sqlParseCtx) buildProteusAST() error {
	if len(ctx.whereStack) == 0 {
		ctx.proteusAST.TsPredicate = libqpu.SnapshotTimePredicate(
			libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
			libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
		)
		return nil
	}

	var optr string

	pred := qpu.AttributePredicate{}
	pred.Attr = &qpu.Attribute{}

	for len(ctx.whereStack) > 0 {
		expr := ctx.whereExprPop()

		switch expr.getType() {
		case collName:
			pred.Attr.AttrKey = string(expr.(colName))
		case val:
			if optr == "=" {
				pred.Lbound = expr.(value).v
				pred.Ubound = expr.(value).v
				pred.Type = qpu.AttributePredicate_EQ
			}
		case op:
			optr = string(expr.(operator))
		default:
		}
	}

	ctx.proteusAST.Predicate = append(ctx.proteusAST.Predicate, &pred)
	ctx.proteusAST.TsPredicate = libqpu.SnapshotTimePredicate(
		libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
		libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
	)

	return nil
}
