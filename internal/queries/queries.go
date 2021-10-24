package queries

import (
	"errors"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	// "github.com/prometheus/common/log"
)

// NewQuerySubscribe ...
func NewQuerySubscribe(table string, projection []string, isNull []string, isNotNull []string, snapshotPredicate *qpu.SnapshotTimePredicate) libqpu.ASTQuery {
	predicate := make([]*qpu.AttributePredicate, 0)
	for _, attributeKey := range isNull {
		predicate = append(predicate,
			&qpu.AttributePredicate{
				Attr: libqpu.Attribute(attributeKey, nil),
				Type: qpu.AttributePredicate_ISNULL,
			},
		)
	}
	for _, attributeKey := range isNotNull {
		predicate = append(predicate,
			&qpu.AttributePredicate{
				Attr: libqpu.Attribute(attributeKey, nil),
				Type: qpu.AttributePredicate_ISNOTNULL,
			},
		)
	}

	return libqpu.ASTQuery{
		Q: libqpu.NewASTQuery(
			table,
			libqpu.SnapshotTimePredicate(
				libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, false),
				libqpu.SnapshotTime(qpu.SnapshotTime_INF, nil, false),
			),
			predicate,
			projection,
			0,
		),
	}
}

// NewQuerySnapshot ...
func NewQuerySnapshot(table string, projection []string, isNull []string, isNotNull []string, limit int64, snapshotPredicate *qpu.SnapshotTimePredicate) libqpu.ASTQuery {
	predicate := make([]*qpu.AttributePredicate, 0)
	for _, attributeKey := range isNull {
		predicate = append(predicate,
			&qpu.AttributePredicate{
				Attr: libqpu.Attribute(attributeKey, nil),
				Type: qpu.AttributePredicate_ISNULL,
			},
		)
	}
	for _, attributeKey := range isNotNull {
		predicate = append(predicate,
			&qpu.AttributePredicate{
				Attr: libqpu.Attribute(attributeKey, nil),
				Type: qpu.AttributePredicate_ISNOTNULL,
			},
		)
	}

	return libqpu.ASTQuery{
		Q: libqpu.NewASTQuery(
			table,
			libqpu.SnapshotTimePredicate(
				libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
				libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
			),
			predicate,
			projection,
			limit,
		),
	}
}

// NewQuerySnapshotAndSubscribe ...
func NewQuerySnapshotAndSubscribe(table string, projection []string, isNull []string, isNotNull []string, attrPredicates []*qpu.AttributePredicate, snapshotPredicate *qpu.SnapshotTimePredicate) libqpu.ASTQuery {
	predicate := make([]*qpu.AttributePredicate, 0)
	for _, attributeKey := range isNull {
		predicate = append(predicate,
			&qpu.AttributePredicate{
				Attr: libqpu.Attribute(attributeKey, nil),
				Type: qpu.AttributePredicate_ISNULL,
			},
		)
	}
	for _, attributeKey := range isNotNull {
		predicate = append(predicate,
			&qpu.AttributePredicate{
				Attr: libqpu.Attribute(attributeKey, nil),
				Type: qpu.AttributePredicate_ISNOTNULL,
			},
		)
	}

	for _, attrPred := range attrPredicates {
		predicate = append(predicate, attrPred)
	}

	return libqpu.ASTQuery{
		Q: libqpu.NewASTQuery(
			table,
			libqpu.SnapshotTimePredicate(
				libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true),
				libqpu.SnapshotTime(qpu.SnapshotTime_INF, nil, false),
			),
			predicate,
			projection,
			0,
		),
	}
}

// QueryType ...
func QueryType(query libqpu.ASTQuery) (bool, bool) {
	var isSnapshot, isSubscribe bool
	if query.GetTsPredicate().GetLbound().GetType() == qpu.SnapshotTime_LATEST {
		if query.GetTsPredicate().GetLbound().GetIsClosed() {
			isSnapshot = true
		} else {
			isSnapshot = false
		}
		if query.GetTsPredicate().GetUbound().GetType() == qpu.SnapshotTime_INF {
			isSubscribe = true
		} else {
			isSubscribe = false
		}
	}
	return isSnapshot, isSubscribe
}

// // IsSubscribeToAllQuery ...
// func IsSubscribeToAllQuery(query libqpu.InternalQuery) bool {
// 	if query.GetTsPredicate().GetLbound().GetType() == qpu.SnapshotTime_LATEST &&
// 		!query.GetTsPredicate().GetLbound().GetIsClosed() &&
// 		query.GetTsPredicate().GetUbound().GetType() == qpu.SnapshotTime_INF &&
// 		!query.GetTsPredicate().GetUbound().GetIsClosed() {
// 		return true
// 	}
// 	return false
// }

// // IsGetSnapshotQuery ...
// func IsGetSnapshotQuery(query libqpu.InternalQuery) bool {
// 	if query.GetTsPredicate().GetLbound().GetType() != qpu.SnapshotTime_LATEST &&
// 		query.GetTsPredicate().GetLbound().GetIsClosed() &&
// 		query.GetTsPredicate().GetUbound().GetType() != qpu.SnapshotTime_LATEST &&
// 		query.GetTsPredicate().GetUbound().GetIsClosed() {
// 		return false
// 	}
// 	return true
// }

// SatisfiesPredicate ...
func SatisfiesPredicate(logOp libqpu.LogOperation, query libqpu.ASTQuery) (bool, error) {
	for _, pred := range query.GetPredicate() {
		attributes := logOp.GetAttributes()
		if attributes == nil {
			return false, utils.Error(errors.New("logOperation state not accessible"))
		}
		val, found := attributes[pred.GetAttr().GetAttrKey()]
		switch pred.GetType() {
		case qpu.AttributePredicate_ISNULL:
			if found {
				return false, nil
			}
		case qpu.AttributePredicate_ISNOTNULL:
			if !found {
				return false, nil
			}
		case qpu.AttributePredicate_EQ:
			c, err := utils.Compare(pred.GetLbound(), val)
			if err != nil {
				return false, err
			}
			if c != 0 {
				return false, nil
			}
		case qpu.AttributePredicate_RANGE:
			c, err := utils.Compare(pred.GetLbound(), pred.GetUbound())
			if err != nil {
				return false, err
			}
			if c == 0 {
				cl, err := utils.Compare(pred.GetLbound(), val)
				if err != nil {
					return false, err
				}
				return cl <= 0, nil
			} else if c < 0 {
				cl, err := utils.Compare(pred.GetLbound(), val)
				if err != nil {
					return false, err
				}
				cu, err := utils.Compare(pred.GetUbound(), val)
				if err != nil {
					return false, err
				}
				if cl <= 0 && cu > 0 {
					return true, nil
				}
				return false, nil
			} else {
				return false, utils.Error(errors.New("invalid range bounds"))
			}
		}
	}
	return true, nil
}
