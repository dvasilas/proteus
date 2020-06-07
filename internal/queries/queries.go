package queries

import (
	"errors"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	// "github.com/prometheus/common/log"
)

// NewQuerySubscribe ...
func NewQuerySubscribe(table string, projection []string, isNull []string, isNotNull []string) libqpu.InternalQuery {
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

	return libqpu.InternalQuery{
		Q: libqpu.QueryInternal(
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
func NewQuerySnapshot(table string, projection []string, isNull []string, isNotNull []string, limit int64) libqpu.InternalQuery {
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

	return libqpu.InternalQuery{
		Q: libqpu.QueryInternal(
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
func NewQuerySnapshotAndSubscribe(table string, projection []string, isNull []string, isNotNull []string) libqpu.InternalQuery {
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

	return libqpu.InternalQuery{
		Q: libqpu.QueryInternal(
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
func QueryType(query libqpu.InternalQuery) (bool, bool) {
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
func SatisfiesPredicate(logOp libqpu.LogOperation, query libqpu.InternalQuery) (bool, error) {
	if query.GetTable() != logOp.GetTable() {
		return false, nil
	}

	for _, pred := range query.GetPredicate() {
		attributes := logOp.GetAttributes()
		if attributes == nil {
			return false, libqpu.Error("logOperation state not accessible")
		}
		_, found := attributes[pred.GetAttr().GetAttrKey()]
		switch pred.GetType() {
		case qpu.AttributePredicate_ISNULL:
			return !found, nil
		case qpu.AttributePredicate_ISNOTNULL:
			return found, nil
		case qpu.AttributePredicate_RANGE:
			panic(errors.New("RANGE check not implemented"))
		}
	}
	return true, nil
}
