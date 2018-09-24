package indexint

import (
	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/google/btree"
)

//Entry ...
type Entry struct {
	Value    int64
	Postings map[string]utils.Posting
}

//BTreeIndexI ...
type BTreeIndexI struct {
}

//New ...
func New() *BTreeIndexI {
	return &BTreeIndexI{}
}

//FilterIndexable ...
func (i *BTreeIndexI) FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	switch attrVal.Val.(type) {
	case *pbQPU.Value_Int:
		if attrKey == attr {
			if attrVal.GetInt() > lb.GetInt() && attrVal.GetInt() <= ub.GetInt() {
				return true, attrKey
			}
		}
	default:
		return false, attrKey
	}
	return false, attrKey
}

//Less ...
func (x Entry) Less(than btree.Item) bool {
	return x.Value < than.(Entry).Value
}

//OpToEntry ...
func (i *BTreeIndexI) OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return Entry{Value: attrVal.GetInt(), Postings: make(map[string]utils.Posting)}
}

//BoundToEntry ...
func (i *BTreeIndexI) BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return Entry{Value: b.GetInt()}
}

//GetPostings ...
func (i *BTreeIndexI) GetPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(Entry).Postings
}

//AppendPostings ...
func (i *BTreeIndexI) AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(Entry).Postings[key] = p
	return entry
}

//ReducedKeyToTagKey ...
func (i *BTreeIndexI) ReducedKeyToTagKey(k string) string {
	return k
}
