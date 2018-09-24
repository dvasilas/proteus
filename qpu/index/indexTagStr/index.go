package indextagstr

import (
	"strings"

	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/google/btree"
)

//Entry ...
type Entry struct {
	Key      string
	Value    string
	Postings map[string]utils.Posting
}

//BTreeIndexS ...
type BTreeIndexS struct {
}

//New ...
func New() *BTreeIndexS {
	return &BTreeIndexS{}
}

//FilterIndexable ...
func (i *BTreeIndexS) FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	switch attrVal.Val.(type) {
	case *pbQPU.Value_Str:
		if strings.HasPrefix(attrKey, "x-amz-meta-") {
			if (attrVal.GetStr() > lb.GetStr() || lb.GetStr() == "any") && (attrVal.GetStr() <= ub.GetStr() || ub.GetStr() == "any") {
				return true, strings.TrimPrefix(attrKey, "x-amz-meta-")
			}
		}
	default:
		return false, attrKey
	}
	return false, attrKey
}

//Less ...
func (x Entry) Less(than btree.Item) bool {
	if x.Key < than.(Entry).Key {
		return true
	} else if x.Key == than.(Entry).Key {
		if x.Value < than.(Entry).Value {
			return true
		}
	}
	return false
}

//OpToEntry ...
func (i *BTreeIndexS) OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return Entry{Key: attrKey, Value: attrVal.GetStr(), Postings: make(map[string]utils.Posting)}
}

//BoundToEntry ...
func (i *BTreeIndexS) BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return Entry{Key: attrKey, Value: b.GetStr()}
}

//GetPostings ...
func (i *BTreeIndexS) GetPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(Entry).Postings
}

//AppendPostings ...
func (i *BTreeIndexS) AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(Entry).Postings[key] = p
	return entry
}

//ReducedKeyToTagKey ...
func (i *BTreeIndexS) ReducedKeyToTagKey(k string) string {
	return "x-amz-meta-" + k
}
