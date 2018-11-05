package indextagfloat

import (
	"strings"

	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/google/btree"
)

//Entry ...
type Entry struct {
	Key      string
	Value    float64
	Postings map[string]utils.Posting
}

//IndexTagFloat ...
type IndexTagFloat struct {
}

//New ...
func New() *IndexTagFloat {
	return &IndexTagFloat{}
}

//FilterIndexable ...
func (i *IndexTagFloat) FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	switch attrVal.Val.(type) {
	case *pbQPU.Value_Flt:
		if strings.HasPrefix(attrKey, "x-amz-meta-f-") {
			if attrVal.GetFlt() > lb.GetFlt() && attrVal.GetFlt() <= ub.GetFlt() {
				return true, strings.TrimPrefix(attrKey, "x-amz-meta-f-")
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
		return x.Value < than.(Entry).Value
	}
	return false
}

//OpToEntry ...
func (i *IndexTagFloat) OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return Entry{Key: attrKey, Value: attrVal.GetFlt(), Postings: make(map[string]utils.Posting)}
}

//BoundToEntry ...
func (i *IndexTagFloat) BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return Entry{Key: attrKey, Value: b.GetFlt()}
}

//GetPostings ...
func (i *IndexTagFloat) GetPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(Entry).Postings
}

//AppendPostings ...
func (i *IndexTagFloat) AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(Entry).Postings[key] = p
	return entry
}

//ReducedKeyToTagKey ...
func (i *IndexTagFloat) ReducedKeyToTagKey(k string) string {
	return "x-amz-meta-f-" + k
}
