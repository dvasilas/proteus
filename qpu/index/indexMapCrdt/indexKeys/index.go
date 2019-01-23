package indexmapcrdtkeys

import (
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

//IndexMapCrdtKeys ...
type IndexMapCrdtKeys struct {
}

//New ...
func New() *IndexMapCrdtKeys {
	return &IndexMapCrdtKeys{}
}

//FilterIndexable ...
func (i *IndexMapCrdtKeys) FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	if (attrKey > lb.GetStr() || lb.GetStr() == "any") && (attrKey <= ub.GetStr() || ub.GetStr() == "any") {
		return true, attrKey
	}
	return false, attrKey
}

//Less ...
func (x Entry) Less(than btree.Item) bool {
	if x.Value < than.(Entry).Value {
		return true
	}
	return false
}

//OpToEntry ...
func (i *IndexMapCrdtKeys) OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return Entry{Key: "MapKey", Value: attrVal.GetStr(), Postings: make(map[string]utils.Posting)}
}

//BoundToEntry ...
func (i *IndexMapCrdtKeys) BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return Entry{Key: "MapKey", Value: b.GetStr()}
}

//GetPostings ...
func (i *IndexMapCrdtKeys) GetPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(Entry).Postings
}

//AppendPostings ...
func (i *IndexMapCrdtKeys) AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(Entry).Postings[key] = p
	return entry
}

//ReducedKeyToTagKey ...
func (i *IndexMapCrdtKeys) ReducedKeyToTagKey(k string) string {
	return k
}
