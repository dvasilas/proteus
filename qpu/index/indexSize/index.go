package indexsize

import (
	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

//Entry ...
type Entry struct {
	Value    int64
	Postings map[string]utils.Posting
}

//IndexSize ...
type IndexSize struct {
}

//New ...
func New() *IndexSize {
	return &IndexSize{}
}

//FilterIndexable ...
func (i *IndexSize) FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	log.WithFields(log.Fields{
		"attrKey": attrKey,
		"attrVal": attrVal,
		"attr":    attr,
		"lb":      lb,
		"ub":      ub,
	}).Debug("IndexSize:FilterIndexable")

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
func (i *IndexSize) OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return Entry{Value: attrVal.GetInt(), Postings: make(map[string]utils.Posting)}
}

//BoundToEntry ...
func (i *IndexSize) BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return Entry{Value: b.GetInt()}
}

//GetPostings ...
func (i *IndexSize) GetPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(Entry).Postings
}

//AppendPostings ...
func (i *IndexSize) AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(Entry).Postings[key] = p
	return entry
}

//ReducedKeyToTagKey ...
func (i *IndexSize) ReducedKeyToTagKey(k string) string {
	return k
}
