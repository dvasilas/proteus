package index

import (
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/google/btree"
)

//Entry ...
type Entry struct {
	Value    int64
	Postings []pbQPU.Object
}

//Less ...
func (x Entry) Less(than btree.Item) bool {
	return x.Value < than.(Entry).Value
}

//Index ...
type Index interface {
	put(op *pbQPU.Operation) error
	Get(p []*pbQPU.Predicate) ([]pbQPU.Object, bool, error)
	Update(op *pbQPU.Operation)
}

//BTreeIndex ...
type BTreeIndex struct {
	attribute string
	lbound    int64
	ubound    int64
	entries   *btree.BTree
}

//New ...
func New(attr string, lb int64, ub int64) *BTreeIndex {
	return &BTreeIndex{
		attribute: attr,
		lbound:    lb,
		ubound:    ub,
		entries:   btree.New(2),
	}
}

//opToIndexKey ...
func (i *BTreeIndex) opToEntry(op *pbQPU.Operation) Entry {
	return Entry{Value: op.Object.Attributes[i.attribute].GetInt()}
}

func (i *BTreeIndex) boundToEntry(b *pbQPU.Value) Entry {
	return Entry{Value: b.GetInt()}
}

//FilterIndexable ...
func (i *BTreeIndex) filterIndexable(op *pbQPU.Operation) bool {
	if attrValue, ok := op.Object.Attributes[i.attribute]; ok {
		if attrValue.GetInt() > i.lbound && attrValue.GetInt() <= i.ubound {
			return true
		}
	}
	return false
}

//Update ...
func (i *BTreeIndex) Update(op *pbQPU.Operation) {
	if i.filterIndexable(op) {
		i.put(op)
	}
}

//Put ...
func (i *BTreeIndex) put(op *pbQPU.Operation) error {
	entry := i.opToEntry(op)
	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		newPostings := append(item.(Entry).Postings, *op.Object)
		entry.Postings = newPostings
		i.entries.ReplaceOrInsert(entry)
	} else {
		entry.Postings = append([]pbQPU.Object{}, *op.Object)
		i.entries.ReplaceOrInsert(entry)
	}
	return nil
}

//Get ...
func (i *BTreeIndex) Get(p []*pbQPU.Predicate) ([]pbQPU.Object, bool, error) {
	if p[0].Lbound.GetInt() == p[0].Ubound.GetInt() {
		entry := i.boundToEntry(p[0].Lbound)
		if i.entries.Has(entry) {
			item := i.entries.Get(entry)
			return item.(Entry).Postings, true, nil
		}
	} else {
		res := []pbQPU.Object{}
		it := func(item btree.Item) bool {
			for _, o := range item.(Entry).Postings {
				res = append(res, o)
			}
			return true
		}
		i.entries.AscendRange(i.boundToEntry(p[0].Lbound), i.boundToEntry(p[0].Ubound), it)
		return res, true, nil
	}
	return nil, false, nil
}
