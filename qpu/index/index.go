package index

import (
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/google/btree"
)

//Entry ...
type Entry struct {
	Value    int64
	Postings map[string]pbQPU.Object
}

//Less ...
func (x Entry) Less(than btree.Item) bool {
	return x.Value < than.(Entry).Value
}

//Index ...
type Index interface {
	put(op *pbQPU.Operation) error
	Get(p []*pbQPU.Predicate) (map[string]pbQPU.Object, bool, error)
	Update(op *pbQPU.Operation) error
}

//BTreeIndex ...
type BTreeIndex struct {
	attribute string
	lbound    int64
	ubound    int64
	entries   *btree.BTree
	state     map[string]pbQPU.Object
}

//New initializes a new BTreeIndex struct.
//It returns a pointer to the BTreeIndex struct.
func New(attr string, lb int64, ub int64) *BTreeIndex {
	return &BTreeIndex{
		attribute: attr,
		lbound:    lb,
		ubound:    ub,
		entries:   btree.New(2),
		state:     make(map[string]pbQPU.Object),
	}
}

func (i *BTreeIndex) opToEntry(op *pbQPU.Operation) Entry {
	return Entry{Value: op.Object.Attributes[i.attribute].GetInt()}
}

func (i *BTreeIndex) boundToEntry(b *pbQPU.Value) Entry {
	return Entry{Value: b.GetInt()}
}

func (i *BTreeIndex) filterIndexable(op *pbQPU.Operation) bool {
	if attrValue, ok := op.Object.Attributes[i.attribute]; ok {
		if attrValue.GetInt() > i.lbound && attrValue.GetInt() <= i.ubound {
			return true
		}
	}
	return false
}

//Update updates the index based on a given datastore operation.
//It returns any error encountered.
func (i *BTreeIndex) Update(op *pbQPU.Operation) error {
	if i.filterIndexable(op) {
		if err := i.put(op); err != nil {
			return err
		}
	}
	return nil
}

func (i *BTreeIndex) removeOldEntry(op *pbQPU.Operation) {
	if s, ok := i.state[(*op.Object).Key]; ok {
		item := i.entries.Get(Entry{Value: s.Attributes[i.attribute].GetInt()})
		delete(item.(Entry).Postings, s.Key)
	}
}

func (i *BTreeIndex) put(op *pbQPU.Operation) error {
	i.removeOldEntry(op)
	entry := i.opToEntry(op)
	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		item.(Entry).Postings[(*op.Object).Key] = *op.Object
		i.entries.ReplaceOrInsert(item)
	} else {
		entry.Postings = make(map[string]pbQPU.Object)
		entry.Postings[(*op.Object).Key] = *op.Object
		i.entries.ReplaceOrInsert(entry)
	}
	i.state[(*op.Object).Key] = *op.Object
	return nil
}

//Get performs and index lookup based on a given query predicate.
//It returns the retrieved objects and any error encountered.
func (i *BTreeIndex) Get(p []*pbQPU.Predicate) (map[string]pbQPU.Object, bool, error) {
	if p[0].Lbound.GetInt() == p[0].Ubound.GetInt() {
		entry := i.boundToEntry(p[0].Lbound)
		if i.entries.Has(entry) {
			item := i.entries.Get(entry)
			return item.(Entry).Postings, true, nil
		}
	} else {
		res := make(map[string]pbQPU.Object)
		it := func(item btree.Item) bool {
			for _, o := range item.(Entry).Postings {
				res[o.Key] = o
			}
			return true
		}
		i.entries.AscendRange(i.boundToEntry(p[0].Lbound), i.boundToEntry(p[0].Ubound), it)
		return res, true, nil
	}
	return nil, false, nil
}
