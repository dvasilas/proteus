package index

import (
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/google/btree"
)

//Posting ...
type Posting struct {
	Object  pbQPU.Object
	Dataset pbQPU.DataSet
}

//EntryI ...
type EntryI struct {
	Value    int64
	Postings map[string]Posting
}

//Less ...
func (x EntryI) Less(than btree.Item) bool {
	return x.Value < than.(EntryI).Value
}

//EntryS ...
type EntryS struct {
	Value    string
	Postings map[string]Posting
}

//Less ...
func (x EntryS) Less(than btree.Item) bool {
	if x.Value < than.(EntryS).Value {
		return true
	}
	return false
}

//Index ...
type Index interface {
	filterIndexable(op *pbQPU.Operation) bool
	put(op *pbQPU.Operation) error
	Get(p []*pbQPU.Predicate) (map[string]Posting, bool, error)
}

//BTreeIndexI ...
type BTreeIndexI struct {
	attribute string
	lbound    int64
	ubound    int64
	entries   *btree.BTree
	state     map[string]pbQPU.Object
}

//BTreeIndexS ...
type BTreeIndexS struct {
	attribute string
	lbound    string
	ubound    string
	entries   *btree.BTree
	state     map[string]pbQPU.Object
}

//NewIndexI initializes a new BTreeIndex struct.
//It returns a pointer to the BTreeIndex struct.
func NewIndexI(attr string, lb int64, ub int64) *BTreeIndexI {
	return &BTreeIndexI{
		attribute: attr,
		lbound:    lb,
		ubound:    ub,
		entries:   btree.New(2),
		state:     make(map[string]pbQPU.Object),
	}
}

//NewIndexS ...
func NewIndexS(attr string, lb string, ub string) *BTreeIndexS {
	return &BTreeIndexS{
		attribute: attr,
		lbound:    lb,
		ubound:    ub,
		entries:   btree.New(2),
		state:     make(map[string]pbQPU.Object),
	}
}

func (i *BTreeIndexI) opToEntry(op *pbQPU.Operation) EntryI {
	return EntryI{Value: op.Object.Attributes[i.attribute].GetInt()}
}

func (i *BTreeIndexI) boundToEntry(b *pbQPU.Value) EntryI {
	return EntryI{Value: b.GetInt()}
}

func (i *BTreeIndexS) opToEntry(op *pbQPU.Operation) EntryS {
	return EntryS{Value: op.Object.Attributes[i.attribute].GetName()}
}

func (i *BTreeIndexS) boundToEntry(b *pbQPU.Value) EntryS {
	return EntryS{Value: b.GetName()}
}

func (i *BTreeIndexI) filterIndexable(op *pbQPU.Operation) bool {
	if attrValue, ok := op.Object.Attributes[i.attribute]; ok {
		if attrValue.GetInt() > i.lbound && attrValue.GetInt() <= i.ubound {
			return true
		}
	}
	return false
}

func (i *BTreeIndexS) filterIndexable(op *pbQPU.Operation) bool {
	if attrValue, ok := op.Object.Attributes[i.attribute]; ok {
		if (attrValue.GetName() > i.lbound || i.lbound == "any") && (attrValue.GetName() <= i.ubound || i.ubound == "any") {
			return true
		}
	}
	return false
}

//Update updates the index based on a given datastore operation.
//It returns any error encountered.
func Update(i Index, op *pbQPU.Operation) error {
	if i.filterIndexable(op) {
		if err := i.put(op); err != nil {
			return err
		}
	}
	return nil
}

func (i *BTreeIndexI) removeOldEntry(op *pbQPU.Operation) {
	if s, ok := i.state[(*op.Object).Key]; ok {
		item := i.entries.Get(EntryI{Value: s.Attributes[i.attribute].GetInt()})
		delete(item.(EntryI).Postings, s.Key)
	}
}

func (i *BTreeIndexS) removeOldEntry(op *pbQPU.Operation) {
	if s, ok := i.state[(*op.Object).Key]; ok {
		item := i.entries.Get(EntryS{Value: s.Attributes[i.attribute].GetName()})
		delete(item.(EntryS).Postings, s.Key)
	}
}

func (i *BTreeIndexI) put(op *pbQPU.Operation) error {
	i.removeOldEntry(op)
	entry := i.opToEntry(op)
	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		item.(EntryI).Postings[(*op.Object).Key] = Posting{Object: *op.GetObject(), Dataset: *op.GetDataSet()}
		i.entries.ReplaceOrInsert(item)
	} else {
		entry.Postings = make(map[string]Posting)
		entry.Postings[(*op.Object).Key] = Posting{Object: *op.GetObject(), Dataset: *op.GetDataSet()}
		i.entries.ReplaceOrInsert(entry)
	}
	i.state[(*op.Object).Key] = *op.Object
	return nil
}

func (i *BTreeIndexS) put(op *pbQPU.Operation) error {
	i.removeOldEntry(op)
	entry := i.opToEntry(op)
	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		item.(EntryS).Postings[(*op.Object).Key] = Posting{Object: *op.GetObject(), Dataset: *op.GetDataSet()}
		i.entries.ReplaceOrInsert(item)
	} else {
		entry.Postings = make(map[string]Posting)
		entry.Postings[(*op.Object).Key] = Posting{Object: *op.GetObject(), Dataset: *op.GetDataSet()}
		i.entries.ReplaceOrInsert(entry)
	}
	i.state[(*op.Object).Key] = *op.Object
	return nil
}

//Get performs and index lookup based on a given query predicate.
//It returns the retrieved objects and any error encountered.
func (i *BTreeIndexI) Get(p []*pbQPU.Predicate) (map[string]Posting, bool, error) {
	if p[0].Lbound.GetInt() == p[0].Ubound.GetInt() {
		entry := i.boundToEntry(p[0].Lbound)
		if i.entries.Has(entry) {
			item := i.entries.Get(entry)
			return item.(EntryI).Postings, true, nil
		}
	} else {
		res := make(map[string]Posting)
		it := func(item btree.Item) bool {
			for _, p := range item.(EntryI).Postings {
				res[p.Object.GetKey()] = p
			}
			return true
		}
		i.entries.AscendRange(i.boundToEntry(p[0].Lbound), i.boundToEntry(p[0].Ubound), it)
		return res, true, nil
	}
	return nil, false, nil
}

//Get ...
func (i *BTreeIndexS) Get(p []*pbQPU.Predicate) (map[string]Posting, bool, error) {
	if p[0].Lbound.GetName() == p[0].Ubound.GetName() {
		entry := i.boundToEntry(p[0].Lbound)
		if i.entries.Has(entry) {
			item := i.entries.Get(entry)
			return item.(EntryS).Postings, true, nil
		}
	} else {
		res := make(map[string]Posting)
		it := func(item btree.Item) bool {
			for _, p := range item.(EntryS).Postings {
				res[p.Object.GetKey()] = p
			}
			return true
		}
		i.entries.AscendRange(i.boundToEntry(p[0].Lbound), i.boundToEntry(p[0].Ubound), it)
		return res, true, nil
	}
	return nil, false, nil
}
