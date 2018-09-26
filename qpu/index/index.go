package index

import (
	"errors"

	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	indexI "github.com/dimitriosvasilas/modqp/qpu/index/indexInt"
	indexTF "github.com/dimitriosvasilas/modqp/qpu/index/indexTagFloat"
	indexTS "github.com/dimitriosvasilas/modqp/qpu/index/indexTagStr"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

//Impl ...
type Impl interface {
	FilterIndexable(attrKey string, attrVal *pbQPU.Value, attr string, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string)
	OpToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item
	GetPostings(entry btree.Item) map[string]utils.Posting
	AppendPostings(entry btree.Item, key string, p utils.Posting) btree.Item
	BoundToEntry(attrKey string, b *pbQPU.Value) btree.Item
	ReducedKeyToTagKey(k string) string
}

//Index ...
type Index struct {
	Index     Impl
	attribute string
	lbound    *pbQPU.Value
	ubound    *pbQPU.Value
	entries   *btree.BTree
	state     map[string]pbQPU.Object
}

//New ...
func New(datatype string, attr string, lb string, ub string) (*Index, error) {
	i := &Index{
		attribute: attr,
		entries:   btree.New(2),
		state:     make(map[string]pbQPU.Object),
	}
	lbound, ubound, err := utils.AttrBoundStrToVal(datatype, lb, ub)
	if err != nil {
		return &Index{}, errors.New("Bounds in index configuration is not the right datatype")
	}
	i.lbound = lbound
	i.ubound = ubound
	if datatype == "float" {
		i.Index = indexTF.New()
	} else if datatype == "int" && attr == "size" {
		i.Index = indexI.New()
	} else if datatype == "string" && attr == "x-amz-meta" {
		i.Index = indexTS.New()
	} else {
		return &Index{}, errors.New("index datatype not implemented")
	}
	return i, nil
}

//Update updates the index based on a given datastore operation.
//It returns any error encountered.
func Update(i *Index, op *pbQPU.Operation) error {
	log.WithFields(log.Fields{
		"operation": op,
	}).Debug("index:Update")
	for k, v := range op.GetObject().GetAttributes() {
		if indexable, k := i.Index.FilterIndexable(k, v, i.attribute, i.lbound, i.ubound); indexable {
			if op.GetOpId() != "catchUp" {
				removeOldEntry(k, v, op.GetObject(), i)
			}
			if err := put(k, v, op.GetObject(), op.GetDataSet(), i); err != nil {
				return err
			}
		}
	}
	return nil
}

func put(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet, i *Index) error {
	entry := i.Index.OpToEntry(attrKey, attrVal)
	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		item = i.Index.AppendPostings(item, obj.Key, utils.Posting{Object: *obj, Dataset: *ds})
		i.entries.ReplaceOrInsert(item)
	} else {
		entry = i.Index.AppendPostings(entry, obj.Key, utils.Posting{Object: *obj, Dataset: *ds})
		i.entries.ReplaceOrInsert(entry)
	}
	i.state[obj.Key] = *obj
	return nil
}

//Get ...
func (i *Index) Get(p []*pbQPU.Predicate) (map[string]utils.Posting, bool, error) {
	res := make(map[string]utils.Posting)
	it := func(item btree.Item) bool {
		for _, p := range i.Index.GetPostings(item) {
			res[p.Object.GetKey()] = p
		}
		return true
	}
	i.entries.AscendRange(i.Index.BoundToEntry(p[0].GetAttribute(), p[0].Lbound), i.Index.BoundToEntry(p[0].GetAttribute(), p[0].Ubound), it)
	return res, true, nil
}

func removeOldEntry(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, i *Index) {
	if s, ok := i.state[obj.Key]; ok {
		entry := i.Index.OpToEntry(attrKey, s.Attributes[i.Index.ReducedKeyToTagKey(attrKey)])
		item := i.entries.Get(entry)
		delete(i.Index.GetPostings(item), s.Key)
	}
}
