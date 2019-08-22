package inmemindex

import (
	"container/list"
	"sync"

	"github.com/dvasilas/proteus/src/protos"

	utils "github.com/dvasilas/proteus/src"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

// InMemIndex represents a generic B-Tree index.
// It can be used for indexing different types of attributes
// by using different implementation of the indexImplementation interface.
type InMemIndex struct {
	index         indexImplementation
	attributeName string
	attributeType pbUtils.Attribute_AttributeType
	mutex         sync.RWMutex
}

// indexImplementation represents a B-Tree index implementation for a specific attribute type.
type indexImplementation interface {
	updateEntry(btree.Item)
	lookup(lbound, ubound *pbUtils.Value) map[string]utils.ObjectState
	getEntry(*pbUtils.Attribute) btree.Item
	removeOldEntry(attr *pbUtils.Attribute, objectID string, ts pbUtils.Vectorclock) error
	print()
}

//---------------- API Functions -------------------

//New creates a new B-Tree index
func New(attrName string, attrType pbUtils.Attribute_AttributeType) (*InMemIndex, error) {
	ind := &InMemIndex{
		attributeName: attrName,
		attributeType: attrType,
	}
	switch ind.attributeType {
	case pbUtils.Attribute_S3TAGFLT:
		ind.index = newIndexFloat()
	case pbUtils.Attribute_S3TAGSTR, pbUtils.Attribute_CRDTLWWREG:
	case pbUtils.Attribute_S3TAGINT, pbUtils.Attribute_CRDTCOUNTER:
	}
	return ind, nil
}

// Update updates the index based on a given operation
func (i *InMemIndex) Update(attrOld *pbUtils.Attribute, attrNew *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	log.WithFields(log.Fields{"attr": attrNew, "obj": object}).Debug("index Put")

	i.mutex.Lock()
	if attrOld != nil {
		i.index.removeOldEntry(attrOld, object.ObjectID, ts)
	}

	if entry := i.index.getEntry(attrNew); entry != nil {
		entry.(bTreeNode).update(object, ts)
		i.index.updateEntry(entry)
	} else {
		entry := newIndexEntry(attrNew, ts, object)
		i.index.updateEntry(entry)
	}
	i.index.print()
	i.mutex.Unlock()
	return nil
}

// UpdateCatchUp updates the index based on a given object state
func (i *InMemIndex) UpdateCatchUp(attr *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	log.WithFields(log.Fields{"attr": attr, "obj": object}).Debug("index PutCatchUp")
	i.mutex.Lock()
	if entry := i.index.getEntry(attr); entry != nil {
		entry.(bTreeNode).updateCatchUp(object)
		i.index.updateEntry(entry)
	} else {
		entry := newCatchUpIndexEntry(attr, ts, object)
		i.index.updateEntry(entry)
	}
	i.index.print()
	i.mutex.Unlock()
	return nil
}

// Lookup performs a range lookup on the index and returns the result.
func (i *InMemIndex) Lookup(attrPred *pbUtils.AttributePredicate, tsPred *pbUtils.SnapshotTimePredicate) (map[string]utils.ObjectState, error) {
	log.WithFields(log.Fields{"predicate": attrPred}).Debug("index Lookup")

	i.mutex.RLock()
	res := i.index.lookup(attrPred.GetLbound(), attrPred.GetUbound())
	i.mutex.RUnlock()
	if len(res) == 0 {
		return nil, nil
	}
	return res, nil
}

//----------------- indexFloat ---------------------

// indexFloat represents a B-Tree index implementation for float64 values.
type indexFloat struct {
	tree *btree.BTree
}

func newIndexFloat() *indexFloat {
	return &indexFloat{
		tree: btree.New(2),
	}
}

func (i *indexFloat) getEntry(attr *pbUtils.Attribute) btree.Item {
	indexEntry := attrToIndexEntry(attr)
	if i.tree.Has(indexEntry) {
		return i.tree.Get(indexEntry)
	}
	return nil
}

func (i *indexFloat) updateEntry(e btree.Item) {
	i.tree.ReplaceOrInsert(e)
}

func (i *indexFloat) lookup(lbound, ubound *pbUtils.Value) map[string]utils.ObjectState {
	res := make(map[string]utils.ObjectState)
	it := func(item btree.Item) bool {
		postings := item.(bTreeNode).getPostings().Back()
		for _, obj := range postings.Value.(Posting).Objects {
			res[obj.ObjectID] = obj
		}
		return true
	}
	lb, ub := predicateToIndexEntries(lbound, ubound)
	i.tree.AscendRange(lb, ub, it)
	return res
}

// removeOldEntry removes an old attribute value from the index.
// It finds the B-Tree item which corresponds to a given attribute value
// and removes the given object from its posting list.
func (i *indexFloat) removeOldEntry(attr *pbUtils.Attribute, objectID string, ts pbUtils.Vectorclock) error {
	if entry := i.getEntry(attr); entry != nil {
		entry.(bTreeNode).updateRemove(objectID, ts)
	}
	return nil
}

func (i *indexFloat) print() {
	log.Debug("Printing index")
	it := func(item btree.Item) bool {
		log.WithFields(log.Fields{"val": item.(bTreeNode).Value}).Debug("value")
		for e := item.(bTreeNode).Postings.Front(); e != nil; e = e.Next() {
			log.WithFields(log.Fields{
				"timestamp": e.Value.(Posting).Timestamp,
			}).Debug("posting version")
			for o := range e.Value.(Posting).Objects {
				log.Debug(o)
			}
		}
		return true
	}
	i.tree.Ascend(it)
	log.Debug()
}

//---------------- btree.BTree ---------------------

// bTreeNode represents a B-Tree item for float64 values.
type bTreeNode struct {
	Value    float64
	Postings *list.List
}

// Posting ...
type Posting struct {
	Objects   map[string]utils.ObjectState
	Timestamp pbUtils.Vectorclock
}

// Less tests whether the current item is less than the given argument.
func (x bTreeNode) Less(than btree.Item) bool {
	return x.Value < than.(bTreeNode).Value
}

func (x bTreeNode) update(obj utils.ObjectState, ts pbUtils.Vectorclock) {
	log.WithFields(log.Fields{
		"timestamp": ts,
	}).Debug("posting update")
	newObjectsM := make(map[string]utils.ObjectState)
	for k, v := range x.Postings.Back().Value.(Posting).Objects {
		newObjectsM[k] = v
	}
	newObjectsM[obj.ObjectID] = obj
	x.Postings.PushBack(Posting{
		Objects:   newObjectsM,
		Timestamp: ts,
	})
	if x.Postings.Len() > 10 {
		x.Postings.Remove(x.Postings.Front())
	}
}

func (x bTreeNode) updateCatchUp(obj utils.ObjectState) {
	x.Postings.Front().Value.(Posting).Objects[obj.ObjectID] = obj
}

func (x bTreeNode) updateRemove(objectID string, ts pbUtils.Vectorclock) {
	newObjectsM := make(map[string]utils.ObjectState)
	for k, v := range x.Postings.Back().Value.(Posting).Objects {
		newObjectsM[k] = v
	}
	delete(newObjectsM, objectID)
	x.Postings.PushBack(Posting{
		Objects:   newObjectsM,
		Timestamp: ts,
	})
}

func (x bTreeNode) getPostings() *list.List {
	return x.Postings
}

//---------------- Internal Functions --------------

func predicateToIndexEntries(lb, ub *pbUtils.Value) (btree.Item, btree.Item) {
	return bTreeNode{Value: lb.GetFlt()}, bTreeNode{Value: ub.GetFlt()}
}

func newCatchUpIndexEntry(attr *pbUtils.Attribute, ts pbUtils.Vectorclock, obj utils.ObjectState) btree.Item {
	zeroTs := make(map[string]uint64)
	for k := range ts.GetVc() {
		zeroTs[k] = 0
	}
	item := bTreeNode{Value: attr.GetValue().GetFlt(), Postings: list.New()}
	posting := Posting{
		Objects:   map[string]utils.ObjectState{obj.ObjectID: obj},
		Timestamp: *protoutils.Vectorclock(zeroTs),
	}
	item.Postings.PushBack(posting)
	return item
}

func newIndexEntry(attr *pbUtils.Attribute, ts pbUtils.Vectorclock, obj utils.ObjectState) btree.Item {
	item := bTreeNode{Value: attr.GetValue().GetFlt(), Postings: list.New()}
	posting := Posting{
		Objects:   map[string]utils.ObjectState{obj.ObjectID: obj},
		Timestamp: ts,
	}
	item.Postings.PushBack(posting)
	return item
}

func attrToIndexEntry(attr *pbUtils.Attribute) btree.Item {
	return bTreeNode{Value: attr.GetValue().GetFlt()}
}
