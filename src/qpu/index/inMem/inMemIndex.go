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
}

// indexImplementation represents a B-Tree index implementation for a specific attribute type.
type indexImplementation interface {
	update(*pbUtils.Attribute, *pbUtils.Attribute, utils.ObjectState, pbUtils.Vectorclock) error
	updateCatchUp(*pbUtils.Attribute, utils.ObjectState, pbUtils.Vectorclock) error
	lookup(*pbUtils.AttributePredicate, *pbUtils.SnapshotTimePredicate) (map[string]utils.ObjectState, error)
	print()
}

//---------------- API Functions -------------------

//New creates a new in-memory index
func New(attrName string, attrType pbUtils.Attribute_AttributeType) (*InMemIndex, error) {
	ind := &InMemIndex{
		attributeName: attrName,
		attributeType: attrType,
	}
	ind.index = newBTreeIndex(ind.attributeType)
	return ind, nil
}

// Update updates the index based on a given operation
func (i *InMemIndex) Update(attrOld *pbUtils.Attribute, attrNew *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	log.WithFields(log.Fields{"attrOld": attrOld, "attrNew": attrNew}).Debug("index:update")
	return i.index.update(attrOld, attrNew, object, ts)
}

// UpdateCatchUp updates the index based on a given object state
func (i *InMemIndex) UpdateCatchUp(attr *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	return i.index.updateCatchUp(attr, object, ts)
}

// Lookup performs a range lookup on the index and returns the result.
func (i *InMemIndex) Lookup(attrPred *pbUtils.AttributePredicate, tsPred *pbUtils.SnapshotTimePredicate) (map[string]utils.ObjectState, error) {
	return i.index.lookup(attrPred, tsPred)
}

//------- indexImplementation interface ------------

// bTreeIndex implements indexImplementation
type bTreeIndex struct {
	tree  *btree.BTree
	mutex sync.RWMutex
	entry indexEntry
}

func newBTreeIndex(t pbUtils.Attribute_AttributeType) *bTreeIndex {
	index := &bTreeIndex{tree: btree.New(2)}
	switch t {
	case pbUtils.Attribute_S3TAGFLT:
		index.entry = newIndexFloat()
	case pbUtils.Attribute_S3TAGSTR, pbUtils.Attribute_CRDTLWWREG:
	case pbUtils.Attribute_S3TAGINT, pbUtils.Attribute_CRDTCOUNTER:
		index.entry = newIndexInt()
	}
	return index
}

func (i *bTreeIndex) update(attrOld *pbUtils.Attribute, attrNew *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {

	i.mutex.Lock()
	if attrOld != nil {
		if indexEntry, found := i.getIndexEntry(attrOld); found {
			indexEntry.removeObjFromEntry(object.ObjectID, ts)
		}
	}
	if attrNew != nil {
		if indexEntry, found := i.getIndexEntry(attrNew); found {
			indexEntry.newVersion(object, ts)
			i.updateIndexEntry(indexEntry)
		} else {
			indexEntry := i.newIndexEntry(attrNew, ts, object)
			i.updateIndexEntry(indexEntry)
		}
	}
	//i.print()
	i.mutex.Unlock()
	return nil
}

func (i *bTreeIndex) updateCatchUp(attr *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	i.mutex.Lock()
	if indexEntry, found := i.getIndexEntry(attr); found {
		indexEntry.updateFirstVersion(object)
		i.updateIndexEntry(indexEntry)
	} else {
		indexEntry = i.newCatchUpIndexEntry(attr, ts, object)
		i.updateIndexEntry(indexEntry)
	}
	i.mutex.Unlock()
	//i.print()
	return nil
}

func (i *bTreeIndex) lookup(attrPred *pbUtils.AttributePredicate, tsPred *pbUtils.SnapshotTimePredicate) (map[string]utils.ObjectState, error) {
	res := make(map[string]utils.ObjectState)
	it := func(node btree.Item) bool {
		postings := node.(treeNode).getLatestVersion()
		for _, obj := range postings.Objects {
			res[obj.ObjectID] = obj
		}
		return true
	}
	lb, ub := i.entry.predicateToIndexEntries(attrPred.GetLbound(), attrPred.GetUbound())

	i.mutex.RLock()
	i.tree.AscendRange(lb, ub, it)
	i.mutex.RUnlock()

	if len(res) == 0 {
		return nil, nil
	}
	return res, nil
}

func (i *bTreeIndex) newIndexEntry(attr *pbUtils.Attribute, ts pbUtils.Vectorclock, obj utils.ObjectState) btree.Item {
	item := i.entry.newIndexEntry(attr)
	posting := Posting{
		Objects:   map[string]utils.ObjectState{obj.ObjectID: obj},
		Timestamp: ts,
	}
	item.createNewVersion(posting)
	return item
}

func (i *bTreeIndex) newCatchUpIndexEntry(attr *pbUtils.Attribute, ts pbUtils.Vectorclock, obj utils.ObjectState) treeNode {
	zeroTs := make(map[string]uint64)
	for k := range ts.GetVc() {
		zeroTs[k] = 0
	}
	entry := i.entry.newIndexEntry(attr)
	posting := Posting{
		Objects:   map[string]utils.ObjectState{obj.ObjectID: obj},
		Timestamp: *protoutils.Vectorclock(zeroTs),
	}
	entry.createNewVersion(posting)
	return entry
}

func (i *bTreeIndex) getIndexEntry(attr *pbUtils.Attribute) (treeNode, bool) {
	indexEntry := i.entry.attrToIndexEntry(attr)
	if i.tree.Has(indexEntry) {
		return i.tree.Get(indexEntry).(treeNode), true
	}
	return treeNode{}, false
}

func (i *bTreeIndex) updateIndexEntry(e btree.Item) {
	i.tree.ReplaceOrInsert(e)
}

func (i *bTreeIndex) print() {
	log.Debug("Printing index")
	it := func(item btree.Item) bool {
		if item != nil {
			log.WithFields(log.Fields{"val": item.(treeNode).Value}).Debug("value")
			for e := item.(treeNode).Postings.Front(); e != nil; e = e.Next() {
				log.WithFields(log.Fields{"timestamp": e.Value.(Posting).Timestamp}).Debug("posting list version")
				for o := range e.Value.(Posting).Objects {
					log.Debug("- ", o)
				}
			}
		}
		return true
	}
	i.tree.Ascend(it)
	log.Debug()
}

//------------ indexEntry interface ----------------

type indexEntry interface {
	newIndexEntry(*pbUtils.Attribute) treeNode
	attrToIndexEntry(attr *pbUtils.Attribute) btree.Item
	predicateToIndexEntries(lb, ub *pbUtils.Value) (btree.Item, btree.Item)
}

// indexFloat implements indexEntry
type indexFloat struct {
}

// indexInt implements indexEntry
type indexInt struct {
}

func newIndexFloat() indexFloat {
	return indexFloat{}
}
func newIndexInt() indexInt {
	return indexInt{}
}

func (i indexFloat) newIndexEntry(attr *pbUtils.Attribute) treeNode {
	return treeNode{Value: valueFloat{Val: attr.GetValue().GetFlt()}, Postings: list.New()}
}
func (i indexFloat) attrToIndexEntry(attr *pbUtils.Attribute) btree.Item {
	return treeNode{Value: valueFloat{Val: attr.GetValue().GetFlt()}}
}
func (i indexFloat) predicateToIndexEntries(lb, ub *pbUtils.Value) (btree.Item, btree.Item) {
	return treeNode{Value: valueFloat{Val: lb.GetFlt()}}, treeNode{Value: valueFloat{Val: ub.GetFlt()}}
}

func (i indexInt) newIndexEntry(attr *pbUtils.Attribute) treeNode {
	return treeNode{Value: valueInt{Val: attr.GetValue().GetInt()}, Postings: list.New()}
}
func (i indexInt) attrToIndexEntry(attr *pbUtils.Attribute) btree.Item {
	return treeNode{Value: valueInt{Val: attr.GetValue().GetInt()}}
}
func (i indexInt) predicateToIndexEntries(lb, ub *pbUtils.Value) (btree.Item, btree.Item) {
	return treeNode{Value: valueInt{Val: lb.GetInt()}}, treeNode{Value: valueInt{Val: ub.GetInt()}}
}

//------------ btree.Item interface ----------------

// treeNode implements btree.Item (need to implement Less)
type treeNode struct {
	Value    comparable
	Postings *list.List
}

// Posting ...
type Posting struct {
	Objects   map[string]utils.ObjectState
	Timestamp pbUtils.Vectorclock
}

func (n treeNode) Less(than btree.Item) bool {
	return n.Value.less(than.(treeNode).Value)
}

func (n treeNode) clodeLatestVerion() map[string]utils.ObjectState {
	newObjMap := make(map[string]utils.ObjectState)
	for k, v := range n.getLatestVersion().Objects {
		newObjMap[k] = v
	}
	return newObjMap
}

func (n treeNode) newVersion(obj utils.ObjectState, ts pbUtils.Vectorclock) {
	objMap := n.clodeLatestVerion()
	objMap[obj.ObjectID] = obj
	n.createNewVersion(Posting{
		Objects:   objMap,
		Timestamp: ts,
	})
	n.trimVersions()
}

func (n treeNode) trimVersions() {
	if n.Postings.Len() > 10 {
		n.Postings.Remove(n.Postings.Front())
	}
}

func (n treeNode) updateFirstVersion(obj utils.ObjectState) {
	n.Postings.Front().Value.(Posting).Objects[obj.ObjectID] = obj
}

func (n treeNode) createNewVersion(p Posting) {
	n.Postings.PushBack(p)
}

func (n treeNode) getLatestVersion() Posting {
	return n.Postings.Back().Value.(Posting)
}

func (n treeNode) removeObjFromEntry(objectID string, ts pbUtils.Vectorclock) {
	objMap := n.clodeLatestVerion()
	delete(objMap, objectID)
	n.createNewVersion(
		Posting{
			Objects:   objMap,
			Timestamp: ts,
		})
}

// ------------ comparable interface ----------------

type comparable interface {
	less(comparable) bool
}

// valueFloat implements comparable
type valueFloat struct {
	Val float64
}

// valueInt implements comparable
type valueInt struct {
	Val int64
}

func (x valueInt) less(than comparable) bool {
	return x.Val < than.(valueInt).Val
}

func (x valueFloat) less(than comparable) bool {
	return x.Val < than.(valueFloat).Val
}
