package btreeindex

import (
	"sync"

	utils "github.com/dvasilas/proteus/src"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

// BTreeIndex represents a generic B-Tree index.
// It can be used for indexing different types of attributes
// by using different implementation of the indexImplementation interface.
type BTreeIndex struct {
	index         indexImplementation
	attributeName string
	attributeType pbUtils.Attribute_AttributeType
	tree          *btree.BTree
	mutex         sync.RWMutex
}

// indexImplementation represents a B-Tree index implementation for a specific attribute type.
type indexImplementation interface {
	// opToItem parses a given operation (represented as an pbUtils.Attribute) and creates a B-Tree item.
	opToItem(*pbUtils.Attribute) btree.Item
	// getPostings retrieves the posting list of a given B-Tree item.
	getPostings(btree.Item) map[string]utils.ObjectState
	// appendPostings appends a given object to the posting list of a given B-Tree item.
	appendPostings(btree.Item, string, utils.ObjectState) btree.Item
	// boundToItem parses a given predicate range bound (represented as an attribute value)
	// and creates a B-Tree item.
	boundToItem(*pbUtils.Value) btree.Item
}

//---------------- API Functions -------------------

//New creates a new B-Tree index
func New(attrName string, attrType pbUtils.Attribute_AttributeType) (*BTreeIndex, error) {
	ind := &BTreeIndex{
		attributeName: attrName,
		attributeType: attrType,
		tree:          btree.New(2),
	}

	switch ind.attributeType {
	case pbUtils.Attribute_S3TAGFLT:
		ind.index = newS3TagFlt()
	case pbUtils.Attribute_S3TAGSTR, pbUtils.Attribute_CRDTLWWREG:
	case pbUtils.Attribute_S3TAGINT, pbUtils.Attribute_CRDTCOUNTER:
	}
	return ind, nil
}

// Put inserts an entry to the index
func (i *BTreeIndex) Put(attribute *pbUtils.Attribute, object utils.ObjectState) error {
	log.WithFields(log.Fields{"attr": attribute, "obj": object}).Debug("index Put")
	entry := i.index.opToItem(attribute)
	i.mutex.Lock()
	if i.tree.Has(entry) {
		item := i.tree.Get(entry)
		item = i.index.appendPostings(item, object.ObjectID, object)
		i.tree.ReplaceOrInsert(item)
	} else {
		entry = i.index.appendPostings(entry, object.ObjectID, object)
		i.tree.ReplaceOrInsert(entry)
	}
	i.mutex.Unlock()
	return nil
}

// Get performs a range query in the index and returns the result.
func (i *BTreeIndex) Get(predicate *pbUtils.AttributePredicate) (map[string]utils.ObjectState, error) {
	log.WithFields(log.Fields{"predicate": predicate}).Debug("index Get")
	res := make(map[string]utils.ObjectState)
	it := func(item btree.Item) bool {
		for _, posting := range i.index.getPostings(item) {
			res[posting.ObjectID] = posting
		}
		return true
	}
	i.mutex.RLock()
	i.tree.AscendRange(
		i.index.boundToItem(predicate.GetLbound()),
		i.index.boundToItem(predicate.GetUbound()),
		it,
	)
	i.mutex.RUnlock()
	if len(res) == 0 {
		return nil, nil
	}
	return res, nil
}

// RemoveOldEntry removes an old attribute value from the index.
// It finds the B-Tree item which corresponds to a given attribute value
// and removes the given object from its posting list.
func (i *BTreeIndex) RemoveOldEntry(attribute *pbUtils.Attribute, objectID string) error {
	key := i.index.opToItem(attribute)
	i.mutex.Lock()
	item := i.tree.Get(key)
	if item != nil {
		delete(i.index.getPostings(item), objectID)
		if len(i.index.getPostings(item)) == 0 {
			i.tree.Delete(item)
		}
	}
	i.mutex.Unlock()
	return nil
}

// Print ...
func (i *BTreeIndex) Print() {
	it := func(item btree.Item) bool {
		return true
	}
	i.tree.Ascend(it)
}

//---------------- Internal Functions --------------

//------------------- s3TagF -----------------------

// S3TagFltItem represents a B-Tree item for float64 values.
type S3TagFltItem struct {
	Value    float64
	Postings map[string]utils.ObjectState
}

// Less tests whether the current item is less than the given argument.
func (x S3TagFltItem) Less(than btree.Item) bool {
	return x.Value < than.(S3TagFltItem).Value
}

// s3TagFlt represents a B-Tree index implementation for float64 values.
type s3TagFlt struct {
}

func newS3TagFlt() *s3TagFlt {
	return &s3TagFlt{}
}

func (i *s3TagFlt) opToItem(attr *pbUtils.Attribute) btree.Item {
	return S3TagFltItem{Value: attr.GetValue().GetFlt(), Postings: make(map[string]utils.ObjectState)}
}

func (i *s3TagFlt) boundToItem(b *pbUtils.Value) btree.Item {
	return S3TagFltItem{Value: b.GetFlt()}
}

func (i *s3TagFlt) getPostings(item btree.Item) map[string]utils.ObjectState {
	return item.(S3TagFltItem).Postings
}

func (i *s3TagFlt) appendPostings(item btree.Item, key string, obj utils.ObjectState) btree.Item {
	item.(S3TagFltItem).Postings[key] = obj
	return item
}
