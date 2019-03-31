package btreeindex

import (
	"errors"
	"strings"

	utils "github.com/dvasilas/proteus"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

//BTreeIndex ...
type BTreeIndex struct {
	impl    Implementation
	entries *btree.BTree
}

//Implementation ...
type Implementation interface {
	filterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string)
	opToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item
	getPostings(entry btree.Item) map[string]utils.Posting
	appendPostings(entry btree.Item, key string, p utils.Posting) btree.Item
	boundToEntry(attrKey string, b *pbQPU.Value) btree.Item
	reducedKeyToTagKey(k string) string
}

//New ...
func New(datatype string) (*BTreeIndex, error) {
	i := BTreeIndex{
		entries: btree.New(2),
	}

	switch datatype {
	case "s3tagF":
		i.impl = newS3TagF()
	case "s3tagStr":
		i.impl = newS3TagStr()
	case "size":
		i.impl = newSizeIndex()
	case "mapCRDTK":
		i.impl = newMapCrdtKeysIndex()
	default:
		return &BTreeIndex{}, errors.New("index datatype not implemented")
	}
	return &i, nil
}

//Put ...
func (i *BTreeIndex) Put(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet) error {
	log.WithFields(log.Fields{
		"attrKey": attrKey,
		"attrVal": attrVal,
		"obj":     obj,
	}).Debug("index Put")

	entry := i.impl.opToEntry(attrKey, attrVal)

	log.WithFields(log.Fields{
		"entry": entry,
	}).Debug("index Put")

	if i.entries.Has(entry) {
		item := i.entries.Get(entry)
		item = i.impl.appendPostings(item, obj.Key, utils.Posting{Object: *obj, Dataset: *ds})
		i.entries.ReplaceOrInsert(item)
	} else {
		entry = i.impl.appendPostings(entry, obj.Key, utils.Posting{Object: *obj, Dataset: *ds})

		log.WithFields(log.Fields{
			"entry": entry,
		}).Debug("index Put")

		i.entries.ReplaceOrInsert(entry)
	}
	return nil
}

//Get ...
func (i *BTreeIndex) Get(p []*pbQPU.AttributePredicate) (map[string]utils.Posting, bool, error) {
	res := make(map[string]utils.Posting)
	it := func(item btree.Item) bool {
		for _, p := range i.impl.getPostings(item) {
			res[p.Object.GetKey()] = p
		}
		return true
	}
	i.entries.AscendRange(i.impl.boundToEntry(p[0].GetAttribute(), p[0].Lbound), i.impl.boundToEntry(p[0].GetAttribute(), p[0].Ubound), it)
	if len(res) == 0 {
		return nil, false, nil
	}
	return res, true, nil
}

//RemoveOldEntry ...
func (i *BTreeIndex) RemoveOldEntry(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, state map[string]pbQPU.Object) error {
	if s, ok := state[obj.Key]; ok {
		entry := i.impl.opToEntry(attrKey, s.Attributes[i.impl.reducedKeyToTagKey(attrKey)])
		item := i.entries.Get(entry)
		delete(i.impl.getPostings(item), s.Key)
	}
	return nil
}

//FilterIndexable ...
func (i *BTreeIndex) FilterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	return i.impl.filterIndexable(attrKey, attrVal, lb, ub)
}

//------------------- s3TagF -----------------------

//S3TagFEntry ...
type S3TagFEntry struct {
	Key      string
	Value    float64
	Postings map[string]utils.Posting
}

type s3TagF struct {
}

func newS3TagF() *s3TagF {
	return &s3TagF{}
}

func (i *s3TagF) filterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
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
func (x S3TagFEntry) Less(than btree.Item) bool {
	if x.Key < than.(S3TagFEntry).Key {
		return true
	} else if x.Key == than.(S3TagFEntry).Key {
		return x.Value < than.(S3TagFEntry).Value
	}
	return false
}

func (i *s3TagF) opToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return S3TagFEntry{Key: attrKey, Value: attrVal.GetFlt(), Postings: make(map[string]utils.Posting)}
}

func (i *s3TagF) boundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return S3TagFEntry{Key: attrKey, Value: b.GetFlt()}
}

func (i *s3TagF) getPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(S3TagFEntry).Postings
}

func (i *s3TagF) appendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(S3TagFEntry).Postings[key] = p
	return entry
}

func (i *s3TagF) reducedKeyToTagKey(k string) string {
	return "x-amz-meta-f-" + k
}

//------------------ s3TagStr ----------------------

//S3TagStrEntry ...
type S3TagStrEntry struct {
	Key      string
	Value    string
	Postings map[string]utils.Posting
}

type s3TagStr struct {
}

func newS3TagStr() *s3TagStr {
	return &s3TagStr{}
}

//FilterIndexable ...
func (i *s3TagStr) filterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	switch attrVal.Val.(type) {
	case *pbQPU.Value_Str:
		if strings.HasPrefix(attrKey, "x-amz-meta-") {
			if (attrVal.GetStr() > lb.GetStr() || lb.GetStr() == "any") && (attrVal.GetStr() <= ub.GetStr() || ub.GetStr() == "any") {
				return true, strings.TrimPrefix(attrKey, "x-amz-meta-")
			}
		}
	default:
		return false, attrKey
	}
	return false, attrKey
}

//Less ...
func (x S3TagStrEntry) Less(than btree.Item) bool {
	if x.Key < than.(S3TagStrEntry).Key {
		return true
	} else if x.Key == than.(S3TagStrEntry).Key {
		if x.Value < than.(S3TagStrEntry).Value {
			return true
		}
	}
	return false
}

func (i *s3TagStr) opToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return S3TagStrEntry{Key: attrKey, Value: attrVal.GetStr(), Postings: make(map[string]utils.Posting)}
}

func (i *s3TagStr) boundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return S3TagStrEntry{Key: attrKey, Value: b.GetStr()}
}

func (i *s3TagStr) getPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(S3TagStrEntry).Postings
}

func (i *s3TagStr) appendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(S3TagStrEntry).Postings[key] = p
	return entry
}

func (i *s3TagStr) reducedKeyToTagKey(k string) string {
	return "x-amz-meta-" + k
}

//-------------------- size ------------------------

//SizeEntry ...
type SizeEntry struct {
	Value    int64
	Postings map[string]utils.Posting
}

type sizeIndex struct {
}

func newSizeIndex() *sizeIndex {
	return &sizeIndex{}
}

func (i *sizeIndex) filterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	switch attrVal.Val.(type) {
	case *pbQPU.Value_Int:
		if attrVal.GetInt() > lb.GetInt() && attrVal.GetInt() <= ub.GetInt() {
			return true, attrKey
		}
	default:
		return false, attrKey
	}
	return false, attrKey
}

//Less ...
func (x SizeEntry) Less(than btree.Item) bool {
	return x.Value < than.(SizeEntry).Value
}

func (i *sizeIndex) opToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return SizeEntry{Value: attrVal.GetInt(), Postings: make(map[string]utils.Posting)}
}

func (i *sizeIndex) boundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return SizeEntry{Value: b.GetInt()}
}

func (i *sizeIndex) getPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(SizeEntry).Postings
}

func (i *sizeIndex) appendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(SizeEntry).Postings[key] = p
	return entry
}

func (i *sizeIndex) reducedKeyToTagKey(k string) string {
	return k
}

//----------------- mapCRDTkeys --------------------

//mCRDtkEntry ...
type mCRDtkEntry struct {
	Key      string
	Value    string
	Postings map[string]utils.Posting
}

type mapCrdtKeysIndex struct {
}

func newMapCrdtKeysIndex() *mapCrdtKeysIndex {
	return &mapCrdtKeysIndex{}
}

//FilterIndexable ...
func (i *mapCrdtKeysIndex) filterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	if (attrKey > lb.GetStr() || lb.GetStr() == "any") && (attrKey <= ub.GetStr() || ub.GetStr() == "any") {
		return true, attrKey
	}
	return false, attrKey
}

//Less ...
func (x mCRDtkEntry) Less(than btree.Item) bool {
	if x.Value < than.(mCRDtkEntry).Value {
		return true
	}
	return false
}

func (i *mapCrdtKeysIndex) opToEntry(attrKey string, attrVal *pbQPU.Value) btree.Item {
	return mCRDtkEntry{Key: "MapKey", Value: attrVal.GetStr(), Postings: make(map[string]utils.Posting)}
}

func (i *mapCrdtKeysIndex) boundToEntry(attrKey string, b *pbQPU.Value) btree.Item {
	return mCRDtkEntry{Key: "MapKey", Value: b.GetStr()}
}

func (i *mapCrdtKeysIndex) getPostings(entry btree.Item) map[string]utils.Posting {
	return entry.(mCRDtkEntry).Postings
}

func (i *mapCrdtKeysIndex) appendPostings(entry btree.Item, key string, p utils.Posting) btree.Item {
	entry.(mCRDtkEntry).Postings[key] = p
	return entry
}

func (i *mapCrdtKeysIndex) reducedKeyToTagKey(k string) string {
	return k
}
