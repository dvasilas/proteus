package cache

import (
	"container/list"
	"fmt"
	"strconv"

	pb "github.com/dimitriosvasilas/modqp/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
)

//Cache ...
type Cache struct {
	MaxEntries int
	ll         *list.List
	items      map[string]*list.Element
	OnEvict    func(key []*pbQPU.Predicate, value []CachedValue)
}

type entry struct {
	key   []*pbQPU.Predicate
	value []CachedValue
}

//CachedValue ...
type CachedValue struct {
	Object  pbQPU.Object
	Dataset pbQPU.DataSet
}

//New initializes a new Cache struct.
//It returns a pointer to the Cache struct.
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[string]*list.Element),
	}
}

//StoreAndRespond stores an entry in the cache and forwards then this object through the given stream.
//It returns any error encountered.
func (c *Cache) StoreAndRespond(obj *pbQPU.Object, ds *pbQPU.DataSet, in []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	if err := c.put(in, *obj, *ds); err != nil {
		return err
	}
	return stream.Send(&pb.QueryResultStream{
		Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
		Dataset: ds,
	})
}

func (c *Cache) put(query []*pbQPU.Predicate, obj pbQPU.Object, ds pbQPU.DataSet) error {
	if c.items == nil {
		c.ll = list.New()
		c.items = make(map[string]*list.Element)
	}
	key := predicateToKey(query)
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		item.Value.(*entry).value = append(item.Value.(*entry).value, CachedValue{Object: obj, Dataset: ds})
	} else {
		item := c.ll.PushFront(&entry{key: query, value: []CachedValue{CachedValue{Object: obj, Dataset: ds}}})
		c.items[key] = item
		if c.ll.Len() > c.MaxEntries {
			c.evict()
		}
	}
	return nil
}

func (c *Cache) evict() error {
	if c.items == nil {
		return nil
	}
	item := c.ll.Back()
	if item != nil {
		c.ll.Remove(item)
		ee := item.Value.(*entry)
		key := predicateToKey(ee.key)
		delete(c.items, key)
		if c.OnEvict != nil {
			c.OnEvict(ee.key, ee.value)
		}
	}
	return nil
}

//Get retrieves a cached entry based on the given query predicate.
//It returns the cached objects stored in that entry.
func (c *Cache) Get(p []*pbQPU.Predicate) ([]CachedValue, bool) {
	if c.items == nil {
		return nil, false
	}
	key := predicateToKey(p)
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		return item.Value.(*entry).value, true
	}
	return nil, false
}

func (c *Cache) print() {
	for e := c.ll.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}

func predicateToKey(p []*pbQPU.Predicate) string {
	entryKey := ""
	for i, pp := range p {
		switch pp.Lbound.Val.(type) {
		case *pbQPU.Value_Int:
			entryKey += pp.Attribute + "/" + strconv.FormatInt(pp.Lbound.GetInt(), 10) + "/" + strconv.FormatInt(pp.Ubound.GetInt(), 10)
		case *pbQPU.Value_Str:
			entryKey += pp.Attribute + "/" + pp.Lbound.GetStr() + "/" + pp.Ubound.GetStr()
		case *pbQPU.Value_Flt:
			entryKey += pp.Attribute + "/" + strconv.FormatFloat(float64(pp.Lbound.GetFlt()), 'E', -1, 32) + "/" + strconv.FormatFloat(float64(pp.Ubound.GetFlt()), 'E', -1, 32)
		}
		if i < len(p)-1 {
			entryKey += "&"
		}
	}
	return entryKey
}
