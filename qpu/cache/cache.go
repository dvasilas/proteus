package cache

import (
	"container/list"
	"fmt"
	"strconv"

	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

//Cache ...
type Cache struct {
	MaxEntries int
	ll         *list.List
	items      map[string]*list.Element
	OnEvict    func(key []*pbQPU.Predicate, value []pbQPU.Object)
}

type entry struct {
	key   []*pbQPU.Predicate
	value []pbQPU.Object
}

//New ...
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[string]*list.Element),
	}
}

//StoreInCache ...
func (c *Cache) StoreInCache(obj *pbQPU.Object, in []*pbQPU.Predicate, stream pb.QPU_FindServer) error {
	if err := c.put(in, *obj); err != nil {
		return err
	}
	stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp}})
	return nil
}

//Put ...
func (c *Cache) put(query []*pbQPU.Predicate, obj pbQPU.Object) error {
	//test if cache == nil - return error
	key := PredicateToKey(query)
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		item.Value.(*entry).value = append(item.Value.(*entry).value, obj)
	} else {
		item := c.ll.PushFront(&entry{query, []pbQPU.Object{obj}})
		c.items[key] = item
		if c.ll.Len() > c.MaxEntries {
			c.Evict()
		}
	}
	return nil
}

//Evict ...
func (c *Cache) Evict() error {
	//check nil
	item := c.ll.Back()
	if item != nil {
		c.ll.Remove(item)
		ee := item.Value.(*entry)
		key := PredicateToKey(ee.key)
		delete(c.items, key)
		if c.OnEvict != nil {
			c.OnEvict(ee.key, ee.value)
		}
	}
	return nil
}

//Get ...
func (c *Cache) Get(p []*pbQPU.Predicate) ([]pbQPU.Object, bool, error) {
	//test if cache == nil - return error
	key := PredicateToKey(p)
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		return item.Value.(*entry).value, true, nil
	}
	return nil, false, nil
}

//Print ...
func (c *Cache) Print() {
	for e := c.ll.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}

//PredicateToKey ...
func PredicateToKey(p []*pbQPU.Predicate) string {
	entryKey := ""
	for i, pp := range p {
		switch pp.Lbound.Val.(type) {
		case *pbQPU.Value_Int:
			entryKey += pp.Attribute + "/" + strconv.FormatInt(pp.Lbound.GetInt(), 10) + "/" + strconv.FormatInt(pp.Ubound.GetInt(), 10)
		case *pbQPU.Value_Name:
			entryKey += pp.Attribute + "/" + pp.Lbound.GetName() + "/" + pp.Ubound.GetName()
		}
		if i < len(p)-1 {
			entryKey += "&"
		}
	}
	return entryKey
}
