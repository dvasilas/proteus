package cache

import (
	"container/list"
	"fmt"
	"strconv"

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

//Put ...
func (c *Cache) Put(p []*pbQPU.Predicate, value pbQPU.Object) error {
	//test if cache == nil - return error
	key := c.predicateToKey(p)
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		item.Value.(*entry).value = append(item.Value.(*entry).value, value)
	} else {
		item := c.ll.PushFront(&entry{p, []pbQPU.Object{value}})
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
		key := c.predicateToKey(ee.key)
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
	key := c.predicateToKey(p)
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

func (c *Cache) predicateToKey(p []*pbQPU.Predicate) string {
	entryKey := ""
	for i, pp := range p {
		entryKey += pp.Attribute + "/" + strconv.FormatInt(pp.Lbound, 10) + "/" + strconv.FormatInt(pp.Ubound, 10)
		if i < len(p)-1 {
			entryKey += "&"
		}
	}
	return entryKey
}
