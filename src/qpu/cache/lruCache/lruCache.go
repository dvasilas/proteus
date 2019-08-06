package lrucache

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
)

// Cache represents an LRU cache
type Cache struct {
	MaxEntries int
	ll         *list.List
	items      map[string]*list.Element
	OnEvict    func(key []*pbUtils.AttributePredicate, value []utils.ObjectState)
	mutex      sync.RWMutex
}

type entry struct {
	key   []*pbUtils.AttributePredicate
	value []utils.ObjectState
}

// New creates a cache instance
func New(conf *config.Config) *Cache {
	return &Cache{
		MaxEntries: conf.CacheConfig.Size,
		ll:         list.New(),
		items:      make(map[string]*list.Element),
	}
}

// Put stores an object in a cache entry
func (c *Cache) Put(predicate []*pbUtils.AttributePredicate, obj utils.ObjectState) error {
	key := predicateToCacheKey(predicate)
	c.mutex.Lock()
	if c.items == nil {
		c.ll = list.New()
		c.items = make(map[string]*list.Element)
	}
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		item.Value.(*entry).value = append(item.Value.(*entry).value, obj)
	} else {
		item := c.ll.PushFront(&entry{key: predicate, value: []utils.ObjectState{obj}})
		c.items[key] = item
		if c.ll.Len() > c.MaxEntries {
			c.evict()
		}
	}
	c.mutex.Unlock()
	return nil
}

// Get retrieves an entry from the cache
func (c *Cache) Get(p []*pbUtils.AttributePredicate) ([]utils.ObjectState, bool) {
	if c.items == nil {
		return nil, false
	}
	key := predicateToCacheKey(p)
	c.mutex.RLock()
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		resp := make([]utils.ObjectState, len(item.Value.(*entry).value))
		copy(resp, item.Value.(*entry).value)
		c.mutex.RUnlock()
		return resp, true
	}
	c.mutex.RUnlock()
	return nil, false
}

// Evicts the LRU entry from the cache
func (c *Cache) evict() error {
	if c.items == nil {
		return nil
	}
	item := c.ll.Back()
	if item != nil {
		c.ll.Remove(item)
		ee := item.Value.(*entry)
		key := predicateToCacheKey(ee.key)
		delete(c.items, key)
		if c.OnEvict != nil {
			c.OnEvict(ee.key, ee.value)
		}
	}
	return nil
}

// Converts a predicate to a cache entry key
func predicateToCacheKey(pred []*pbUtils.AttributePredicate) string {
	key := ""
	for _, p := range pred {
		key += p.GetAttr().GetAttrKey() + "/" + attributeTypeToString(p.GetAttr().GetAttrType()) + "/" + utils.ValueToString(p.GetLbound()) + "/" + utils.ValueToString(p.GetUbound())
		key += "&"
	}
	return key[:len(key)-1]
}

func (c *Cache) print() {
	for e := c.ll.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
	fmt.Println()
}

// attributeTypeToString ...
func attributeTypeToString(t pbUtils.Attribute_AttributeType) string {
	switch t {
	case pbUtils.Attribute_S3TAGSTR:
		return "S3TAGSTR"
	case pbUtils.Attribute_S3TAGINT:
		return "S3TAGINT"
	case pbUtils.Attribute_S3TAGFLT:
		return "S3TAGFLT"
	case pbUtils.Attribute_CRDTCOUNTER:
		return "CRDTCOUNTER"
	case pbUtils.Attribute_CRDTLWWREG:
		return "CRDTLWWREG"
	}
	return ""
}
