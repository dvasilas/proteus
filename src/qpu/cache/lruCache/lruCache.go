package lrucache

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/client"
)

// Cache represents an LRU cache
type Cache struct {
	maxSize      int
	usedCapacity int
	ll           *list.List
	items        map[string]*list.Element
	onEvict      func(key []*pbUtils.AttributePredicate)
	mutex        sync.Mutex
}

type entry struct {
	key    []*pbUtils.AttributePredicate
	value  []utils.ObjectState
	size   int
	cancel context.CancelFunc
}

func (c *Cache) onEvictFunc(predicate []*pbUtils.AttributePredicate) {
	key := predicateToCacheKey(predicate)
	if item, ok := c.items[key]; ok {
		item.Value.(*entry).cancel()
	}
}

// New creates a cache instance
func New(conf *config.Config) *Cache {
	c := &Cache{
		maxSize: conf.CacheConfig.Size,
		ll:      list.New(),
		items:   make(map[string]*list.Element),
	}
	c.onEvict = c.onEvictFunc
	return c
}

// Put stores an object in a cache entry
func (c *Cache) Put(predicate []*pbUtils.AttributePredicate, objects []utils.ObjectState, newEntrySize int, client client.Client) error {
	c.mutex.Lock()
	if c.items == nil {
		c.ll = list.New()
		c.items = make(map[string]*list.Element)
	}
	if newEntrySize <= c.maxSize {
		for c.usedCapacity+newEntrySize > c.maxSize {
			c.evict()
		}
		stream, cancel, err := client.Query(
			predicate,
			protoutils.SnapshotTimePredicate(
				protoutils.SnapshotTime(pbUtils.SnapshotTime_INF, nil),
				protoutils.SnapshotTime(pbUtils.SnapshotTime_INF, nil),
			),
			nil,
			false,
		)
		if err != nil {
			return err
		}
		key := predicateToCacheKey(predicate)
		item := c.ll.PushFront(&entry{key: predicate, value: objects, size: newEntrySize, cancel: cancel})
		c.items[key] = item
		c.usedCapacity += newEntrySize
		go c.WaitInvalidation(predicate, stream, cancel)
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
	c.mutex.Lock()
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		resp := make([]utils.ObjectState, len(item.Value.(*entry).value))
		copy(resp, item.Value.(*entry).value)
		c.mutex.Unlock()
		return resp, true
	}
	c.mutex.Unlock()
	return nil, false
}

// WaitInvalidation ..
func (c *Cache) WaitInvalidation(p []*pbUtils.AttributePredicate, stream pbQPU.QPU_QueryClient, cancel context.CancelFunc) error {
	for {
		streamRec, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			break
		} else {
			if streamRec.GetType() == pbQPU.ResponseStreamRecord_UPDATEDELTA {
				break
			}
		}
	}
	c.Invalidate(p)
	cancel()
	return nil
}

// Invalidate ..
func (c *Cache) Invalidate(p []*pbUtils.AttributePredicate) {
	if c.items == nil {
		return
	}
	key := predicateToCacheKey(p)
	c.mutex.Lock()
	if item, ok := c.items[key]; ok {
		c.ll.MoveToBack(item)
		c.evict()
	}
	c.mutex.Unlock()
}

// Evicts the LRU entry from the cache
func (c *Cache) evict() {
	if c.items == nil {
		return
	}
	item := c.ll.Back()
	if item != nil {
		c.ll.Remove(item)
		ee := item.Value.(*entry)
		key := predicateToCacheKey(ee.key)
		if c.onEvict != nil {
			c.onEvict(ee.key)
		}
		delete(c.items, key)
		c.usedCapacity -= ee.size
	}
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
		ee := e.Value.(*entry)
		fmt.Println(ee.key)
		fmt.Println(ee.value)
		fmt.Println(ee.size)
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
