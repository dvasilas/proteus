package lrucache

import (
	"container/list"
	"context"
	"io"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
)

// Cache represents an LRU cache
type Cache struct {
	maxSize      int
	ttl          int
	usedCapacity int
	ll           *list.List
	items        map[string]*list.Element
	onEvict      func(string)
	mutex        sync.Mutex
}

type entry struct {
	key    string
	value  *qpuextapi.QueryResp
	size   int
	cancel context.CancelFunc
}

func (c *Cache) onEvictFunc(key string) {
}

// New creates a cache instance
func New(config *libqpu.QPUConfig) *Cache {
	c := &Cache{
		maxSize: config.CacheConfig.Size,
		ttl:     config.CacheConfig.TTL,
		ll:      list.New(),
		items:   make(map[string]*list.Element),
	}
	c.onEvict = c.onEvictFunc
	return c
}

// Put stores an object in a cache entry
func (c *Cache) Put(key string, response *qpuextapi.QueryResp, size int, client libqpu.APIClient) error {
	c.mutex.Lock()
	if c.items == nil {
		c.ll = list.New()
		c.items = make(map[string]*list.Element)
	}
	if size <= c.maxSize {
		for c.usedCapacity+size > c.maxSize {
			c.evict()
		}

		item := c.ll.PushFront(&entry{key: key, value: response, size: size})
		c.items[key] = item
		c.usedCapacity += size
		// go c.WaitInvalidation(key)
	}
	c.mutex.Unlock()
	return nil
}

func (c *Cache) waitInvalidate(stream qpuapi.QPUAPI_QuerySubscribeClient, cancel context.CancelFunc, key string) {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			break
		} else {
			break
		}
	}
	cancel()
	c.Invalidate(key)
}

// for {
// 	streamRec, err := stream.Recv()
// 	if err == io.EOF {
// 		break
// 	} else if err != nil {
// 		break
// 	} else {
// 		if streamRec.GetType() == pbQPU.ResponseStreamRecord_UPDATEDELTA {
// 			break
// 		}
// 	}
// }
// c.Invalidate(p)
// cancel()
// return nil

// WaitInvalidate ...
func (c *Cache) WaitInvalidate(stream qpuapi.QPUAPI_QuerySubscribeClient, cancel context.CancelFunc, key string) {
	go c.waitInvalidate(stream, cancel, key)
}

// Get retrieves an entry from the cache
func (c *Cache) Get(key string) (*qpuextapi.QueryResp, bool) {
	if c.items == nil {
		return nil, false
	}
	c.mutex.Lock()
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		resp := item.Value.(*entry).value
		c.mutex.Unlock()
		return resp, true
	}
	c.mutex.Unlock()
	return nil, false
}

// WaitInvalidation ..
func (c *Cache) WaitInvalidation(key string) {
	time.Sleep(time.Duration(c.ttl) * time.Second)
	c.Invalidate(key)
}

// Invalidate ..
func (c *Cache) Invalidate(key string) {
	if c.items == nil {
		return
	}
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
		if c.onEvict != nil {
			c.onEvict(ee.key)
		}
		delete(c.items, ee.key)
		c.usedCapacity -= ee.size
	}
}
