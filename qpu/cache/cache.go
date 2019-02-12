package cache

import (
	"container/list"
	"fmt"
	"strconv"

	utils "github.com/dimitriosvasilas/proteus"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	cli "github.com/dimitriosvasilas/proteus/qpu/client"
	partitionManager "github.com/dimitriosvasilas/proteus/qpu/partition_manager"
	log "github.com/sirupsen/logrus"
)

//CQPU implements a cache QPU
type CQPU struct {
	cache *cache
}

type cache struct {
	MaxEntries int
	ll         *list.List
	items      map[string]*list.Element
	OnEvict    func(key []*pbQPU.Predicate, value []cachedValue)
}

type entry struct {
	key   []*pbQPU.Predicate
	value []cachedValue
}

type cachedValue struct {
	Object  pbQPU.Object
	Dataset pbQPU.DataSet
}

//---------------- API Functions -------------------

//QPU creates a cache QPU
func QPU() (*CQPU, error) {
	return &CQPU{
		cache: new(10),
	}, nil
}

//Find implements the Find API for the cache QPU
func (q *CQPU) Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error {
	cachedResult, hit := q.cache.get(in.Predicate)
	if hit {
		log.WithFields(log.Fields{
			"cache entry": cachedResult,
		}).Debug("cache hit, responding")

		for _, item := range cachedResult {
			if err := streamOut.Send(&pb.QueryResultStream{Object: &item.Object, Dataset: &item.Dataset}); err != nil {
				return err
			}
		}
		return nil
	}
	log.WithFields(log.Fields{}).Debug("cache miss")

	pred := make([]cli.Predicate, 0)
	pred = append(pred, cli.Predicate{
		Attribute: in.Predicate[0].Attribute,
		Datatype:  in.Predicate[0].Datatype,
		LBound:    in.Predicate[0].Lbound,
		UBound:    in.Predicate[0].Ubound,
	})

	clients, err := partitionManager.ForwardQuery(conns, in.Predicate)
	if err != nil {
		return err
	}
	msg := make(chan *pb.QueryResultStream)
	done := make(chan bool)
	errs := make(chan error)
	errsFind := make(chan error)

	go q.findResultConsumer(in.Predicate, streamOut, msg, done, errsFind, errs, q.cache.storeAndRespond)
	go clients[0].Find(in.Timestamp, pred, msg, done, errsFind)

	err = <-errs
	return err
}

//----------- Stream Consumer Functions ------------

//Receives stream of query results from cache miss
//Stores objects in cache and forwards upwards
func (q *CQPU) findResultConsumer(pred []*pbQPU.Predicate, streamOut pb.QPU_FindServer, msg chan *pb.QueryResultStream, done chan bool, errFind chan error, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errFind
			errs <- err
		}
		streamMsg := <-msg
		if err := process(streamMsg.GetObject(), streamMsg.GetDataset(), pred, streamOut); err != nil {
			errs <- err
		}
	}
}

//---------------- Internal Functions --------------

//Create new cache instance
//Receives the cache size as argument
func new(maxEntries int) *cache {
	return &cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[string]*list.Element),
	}
}

//Stores an object in the cache and send it upwards through an output stream
func (c *cache) storeAndRespond(obj *pbQPU.Object, ds *pbQPU.DataSet, in []*pbQPU.Predicate, streamOut pb.QPU_FindServer) error {
	if err := c.put(in, *obj, *ds); err != nil {
		return err
	}
	return streamOut.Send(&pb.QueryResultStream{
		Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
		Dataset: ds,
	})
}

//Stores an object in a cache entry
func (c *cache) put(query []*pbQPU.Predicate, obj pbQPU.Object, ds pbQPU.DataSet) error {
	if c.items == nil {
		c.ll = list.New()
		c.items = make(map[string]*list.Element)
	}
	key := predicateToKey(query)
	if item, ok := c.items[key]; ok {
		c.ll.MoveToFront(item)
		item.Value.(*entry).value = append(item.Value.(*entry).value, cachedValue{Object: obj, Dataset: ds})
	} else {
		item := c.ll.PushFront(&entry{key: query, value: []cachedValue{cachedValue{Object: obj, Dataset: ds}}})
		c.items[key] = item
		if c.ll.Len() > c.MaxEntries {
			c.evict()
		}
	}
	return nil
}

//Retrieves an enty from the cache
func (c *cache) get(p []*pbQPU.Predicate) ([]cachedValue, bool) {
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

//Evicts an entry from the cache (LRU)
func (c *cache) evict() error {
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

//---------------- Auxiliary Functions -------------

//Converts a predicate to a cache entry key
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

//Prints the contents of the cache
func (c *cache) print() {
	for e := c.ll.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}
