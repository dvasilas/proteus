package cache

import (
	"container/list"
	"errors"
	"fmt"
	"strconv"

	"github.com/dimitriosvasilas/proteus"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	cli "github.com/dimitriosvasilas/proteus/qpu/client"
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
	OnEvict    func(key []*pbQPU.AttributePredicate, value []cachedValue)
}

type entry struct {
	key   []*pbQPU.AttributePredicate
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

	clients, err := forwardQuery(conns, in.Predicate)
	if err != nil {
		return err
	}
	msg := make(chan *pb.QueryResultStream)
	done := make(chan bool)
	errs := make(chan error)
	errsFind := make(chan error)

	go q.findResultConsumer(in.Predicate, streamOut, msg, done, errsFind, errs, q.cache.storeAndRespond)
	go clients[0].Find(in.Timestamp, in.Predicate, msg, done, errsFind)

	err = <-errs
	return err
}

//GetSnapshot ...
func (q *CQPU) GetSnapshot(in *pb.SubRequest, stream pb.QPU_GetSnapshotServer) error {
	return errors.New("cache QPU does not support GetSnapshot()")
}

//SubscribeOpsAsync ...
func (q *CQPU) SubscribeOpsAsync(in *pb.SubRequest, stream pb.QPU_SubscribeOpsAsyncServer) error {
	return errors.New("cache QPU does not support SubscribeOpsAsync()")
}

//SubscribeOpsSync ...
func (q *CQPU) SubscribeOpsSync(stream pb.QPU_SubscribeOpsSyncServer) error {
	return errors.New("cache QPU does not support SubscribeOpsSync()")
}

//Cleanup ...
func (q *CQPU) Cleanup() {
	log.Info("cache QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//Receives stream of query results from cache miss
//Stores objects in cache and forwards upwards
func (q *CQPU) findResultConsumer(pred []*pbQPU.AttributePredicate, streamOut pb.QPU_FindServer, msg chan *pb.QueryResultStream, done chan bool, errFind chan error, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.AttributePredicate, pb.QPU_FindServer) error) {
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
func (c *cache) storeAndRespond(obj *pbQPU.Object, ds *pbQPU.DataSet, in []*pbQPU.AttributePredicate, streamOut pb.QPU_FindServer) error {
	if err := c.put(in, *obj, *ds); err != nil {
		return err
	}
	return streamOut.Send(&pb.QueryResultStream{
		Object:  &pbQPU.Object{Key: obj.Key, Attributes: obj.Attributes, Timestamp: obj.Timestamp},
		Dataset: ds,
	})
}

//Stores an object in a cache entry
func (c *cache) put(query []*pbQPU.AttributePredicate, obj pbQPU.Object, ds pbQPU.DataSet) error {
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
func (c *cache) get(p []*pbQPU.AttributePredicate) ([]cachedValue, bool) {
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

//forwardQuery selects a set of downward connections for forwarding a query, based on the available QPUs and their configuration.
//Returns an array connections for initiating Find queries, and any error encountered.
func forwardQuery(conns utils.DownwardConns, query []*pbQPU.AttributePredicate) ([]cli.Client, error) {
	forwardTo := make([]cli.Client, 0)
	for _, db := range conns.DBs {
		for _, r := range db.DCs {
			for _, sh := range r.Shards {
				for _, q := range sh.QPUs {
					if (q.QpuType == "index" || q.QpuType == "cache") && utils.CanProcessQuery(q, query) {
						if utils.QueryInAttrRange(q, query) {
							forwardTo = append(forwardTo, q.Client)
						}
					}
				}
				for _, q := range sh.QPUs {
					if q.QpuType == "filter" {
						forwardTo = append(forwardTo, q.Client)
					}
				}
			}
		}
	}
	if len(forwardTo) == 0 {
		return forwardTo, errors.New("dispatch found no QPU to forward query")
	}
	return forwardTo, nil

}

//Converts a predicate to a cache entry key
func predicateToKey(p []*pbQPU.AttributePredicate) string {
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
