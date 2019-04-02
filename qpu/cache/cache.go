package cache

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/dvasilas/proteus"
	"github.com/dvasilas/proteus/protos"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	cli "github.com/dvasilas/proteus/qpu/client"
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

//Query implements the Query API for the cache QPU
func (q *CQPU) Query(streamOut pb.QPU_QueryServer, conns utils.DownwardConns) error {
	msg, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}
	req := msg.GetRequest()

	if req.GetOps() {
		return errors.New("not supported")
	}

	cachedResult, hit := q.cache.get(req.GetPredicate())
	if hit {
		log.WithFields(log.Fields{
			"cache entry": cachedResult,
		}).Debug("cache hit, responding")

		for _, item := range cachedResult {
			if err := streamOut.Send(protoutils.QueryResponseStreamState(&item.Object, &item.Dataset)); err != nil {
				return err
			}
		}
		return nil
	}
	log.WithFields(log.Fields{}).Debug("cache miss")

	clients, err := forwardQuery(conns, req.GetPredicate())
	if err != nil {
		return err
	}

	errs := make(chan error)

	streamIn, _, err := clients[0].Query(req.GetPredicate(), req.GetTimestamp(), false, false)
	if err != nil {
		return err
	}

	go utils.QueryResponseConsumer(req.GetPredicate(), streamIn, streamOut, errs, q.cache.storeAndRespond)

	err = <-errs
	return err
}

//Cleanup ...
func (q *CQPU) Cleanup() {
	log.Info("cache QPU cleanup")
}

//----------- Stream Consumer Functions ------------

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
func (c *cache) storeAndRespond(in []*pbQPU.AttributePredicate, streamMsg *pb.QueryResponseStream, streamOut pb.QPU_QueryServer) error {
	if err := c.put(in, *streamMsg.GetState().GetObject(), *streamMsg.GetState().GetDataset()); err != nil {
		return err
	}
	return streamOut.Send(protoutils.QueryResponseStreamState(streamMsg.GetState().GetObject(), streamMsg.GetState().GetDataset()))
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
//Returns an array connections for initiating Query queries, and any error encountered.
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
