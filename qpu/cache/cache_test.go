package cache

import (
	"os"
	"strconv"
	"testing"

	utils "github.com/dimitriosvasilas/proteus"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	"github.com/stretchr/testify/assert"
)

var keyTests = []struct {
	query       []*pbQPU.Predicate
	expectedKey string
}{
	{[]*pbQPU.Predicate{
		{
			Attribute: "size",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(10),
		},
	}, "size/0/10"},
	{[]*pbQPU.Predicate{
		{
			Attribute: "size",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(10),
		},
		{
			Attribute: "type",
			Lbound:    utils.ValInt(1),
			Ubound:    utils.ValInt(2),
		},
	}, "size/0/10&type/1/2"},
}

var getTests = []struct {
	toAdd       []*pbQPU.Predicate
	toGet       []*pbQPU.Predicate
	expectedHit bool
}{
	{[]*pbQPU.Predicate{
		{
			Attribute: "hit",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(1),
		},
	}, []*pbQPU.Predicate{
		{
			Attribute: "hit",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(1),
		},
	}, true},
	{[]*pbQPU.Predicate{
		{
			Attribute: "nothit",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(1),
		},
	},
		[]*pbQPU.Predicate{
			{
				Attribute: "miss",
				Lbound:    utils.ValInt(0),
				Ubound:    utils.ValInt(0),
			},
		}, false},
}

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestPredicateToString(t *testing.T) {
	for _, tt := range keyTests {
		assert.Equal(t, predicateToKey(tt.query), tt.expectedKey, "")
	}
}

func TestGet(t *testing.T) {
	for _, tt := range getTests {
		cache := new(1)
		cache.put(tt.toAdd, pbQPU.Object{}, pbQPU.DataSet{})
		_, hit := cache.get(tt.toGet)
		assert.Equal(t, hit, tt.expectedHit, "")
	}
}

func TestEvict(t *testing.T) {
	var evicted [][]*pbQPU.Predicate
	onEvictF := func(key []*pbQPU.Predicate, value []cachedValue) {
		evicted = append(evicted, key)
	}
	cache := new(10)
	cache.OnEvict = onEvictF
	for i := 0; i < 12; i++ {
		cache.put([]*pbQPU.Predicate{
			{
				Attribute: "attr" + strconv.Itoa(i),
				Lbound:    utils.ValInt(0),
				Ubound:    utils.ValInt(0),
			},
		}, pbQPU.Object{}, pbQPU.DataSet{})
	}
	assert.Equal(t, evicted[0][0].Attribute, "attr0", "")
	assert.Equal(t, evicted[1][0].Attribute, "attr1", "")
}

func TestEvictLRU(t *testing.T) {
	var evicted [][]*pbQPU.Predicate
	onEvictF := func(key []*pbQPU.Predicate, value []cachedValue) {
		evicted = append(evicted, key)
	}
	cache := new(10)
	cache.OnEvict = onEvictF
	for i := 0; i < 5; i++ {
		cache.put([]*pbQPU.Predicate{
			{
				Attribute: "attr" + strconv.Itoa(i),
				Lbound:    utils.ValInt(0),
				Ubound:    utils.ValInt(0),
			},
		}, pbQPU.Object{}, pbQPU.DataSet{})
	}
	_, _ = cache.get([]*pbQPU.Predicate{
		{
			Attribute: "attr0",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(0),
		},
	})
	_, _ = cache.get([]*pbQPU.Predicate{
		{
			Attribute: "attr1",
			Lbound:    utils.ValInt(0),
			Ubound:    utils.ValInt(0),
		},
	})
	for i := 5; i < 12; i++ {
		cache.put([]*pbQPU.Predicate{
			{
				Attribute: "attr" + strconv.Itoa(i),
				Lbound:    utils.ValInt(0),
				Ubound:    utils.ValInt(0),
			},
		}, pbQPU.Object{}, pbQPU.DataSet{})
	}
	assert.Equal(t, evicted[0][0].Attribute, "attr2", "")
	assert.Equal(t, evicted[1][0].Attribute, "attr3", "")
}
