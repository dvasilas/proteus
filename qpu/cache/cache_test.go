package cache

import (
	"os"
	"strconv"
	"testing"

	"github.com/dvasilas/proteus/protos"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	"github.com/stretchr/testify/assert"
)

var keyTests = []struct {
	query       []*pbQPU.AttributePredicate
	expectedKey string
}{
	{[]*pbQPU.AttributePredicate{
		{
			Attribute: "size",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(10),
		},
	}, "size/0/10"},
	{[]*pbQPU.AttributePredicate{
		{
			Attribute: "size",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(10),
		},
		{
			Attribute: "type",
			Lbound:    protoutils.ValueInt(1),
			Ubound:    protoutils.ValueInt(2),
		},
	}, "size/0/10&type/1/2"},
}

var getTests = []struct {
	toAdd       []*pbQPU.AttributePredicate
	toGet       []*pbQPU.AttributePredicate
	expectedHit bool
}{
	{[]*pbQPU.AttributePredicate{
		{
			Attribute: "hit",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(1),
		},
	}, []*pbQPU.AttributePredicate{
		{
			Attribute: "hit",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(1),
		},
	}, true},
	{[]*pbQPU.AttributePredicate{
		{
			Attribute: "nothit",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(1),
		},
	},
		[]*pbQPU.AttributePredicate{
			{
				Attribute: "miss",
				Lbound:    protoutils.ValueInt(0),
				Ubound:    protoutils.ValueInt(0),
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
	var evicted [][]*pbQPU.AttributePredicate
	onEvictF := func(key []*pbQPU.AttributePredicate, value []cachedValue) {
		evicted = append(evicted, key)
	}
	cache := new(10)
	cache.OnEvict = onEvictF
	for i := 0; i < 12; i++ {
		cache.put([]*pbQPU.AttributePredicate{
			{
				Attribute: "attr" + strconv.Itoa(i),
				Lbound:    protoutils.ValueInt(0),
				Ubound:    protoutils.ValueInt(0),
			},
		}, pbQPU.Object{}, pbQPU.DataSet{})
	}
	assert.Equal(t, evicted[0][0].Attribute, "attr0", "")
	assert.Equal(t, evicted[1][0].Attribute, "attr1", "")
}

func TestEvictLRU(t *testing.T) {
	var evicted [][]*pbQPU.AttributePredicate
	onEvictF := func(key []*pbQPU.AttributePredicate, value []cachedValue) {
		evicted = append(evicted, key)
	}
	cache := new(10)
	cache.OnEvict = onEvictF
	for i := 0; i < 5; i++ {
		cache.put([]*pbQPU.AttributePredicate{
			{
				Attribute: "attr" + strconv.Itoa(i),
				Lbound:    protoutils.ValueInt(0),
				Ubound:    protoutils.ValueInt(0),
			},
		}, pbQPU.Object{}, pbQPU.DataSet{})
	}
	_, _ = cache.get([]*pbQPU.AttributePredicate{
		{
			Attribute: "attr0",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(0),
		},
	})
	_, _ = cache.get([]*pbQPU.AttributePredicate{
		{
			Attribute: "attr1",
			Lbound:    protoutils.ValueInt(0),
			Ubound:    protoutils.ValueInt(0),
		},
	})
	for i := 5; i < 12; i++ {
		cache.put([]*pbQPU.AttributePredicate{
			{
				Attribute: "attr" + strconv.Itoa(i),
				Lbound:    protoutils.ValueInt(0),
				Ubound:    protoutils.ValueInt(0),
			},
		}, pbQPU.Object{}, pbQPU.DataSet{})
	}
	assert.Equal(t, evicted[0][0].Attribute, "attr2", "")
	assert.Equal(t, evicted[1][0].Attribute, "attr3", "")
}
