package cache

import (
	"os"
	"strconv"
	"testing"

	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/stretchr/testify/assert"
)

var keyTests = []struct {
	query       []*pbQPU.Predicate
	expectedKey string
}{
	{[]*pbQPU.Predicate{
		&pbQPU.Predicate{Attribute: "size", Lbound: 0, Ubound: 10},
	}, "size/0/10"},
	{[]*pbQPU.Predicate{
		&pbQPU.Predicate{Attribute: "size", Lbound: 0, Ubound: 10},
		&pbQPU.Predicate{Attribute: "type", Lbound: 1, Ubound: 2},
	}, "size/0/10&type/1/2"},
}

var getTests = []struct {
	toAdd       []*pbQPU.Predicate
	toGet       []*pbQPU.Predicate
	expectedHit bool
}{
	{[]*pbQPU.Predicate{
		&pbQPU.Predicate{Attribute: "hit", Lbound: 0, Ubound: 1},
	}, []*pbQPU.Predicate{
		&pbQPU.Predicate{Attribute: "hit", Lbound: 0, Ubound: 1},
	}, true},
	{[]*pbQPU.Predicate{
		&pbQPU.Predicate{Attribute: "nothit", Lbound: 0, Ubound: 1},
	},
		[]*pbQPU.Predicate{
			&pbQPU.Predicate{Attribute: "miss", Lbound: 0, Ubound: 0},
		}, false},
}

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestPredicateToString(t *testing.T) {
	for _, tt := range keyTests {
		assert.Equal(t, PredicateToKey(tt.query), tt.expectedKey, "")
	}
}

func TestGet(t *testing.T) {
	for _, tt := range getTests {
		cache := New(1)
		cache.Put(tt.toAdd, pbQPU.Object{})
		_, hit, err := cache.Get(tt.toGet)
		assert.Nil(t, err)
		assert.Equal(t, hit, tt.expectedHit, "")
	}
}

func TestEvict(t *testing.T) {
	var evicted [][]*pbQPU.Predicate
	onEvictF := func(key []*pbQPU.Predicate, value []pbQPU.Object) {
		evicted = append(evicted, key)
	}
	cache := New(10)
	cache.OnEvict = onEvictF
	for i := 0; i < 12; i++ {
		cache.Put([]*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr" + strconv.Itoa(i), Lbound: 0, Ubound: 0}}, pbQPU.Object{})
	}
	assert.Equal(t, evicted[0], []*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr0"}}, "")
	assert.Equal(t, evicted[1], []*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr1"}}, "")
}

func TestEvictLRU(t *testing.T) {
	var evicted [][]*pbQPU.Predicate
	onEvictF := func(key []*pbQPU.Predicate, value []pbQPU.Object) {
		evicted = append(evicted, key)
	}
	cache := New(10)
	cache.OnEvict = onEvictF
	for i := 0; i < 5; i++ {
		cache.Put([]*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr" + strconv.Itoa(i), Lbound: 0, Ubound: 0}}, pbQPU.Object{})
	}
	_, _, _ = cache.Get([]*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr0", Lbound: 0, Ubound: 0}})
	_, _, _ = cache.Get([]*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr1", Lbound: 0, Ubound: 0}})
	for i := 5; i < 12; i++ {
		cache.Put([]*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr" + strconv.Itoa(i), Lbound: 0, Ubound: 0}}, pbQPU.Object{})
	}
	assert.Equal(t, evicted[0], []*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr2"}}, "")
	assert.Equal(t, evicted[1], []*pbQPU.Predicate{&pbQPU.Predicate{Attribute: "attr3"}}, "")
}
