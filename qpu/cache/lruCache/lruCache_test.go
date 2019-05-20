package lrucache

import (
	"os"
	"testing"

	"github.com/dvasilas/proteus/config"
	"github.com/dvasilas/proteus/protos"
	pbUtils "github.com/dvasilas/proteus/protos/utils"
	"github.com/dvasilas/proteus/testUtils"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestPredicateToKey(t *testing.T) {
	key := predicateToCacheKey(
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGINT, nil), protoutils.ValueInt(0), protoutils.ValueInt(42)),
		},
	)
	assert.Equal(t, "test/S3TAGINT/0/42", key, "")
}

func TestPut(t *testing.T) {
	conf := &config.Config{}
	conf.CacheConfig.Size = 10
	cache := New(conf)

	pred := []*pbUtils.AttributePredicate{
		protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(4), protoutils.ValueFlt(6)),
	}

	cache.Put(pred, testutils.ObjectState("object1", "bucket", "test", pbUtils.Attribute_S3TAGFLT, protoutils.ValueFlt(4.2)))
	cache.Put(pred, testutils.ObjectState("object2", "bucket", "test", pbUtils.Attribute_S3TAGFLT, protoutils.ValueFlt(5.2)))

	_, found := cache.Get(
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(4), protoutils.ValueFlt(7)),
		},
	)
	assert.Equal(t, false, found, "")

	cachedVal, found := cache.Get(
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(4), protoutils.ValueFlt(6)),
		},
	)
	assert.Equal(t, true, found, "")
	assert.Equal(t, 2, len(cachedVal), "")

	for _, expected := range []struct {
		key   string
		value float64
	}{
		{"object1", 4.2},
		{"object2", 5.2},
	} {
		found := false
		for _, obj := range cachedVal {
			if obj.ObjectID == expected.key && obj.State.GetAttrs()[0].GetValue().GetFlt() == expected.value {
				found = true
				break
			}
		}
		assert.Equal(t, true, found, "")
	}
}

func TestEvict(t *testing.T) {
	conf := &config.Config{}
	conf.CacheConfig.Size = 2
	cache := New(conf)

	pred1 := []*pbUtils.AttributePredicate{
		protoutils.AttributePredicate(protoutils.Attribute("1", pbUtils.Attribute_CRDTCOUNTER, nil), protoutils.ValueInt(0), protoutils.ValueInt(2)),
	}
	pred2 := []*pbUtils.AttributePredicate{
		protoutils.AttributePredicate(protoutils.Attribute("2", pbUtils.Attribute_CRDTCOUNTER, nil), protoutils.ValueInt(2), protoutils.ValueInt(4)),
	}
	pred3 := []*pbUtils.AttributePredicate{
		protoutils.AttributePredicate(protoutils.Attribute("3", pbUtils.Attribute_CRDTCOUNTER, nil), protoutils.ValueInt(4), protoutils.ValueInt(6)),
	}

	cache.Put(pred1, testutils.ObjectState("object1", "bucket", "1", pbUtils.Attribute_CRDTCOUNTER, nil))
	cache.Put(pred2, testutils.ObjectState("object2", "bucket", "2", pbUtils.Attribute_CRDTCOUNTER, nil))
	cache.Put(pred3, testutils.ObjectState("object3", "bucket", "3", pbUtils.Attribute_CRDTCOUNTER, nil))

	_, found := cache.Get(pred1)
	assert.Equal(t, false, found, "")
	_, found = cache.Get(pred2)
	assert.Equal(t, true, found, "")
	_, found = cache.Get(pred3)
	assert.Equal(t, true, found, "")

	_, _ = cache.Get(pred2)
	cache.Put(pred1, testutils.ObjectState("object4", "bucket", "4", pbUtils.Attribute_CRDTCOUNTER, nil))

	_, found = cache.Get(pred1)
	assert.Equal(t, true, found, "")
	_, found = cache.Get(pred2)
	assert.Equal(t, true, found, "")
	_, found = cache.Get(pred3)
	assert.Equal(t, false, found, "")

}
