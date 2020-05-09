package utils

import (
	"os"
	"testing"

	"github.com/dvasilas/proteus/src/proto"
	"github.com/dvasilas/proteus/src/proto/qpu"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestMergeDatasets(t *testing.T) {
	ds := protoutils.DataSet(map[string]map[string][]string{
		"s3server": {
			"site1": {"s1", "s2"},
		},
	})
	ds1 := protoutils.DataSet(map[string]map[string][]string{
		"s3server": {
			"site1": {"s3", "s4", "s5"},
		},
	})
	ds2 := protoutils.DataSet(map[string]map[string][]string{
		"s3server": {
			"site2": {"s1", "s2"},
		},
	})
	ds3 := protoutils.DataSet(map[string]map[string][]string{
		"antidote": {
			"dc1": {"s1"},
			"dc2": {"s1", "s2"},
		},
	})

	mergeDatasets(ds, ds1)
	mergeDatasets(ds, ds2)
	mergeDatasets(ds, ds3)
}

func TestCanRespondToQuery(t *testing.T) {
	for _, tt := range capabilitiesTests {
		canResp, _ := CanRespondToQuery(tt.predicate, tt.capabilities)
		assert.Equal(t, tt.expectedRes, canResp, "")
	}
}

var capabilitiesTests = []struct {
	predicate    []*qpu.AttributePredicate
	capabilities []*qpu.AttributePredicate
	expectedRes  bool
}{
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		[]*qpu.AttributePredicate{},
		true,
	},
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueStr("a"), protoutils.ValueStr("z")),
		},
		false,
	},
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.5)),
		},
		false,
	},
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
			protoutils.AttributePredicate(protoutils.Attribute("int", nil), protoutils.ValueInt(3), protoutils.ValueInt(6)),
		},
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", nil), protoutils.ValueFlt(0.2), protoutils.ValueFlt(0.6)),
		},
		false,
	},
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
			protoutils.AttributePredicate(protoutils.Attribute("int", nil), protoutils.ValueInt(3), protoutils.ValueInt(6)),
		},
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", nil), protoutils.ValueFlt(0.2), protoutils.ValueFlt(0.6)),
			protoutils.AttributePredicate(protoutils.Attribute("int", nil), protoutils.ValueInt(2), protoutils.ValueInt(7)),
		},
		true,
	},
}
