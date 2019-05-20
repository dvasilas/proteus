package utils

import (
	"os"
	"testing"

	"github.com/dvasilas/proteus/protos"
	pbUtils "github.com/dvasilas/proteus/protos/utils"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestMergeDatasets(t *testing.T) {
	ds := protoutils.DataSet(map[string]map[string][]string{
		"s3server": map[string][]string{
			"site1": []string{"s1", "s2"},
		},
	})
	ds1 := protoutils.DataSet(map[string]map[string][]string{
		"s3server": map[string][]string{
			"site1": []string{"s3", "s4", "s5"},
		},
	})
	ds2 := protoutils.DataSet(map[string]map[string][]string{
		"s3server": map[string][]string{
			"site2": []string{"s1", "s2"},
		},
	})
	ds3 := protoutils.DataSet(map[string]map[string][]string{
		"antidote": map[string][]string{
			"dc1": []string{"s1"},
			"dc2": []string{"s1", "s2"},
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
	predicate    []*pbUtils.AttributePredicate
	capabilities []*pbUtils.AttributePredicate
	expectedRes  bool
}{
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		[]*pbUtils.AttributePredicate{},
		true,
	},
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGSTR, nil), protoutils.ValueStr("a"), protoutils.ValueStr("z")),
		},
		false,
	},
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.5)),
		},
		false,
	},
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
			protoutils.AttributePredicate(protoutils.Attribute("int", pbUtils.Attribute_S3TAGINT, nil), protoutils.ValueInt(3), protoutils.ValueInt(6)),
		},
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.2), protoutils.ValueFlt(0.6)),
		},
		false,
	},
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
			protoutils.AttributePredicate(protoutils.Attribute("int", pbUtils.Attribute_S3TAGINT, nil), protoutils.ValueInt(3), protoutils.ValueInt(6)),
		},
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("float", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.2), protoutils.ValueFlt(0.6)),
			protoutils.AttributePredicate(protoutils.Attribute("int", pbUtils.Attribute_S3TAGINT, nil), protoutils.ValueInt(2), protoutils.ValueInt(7)),
		},
		true,
	},
}
