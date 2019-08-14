package filter

import (
	"os"
	"testing"

	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	testutils "github.com/dvasilas/proteus/src/testUtils"
	"github.com/stretchr/testify/assert"
)

var filterTests = []struct {
	predicate   []*pbUtils.AttributePredicate
	obj         *pbQPU.ResponseStreamRecord
	expectedRes bool
}{
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		protoutils.ResponseStreamRecord(0, pbQPU.ResponseStreamRecord_STATE, testutils.ObjectLogOp("object", "bucket", "test", pbUtils.Attribute_S3TAGFLT, protoutils.ValueFlt(0.4))),
		true,
	},
	{
		[]*pbUtils.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", pbUtils.Attribute_S3TAGFLT, nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		protoutils.ResponseStreamRecord(0, pbQPU.ResponseStreamRecord_STATE, testutils.ObjectLogOp("object", "bucket", "test", pbUtils.Attribute_S3TAGFLT, protoutils.ValueFlt(0.6))),
		false,
	},
}

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestFilter(t *testing.T) {
	for _, tt := range filterTests {
		match, _ := Filter(tt.predicate, tt.obj)
		assert.Equal(t, tt.expectedRes, match, "")
	}
}
