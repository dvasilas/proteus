package filter

import (
	"os"
	"testing"

	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	testutils "github.com/dvasilas/proteus/internal/testUtils"
	"github.com/stretchr/testify/assert"
)

var filterTests = []struct {
	predicate   []*qpu.AttributePredicate
	obj         *qpu_api.ResponseStreamRecord
	expectedRes bool
}{
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		protoutils.ResponseStreamRecord(0, qpu_api.ResponseStreamRecord_STATE, testutils.ObjectLogOp("object", "bucket", "test", protoutils.ValueFlt(0.4))),
		true,
	},
	{
		[]*qpu.AttributePredicate{
			protoutils.AttributePredicate(protoutils.Attribute("test", nil), protoutils.ValueFlt(0.4), protoutils.ValueFlt(0.6)),
		},
		protoutils.ResponseStreamRecord(0, qpu_api.ResponseStreamRecord_STATE, testutils.ObjectLogOp("object", "bucket", "test", protoutils.ValueFlt(0.6))),
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
