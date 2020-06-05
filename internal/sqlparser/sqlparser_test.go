package sqlparser

import (
	"os"
	"testing"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

var filterTests = []struct {
	querySQL       string
	expectedQueryI *qpu_api.QueryInternalQuery
}{
	{
		"select * from t where x = 42",
		&qpu_api.QueryInternalQuery{
			Table: "t",
			Predicate: []*qpu.AttributePredicate{
				libqpu.AttributePredicate(libqpu.Attribute("x", nil), libqpu.ValueInt(42), libqpu.ValueInt(42)),
			},
			TsPredicate: libqpu.SnapshotTimePredicate(libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true), libqpu.SnapshotTime(qpu.SnapshotTime_LATEST, nil, true)),
		},
	},
}

func TestParse(t *testing.T) {
	for _, tt := range filterTests {
		queryI, _ := Parse(tt.querySQL)
		assert.Equal(t, tt.expectedQueryI, queryI.Q, "")
	}
}
