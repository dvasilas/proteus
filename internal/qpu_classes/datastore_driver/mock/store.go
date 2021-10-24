package mockdriver

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

// MockDataStore ...
type MockDataStore struct {
	state []libqpu.LogOperation
}

//---------------- API Functions -------------------

// NewDatastore ...
func NewDatastore(conf *libqpu.QPUConfig, inputSchema libqpu.Schema) (MockDataStore, error) {

	tableN := ""
	attributeN := ""
	for tableName, table := range inputSchema {
		tableN = tableName
		for attributeName, _ := range table.Attributes {
			attributeN = attributeName
			break
		}
		break
	}

	N := 1000
	state := make([]libqpu.LogOperation, N)
	for i := 0; i < N; i++ {
		recordID := fmt.Sprintf("obj%d", rand.Int())

		timestamp, err := ptypes.TimestampProto(time.Now())
		if err != nil {
			return MockDataStore{}, err
		}

		attributes := make(map[string]*qpu.Value)

		attributes[attributeN] = libqpu.ValueInt(int32(rand.Intn(100)))

		state[i] = libqpu.LogOperationState(
			recordID,
			tableN,
			libqpu.Vectorclock(map[string]*tspb.Timestamp{"t": timestamp}),
			attributes)
	}

	s := MockDataStore{state: state}

	return s, nil
}

func (ds MockDataStore) SubscribeOps(table string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error) {

	return nil, nil, nil
}

// GetSnapshot ...
func (ds MockDataStore) GetSnapshot(table string, projection, isNull, isNotNull []string) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	go func() {
		for _, object := range ds.state {
			logOpCh <- object
		}

		close(logOpCh)
		close(errCh)
	}()

	return logOpCh, errCh
}

// GetNotificationLanency ...
func (ds MockDataStore) GetNotificationLanency() (float64, float64, float64, float64) {
	return -1, -1, -1, -1
}
