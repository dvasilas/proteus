package proteusclient

import (
	"fmt"
	"strconv"

	"github.com/dvasilas/proteus/internal/apiclient"
	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

// Client represents a connection to Proteus.
type Client struct {
	// client apiclient.QPUAPIClient
	client *libqpu.AdjacentQPU
}

// Host represents a QPU server.
type Host struct {
	Name string
	Port int
}

// ResponseRecord ...
type ResponseRecord struct {
	SequenceID int64
	ObjectID   string
	Bucket     string
	State      map[string]string
	Timestamp  Vectorclock
}

// Vectorclock ...
type Vectorclock map[string]*tspb.Timestamp

// NewClient creates a new Proteus client connected to the given QPU server.
func NewClient(host Host) (*Client, error) {
	proteusClient, err := apiclient.NewClient(host.Name + ":" + strconv.Itoa(host.Port))
	if err != nil {
		return nil, err
	}

	return &Client{
		client: &libqpu.AdjacentQPU{
			APIClient: proteusClient,
		},
	}, nil
}

// Close closes the connection to Proteus.
func (c *Client) Close() error {
	return c.client.APIClient.CloseConnection()
}

// func (c *Client) getResponse(stream qpu_api.QPUAPI_QueryClient, responseCh chan ResponseRecord, errorCh chan error) {
// 	for {
// 		streamRec, err := stream.Recv()
// 		if err != nil {
// 			errorCh <- err
// 			close(responseCh)
// 			close(errorCh)
// 			return
// 		}
// 		if streamRec.GetType() == qpu_api.ResponseStreamRecord_HEARTBEAT {
// 		} else if streamRec.GetType() == qpu_api.ResponseStreamRecord_END_OF_STREAM {
// 			close(responseCh)
// 			close(errorCh)
// 			return
// 		} else {
// 			responseCh <- ResponseRecord{
// 				SequenceID: streamRec.GetSequenceId(),
// 				ObjectID:   streamRec.GetLogOp().GetObjectId(),
// 				Bucket:     streamRec.GetLogOp().GetBucket(),
// 				State:      logOpToObjectState(streamRec),
// 				Timestamp:  streamRec.GetLogOp().GetTimestamp().GetVc(),
// 			}
// 		}
// 	}
// }

// QueryInternal ...
func (c *Client) QueryInternal(table string, predicate []*qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, limit int64, metadata map[string]string, sync bool) ([]ResponseRecord, error) {
	query := queries.NewQuerySnapshot(table, []string{}, []string{}, []string{}, limit)
	resp, err := qpugraph.SendQueryUnary(query, c.client)
	if err != nil {
		return nil, err
	}

	// respCh := make(chan ResponseRecord)
	// errCh := make(chan error)
	// err = responsestream.StreamConsumer(responseStream, processRespRecord, respCh, nil)

	response := make([]ResponseRecord, len(resp))
	for i, entry := range resp {
		response[i] = ResponseRecord{
			ObjectID:  entry.Op.GetObjectId(),
			Bucket:    entry.Op.GetBucket(),
			State:     logOpToObjectState(entry.Op),
			Timestamp: entry.Op.GetTimestamp().GetVc(),
		}
	}

	return response, err
}

func processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	return nil
}

// Query ...
func (c *Client) Query(query string) (<-chan ResponseRecord, <-chan error, error) {
	responseStream, err := qpugraph.SendQuerySQL(query, c.client)
	if err != nil {
		return nil, nil, err
	}

	respCh := make(chan ResponseRecord)
	errCh := make(chan error)
	err = responsestream.StreamConsumer(responseStream, processRespRecord, respCh, nil)

	return respCh, errCh, err
}

// GetDataTransfer ...
// func (c *Client) GetDataTransfer() (float64, error) {
// 	dataTransferred, err := c.client.GetDataTransfer()
// 	if err != nil {
// 		return -1.0, err
// 	}
// 	return float64(dataTransferred.GetKBytesTranferred()), nil
// }

func logOpToObjectState(logOp *qpu.LogOperation) map[string]string {
	attrs := logOp.GetPayload().GetState().GetAttributes()
	state := make(map[string]string, 0)
	for attrKey, attrVal := range attrs {
		state[attrKey] = valueToString(attrVal)
	}
	return state
}

func valueToString(val *qpu.Value) string {
	switch val.Val.(type) {
	case *qpu.Value_Int:
		return strconv.Itoa(int(val.GetInt()))
	case *qpu.Value_Flt:
		return fmt.Sprintf("%f", val.GetFlt())
	case *qpu.Value_Str:
		return val.GetStr()
	default:
		return ""
	}
}
