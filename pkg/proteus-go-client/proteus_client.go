package proteusclient

import (
	"strconv"

	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/dvasilas/proteus/internal/qpu/client"
)

// Client represents a connection to Proteus.
type Client struct {
	client client.Client
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
	State      []Attribute
	Timestamp  Vectorclock
}

// Attribute ...
type Attribute struct {
	Name  string
	Value string
}

// Vectorclock ...
type Vectorclock map[string]uint64

// NewClient creates a new Proteus client connected to the given QPU server.
func NewClient(host Host) (*Client, error) {
	c, err := client.NewClient(host.Name + ":" + strconv.Itoa(host.Port))
	if err != nil {
		return nil, err
	}
	return &Client{
		client: c,
	}, nil
}

// Close closes the connection to Proteus.
func (c *Client) Close() {
	c.client.CloseConnection()
}
func (c *Client) getResponse(stream qpu_api.QPU_QueryClient, responseCh chan ResponseRecord, errorCh chan error) {
	for {
		streamRec, err := stream.Recv()
		if err != nil {
			errorCh <- err
			close(responseCh)
			close(errorCh)
			return
		}
		if streamRec.GetType() == qpu_api.ResponseStreamRecord_HEARTBEAT {
		} else if streamRec.GetType() == qpu_api.ResponseStreamRecord_END_OF_STREAM {
			close(responseCh)
			close(errorCh)
			return
		} else {
			responseCh <- ResponseRecord{
				SequenceID: streamRec.GetSequenceId(),
				ObjectID:   streamRec.GetLogOp().GetObjectId(),
				Bucket:     streamRec.GetLogOp().GetBucket(),
				State:      logOpToObjectState(streamRec),
				Timestamp:  streamRec.GetLogOp().GetTimestamp().GetVc(),
			}
		}
	}
}

// QueryInternal ...
func (c *Client) QueryInternal(bucket string, predicate []*qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, metadata map[string]string, sync bool) (<-chan ResponseRecord, <-chan error, error) {
	stream, _, err := c.client.Query(bucket, predicate, ts, metadata, sync)
	if err != nil {
		return nil, nil, err
	}

	respCh := make(chan ResponseRecord)
	errCh := make(chan error)

	go c.getResponse(stream, respCh, errCh)

	return respCh, errCh, nil
}

// Query ...
func (c *Client) Query(query string) (<-chan ResponseRecord, <-chan error, error) {
	stream, _, err := c.client.QuerySQL(query, nil, false)
	if err != nil {
		return nil, nil, err
	}

	respCh := make(chan ResponseRecord)
	errCh := make(chan error)

	go c.getResponse(stream, respCh, errCh)

	return respCh, errCh, nil
}

// GetDataTransfer ...
func (c *Client) GetDataTransfer() (float64, error) {
	dataTransferred, err := c.client.GetDataTransfer()
	if err != nil {
		return -1.0, err
	}
	return float64(dataTransferred.GetKBytesTranferred()), nil
}

func logOpToObjectState(record *qpu_api.ResponseStreamRecord) []Attribute {
	logOp := record.GetLogOp()
	var attrs []*qpu.Attribute
	if record.GetType() == qpu_api.ResponseStreamRecord_STATE {
		attrs = logOp.GetPayload().GetState().GetAttrs()
	} else if record.GetType() == qpu_api.ResponseStreamRecord_UPDATEDELTA {
		attrs = logOp.GetPayload().GetDelta().GetNew().GetAttrs()
	}
	state := make([]Attribute, len(attrs))
	for i, attr := range attrs {
		state[i] = Attribute{
			Name:  attr.GetAttrKey(),
			Value: utils.ValueToString(attr.GetValue()),
		}
	}
	return state
}
