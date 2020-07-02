package proteusclient

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/dvasilas/proteus/internal/queries"
	"github.com/dvasilas/proteus/internal/tracer"
	connpool "github.com/dvasilas/proteus/pkg/proteus-go-client/connection_pool"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"
)

// Client represents a connection to Proteus.
type Client struct {
	pool   *connpool.ConnectionPool
	closer io.Closer
	// conn *grpcutils.GrpcClientConn
	// cli  qpu_api.QPUAPIClient
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

var poolSize = 64
var poolOverflow = 16

// Vectorclock ...
type Vectorclock map[string]*tspb.Timestamp

// NewClient creates a new Proteus client connected to the given QPU server.
func NewClient(host Host, tracing bool) (*Client, error) {
	var closer io.Closer
	closer = nil
	if tracing {
		tracer, cl, err := tracer.NewTracer()
		if err != nil {
			return nil, err
		}
		opentracing.SetGlobalTracer(tracer)
		closer = cl
	}

	return &Client{
		pool:   connpool.NewConnectionPool(host.Name+":"+strconv.Itoa(host.Port), true, poolSize, poolOverflow, tracing),
		closer: closer,
	}, nil
}

// Close closes the connection to Proteus.
func (c *Client) Close() error {
	if c.closer != nil {
		c.closer.Close()
	}
	return c.pool.Close()
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

func (c *Client) query(req libqpu.QueryRequest, parentSpan opentracing.Span) (*qpu_api.QueryResponse, error) {
	var queryISp opentracing.Span
	queryISp = nil
	ctx := context.TODO()
	if parentSpan != nil {
		if tracer := opentracing.GlobalTracer(); tracer != nil {
			queryISp = tracer.StartSpan("client/query_internal", opentracing.ChildOf(parentSpan.Context()))
			defer queryISp.Finish()

			ctx = opentracing.ContextWithSpan(context.Background(), queryISp)
		}
	}

	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	resp, err := client.Cli.QueryUnary(ctx, req.Req)

	c.pool.Return(client)

	return resp, err
}

func (c *Client) QueryNoOp() (string, error) {
	client, err := c.pool.Get()
	if err != nil {
		return "", err
	}

	resp, err := client.Cli.QueryNoOp(context.TODO(), &qpu_api.NoOpReq{Str: "hey"})

	c.pool.Return(client)

	return resp.GetStr(), nil
}

// QueryInternal ...
func (c *Client) QueryInternal(table string, predicate []*qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, limit int64, metadata map[string]string, sync bool) ([]ResponseRecord, error) {
	var querySp opentracing.Span
	querySp = nil
	if tracer := opentracing.GlobalTracer(); tracer != nil {
		querySp = tracer.StartSpan("client/query")
		defer querySp.Finish()
	}

	query := queries.NewQuerySnapshot(table, predicate, []string{}, []string{}, []string{}, limit, ts)
	resp, err := c.query(libqpu.NewQueryRequestI(query, nil, false), querySp)
	if err != nil {
		return nil, err
	}

	logOps := make([]libqpu.LogOperation, len(resp.GetResults()))
	for i, entry := range resp.GetResults() {
		logOps[i] = libqpu.LogOperation{Op: entry}
	}

	response := make([]ResponseRecord, len(logOps))
	for i, entry := range logOps {
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
