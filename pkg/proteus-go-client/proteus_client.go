package proteusclient

import (
	"context"
	"io"
	"strconv"

	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/dvasilas/proteus/internal/tracer"
	connpool "github.com/dvasilas/proteus/pkg/proteus-go-client/connection_pool"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"
)

// Client represents a connection to Proteus.
type Client struct {
	pool   *connpool.ConnectionPool
	closer io.Closer
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
func NewClient(host Host, poolSize, poolOverflow int, tracing bool) (*Client, error) {
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

// Query ...
func (c *Client) Query(queryStmt string) (*pb.QueryResp, error) {
	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	r := &qpu_api.QueryReq{
		QueryStr: queryStmt,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Cli.QueryUnary(ctx, r)

	c.pool.Return(client)

	response := make([]*pb.QueryRespRecord, len(resp.GetRespRecord()))
	for i, rec := range resp.GetRespRecord() {
		response[i] = &pb.QueryRespRecord{
			RecordId:   rec.GetRecordId(),
			Attributes: rec.GetAttributes(),
			Timestamp:  rec.GetTimestamp(),
		}
	}

	return &pb.QueryResp{
		RespRecord: response,
	}, err
}
