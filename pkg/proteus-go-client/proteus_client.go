package proteusclient

import (
	"context"
	"io"
	"strconv"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/dvasilas/proteus/internal/tracer"
	connpool "github.com/dvasilas/proteus/pkg/proteus-go-client/connection_pool"
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

// Query ...
func (c *Client) Query(queryStmt string) (*qpu_api.QueryResp, error) {
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

	return resp, err
}

// QueryArgs ...
func (c *Client) QueryArgs() (*qpu_api.QueryResponse, error) {
	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	req := &qpu_api.QueryRequest{
		Query: &qpu_api.Query{
			Query: &qpu_api.Query_QueryAst{
				QueryAst: &qpu_api.ASTQuery{
					Table:      "table",
					Projection: []string{"a", "b", "c"},
					Predicate: []*qpu.AttributePredicate{
						&qpu.AttributePredicate{
							Attr:   &qpu.Attribute{AttrKey: "attr"},
							Lbound: &qpu.Value{Val: &qpu.Value_Int{Int: 0}},
							Ubound: &qpu.Value{Val: &qpu.Value_Int{Int: 100}},
						},
					},
					TsPredicate: &qpu.SnapshotTimePredicate{
						Lbound: &qpu.SnapshotTime{Type: qpu.SnapshotTime_LATEST},
						Ubound: &qpu.SnapshotTime{Type: qpu.SnapshotTime_INF},
					},
					Limit: 10,
				},
			},
		},
		Metadata: nil,
		Sync:     false,
	}

	resp, err := client.Cli.QueryArgs(context.TODO(), req)

	c.pool.Return(client)

	return resp, err
}

// QueryNoOp ...
func (c *Client) QueryNoOp() (string, error) {
	client, err := c.pool.Get()
	if err != nil {
		return "", err
	}

	resp, err := client.Cli.QueryNoOp(context.TODO(), &qpu_api.NoOpReq{Str: "hey"})

	c.pool.Return(client)

	return resp.GetStr(), err
}
