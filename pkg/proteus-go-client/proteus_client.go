package proteusclient

import (
	"context"
	//"fmt"
	"io"
	//"runtime/debug"
	"strconv"

	"github.com/dvasilas/proteus/internal/tracer"
	connpool "github.com/dvasilas/proteus/pkg/proteus-go-client/connection_pool"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
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

	r := &pb.QueryReq{
		QueryStr: queryStmt,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Cli.QueryUnary(ctx, r)

//	if err != nil {
//		fmt.Println(err)
//		debug.PrintStack()
//	}

	c.pool.Return(client)

	return resp, err
}

// GetMetrics ...
func (c *Client) GetMetrics() (*pb.MetricsResponse, error) {
	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	r := &pb.MetricsRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Cli.GetMetrics(ctx, r)

	c.pool.Return(client)

	return resp, err
}

// LobstersFrontpage ...
func (c *Client) LobstersFrontpage() (*pb.LobFrontpageResp, error) {
	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	r := &pb.LobFrontpageReq{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Cli.LobstersFrontpage(ctx, r)

	c.pool.Return(client)

	return resp, err
}

// LobstersStoryVote ...
func (c *Client) LobstersStoryVote(storyID int64, vote int) (*pb.LobStoryVoteResp, error) {
	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	r := &pb.LobStoryVoteReq{
		StoryID: storyID,
		Vote:    int64(vote),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Cli.LobstersStoryVote(ctx, r)

	c.pool.Return(client)

	return resp, err
}

// LobstersStoryVoteInsert ...
func (c *Client) LobstersStoryVoteInsert(storyID int64, vote int) (*pb.LobStoryVoteResp, error) {
	client, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	r := &pb.LobStoryVoteReq{
		StoryID: storyID,
		Vote:    int64(vote),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Cli.LobstersStoryVoteInsert(ctx, r)

	c.pool.Return(client)

	return resp, err
}
