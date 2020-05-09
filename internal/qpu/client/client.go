package client

import (
	"context"

	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"google.golang.org/grpc"
)

//Client ...
type Client struct {
	cli  qpu_api.QPUClient
	conn *grpc.ClientConn
}

// Query ...
func (c *Client) Query(bucket string, predicate []*qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, metadata map[string]string, sync bool) (qpu_api.QPU_QueryClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	err = stream.Send(
		protoutils.RequestStreamRequest(
			protoutils.QueryInternal(bucket, ts, predicate),
			metadata,
			sync,
		),
	)
	return stream, cancel, err
}

// QuerySQL ...
func (c *Client) QuerySQL(query string, metadata map[string]string, sync bool) (qpu_api.QPU_QueryClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	err = stream.Send(
		protoutils.RequestStreamRequest(
			protoutils.QuerySQL(query),
			metadata,
			sync,
		),
	)
	return stream, cancel, err
}

//GetConfig ...
func (c *Client) GetConfig() (*qpu_api.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, protoutils.ConfigRequest())
	return resp, err
}

//GetDataTransfer ...
func (c *Client) GetDataTransfer() (*qpu_api.DataTransferResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetDataTransfer(ctx, &qpu_api.GetDataRequest{})
	return resp, err
}

// Forward ...
func (c *Client) Forward() (qpu_api.QPU_QueryClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	return stream, cancel, err
}

//CloseConnection ...
func (c *Client) CloseConnection() error {
	return c.conn.Close()
}

//NewClient ...
func NewClient(address string) (Client, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return Client{}, err
	}
	return Client{cli: qpu_api.NewQPUClient(conn), conn: conn}, nil
}
