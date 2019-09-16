package client

import (
	"context"

	"github.com/dvasilas/proteus/src/protos"
	pb "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"google.golang.org/grpc"
)

//Client ...
type Client struct {
	cli  pb.QPUClient
	conn *grpc.ClientConn
}

//Query ...
func (c *Client) Query(predicate []*pbUtils.AttributePredicate, ts *pbUtils.SnapshotTimePredicate, sync bool) (pb.QPU_QueryClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	err = stream.Send(protoutils.RequestStreamRequest(ts, predicate, sync))
	return stream, cancel, err
}

//GetConfig ...
func (c *Client) GetConfig() (*pb.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, protoutils.ConfigRequest())
	return resp, err
}

// Ping ...
func (c *Client) Forward() (pb.QPU_QueryClient, context.CancelFunc, error) {
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
	return Client{cli: pb.NewQPUClient(conn), conn: conn}, nil
}
