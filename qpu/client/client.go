package client

import (
	"context"

	"github.com/dvasilas/proteus/protos"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	"google.golang.org/grpc"
)

//Client ...
type Client struct {
	cli  pb.QPUClient
	conn *grpc.ClientConn
}

//Find ...
func (c *Client) Query(predicate []*pbQPU.AttributePredicate, ts *pbQPU.TimestampPredicate, ops bool, sync bool) (pb.QPU_QueryClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	err = stream.Send(protoutils.RequestStreamRequest(ts, predicate, ops, sync))
	return stream, cancel, err
}

//GetConfig ...
func (c *Client) GetConfig() (*pb.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, protoutils.ConfigRequest())
	return resp, err
}

//CloseConnection ...
func (c *Client) CloseConnection() error {
	return c.conn.Close()
}

//NewClient ...
func NewClient(address string) (Client, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return Client{}, nil, err
	}
	return Client{cli: pb.NewQPUClient(conn), conn: conn}, conn, nil
}
