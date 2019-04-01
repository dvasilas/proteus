package client

import (
	"context"

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
func (c *Client) Find(ts *pbQPU.TimestampPredicate, predicate []*pbQPU.AttributePredicate) (pb.QPU_FindClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.FindRequest{Timestamp: ts, Predicate: predicate}
	stream, err := c.cli.Find(ctx, req)
	return stream, cancel, err
}

//GetConfig ...
func (c *Client) GetConfig() (*pb.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, &pb.ConfigRequest{})
	return resp, err
}

//SubscribeOps ...
func (c *Client) SubscribeOps(ts *pbQPU.TimestampPredicate, sync bool) (pb.QPU_SubscribeOpsClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.SubscribeOps(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	err = stream.Send(&pb.ReqStream{Payload: &pb.ReqStream_Request{Request: &pb.SubRequest{Timestamp: ts, Sync: sync}}})
	return stream, cancel, err
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
