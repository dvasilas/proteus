package client

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"google.golang.org/grpc"
)

//Client ...
type Client struct {
	dsClient pb.DataStoreQPUClient
}

//SubscribeStates ...
func (c *Client) SubscribeStates(ts int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := c.dsClient.SubscribeStates(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		return err
	}
	for {
		getObjReply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println("SubscribeOps: ", getObjReply)
	}
	return nil
}

// SubscribeOps ...
func (c *Client) SubscribeOps(ts int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := c.dsClient.SubscribeOps(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		return err
	}
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		log.Println("SubscribeOps: ", streamMsg)
	}
	return nil
}

//GetSnapshot ...
func (c *Client) GetSnapshot(ts int64, msg chan *pbQPU.Object, done chan bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := c.dsClient.GetSnapshot(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		return err
	}
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			done <- true
			break
		}
		if err != nil {
			return err
		}
		done <- false
		msg <- streamMsg.Object
	}
	return nil
}

//NewClient ...
func NewClient(address string) (Client, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return Client{}, nil, err
	}
	return Client{pb.NewDataStoreQPUClient(conn)}, conn, nil
}
