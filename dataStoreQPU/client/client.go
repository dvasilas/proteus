package client

import (
	"context"
	"fmt"
	"io"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"google.golang.org/grpc"
)

type activeStreams struct {
	opSubStreams map[int64]context.CancelFunc
}

//Client ...
type Client struct {
	dsClient      pb.DataStoreQPUClient
	activeStreams activeStreams
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
		fmt.Println("SubscribeStates: ", getObjReply)
	}
	return nil
}

// SubscribeOps ...
func (c *Client) SubscribeOps(ts int64, msg chan *pbQPU.Operation, done chan bool) (int64, error) {
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := c.dsClient.SubscribeOps(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		cancel()
		return 0, err
	}
	c.activeStreams.opSubStreams[ts] = cancel
	go func() {
		for {
			streamMsg, err := stream.Recv()
			fmt.Println(streamMsg, err)
			if err == io.EOF {
				break
			}
			if err != nil {
				return
			}
			done <- false
			msg <- streamMsg.Operation
		}
	}()
	return ts, nil
}

//StopOpsSubscription ...
func (c *Client) StopOpsSubscription(subID int64) {
	fmt.Println("StopOpsSubscription")
	cancel := c.activeStreams.opSubStreams[subID]
	cancel()
	return
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
	activeStrMap := make(map[int64]context.CancelFunc)
	return Client{pb.NewDataStoreQPUClient(conn), activeStreams{activeStrMap}}, conn, nil
}
