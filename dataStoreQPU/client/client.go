package client

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpu"

	"google.golang.org/grpc"
)

const (
	address = "localhost:50052"
)

//Client ...
type Client struct {
	dsClient pb.DataStoreQPUClient
}

//SubscribeStates ...
func (c *Client) SubscribeStates(ts int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.SubscribeStates(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		log.Fatalf("getOperations failed %v", err)
	}
	for {
		getObjReply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("stream.Recv failed %v", err)
		}
		fmt.Println("SubscribeOps: ", getObjReply)
	}
}

// SubscribeOps ...
func (c *Client) SubscribeOps(ts int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.SubscribeOps(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		log.Fatalf("subscribe failed %v", err)
	}
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("stream.Recv failed %v", err)
		}
		log.Println("SubscribeOps: ", streamMsg)
	}
	return nil
}

//List ...
func (c *Client) List(ts int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.GetSnapshot(ctx, &pb.SubRequest{Timestamp: ts})
	if err != nil {
		log.Fatalf("List failed %v", err)
	}
	for {
		Object, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("stream.Recv failed %v", err)
		}
		fmt.Println(Object)
	}
	return nil
}

//Query ...
func (c *Client) Query(ts int64, predicate map[string][2]int64, msg chan *pb.ObjectMD, done chan bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := new(pb.QueryRequest)
	req.Timestamp = ts
	for attr, bounds := range predicate {
		pred := new(pb.Predicate)
		pred.Attribute = attr
		pred.Lbound = bounds[0]
		pred.Ubound = bounds[1]
		req.Predicate = append(req.Predicate, pred)
	}

	stream, err := c.dsClient.Query(ctx, req)

	if err != nil {
		log.Fatalf("Query failed %v", err)
	}
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			done <- true
			break
		}
		if err != nil {
			log.Fatalf("stream.Recv failed %v", err)
		}
		done <- false
		msg <- streamMsg.State
	}
	return nil
}

//NewDSQPUClient ...
func NewDSQPUClient(address string) (Client, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return Client{pb.NewDataStoreQPUClient(conn)}, conn
}
