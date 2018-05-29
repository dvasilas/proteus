package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

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
	stream, err := c.dsClient.SubscribeStates(ctx, &pb.QueryRequest{Timestamp: ts})
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
	stream, err := c.dsClient.SubscribeOps(ctx, &pb.QueryRequest{Timestamp: ts})
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

//NewDSQPUClient ...
func NewDSQPUClient(address string) (Client, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return Client{pb.NewDataStoreQPUClient(conn)}, conn
}

func main() {
	c, conn := NewDSQPUClient(address)
	defer conn.Close()

	c.SubscribeStates(time.Now().UnixNano())
}
