package client

import (
	"context"
	"io"
	"log"
	"time"

	pb "github.com/dimitriosvasilas/modqp/dataStore/datastore"

	"google.golang.org/grpc"
)

// Client ...
type Client struct {
	dsClient pb.DataStoreClient
}

// PutObjectMD ...
func (c *Client) PutObjectMD(key string, attributes map[string]string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.dsClient.PutObjectMD(ctx, &pb.PutObjMDRequest{Key: key, Attributes: attributes})
	if err != nil {
		log.Fatalf("failed: %v", err)
	}
	return r.Message, r.Timestamp, nil
}

// GetObjectMD ...
func (c *Client) GetObjectMD(key string, ts int64) (string, *pb.ObjectMD, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.dsClient.GetObjectMD(ctx, &pb.GetObjMDRequest{Key: key, Timestamp: ts})
	if err != nil {
		log.Fatalf("failed: %v", err)
	}
	return r.Message, r.Object, nil
}

// GetSnapshot ...
func (c *Client) GetSnapshot(ts int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.GetSnapshot(ctx, &pb.SubscribeRequest{Timestamp: ts})
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
		log.Println(getObjReply)
	}
}

// SubscribeOps ...
func (c *Client) SubscribeOps(ts int64, msg chan *pb.StreamMsg, done chan bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.SubscribeOps(ctx, &pb.SubscribeRequest{Timestamp: ts})
	if err != nil {
		log.Fatalf("subscribe failed %v", err)
	}
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			done <- true
		}
		if err != nil {
			log.Fatalf("stream.Recv failed %v", err)
		}
		done <- false
		msg <- streamMsg
	}
}

// NewDSClient ...
func NewDSClient(address string) (Client, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return Client{pb.NewDataStoreClient(conn)}, conn
}

func init() {
}
