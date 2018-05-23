package client

import (
	"context"
	"io"
	"log"
	"time"

	pb "modqp/data_store/datastore"

	"google.golang.org/grpc"
)

type Client struct {
	dsClient pb.DataStoreClient
}

func (c *Client) PutObjectMD(key string, mdKey string, mdVal string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.dsClient.PutObjectMD(ctx, &pb.PutObjMDRequest{Key: key, MdKey: mdKey, MdValue: mdVal})
	if err != nil {
		log.Fatalf("failed: %v", err)
	}
	return r.Message, r.Timestamp, nil
}

func (c *Client) GetObjectMD(key string, ts int64) (string, *pb.ObjectMD, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.dsClient.GetObjectMD(ctx, &pb.GetObjMDRequest{Key: key, Timestamp: ts})
	if err != nil {
		log.Fatalf("failed: %v", err)
	}
	return r.Message, r.Object, nil
}

func (c *Client) GetOperations(ts int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.GetOperations(ctx, &pb.SubscribeRequest{Timestamp: ts})
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

func (c *Client) Subscribe(ts int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.dsClient.Subscribe(ctx, &pb.SubscribeRequest{Timestamp: ts})
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
		log.Println(streamMsg)
	}
	return nil
}

func NewDSClient(address string) (Client, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return Client{pb.NewDataStoreClient(conn)}, conn
}

func init() {
}
