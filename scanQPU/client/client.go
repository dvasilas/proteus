package client

import (
	"context"
	"fmt"
	"io"

	pbQPU "github.com/dimitriosvasilas/modqp/qpupb"
	pb "github.com/dimitriosvasilas/modqp/scanQPU/sqpupb"

	"google.golang.org/grpc"
)

//Client ...
type Client struct {
	sQPUClient pb.ScanQPUClient
}

func queryresultConsumer(msg chan *pbQPU.Object, done chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			return
		}
		result := <-msg
		fmt.Println(result)
	}
}

//Find ...
func (c *Client) Find(ts int64, predicate map[string][2]int64, msg chan *pbQPU.Object, done chan bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := new(pb.FindRequest)
	req.Timestamp = ts
	for attr, bounds := range predicate {
		pred := new(pbQPU.Predicate)
		pred.Attribute = attr
		pred.Lbound = bounds[0]
		pred.Ubound = bounds[1]
		req.Predicate = append(req.Predicate, pred)
	}

	stream, err := c.sQPUClient.Find(ctx, req)

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
	return Client{pb.NewScanQPUClient(conn)}, conn, nil
}
