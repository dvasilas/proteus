package client

import (
	"context"
	"fmt"
	"io"

	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"

	"google.golang.org/grpc"
)

//Client ...
type Client struct {
	sQPUClient pb.QPUClient
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
func (c *Client) Find(ts int64, predicate map[string][2]*pbQPU.Value, msg chan *pbQPU.Object, done chan bool) error {
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
	return Client{pb.NewQPUClient(conn)}, conn, nil
}
