package client

import (
	"context"
	"io"

	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	"google.golang.org/grpc"
)

//Predicate ..
type Predicate struct {
	Attribute string
	Datatype  string
	LBound    *pbQPU.Value
	UBound    *pbQPU.Value
}

//Client ...
type Client struct {
	cli pb.QPUClient
}

//Find ...
func (c *Client) Find(ts int64, predicate []Predicate, msg chan *pb.QueryResultStream, done chan bool, errs chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := new(pb.FindRequest)
	req.Timestamp = ts
	for _, p := range predicate {
		req.Predicate = append(req.Predicate, &pbQPU.Predicate{Datatype: p.Datatype, Attribute: p.Attribute, Lbound: p.LBound, Ubound: p.UBound})
	}

	stream, err := c.cli.Find(ctx, req)
	if err != nil {
		done <- true
		errs <- err
	}
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			done <- true
			errs <- nil
			break
		}
		if err != nil {
			done <- true
			errs <- err
		}
		done <- false
		msg <- streamMsg
	}
}

//GetConfig ...
func (c *Client) GetConfig() (*pb.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, &pb.ConfigRequest{})
	return resp, err
}

//NewClient ...
func NewClient(address string) (Client, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return Client{}, nil, err
	}
	return Client{pb.NewQPUClient(conn)}, conn, nil
}
