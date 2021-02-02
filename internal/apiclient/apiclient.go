package apiclient

import (
	"context"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	"google.golang.org/grpc"
)

// This package is responsible for providing an API for the QPU to communicate
// with adjacent QPUs.
// It provides the QPU API as local methods (the RPC model).

// QPUAPIClient represents a connection to an adjacent QPU
type QPUAPIClient struct {
	cli  qpuapi.QPUAPIClient
	conn *grpc.ClientConn
}

// NewClient creates an instance of QPUAPIClient and initiates a connection
// with QPU at the given address
func NewClient(address string) (QPUAPIClient, error) {
	maxMsgSize := 1024 * 1024 * 256
	conn, err := grpc.Dial(address,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithInsecure())
	if err != nil {
		return QPUAPIClient{}, err
	}
	return QPUAPIClient{
		cli:  qpuapi.NewQPUAPIClient(conn),
		conn: conn,
	}, nil
}

// Query implements the QPU's API Query method.
// Tt is called the Query method is invoked.
func (c QPUAPIClient) Query(queryReq libqpu.QueryRequest) (libqpu.ResponseStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		cancel()
		return libqpu.ResponseStream{}, nil
	}
	err = stream.Send(
		&qpuapi.RequestStreamRecord{
			Request: &qpuapi.RequestStreamRecord_QueryRequest{
				QueryRequest: queryReq.Req,
			},
		},
	)
	return libqpu.ResponseStream{
		Stream:     stream,
		CancelFunc: cancel,
	}, err
}

// QuerySQL implements the QPU's API QuerySQL method.
func (c QPUAPIClient) QuerySQL(query string, metadata map[string]string, sync bool) (libqpu.ResponseStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		cancel()
		return libqpu.ResponseStream{}, err
	}
	err = stream.Send(
		libqpu.RequestStreamRequest(
			libqpu.NewQuery(libqpu.NewSQLQuery(query), nil),
			metadata,
			sync,
		),
	)
	return libqpu.ResponseStream{
		Stream:     stream,
		CancelFunc: cancel,
	}, err
}

// QueryUnary ...
func (c QPUAPIClient) QueryUnary(query string) (*qpuextapi.QueryResp, error) {
	return c.cli.QueryUnary(context.Background(), &qpuextapi.QueryReq{
		QueryStr: query,
	})
}

// QueryUnary1 ...
func (c QPUAPIClient) QueryUnary1(query string) (*qpuextapi.QueryResp1, error) {
	return c.cli.QueryUnary1(context.Background(), &qpuextapi.QueryReq{
		QueryStr: query,
	})
}

// GetConfig implements the QPU's API GetConfig method.
func (c QPUAPIClient) GetConfig() (*qpuapi.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, libqpu.ConfigRequest())
	return resp, err
}

// GetWriteLog ...
func (c QPUAPIClient) GetWriteLog() (qpuapi.QPUAPI_GetWriteLogClient, error) {
	ctx, _ := context.WithCancel(context.Background())
	return c.cli.GetWriteLog(ctx, &qpuextapi.GetWriteLogReq{})
}

// GetDataTransfer implements the QPU's API GetDataTransfer method.
// func (c QPUAPIClient) GetDataTransfer() (*qpuapi.DataTransferResponse, error) {
// 	ctx := context.TODO()
// 	resp, err := c.cli.GetDataTransfer(ctx, &qpuapi.GetDataRequest{})
// 	return resp, err
// }

// Forward implements the QPU's API Forward method.
func (c *QPUAPIClient) Forward() (qpuapi.QPUAPI_QueryClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.cli.Query(ctx)
	if err != nil {
		return nil, cancel, nil
	}
	return stream, cancel, err
}

// CloseConnection closes the given grpc.ClientConn
func (c QPUAPIClient) CloseConnection() error {
	return c.conn.Close()
}
