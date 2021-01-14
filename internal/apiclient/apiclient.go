package apiclient

import (
	"context"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
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
	conn, err := grpc.Dial(address, grpc.WithInsecure())
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
func (c QPUAPIClient) QueryUnary(req libqpu.QueryRequest) (*qpuapi.QueryResponse, error) {
	// ctx := context.TODO()
	// resp, err := c.cli.QueryUnary(ctx, req.Req)
	return nil, nil
}

// GetConfig implements the QPU's API GetConfig method.
func (c QPUAPIClient) GetConfig() (*qpuapi.ConfigResponse, error) {
	ctx := context.TODO()
	resp, err := c.cli.GetConfig(ctx, libqpu.ConfigRequest())
	return resp, err
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
