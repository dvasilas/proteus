package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// This package implements the low-level API server functionality of the QPU
// using a grpc server.

// Server represents the QPU's API server.
type Server struct {
	port       string
	grpcServer *grpc.Server
	api        libqpu.APIProcessor
}

// NewServer initializes a grpc server.
func NewServer(port string, api libqpu.APIProcessor) (*Server, error) {
	server := Server{
		port:       port,
		grpcServer: grpc.NewServer(),
		api:        api,
	}

	qpu_api.RegisterQPUAPIServer(server.grpcServer, &server)
	reflection.Register(server.grpcServer)

	return &server, nil
}

// Serve starts the server and listents for connections.
// It returns only when an error occurs.
func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return err
	}

	fmt.Println("QPU service starting")
	return s.grpcServer.Serve(lis)
}

// Query implements the QPU's low level Query API.
func (s *Server) Query(stream qpu_api.QPUAPI_QueryServer) error {
	requestRecord, err := stream.Recv()
	if err == io.EOF {
		return libqpu.Error(errors.New("Query:stream.Recv EOF"))
	}
	if err != nil {
		return err
	}

	switch requestRecord.GetRequest().(type) {
	case *qpu_api.RequestStreamRecord_QueryRequest:
		return s.api.Query(
			libqpu.QueryRequest{Req: requestRecord.GetQueryRequest()},
			libqpu.RequestStream{Stream: stream},
		)
	default:
		return libqpu.Error(errors.New("Query expects RequestStream_Request"))
	}
}

// QueryUnary ...
func (s *Server) QueryUnary(ctx context.Context, req *qpu_api.QueryRequest) (*qpu_api.QueryResponse, error) {
	resp, err := s.api.QueryUnary(
		libqpu.QueryRequest{Req: req},
	)
	results := make([]*qpu.LogOperation, len(resp))
	for i, entry := range resp {
		results[i] = entry.Op
	}
	return &qpu_api.QueryResponse{
		Results: results,
	}, err
}

// GetConfig implements the QPU's low level GetConfig API.
func (s *Server) GetConfig(ctx context.Context, in *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error) {
	return s.api.GetConfig(ctx, in)
}

// GetDataTransfer implements the QPU's low level GetDataTransfer API.
func (s *Server) GetDataTransfer(ctx context.Context, in *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error) {
	return s.api.GetDataTransfer(ctx, in)
}
