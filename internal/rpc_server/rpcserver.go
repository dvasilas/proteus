package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	grpcutils "github.com/dvasilas/proteus/internal/grpc"
	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	workerpool "github.com/dvasilas/proteus/internal/worker_pool"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"google.golang.org/grpc/reflection"
)

// This package implements the low-level API server functionality of the QPU
// using a grpc server.

// Server represents the QPU's API server.
type Server struct {
	port       string
	tracing    bool
	grpcServer grpcutils.GrpcServer
	api        libqpu.APIProcessor
	// temporarily here
	state      libqpu.QPUState
	dispatcher *workerpool.Dispatcher
}

// NewServer initializes a grpc server.
func NewServer(port string, tracing bool, api libqpu.APIProcessor, state libqpu.QPUState, conf *libqpu.QPUConfig) (*Server, error) {
	grpcServer, err := grpcutils.NewServer(tracing)
	if err != nil {
		return nil, err
	}

	server := Server{
		port:       port,
		tracing:    tracing,
		grpcServer: grpcServer,
		api:        api,
		state:      state,
	}

	server.dispatcher = workerpool.NewDispatcher(conf.MaxWorkers, conf.MaxJobQueue)
	server.dispatcher.Run()

	qpu_api.RegisterQPUAPIServer(grpcServer.Server, &server)
	reflection.Register(grpcServer.Server)

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
	return s.grpcServer.Server.Serve(lis)
}

// Query implements the QPU's low level Query API.
func (s *Server) Query(stream qpu_api.QPUAPI_QueryServer) error {
	requestRecord, err := stream.Recv()
	if err == io.EOF {
		return utils.Error(errors.New("Query:stream.Recv EOF"))
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
		return utils.Error(errors.New("Query expects RequestStream_Request"))
	}
}

// Job ...
type Job struct {
	server *Server
	ctx    context.Context
	req    libqpu.QueryRequest
	result *jobResult
	done   chan bool
	// do     func(*Server, context.Context, *qpu_api.QueryReq, interface{})
}

// Do ...
func (j *Job) Do() {
	j.do(j.ctx, j.server, j.req)
	j.done <- true
}

func (j *Job) do(ctx context.Context, s *Server, req libqpu.QueryRequest) {
	resp, err := s.api.QueryUnary(req, nil)

	j.result.response = resp
	j.result.err = err
}

type jobResult struct {
	response *pb.QueryResp
	err      error
}

// QueryUnary ...
func (s *Server) QueryUnary(ctx context.Context, req *pb.QueryReq) (*pb.QueryResp, error) {
	work := &Job{
		server: s,
		ctx:    ctx,
		req: libqpu.NewQueryRequest(
			libqpu.NewQuery(libqpu.NewSQLQuery(req.QueryStr), nil),
			nil,
			false),
		result: &jobResult{},
		done:   make(chan bool),
	}

	s.dispatcher.JobQueue <- work

	<-work.done

	return work.result.response, work.result.err
}

// GetConfig implements the QPU's low level GetConfig API.
func (s *Server) GetConfig(ctx context.Context, req *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error) {
	return s.api.GetConfig(ctx, req)
}

// GetDataTransfer implements the QPU's low level GetDataTransfer API.
// func (s *Server) GetDataTransfer(ctx context.Context, in *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error) {
// 	return s.api.GetDataTransfer(ctx, in)
// }

// GetMetrics ...
func (s *Server) GetMetrics(ctx context.Context, req *pb.MetricsRequest) (*pb.MetricsResponse, error) {
	return s.api.GetMetrics(ctx, req)
}
