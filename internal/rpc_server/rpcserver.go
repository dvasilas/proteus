package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	grpcutils "github.com/dvasilas/proteus/internal/grpc"
	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	workerpool "github.com/dvasilas/proteus/internal/worker_pool"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"google.golang.org/grpc/reflection"
)

// This package implements the low-level API server functionality of the QPU
// using a grpc server.

// Server represents the QPU's API server.
type Server struct {
	port          string
	tracing       bool
	grpcServers   []grpcutils.GrpcServer
	api           libqpu.APIProcessor
	state         libqpu.QPUState
	dispatcher    *workerpool.Dispatcher
	responseTimeM metrics.LatencyM
}

// NewServer initializes a grpc server.
func NewServer(port string, tracing bool, api libqpu.APIProcessor, state libqpu.QPUState, conf *libqpu.QPUConfig) (*Server, error) {
	grpcServers := make([]grpcutils.GrpcServer, conf.ProcessingConfig.GrpcServers)

	for i := 0; i < conf.ProcessingConfig.GrpcServers; i++ {
		grpcServer, err := grpcutils.NewServer(tracing)
		if err != nil {
			return nil, err
		}
		grpcServers[i] = grpcServer
	}

	server := Server{
		port:          port,
		tracing:       tracing,
		grpcServers:   grpcServers,
		api:           api,
		state:         state,
		responseTimeM: metrics.NewLatencyM(),
	}

	server.dispatcher = workerpool.NewDispatcher(conf.ProcessingConfig.API.MaxWorkers, conf.ProcessingConfig.API.MaxJobQueue)
	server.dispatcher.Run()

	for _, s := range grpcServers {
		qpu_api.RegisterQPUAPIServer(s.Server, &server)
		reflection.Register(s.Server)
	}

	return &server, nil
}

// Serve starts the server and listents for connections.
// It returns only when an error occurs.
func (s *Server) Serve() error {
	fmt.Println("Serve()")
	port, err := strconv.ParseInt(s.port, 10, 64)
	if err != nil {
		return err
	}
	for i, grpcS := range s.grpcServers {
		lis, err := net.Listen("tcp", ":"+strconv.FormatInt(port+int64(i), 10))
		if err != nil {
			return err
		}

		if i < len(s.grpcServers)-1 {
			go func(s grpcutils.GrpcServer) {
				err = s.Server.Serve(lis)
				if err != nil {
					log.Fatal(err)
				}
			}(grpcS)
		} else {
			fmt.Println("QPU service starting")
			return grpcS.Server.Serve(lis)
		}
	}
	return errors.New("should not have reached this point")
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
	t0 := time.Now()

	work := &Job{
		server: s,
		ctx:    ctx,
		req: libqpu.NewQueryRequest(
			libqpu.NewQuery(libqpu.NewSQLQuery(req.QueryStr), nil),
			nil,
			false,
			false),
		result: &jobResult{},
		done:   make(chan bool),
	}

	s.dispatcher.JobQueue <- work

	<-work.done

	if err := s.responseTimeM.AddFromTs(t0); err != nil {
		return nil, err
	}

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
	metrics, err := s.api.GetMetrics(ctx, req)

	var RT50, RT90, RT95, RT99 float64
	RT50, RT90, RT95, RT99 = s.responseTimeM.GetMetrics()

	metrics.ResponseTimeP50 = RT50
	metrics.ResponseTimeP90 = RT90
	metrics.ResponseTimeP95 = RT95
	metrics.ResponseTimeP99 = RT99

	return metrics, err
}

// LobstersFrontpage ...
func (s *Server) LobstersFrontpage(ctx context.Context, req *pb.LobFrontpageReq) (*pb.LobFrontpageResp, error) {
	work := &JobFrontPage{
		server: s,
		ctx:    ctx,
		result: &JobFrontPageResult{},
		done:   make(chan bool),
	}

	s.dispatcher.JobQueue <- work

	<-work.done

	return work.result.response, work.result.err
}

// LobstersStoryVote ...
func (s *Server) LobstersStoryVote(ctx context.Context, req *pb.LobStoryVoteReq) (*pb.LobStoryVoteResp, error) {
	work := &JobStoryVote{
		server: s,
		ctx:    ctx,
		req:    req,
		result: &JobStoryVoteResult{},
		done:   make(chan bool),
	}

	s.dispatcher.JobQueue <- work

	<-work.done

	return &pb.LobStoryVoteResp{}, work.result.err
}

// JobFrontPage ...
type JobFrontPage struct {
	server *Server
	ctx    context.Context
	result *JobFrontPageResult
	done   chan bool
}

// Do ...
func (j *JobFrontPage) Do() {
	j.do(j.ctx, j.server)
	j.done <- true
}

func (j *JobFrontPage) do(ctx context.Context, s *Server) {
	resp, err := s.state.LobstersFrontpage()

	j.result.response = resp
	j.result.err = err
}

// JobFrontPageResult ...
type JobFrontPageResult struct {
	response *pb.LobFrontpageResp
	err      error
}

// JobStoryVote ...
type JobStoryVote struct {
	server *Server
	ctx    context.Context
	req    *pb.LobStoryVoteReq
	result *JobStoryVoteResult
	done   chan bool
}

// Do ...
func (j *JobStoryVote) Do() {
	j.do(j.ctx, j.server)
	j.done <- true
}

func (j *JobStoryVote) do(ctx context.Context, s *Server) {
	err := s.state.LobstersStoryVote(j.req)
	j.result.err = err
}

// JobStoryVoteResult ...
type JobStoryVoteResult struct {
	err error
}
