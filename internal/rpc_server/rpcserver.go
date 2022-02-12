package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"

	//	"time"

	grpcutils "github.com/dvasilas/proteus/internal/grpc"
	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	workerpool "github.com/dvasilas/proteus/internal/worker_pool"
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
	qpuapi.UnimplementedQPUAPIServer
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
	//	server.dispatcher.Run()

	for _, s := range grpcServers {
		qpuapi.RegisterQPUAPIServer(s.Server, &server)
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
func (s *Server) Query(stream qpuapi.QPUAPI_QueryServer) error {
	requestRecord, err := stream.Recv()
	if err == io.EOF {
		return utils.Error(errors.New("Query:stream.Recv EOF"))
	}
	if err != nil {
		return err
	}

	switch requestRecord.GetRequest().(type) {
	case *qpuapi.RequestStreamRecord_QueryRequest:
		return s.api.Query(
			libqpu.QueryRequest{Req: requestRecord.GetQueryRequest()},
			libqpu.RequestStream{Stream: stream},
		)
	default:
		return utils.Error(errors.New("Query expects RequestStream_Request"))
	}
}

// QuerySubscribe ...
func (s *Server) QuerySubscribe(req *qpuextapi.QueryReq, stream qpuapi.QPUAPI_QuerySubscribeServer) error {
	// requestRecord, err := stream.Recv()
	// if err == io.EOF {
	// 	return utils.Error(errors.New("Query:stream.Recv EOF"))
	// }
	// if err != nil {
	// 	return err
	// }

	// switch requestRecord.GetRequest().(type) {
	// case *qpuapi.RequestStreamRecord_QueryRequest:
	return s.api.QuerySubscribe(req, stream)
	// default:
	// 	return utils.Error(errors.New("Query expects RequestStream_Request"))
	// }
}

// // Job ...
// type Job struct {
// 	server *Server
// 	ctx    context.Context
// 	req    libqpu.QueryRequest
// 	result *jobResult
// 	done   chan bool
// }

// // Do ...
// func (j *Job) Do() {
// 	j.do(j.ctx, j.server, j.req)
// 	j.done <- true
// }

// func (j *Job) do(ctx context.Context, s *Server, req libqpu.QueryRequest) {
// 	resp, err := s.api.QueryUnary(req, nil)

// 	j.result.response = resp
// 	j.result.err = err
// }

// type jobResult struct {
// 	response *qpuextapi.QueryResp
// 	err      error
// }

// QueryUnary ...
func (s *Server) QueryUnary(ctx context.Context, req *qpuextapi.QueryReq) (*qpuextapi.QueryResp, error) {
	//	t0 := time.Now()

	//	work := &Job{
	//		server: s,
	//		ctx:    ctx,
	//		req: libqpu.NewQueryRequest(
	//			libqpu.NewQuery(libqpu.NewSQLQuery(req.QueryStr), nil),
	//			nil,
	//			false,
	//			false),
	//		result: &jobResult{},
	//		done:   make(chan bool),
	//	}

	//	s.dispatcher.JobQueue <- work

	//	<-work.done

	return s.api.QueryUnary(libqpu.NewQueryRequest(
		libqpu.NewQuery(libqpu.NewSQLQuery(req.QueryStr), nil),
		nil,
		false,
		false),
		nil)

	//	if err := s.responseTimeM.AddFromTs(t0); err != nil {
	//		return nil, err
	//	}

	//	return work.result.response, work.result.err
	//	return resp, err
}

// GetConfig implements the QPU's low level GetConfig API.
func (s *Server) GetConfig(ctx context.Context, req *qpuapi.ConfigRequest) (*qpuapi.ConfigResponse, error) {
	return s.api.GetConfig(ctx, req)
}
