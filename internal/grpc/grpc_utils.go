package grpcutils

import (
	"io"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/tracer"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// GrpcServer ...
type GrpcServer struct {
	Server *grpc.Server
	closer io.Closer
}

// GrpcClientConn ...
type GrpcClientConn struct {
	Conn *grpc.ClientConn
	Cli  qpuapi.QPUAPIClient
}

// NewServer ...
func NewServer(tracing bool) (GrpcServer, error) {
	server := GrpcServer{}
	if tracing {
		tracer, closer, err := tracer.NewTracer()
		if err != nil {
			return server, err
		}
		opentracing.SetGlobalTracer(tracer)
		s := grpc.NewServer(
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				grpc_opentracing.StreamServerInterceptor(grpc_opentracing.WithTracer(tracer)),
			)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_opentracing.UnaryServerInterceptor(grpc_opentracing.WithTracer(tracer)),
			)),
		)
		server.Server = s
		server.closer = closer

		return server, nil
	}

	server.Server = grpc.NewServer(
		grpc.MaxMsgSize(32 * 1024 * 1024),
	)
	server.closer = nil

	return server, nil
}

// Close ...
func (s GrpcServer) Close() {
	if s.closer != nil {
		s.closer.Close()
	}
}

// NewClientConn ...
func NewClientConn(address string, tracing bool) (*GrpcClientConn, error) {

	clientConn := GrpcClientConn{}
	if tracing {
		if tracer := opentracing.GlobalTracer(); tracer != nil {
			maxMsgSize := 1024*1024*256
			c, err := grpc.Dial(
				address,
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
				grpc.WithInsecure(),
				grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
					grpc_opentracing.StreamClientInterceptor(grpc_opentracing.WithTracer(tracer)),
				)),
				grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
					grpc_opentracing.UnaryClientInterceptor(grpc_opentracing.WithTracer(tracer)),
				)),
			)
			if err != nil {
				return &clientConn, err
			}

			clientConn.Conn = c
			clientConn.Cli = qpuapi.NewQPUAPIClient(c)

			return &clientConn, nil
		}
	}

	c, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return &clientConn, err
	}

	clientConn.Conn = c
	clientConn.Cli = qpuapi.NewQPUAPIClient(c)

	return &clientConn, nil

}

// Close ...
func (c *GrpcClientConn) Close() error {
	return c.Conn.Close()
}
