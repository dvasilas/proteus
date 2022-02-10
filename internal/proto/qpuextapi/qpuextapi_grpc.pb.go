// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.12.4
// source: qpuextapi.proto

package qpuextapi

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// QPUAPIClient is the client API for QPUAPI service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type QPUAPIClient interface {
	QueryUnary(ctx context.Context, in *QueryReq, opts ...grpc.CallOption) (*QueryResp, error)
	QueryUnary1(ctx context.Context, in *QueryReq, opts ...grpc.CallOption) (*QueryResp1, error)
	GetMetrics(ctx context.Context, in *MetricsRequest, opts ...grpc.CallOption) (*MetricsResponse, error)
}

type qPUAPIClient struct {
	cc grpc.ClientConnInterface
}

func NewQPUAPIClient(cc grpc.ClientConnInterface) QPUAPIClient {
	return &qPUAPIClient{cc}
}

func (c *qPUAPIClient) QueryUnary(ctx context.Context, in *QueryReq, opts ...grpc.CallOption) (*QueryResp, error) {
	out := new(QueryResp)
	err := c.cc.Invoke(ctx, "/qpuextapi.QPUAPI/QueryUnary", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *qPUAPIClient) QueryUnary1(ctx context.Context, in *QueryReq, opts ...grpc.CallOption) (*QueryResp1, error) {
	out := new(QueryResp1)
	err := c.cc.Invoke(ctx, "/qpuextapi.QPUAPI/QueryUnary1", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *qPUAPIClient) GetMetrics(ctx context.Context, in *MetricsRequest, opts ...grpc.CallOption) (*MetricsResponse, error) {
	out := new(MetricsResponse)
	err := c.cc.Invoke(ctx, "/qpuextapi.QPUAPI/GetMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QPUAPIServer is the server API for QPUAPI service.
// All implementations must embed UnimplementedQPUAPIServer
// for forward compatibility
type QPUAPIServer interface {
	QueryUnary(context.Context, *QueryReq) (*QueryResp, error)
	QueryUnary1(context.Context, *QueryReq) (*QueryResp1, error)
	GetMetrics(context.Context, *MetricsRequest) (*MetricsResponse, error)
	mustEmbedUnimplementedQPUAPIServer()
}

// UnimplementedQPUAPIServer must be embedded to have forward compatible implementations.
type UnimplementedQPUAPIServer struct {
}

func (UnimplementedQPUAPIServer) QueryUnary(context.Context, *QueryReq) (*QueryResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryUnary not implemented")
}
func (UnimplementedQPUAPIServer) QueryUnary1(context.Context, *QueryReq) (*QueryResp1, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryUnary1 not implemented")
}
func (UnimplementedQPUAPIServer) GetMetrics(context.Context, *MetricsRequest) (*MetricsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMetrics not implemented")
}
func (UnimplementedQPUAPIServer) mustEmbedUnimplementedQPUAPIServer() {}

// UnsafeQPUAPIServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to QPUAPIServer will
// result in compilation errors.
type UnsafeQPUAPIServer interface {
	mustEmbedUnimplementedQPUAPIServer()
}

func RegisterQPUAPIServer(s grpc.ServiceRegistrar, srv QPUAPIServer) {
	s.RegisterService(&QPUAPI_ServiceDesc, srv)
}

func _QPUAPI_QueryUnary_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QPUAPIServer).QueryUnary(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/qpuextapi.QPUAPI/QueryUnary",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QPUAPIServer).QueryUnary(ctx, req.(*QueryReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _QPUAPI_QueryUnary1_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QPUAPIServer).QueryUnary1(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/qpuextapi.QPUAPI/QueryUnary1",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QPUAPIServer).QueryUnary1(ctx, req.(*QueryReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _QPUAPI_GetMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QPUAPIServer).GetMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/qpuextapi.QPUAPI/GetMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QPUAPIServer).GetMetrics(ctx, req.(*MetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// QPUAPI_ServiceDesc is the grpc.ServiceDesc for QPUAPI service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var QPUAPI_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "qpuextapi.QPUAPI",
	HandlerType: (*QPUAPIServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "QueryUnary",
			Handler:    _QPUAPI_QueryUnary_Handler,
		},
		{
			MethodName: "QueryUnary1",
			Handler:    _QPUAPI_QueryUnary1_Handler,
		},
		{
			MethodName: "GetMetrics",
			Handler:    _QPUAPI_GetMetrics_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "qpuextapi.proto",
}
