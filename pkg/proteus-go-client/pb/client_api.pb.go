// Code generated by protoc-gen-go. DO NOT EDIT.
// source: api/protobuf-spec/client_api.proto

package pb

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type QueryReq struct {
	QueryStr             string   `protobuf:"bytes,1,opt,name=queryStr,proto3" json:"queryStr,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *QueryReq) Reset()         { *m = QueryReq{} }
func (m *QueryReq) String() string { return proto.CompactTextString(m) }
func (*QueryReq) ProtoMessage()    {}
func (*QueryReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_a58988a9bf4976bc, []int{0}
}

func (m *QueryReq) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryReq.Unmarshal(m, b)
}
func (m *QueryReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryReq.Marshal(b, m, deterministic)
}
func (m *QueryReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryReq.Merge(m, src)
}
func (m *QueryReq) XXX_Size() int {
	return xxx_messageInfo_QueryReq.Size(m)
}
func (m *QueryReq) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryReq.DiscardUnknown(m)
}

var xxx_messageInfo_QueryReq proto.InternalMessageInfo

func (m *QueryReq) GetQueryStr() string {
	if m != nil {
		return m.QueryStr
	}
	return ""
}

type QueryResp struct {
	RespRecord           []*QueryRespRecord `protobuf:"bytes,1,rep,name=respRecord,proto3" json:"respRecord,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *QueryResp) Reset()         { *m = QueryResp{} }
func (m *QueryResp) String() string { return proto.CompactTextString(m) }
func (*QueryResp) ProtoMessage()    {}
func (*QueryResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_a58988a9bf4976bc, []int{1}
}

func (m *QueryResp) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryResp.Unmarshal(m, b)
}
func (m *QueryResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryResp.Marshal(b, m, deterministic)
}
func (m *QueryResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryResp.Merge(m, src)
}
func (m *QueryResp) XXX_Size() int {
	return xxx_messageInfo_QueryResp.Size(m)
}
func (m *QueryResp) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryResp.DiscardUnknown(m)
}

var xxx_messageInfo_QueryResp proto.InternalMessageInfo

func (m *QueryResp) GetRespRecord() []*QueryRespRecord {
	if m != nil {
		return m.RespRecord
	}
	return nil
}

type QueryRespRecord struct {
	RecordId             string                          `protobuf:"bytes,1,opt,name=record_id,json=recordId,proto3" json:"record_id,omitempty"`
	Attributes           map[string]string               `protobuf:"bytes,2,rep,name=attributes,proto3" json:"attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Timestamp            map[string]*timestamp.Timestamp `protobuf:"bytes,3,rep,name=timestamp,proto3" json:"timestamp,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}                        `json:"-"`
	XXX_unrecognized     []byte                          `json:"-"`
	XXX_sizecache        int32                           `json:"-"`
}

func (m *QueryRespRecord) Reset()         { *m = QueryRespRecord{} }
func (m *QueryRespRecord) String() string { return proto.CompactTextString(m) }
func (*QueryRespRecord) ProtoMessage()    {}
func (*QueryRespRecord) Descriptor() ([]byte, []int) {
	return fileDescriptor_a58988a9bf4976bc, []int{2}
}

func (m *QueryRespRecord) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryRespRecord.Unmarshal(m, b)
}
func (m *QueryRespRecord) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryRespRecord.Marshal(b, m, deterministic)
}
func (m *QueryRespRecord) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryRespRecord.Merge(m, src)
}
func (m *QueryRespRecord) XXX_Size() int {
	return xxx_messageInfo_QueryRespRecord.Size(m)
}
func (m *QueryRespRecord) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryRespRecord.DiscardUnknown(m)
}

var xxx_messageInfo_QueryRespRecord proto.InternalMessageInfo

func (m *QueryRespRecord) GetRecordId() string {
	if m != nil {
		return m.RecordId
	}
	return ""
}

func (m *QueryRespRecord) GetAttributes() map[string]string {
	if m != nil {
		return m.Attributes
	}
	return nil
}

func (m *QueryRespRecord) GetTimestamp() map[string]*timestamp.Timestamp {
	if m != nil {
		return m.Timestamp
	}
	return nil
}

type MetricsRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MetricsRequest) Reset()         { *m = MetricsRequest{} }
func (m *MetricsRequest) String() string { return proto.CompactTextString(m) }
func (*MetricsRequest) ProtoMessage()    {}
func (*MetricsRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_a58988a9bf4976bc, []int{3}
}

func (m *MetricsRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MetricsRequest.Unmarshal(m, b)
}
func (m *MetricsRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MetricsRequest.Marshal(b, m, deterministic)
}
func (m *MetricsRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MetricsRequest.Merge(m, src)
}
func (m *MetricsRequest) XXX_Size() int {
	return xxx_messageInfo_MetricsRequest.Size(m)
}
func (m *MetricsRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MetricsRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MetricsRequest proto.InternalMessageInfo

type MetricsResponse struct {
	NotificationLatencyP50 float64  `protobuf:"fixed64,1,opt,name=notificationLatencyP50,proto3" json:"notificationLatencyP50,omitempty"`
	NotificationLatencyP90 float64  `protobuf:"fixed64,2,opt,name=notificationLatencyP90,proto3" json:"notificationLatencyP90,omitempty"`
	NotificationLatencyP95 float64  `protobuf:"fixed64,3,opt,name=notificationLatencyP95,proto3" json:"notificationLatencyP95,omitempty"`
	NotificationLatencyP99 float64  `protobuf:"fixed64,4,opt,name=notificationLatencyP99,proto3" json:"notificationLatencyP99,omitempty"`
	ProcessingLatencyP50   float64  `protobuf:"fixed64,5,opt,name=processingLatencyP50,proto3" json:"processingLatencyP50,omitempty"`
	ProcessingLatencyP90   float64  `protobuf:"fixed64,6,opt,name=processingLatencyP90,proto3" json:"processingLatencyP90,omitempty"`
	ProcessingLatencyP95   float64  `protobuf:"fixed64,7,opt,name=processingLatencyP95,proto3" json:"processingLatencyP95,omitempty"`
	ProcessingLatencyP99   float64  `protobuf:"fixed64,8,opt,name=processingLatencyP99,proto3" json:"processingLatencyP99,omitempty"`
	FreshnessLatencyP50    float64  `protobuf:"fixed64,9,opt,name=freshnessLatencyP50,proto3" json:"freshnessLatencyP50,omitempty"`
	FreshnessLatencyP90    float64  `protobuf:"fixed64,10,opt,name=freshnessLatencyP90,proto3" json:"freshnessLatencyP90,omitempty"`
	FreshnessLatencyP95    float64  `protobuf:"fixed64,11,opt,name=freshnessLatencyP95,proto3" json:"freshnessLatencyP95,omitempty"`
	FreshnessLatencyP99    float64  `protobuf:"fixed64,12,opt,name=freshnessLatencyP99,proto3" json:"freshnessLatencyP99,omitempty"`
	XXX_NoUnkeyedLiteral   struct{} `json:"-"`
	XXX_unrecognized       []byte   `json:"-"`
	XXX_sizecache          int32    `json:"-"`
}

func (m *MetricsResponse) Reset()         { *m = MetricsResponse{} }
func (m *MetricsResponse) String() string { return proto.CompactTextString(m) }
func (*MetricsResponse) ProtoMessage()    {}
func (*MetricsResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_a58988a9bf4976bc, []int{4}
}

func (m *MetricsResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MetricsResponse.Unmarshal(m, b)
}
func (m *MetricsResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MetricsResponse.Marshal(b, m, deterministic)
}
func (m *MetricsResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MetricsResponse.Merge(m, src)
}
func (m *MetricsResponse) XXX_Size() int {
	return xxx_messageInfo_MetricsResponse.Size(m)
}
func (m *MetricsResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MetricsResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MetricsResponse proto.InternalMessageInfo

func (m *MetricsResponse) GetNotificationLatencyP50() float64 {
	if m != nil {
		return m.NotificationLatencyP50
	}
	return 0
}

func (m *MetricsResponse) GetNotificationLatencyP90() float64 {
	if m != nil {
		return m.NotificationLatencyP90
	}
	return 0
}

func (m *MetricsResponse) GetNotificationLatencyP95() float64 {
	if m != nil {
		return m.NotificationLatencyP95
	}
	return 0
}

func (m *MetricsResponse) GetNotificationLatencyP99() float64 {
	if m != nil {
		return m.NotificationLatencyP99
	}
	return 0
}

func (m *MetricsResponse) GetProcessingLatencyP50() float64 {
	if m != nil {
		return m.ProcessingLatencyP50
	}
	return 0
}

func (m *MetricsResponse) GetProcessingLatencyP90() float64 {
	if m != nil {
		return m.ProcessingLatencyP90
	}
	return 0
}

func (m *MetricsResponse) GetProcessingLatencyP95() float64 {
	if m != nil {
		return m.ProcessingLatencyP95
	}
	return 0
}

func (m *MetricsResponse) GetProcessingLatencyP99() float64 {
	if m != nil {
		return m.ProcessingLatencyP99
	}
	return 0
}

func (m *MetricsResponse) GetFreshnessLatencyP50() float64 {
	if m != nil {
		return m.FreshnessLatencyP50
	}
	return 0
}

func (m *MetricsResponse) GetFreshnessLatencyP90() float64 {
	if m != nil {
		return m.FreshnessLatencyP90
	}
	return 0
}

func (m *MetricsResponse) GetFreshnessLatencyP95() float64 {
	if m != nil {
		return m.FreshnessLatencyP95
	}
	return 0
}

func (m *MetricsResponse) GetFreshnessLatencyP99() float64 {
	if m != nil {
		return m.FreshnessLatencyP99
	}
	return 0
}

func init() {
	proto.RegisterType((*QueryReq)(nil), "pb.QueryReq")
	proto.RegisterType((*QueryResp)(nil), "pb.QueryResp")
	proto.RegisterType((*QueryRespRecord)(nil), "pb.QueryRespRecord")
	proto.RegisterMapType((map[string]string)(nil), "pb.QueryRespRecord.AttributesEntry")
	proto.RegisterMapType((map[string]*timestamp.Timestamp)(nil), "pb.QueryRespRecord.TimestampEntry")
	proto.RegisterType((*MetricsRequest)(nil), "pb.MetricsRequest")
	proto.RegisterType((*MetricsResponse)(nil), "pb.MetricsResponse")
}

func init() { proto.RegisterFile("api/protobuf-spec/client_api.proto", fileDescriptor_a58988a9bf4976bc) }

var fileDescriptor_a58988a9bf4976bc = []byte{
	// 537 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x94, 0x61, 0x6b, 0xdb, 0x3c,
	0x10, 0xc7, 0x9b, 0xe4, 0x49, 0x9e, 0xe4, 0xd2, 0x35, 0x45, 0x29, 0x43, 0x78, 0x2f, 0x56, 0x3c,
	0x18, 0x85, 0x11, 0xdb, 0xa4, 0x73, 0x37, 0x0d, 0x06, 0xed, 0xc6, 0x18, 0x85, 0x0d, 0x52, 0x6f,
	0x85, 0xb1, 0x37, 0xc5, 0x76, 0x14, 0x57, 0x34, 0xb1, 0x15, 0x49, 0x2e, 0xe4, 0x23, 0xec, 0xcb,
	0xed, 0x33, 0x8d, 0xc8, 0x89, 0xeb, 0x14, 0xab, 0xef, 0xce, 0xf7, 0xff, 0xff, 0xee, 0x4e, 0x58,
	0x27, 0xb0, 0x43, 0xce, 0x5c, 0x2e, 0x32, 0x95, 0x45, 0xf9, 0x6c, 0x24, 0x39, 0x8d, 0xdd, 0x78,
	0xce, 0x68, 0xaa, 0x6e, 0x42, 0xce, 0x1c, 0x2d, 0xa0, 0x26, 0x8f, 0xac, 0x97, 0x49, 0x96, 0x25,
	0x73, 0x5a, 0x5a, 0x5d, 0xc5, 0x16, 0x54, 0xaa, 0x70, 0xc1, 0x0b, 0x93, 0xfd, 0x1a, 0xba, 0x57,
	0x39, 0x15, 0xab, 0x80, 0x2e, 0x91, 0x05, 0xdd, 0xe5, 0x3a, 0xfe, 0xa1, 0x04, 0x6e, 0x1c, 0x37,
	0x4e, 0x7a, 0x41, 0xf9, 0x6d, 0x9f, 0x43, 0x6f, 0xe3, 0x93, 0x1c, 0x9d, 0x02, 0x08, 0x2a, 0x79,
	0x40, 0xe3, 0x4c, 0x4c, 0x71, 0xe3, 0xb8, 0x75, 0xd2, 0x1f, 0x0f, 0x1d, 0x1e, 0x39, 0xa5, 0xa5,
	0x90, 0x82, 0x8a, 0xcd, 0xfe, 0xdb, 0x84, 0xc1, 0x23, 0x1d, 0xbd, 0x80, 0x9e, 0xd0, 0xd1, 0x0d,
	0x9b, 0x6e, 0x5b, 0x16, 0x89, 0xcb, 0x29, 0xfa, 0x0c, 0x10, 0x2a, 0x25, 0x58, 0x94, 0x2b, 0x2a,
	0x71, 0x53, 0x77, 0x79, 0x55, 0xd3, 0xc5, 0xb9, 0x28, 0x5d, 0x5f, 0x52, 0x25, 0x56, 0x41, 0x05,
	0x43, 0xe7, 0xd0, 0x2b, 0x8f, 0x8c, 0x5b, 0xba, 0x86, 0x5d, 0x57, 0xe3, 0xe7, 0xd6, 0x54, 0x94,
	0x78, 0x80, 0xac, 0x8f, 0x30, 0x78, 0xd4, 0x00, 0x1d, 0x42, 0xeb, 0x8e, 0xae, 0x36, 0x03, 0xaf,
	0x43, 0x74, 0x04, 0xed, 0xfb, 0x70, 0x9e, 0x53, 0xdc, 0xd4, 0xb9, 0xe2, 0xe3, 0x43, 0xf3, 0x7d,
	0xc3, 0xfa, 0x05, 0x07, 0xbb, 0xb5, 0x6b, 0x68, 0xaf, 0x4a, 0xf7, 0xc7, 0x96, 0x53, 0xfc, 0x35,
	0x67, 0xfb, 0xd7, 0x1e, 0xa6, 0xab, 0x54, 0xb6, 0x0f, 0xe1, 0xe0, 0x3b, 0x55, 0x82, 0xc5, 0x32,
	0xa0, 0xcb, 0x9c, 0x4a, 0x65, 0xff, 0x69, 0xc3, 0xa0, 0x4c, 0x49, 0x9e, 0xa5, 0x92, 0xa2, 0x33,
	0x78, 0x9e, 0x66, 0x8a, 0xcd, 0x58, 0x1c, 0x2a, 0x96, 0xa5, 0xdf, 0x42, 0x45, 0xd3, 0x78, 0x35,
	0xf1, 0x3d, 0x3d, 0x40, 0x23, 0x30, 0xa8, 0x26, 0x8e, 0x78, 0x7a, 0x48, 0x03, 0x47, 0xcc, 0x9c,
	0x8f, 0x5b, 0x4f, 0x70, 0xbe, 0x91, 0x23, 0xf8, 0xbf, 0x27, 0x38, 0x82, 0xc6, 0x70, 0xc4, 0x45,
	0x16, 0x53, 0x29, 0x59, 0x9a, 0x54, 0x4e, 0xd7, 0xd6, 0x54, 0xad, 0x56, 0xcf, 0x10, 0x0f, 0x77,
	0x4c, 0x0c, 0x31, 0x31, 0x3e, 0xfe, 0xdf, 0xc8, 0xf8, 0x06, 0x86, 0xe0, 0xae, 0x91, 0x21, 0xc8,
	0x83, 0xe1, 0x4c, 0x50, 0x79, 0x9b, 0x52, 0x29, 0x2b, 0xc7, 0xe9, 0x69, 0xa4, 0x4e, 0xaa, 0x25,
	0x88, 0x87, 0xc1, 0x40, 0x10, 0x03, 0xe1, 0xe3, 0xbe, 0x89, 0xf0, 0xeb, 0x09, 0x82, 0xf7, 0x4d,
	0x04, 0x19, 0xa7, 0xd0, 0xb9, 0x9a, 0x5c, 0x5f, 0x4c, 0x2e, 0xd1, 0x1b, 0x00, 0xbd, 0x6d, 0xd7,
	0x69, 0x28, 0x56, 0x68, 0xbf, 0xb2, 0x7d, 0x4b, 0xeb, 0xd9, 0xce, 0x2e, 0xda, 0x7b, 0xe8, 0x1d,
	0xc0, 0x57, 0xaa, 0x36, 0x97, 0x18, 0xa1, 0xb5, 0xbc, 0x7b, 0xc9, 0xad, 0xe1, 0x4e, 0xae, 0xb8,
	0xe5, 0xf6, 0xde, 0xa7, 0xb3, 0xdf, 0x6f, 0x13, 0xa6, 0x6e, 0xf3, 0xc8, 0x89, 0xb3, 0x85, 0x3b,
	0xbd, 0x0f, 0x25, 0x9b, 0x87, 0x52, 0x3f, 0x7c, 0x34, 0x97, 0x2e, 0xbf, 0x4b, 0xb6, 0xf1, 0x28,
	0xc9, 0x46, 0xc5, 0x63, 0xe9, 0xf2, 0x28, 0xea, 0xe8, 0x25, 0x3b, 0xfd, 0x17, 0x00, 0x00, 0xff,
	0xff, 0x36, 0x26, 0x50, 0x31, 0x52, 0x05, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// QPUAPIClient is the client API for QPUAPI service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type QPUAPIClient interface {
	QueryUnary(ctx context.Context, in *QueryReq, opts ...grpc.CallOption) (*QueryResp, error)
	GetMetrics(ctx context.Context, in *MetricsRequest, opts ...grpc.CallOption) (*MetricsResponse, error)
}

type qPUAPIClient struct {
	cc *grpc.ClientConn
}

func NewQPUAPIClient(cc *grpc.ClientConn) QPUAPIClient {
	return &qPUAPIClient{cc}
}

func (c *qPUAPIClient) QueryUnary(ctx context.Context, in *QueryReq, opts ...grpc.CallOption) (*QueryResp, error) {
	out := new(QueryResp)
	err := c.cc.Invoke(ctx, "/pb.QPUAPI/QueryUnary", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *qPUAPIClient) GetMetrics(ctx context.Context, in *MetricsRequest, opts ...grpc.CallOption) (*MetricsResponse, error) {
	out := new(MetricsResponse)
	err := c.cc.Invoke(ctx, "/pb.QPUAPI/GetMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QPUAPIServer is the server API for QPUAPI service.
type QPUAPIServer interface {
	QueryUnary(context.Context, *QueryReq) (*QueryResp, error)
	GetMetrics(context.Context, *MetricsRequest) (*MetricsResponse, error)
}

func RegisterQPUAPIServer(s *grpc.Server, srv QPUAPIServer) {
	s.RegisterService(&_QPUAPI_serviceDesc, srv)
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
		FullMethod: "/pb.QPUAPI/QueryUnary",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QPUAPIServer).QueryUnary(ctx, req.(*QueryReq))
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
		FullMethod: "/pb.QPUAPI/GetMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QPUAPIServer).GetMetrics(ctx, req.(*MetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _QPUAPI_serviceDesc = grpc.ServiceDesc{
	ServiceName: "pb.QPUAPI",
	HandlerType: (*QPUAPIServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "QueryUnary",
			Handler:    _QPUAPI_QueryUnary_Handler,
		},
		{
			MethodName: "GetMetrics",
			Handler:    _QPUAPI_GetMetrics_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/protobuf-spec/client_api.proto",
}
