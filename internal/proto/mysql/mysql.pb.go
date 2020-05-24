// Code generated by protoc-gen-go. DO NOT EDIT.
// source: api/protobuf-spec/mysql.proto

package mysql

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
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

type NotificationStream struct {
	SequenceId           int64    `protobuf:"varint,1,opt,name=sequence_id,json=sequenceId,proto3" json:"sequence_id,omitempty"`
	Payload              string   `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	Timestamp            int64    `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *NotificationStream) Reset()         { *m = NotificationStream{} }
func (m *NotificationStream) String() string { return proto.CompactTextString(m) }
func (*NotificationStream) ProtoMessage()    {}
func (*NotificationStream) Descriptor() ([]byte, []int) {
	return fileDescriptor_29699452324296ed, []int{0}
}

func (m *NotificationStream) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_NotificationStream.Unmarshal(m, b)
}
func (m *NotificationStream) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_NotificationStream.Marshal(b, m, deterministic)
}
func (m *NotificationStream) XXX_Merge(src proto.Message) {
	xxx_messageInfo_NotificationStream.Merge(m, src)
}
func (m *NotificationStream) XXX_Size() int {
	return xxx_messageInfo_NotificationStream.Size(m)
}
func (m *NotificationStream) XXX_DiscardUnknown() {
	xxx_messageInfo_NotificationStream.DiscardUnknown(m)
}

var xxx_messageInfo_NotificationStream proto.InternalMessageInfo

func (m *NotificationStream) GetSequenceId() int64 {
	if m != nil {
		return m.SequenceId
	}
	return 0
}

func (m *NotificationStream) GetPayload() string {
	if m != nil {
		return m.Payload
	}
	return ""
}

func (m *NotificationStream) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

type Payload struct {
	ObjectId             string               `protobuf:"bytes,1,opt,name=object_id,json=objectId,proto3" json:"object_id,omitempty"`
	Bucket               string               `protobuf:"bytes,2,opt,name=bucket,proto3" json:"bucket,omitempty"`
	OldState             *Payload_ObjectState `protobuf:"bytes,3,opt,name=old_state,json=oldState,proto3" json:"old_state,omitempty"`
	NewState             *Payload_ObjectState `protobuf:"bytes,4,opt,name=new_state,json=newState,proto3" json:"new_state,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *Payload) Reset()         { *m = Payload{} }
func (m *Payload) String() string { return proto.CompactTextString(m) }
func (*Payload) ProtoMessage()    {}
func (*Payload) Descriptor() ([]byte, []int) {
	return fileDescriptor_29699452324296ed, []int{1}
}

func (m *Payload) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Payload.Unmarshal(m, b)
}
func (m *Payload) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Payload.Marshal(b, m, deterministic)
}
func (m *Payload) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Payload.Merge(m, src)
}
func (m *Payload) XXX_Size() int {
	return xxx_messageInfo_Payload.Size(m)
}
func (m *Payload) XXX_DiscardUnknown() {
	xxx_messageInfo_Payload.DiscardUnknown(m)
}

var xxx_messageInfo_Payload proto.InternalMessageInfo

func (m *Payload) GetObjectId() string {
	if m != nil {
		return m.ObjectId
	}
	return ""
}

func (m *Payload) GetBucket() string {
	if m != nil {
		return m.Bucket
	}
	return ""
}

func (m *Payload) GetOldState() *Payload_ObjectState {
	if m != nil {
		return m.OldState
	}
	return nil
}

func (m *Payload) GetNewState() *Payload_ObjectState {
	if m != nil {
		return m.NewState
	}
	return nil
}

type Payload_ObjectState struct {
	ContentLength        int64             `protobuf:"varint,1,opt,name=contentLength,proto3" json:"contentLength,omitempty"`
	LastModified         int64             `protobuf:"varint,2,opt,name=lastModified,proto3" json:"lastModified,omitempty"`
	Attributes           map[string]string `protobuf:"bytes,3,rep,name=attributes,proto3" json:"attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *Payload_ObjectState) Reset()         { *m = Payload_ObjectState{} }
func (m *Payload_ObjectState) String() string { return proto.CompactTextString(m) }
func (*Payload_ObjectState) ProtoMessage()    {}
func (*Payload_ObjectState) Descriptor() ([]byte, []int) {
	return fileDescriptor_29699452324296ed, []int{1, 0}
}

func (m *Payload_ObjectState) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Payload_ObjectState.Unmarshal(m, b)
}
func (m *Payload_ObjectState) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Payload_ObjectState.Marshal(b, m, deterministic)
}
func (m *Payload_ObjectState) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Payload_ObjectState.Merge(m, src)
}
func (m *Payload_ObjectState) XXX_Size() int {
	return xxx_messageInfo_Payload_ObjectState.Size(m)
}
func (m *Payload_ObjectState) XXX_DiscardUnknown() {
	xxx_messageInfo_Payload_ObjectState.DiscardUnknown(m)
}

var xxx_messageInfo_Payload_ObjectState proto.InternalMessageInfo

func (m *Payload_ObjectState) GetContentLength() int64 {
	if m != nil {
		return m.ContentLength
	}
	return 0
}

func (m *Payload_ObjectState) GetLastModified() int64 {
	if m != nil {
		return m.LastModified
	}
	return 0
}

func (m *Payload_ObjectState) GetAttributes() map[string]string {
	if m != nil {
		return m.Attributes
	}
	return nil
}

type RequestStream struct {
	// Types that are valid to be assigned to Val:
	//	*RequestStream_Request
	//	*RequestStream_Ack
	Val                  isRequestStream_Val `protobuf_oneof:"val"`
	XXX_NoUnkeyedLiteral struct{}            `json:"-"`
	XXX_unrecognized     []byte              `json:"-"`
	XXX_sizecache        int32               `json:"-"`
}

func (m *RequestStream) Reset()         { *m = RequestStream{} }
func (m *RequestStream) String() string { return proto.CompactTextString(m) }
func (*RequestStream) ProtoMessage()    {}
func (*RequestStream) Descriptor() ([]byte, []int) {
	return fileDescriptor_29699452324296ed, []int{2}
}

func (m *RequestStream) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RequestStream.Unmarshal(m, b)
}
func (m *RequestStream) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RequestStream.Marshal(b, m, deterministic)
}
func (m *RequestStream) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RequestStream.Merge(m, src)
}
func (m *RequestStream) XXX_Size() int {
	return xxx_messageInfo_RequestStream.Size(m)
}
func (m *RequestStream) XXX_DiscardUnknown() {
	xxx_messageInfo_RequestStream.DiscardUnknown(m)
}

var xxx_messageInfo_RequestStream proto.InternalMessageInfo

type isRequestStream_Val interface {
	isRequestStream_Val()
}

type RequestStream_Request struct {
	Request *SubRequest `protobuf:"bytes,1,opt,name=request,proto3,oneof"`
}

type RequestStream_Ack struct {
	Ack *AckMsg `protobuf:"bytes,2,opt,name=ack,proto3,oneof"`
}

func (*RequestStream_Request) isRequestStream_Val() {}

func (*RequestStream_Ack) isRequestStream_Val() {}

func (m *RequestStream) GetVal() isRequestStream_Val {
	if m != nil {
		return m.Val
	}
	return nil
}

func (m *RequestStream) GetRequest() *SubRequest {
	if x, ok := m.GetVal().(*RequestStream_Request); ok {
		return x.Request
	}
	return nil
}

func (m *RequestStream) GetAck() *AckMsg {
	if x, ok := m.GetVal().(*RequestStream_Ack); ok {
		return x.Ack
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*RequestStream) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*RequestStream_Request)(nil),
		(*RequestStream_Ack)(nil),
	}
}

type SubRequest struct {
	Timestamp            int64    `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Sync                 bool     `protobuf:"varint,2,opt,name=sync,proto3" json:"sync,omitempty"`
	Table                string   `protobuf:"bytes,3,opt,name=table,proto3" json:"table,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SubRequest) Reset()         { *m = SubRequest{} }
func (m *SubRequest) String() string { return proto.CompactTextString(m) }
func (*SubRequest) ProtoMessage()    {}
func (*SubRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_29699452324296ed, []int{3}
}

func (m *SubRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SubRequest.Unmarshal(m, b)
}
func (m *SubRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SubRequest.Marshal(b, m, deterministic)
}
func (m *SubRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SubRequest.Merge(m, src)
}
func (m *SubRequest) XXX_Size() int {
	return xxx_messageInfo_SubRequest.Size(m)
}
func (m *SubRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SubRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SubRequest proto.InternalMessageInfo

func (m *SubRequest) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *SubRequest) GetSync() bool {
	if m != nil {
		return m.Sync
	}
	return false
}

func (m *SubRequest) GetTable() string {
	if m != nil {
		return m.Table
	}
	return ""
}

type AckMsg struct {
	Timestamp            int64    `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AckMsg) Reset()         { *m = AckMsg{} }
func (m *AckMsg) String() string { return proto.CompactTextString(m) }
func (*AckMsg) ProtoMessage()    {}
func (*AckMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_29699452324296ed, []int{4}
}

func (m *AckMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AckMsg.Unmarshal(m, b)
}
func (m *AckMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AckMsg.Marshal(b, m, deterministic)
}
func (m *AckMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AckMsg.Merge(m, src)
}
func (m *AckMsg) XXX_Size() int {
	return xxx_messageInfo_AckMsg.Size(m)
}
func (m *AckMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_AckMsg.DiscardUnknown(m)
}

var xxx_messageInfo_AckMsg proto.InternalMessageInfo

func (m *AckMsg) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func init() {
	proto.RegisterType((*NotificationStream)(nil), "mysql.NotificationStream")
	proto.RegisterType((*Payload)(nil), "mysql.Payload")
	proto.RegisterType((*Payload_ObjectState)(nil), "mysql.Payload.ObjectState")
	proto.RegisterMapType((map[string]string)(nil), "mysql.Payload.ObjectState.AttributesEntry")
	proto.RegisterType((*RequestStream)(nil), "mysql.RequestStream")
	proto.RegisterType((*SubRequest)(nil), "mysql.SubRequest")
	proto.RegisterType((*AckMsg)(nil), "mysql.AckMsg")
}

func init() { proto.RegisterFile("api/protobuf-spec/mysql.proto", fileDescriptor_29699452324296ed) }

var fileDescriptor_29699452324296ed = []byte{
	// 519 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x53, 0xdf, 0x6f, 0xd3, 0x30,
	0x10, 0x5e, 0x96, 0x6d, 0x6d, 0x2e, 0x94, 0x1f, 0xd6, 0x84, 0x4a, 0x01, 0x51, 0x22, 0x84, 0x2a,
	0xa4, 0xb5, 0x53, 0x78, 0x00, 0x21, 0xf1, 0xb0, 0x49, 0x48, 0x1b, 0x30, 0x98, 0xd2, 0xf1, 0x82,
	0x84, 0x26, 0xc7, 0x71, 0x5b, 0x53, 0xc7, 0xce, 0xe2, 0x4b, 0xa7, 0xfe, 0x21, 0xfc, 0x83, 0xfc,
	0x25, 0x28, 0x8e, 0xcb, 0xd6, 0x21, 0xe0, 0xed, 0xbe, 0xcf, 0xdf, 0x7d, 0xf6, 0x9d, 0xef, 0xe0,
	0x31, 0x2d, 0xc4, 0xa8, 0x28, 0x35, 0xea, 0xb4, 0x9a, 0xec, 0x99, 0x82, 0xb3, 0x51, 0xbe, 0x34,
	0x17, 0x72, 0x68, 0x39, 0xb2, 0x6d, 0x41, 0x94, 0x03, 0xf9, 0xa4, 0x51, 0x4c, 0x04, 0xa3, 0x28,
	0xb4, 0x1a, 0x63, 0xc9, 0x69, 0x4e, 0x9e, 0x40, 0x68, 0xf8, 0x45, 0xc5, 0x15, 0xe3, 0xe7, 0x22,
	0xeb, 0x7a, 0x7d, 0x6f, 0xe0, 0x27, 0xb0, 0xa2, 0x8e, 0x33, 0xd2, 0x85, 0x56, 0x41, 0x97, 0x52,
	0xd3, 0xac, 0xbb, 0xd9, 0xf7, 0x06, 0x41, 0xb2, 0x82, 0xe4, 0x11, 0x04, 0x28, 0x72, 0x6e, 0x90,
	0xe6, 0x45, 0xd7, 0xb7, 0x89, 0x57, 0x44, 0xf4, 0xc3, 0x87, 0xd6, 0xa9, 0x53, 0x3e, 0x84, 0x40,
	0xa7, 0xdf, 0x39, 0xc3, 0xd5, 0x15, 0x41, 0xd2, 0x6e, 0x88, 0xe3, 0x8c, 0xdc, 0x87, 0x9d, 0xb4,
	0x62, 0x73, 0x8e, 0xce, 0xdf, 0x21, 0xf2, 0x0a, 0x02, 0x2d, 0xb3, 0x73, 0x83, 0x14, 0xb9, 0xb5,
	0x0f, 0xe3, 0xde, 0xb0, 0xa9, 0xcb, 0xf9, 0x0e, 0x3f, 0x5b, 0x8f, 0x71, 0xad, 0x48, 0xda, 0x5a,
	0x66, 0x36, 0xaa, 0x13, 0x15, 0xbf, 0x74, 0x89, 0x5b, 0xff, 0x4f, 0x54, 0xfc, 0xd2, 0x46, 0xbd,
	0x9f, 0x1e, 0x84, 0xd7, 0x4e, 0xc8, 0x33, 0xe8, 0x30, 0xad, 0x90, 0x2b, 0xfc, 0xc8, 0xd5, 0x14,
	0x67, 0xae, 0x3b, 0xeb, 0x24, 0x89, 0xe0, 0x96, 0xa4, 0x06, 0x4f, 0x74, 0x26, 0x26, 0x82, 0x37,
	0x5d, 0xf2, 0x93, 0x35, 0x8e, 0xbc, 0x07, 0xa0, 0x88, 0xa5, 0x48, 0x2b, 0xe4, 0xa6, 0xeb, 0xf7,
	0xfd, 0x41, 0x18, 0xbf, 0xf8, 0xfb, 0x9b, 0x86, 0x07, 0xbf, 0xc5, 0xef, 0x14, 0x96, 0xcb, 0xe4,
	0x5a, 0x76, 0xef, 0x2d, 0xdc, 0xb9, 0x71, 0x4c, 0xee, 0x82, 0x3f, 0xe7, 0x4b, 0xd7, 0xd9, 0x3a,
	0x24, 0xbb, 0xb0, 0xbd, 0xa0, 0xb2, 0xe2, 0xae, 0xa7, 0x0d, 0x78, 0xb3, 0xf9, 0xda, 0x8b, 0x66,
	0xd0, 0x49, 0xea, 0xdf, 0x35, 0xe8, 0x26, 0x60, 0x0f, 0x5a, 0x65, 0x43, 0x58, 0x83, 0x30, 0xbe,
	0xe7, 0x1e, 0x36, 0xae, 0x52, 0xa7, 0x3c, 0xda, 0x48, 0x56, 0x1a, 0xf2, 0x14, 0x7c, 0xca, 0xe6,
	0xd6, 0x37, 0x8c, 0x3b, 0x4e, 0x7a, 0xc0, 0xe6, 0x27, 0x66, 0x7a, 0xb4, 0x91, 0xd4, 0x67, 0x87,
	0xdb, 0xe0, 0x2f, 0xa8, 0x8c, 0xce, 0x00, 0xae, 0x2c, 0xd6, 0xa7, 0xc5, 0xbb, 0x31, 0x2d, 0x84,
	0xc0, 0x96, 0x59, 0x2a, 0x66, 0x6d, 0xdb, 0x89, 0x8d, 0xeb, 0x1a, 0x90, 0xa6, 0xb2, 0xf9, 0xfc,
	0x20, 0x69, 0x40, 0xf4, 0x1c, 0x76, 0x9a, 0xdb, 0xfe, 0xed, 0x18, 0x7f, 0x83, 0xdb, 0xa7, 0x55,
	0x2a, 0x85, 0x99, 0x7d, 0x29, 0x32, 0x8a, 0xdc, 0x90, 0x0f, 0x40, 0xc6, 0x55, 0x6a, 0x58, 0x29,
	0x52, 0x7e, 0xa6, 0x57, 0xec, 0xae, 0x2b, 0x61, 0xad, 0x29, 0xbd, 0x07, 0x8e, 0xfd, 0x73, 0x63,
	0xa2, 0x8d, 0x81, 0xb7, 0xef, 0x1d, 0xc6, 0x5f, 0xf7, 0xa7, 0x02, 0x67, 0x55, 0x3a, 0x64, 0x3a,
	0x1f, 0x65, 0x0b, 0x6a, 0x84, 0xa4, 0xc6, 0x6e, 0x21, 0xaf, 0xcc, 0x48, 0x28, 0xe4, 0xa5, 0xa2,
	0xb2, 0x59, 0xcb, 0x66, 0x1d, 0xd3, 0x1d, 0x0b, 0x5e, 0xfe, 0x0a, 0x00, 0x00, 0xff, 0xff, 0xc3,
	0x21, 0xdf, 0x33, 0xb0, 0x03, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// PublishUpdatesClient is the client API for PublishUpdates service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PublishUpdatesClient interface {
	SubscribeToUpdates(ctx context.Context, opts ...grpc.CallOption) (PublishUpdates_SubscribeToUpdatesClient, error)
}

type publishUpdatesClient struct {
	cc *grpc.ClientConn
}

func NewPublishUpdatesClient(cc *grpc.ClientConn) PublishUpdatesClient {
	return &publishUpdatesClient{cc}
}

func (c *publishUpdatesClient) SubscribeToUpdates(ctx context.Context, opts ...grpc.CallOption) (PublishUpdates_SubscribeToUpdatesClient, error) {
	stream, err := c.cc.NewStream(ctx, &_PublishUpdates_serviceDesc.Streams[0], "/mysql.PublishUpdates/SubscribeToUpdates", opts...)
	if err != nil {
		return nil, err
	}
	x := &publishUpdatesSubscribeToUpdatesClient{stream}
	return x, nil
}

type PublishUpdates_SubscribeToUpdatesClient interface {
	Send(*RequestStream) error
	Recv() (*NotificationStream, error)
	grpc.ClientStream
}

type publishUpdatesSubscribeToUpdatesClient struct {
	grpc.ClientStream
}

func (x *publishUpdatesSubscribeToUpdatesClient) Send(m *RequestStream) error {
	return x.ClientStream.SendMsg(m)
}

func (x *publishUpdatesSubscribeToUpdatesClient) Recv() (*NotificationStream, error) {
	m := new(NotificationStream)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// PublishUpdatesServer is the server API for PublishUpdates service.
type PublishUpdatesServer interface {
	SubscribeToUpdates(PublishUpdates_SubscribeToUpdatesServer) error
}

func RegisterPublishUpdatesServer(s *grpc.Server, srv PublishUpdatesServer) {
	s.RegisterService(&_PublishUpdates_serviceDesc, srv)
}

func _PublishUpdates_SubscribeToUpdates_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(PublishUpdatesServer).SubscribeToUpdates(&publishUpdatesSubscribeToUpdatesServer{stream})
}

type PublishUpdates_SubscribeToUpdatesServer interface {
	Send(*NotificationStream) error
	Recv() (*RequestStream, error)
	grpc.ServerStream
}

type publishUpdatesSubscribeToUpdatesServer struct {
	grpc.ServerStream
}

func (x *publishUpdatesSubscribeToUpdatesServer) Send(m *NotificationStream) error {
	return x.ServerStream.SendMsg(m)
}

func (x *publishUpdatesSubscribeToUpdatesServer) Recv() (*RequestStream, error) {
	m := new(RequestStream)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _PublishUpdates_serviceDesc = grpc.ServiceDesc{
	ServiceName: "mysql.PublishUpdates",
	HandlerType: (*PublishUpdatesServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SubscribeToUpdates",
			Handler:       _PublishUpdates_SubscribeToUpdates_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "api/protobuf-spec/mysql.proto",
}