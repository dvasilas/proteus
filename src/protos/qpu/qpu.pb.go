// Code generated by protoc-gen-go. DO NOT EDIT.
// source: qpu.proto

package qpu

import (
	context "context"
	fmt "fmt"
	utils "github.com/dvasilas/proteus/src/protos/utils"
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

type ResponseStreamRecord_StreamRecordType int32

const (
	ResponseStreamRecord_UPDATEOP      ResponseStreamRecord_StreamRecordType = 0
	ResponseStreamRecord_UPDATEDELTA   ResponseStreamRecord_StreamRecordType = 1
	ResponseStreamRecord_STATE         ResponseStreamRecord_StreamRecordType = 2
	ResponseStreamRecord_HEARTBEAT     ResponseStreamRecord_StreamRecordType = 3
	ResponseStreamRecord_END_OF_STREAM ResponseStreamRecord_StreamRecordType = 4
)

var ResponseStreamRecord_StreamRecordType_name = map[int32]string{
	0: "UPDATEOP",
	1: "UPDATEDELTA",
	2: "STATE",
	3: "HEARTBEAT",
	4: "END_OF_STREAM",
}

var ResponseStreamRecord_StreamRecordType_value = map[string]int32{
	"UPDATEOP":      0,
	"UPDATEDELTA":   1,
	"STATE":         2,
	"HEARTBEAT":     3,
	"END_OF_STREAM": 4,
}

func (x ResponseStreamRecord_StreamRecordType) String() string {
	return proto.EnumName(ResponseStreamRecord_StreamRecordType_name, int32(x))
}

func (ResponseStreamRecord_StreamRecordType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{1, 0}
}

type ConfigResponse_QPUType int32

const (
	ConfigResponse_DBDRIVER              ConfigResponse_QPUType = 0
	ConfigResponse_FILTER                ConfigResponse_QPUType = 1
	ConfigResponse_INDEX                 ConfigResponse_QPUType = 2
	ConfigResponse_CACHE                 ConfigResponse_QPUType = 3
	ConfigResponse_FEDERATION_DISPATCHER ConfigResponse_QPUType = 4
	ConfigResponse_LOAD_BALANCER         ConfigResponse_QPUType = 5
	ConfigResponse_LAMBDA                ConfigResponse_QPUType = 6
	ConfigResponse_NETWORK               ConfigResponse_QPUType = 7
	ConfigResponse_INTERSECTION          ConfigResponse_QPUType = 8
)

var ConfigResponse_QPUType_name = map[int32]string{
	0: "DBDRIVER",
	1: "FILTER",
	2: "INDEX",
	3: "CACHE",
	4: "FEDERATION_DISPATCHER",
	5: "LOAD_BALANCER",
	6: "LAMBDA",
	7: "NETWORK",
	8: "INTERSECTION",
}

var ConfigResponse_QPUType_value = map[string]int32{
	"DBDRIVER":              0,
	"FILTER":                1,
	"INDEX":                 2,
	"CACHE":                 3,
	"FEDERATION_DISPATCHER": 4,
	"LOAD_BALANCER":         5,
	"LAMBDA":                6,
	"NETWORK":               7,
	"INTERSECTION":          8,
}

func (x ConfigResponse_QPUType) String() string {
	return proto.EnumName(ConfigResponse_QPUType_name, int32(x))
}

func (ConfigResponse_QPUType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{5, 0}
}

type RequestStream struct {
	// Types that are valid to be assigned to Payload:
	//	*RequestStream_Request
	//	*RequestStream_Ack
	//	*RequestStream_Ping
	Payload              isRequestStream_Payload `protobuf_oneof:"payload"`
	XXX_NoUnkeyedLiteral struct{}                `json:"-"`
	XXX_unrecognized     []byte                  `json:"-"`
	XXX_sizecache        int32                   `json:"-"`
}

func (m *RequestStream) Reset()         { *m = RequestStream{} }
func (m *RequestStream) String() string { return proto.CompactTextString(m) }
func (*RequestStream) ProtoMessage()    {}
func (*RequestStream) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{0}
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

type isRequestStream_Payload interface {
	isRequestStream_Payload()
}

type RequestStream_Request struct {
	Request *QueryRequest `protobuf:"bytes,1,opt,name=request,proto3,oneof"`
}

type RequestStream_Ack struct {
	Ack *AckMsg `protobuf:"bytes,2,opt,name=ack,proto3,oneof"`
}

type RequestStream_Ping struct {
	Ping *PingMsg `protobuf:"bytes,3,opt,name=ping,proto3,oneof"`
}

func (*RequestStream_Request) isRequestStream_Payload() {}

func (*RequestStream_Ack) isRequestStream_Payload() {}

func (*RequestStream_Ping) isRequestStream_Payload() {}

func (m *RequestStream) GetPayload() isRequestStream_Payload {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (m *RequestStream) GetRequest() *QueryRequest {
	if x, ok := m.GetPayload().(*RequestStream_Request); ok {
		return x.Request
	}
	return nil
}

func (m *RequestStream) GetAck() *AckMsg {
	if x, ok := m.GetPayload().(*RequestStream_Ack); ok {
		return x.Ack
	}
	return nil
}

func (m *RequestStream) GetPing() *PingMsg {
	if x, ok := m.GetPayload().(*RequestStream_Ping); ok {
		return x.Ping
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*RequestStream) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*RequestStream_Request)(nil),
		(*RequestStream_Ack)(nil),
		(*RequestStream_Ping)(nil),
	}
}

type ResponseStreamRecord struct {
	SequenceId           int64                                 `protobuf:"varint,1,opt,name=sequence_id,json=sequenceId,proto3" json:"sequence_id,omitempty"`
	Type                 ResponseStreamRecord_StreamRecordType `protobuf:"varint,2,opt,name=type,proto3,enum=qpu.ResponseStreamRecord_StreamRecordType" json:"type,omitempty"`
	LogOp                *utils.LogOperation                   `protobuf:"bytes,3,opt,name=logOp,proto3" json:"logOp,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                              `json:"-"`
	XXX_unrecognized     []byte                                `json:"-"`
	XXX_sizecache        int32                                 `json:"-"`
}

func (m *ResponseStreamRecord) Reset()         { *m = ResponseStreamRecord{} }
func (m *ResponseStreamRecord) String() string { return proto.CompactTextString(m) }
func (*ResponseStreamRecord) ProtoMessage()    {}
func (*ResponseStreamRecord) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{1}
}

func (m *ResponseStreamRecord) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ResponseStreamRecord.Unmarshal(m, b)
}
func (m *ResponseStreamRecord) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ResponseStreamRecord.Marshal(b, m, deterministic)
}
func (m *ResponseStreamRecord) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ResponseStreamRecord.Merge(m, src)
}
func (m *ResponseStreamRecord) XXX_Size() int {
	return xxx_messageInfo_ResponseStreamRecord.Size(m)
}
func (m *ResponseStreamRecord) XXX_DiscardUnknown() {
	xxx_messageInfo_ResponseStreamRecord.DiscardUnknown(m)
}

var xxx_messageInfo_ResponseStreamRecord proto.InternalMessageInfo

func (m *ResponseStreamRecord) GetSequenceId() int64 {
	if m != nil {
		return m.SequenceId
	}
	return 0
}

func (m *ResponseStreamRecord) GetType() ResponseStreamRecord_StreamRecordType {
	if m != nil {
		return m.Type
	}
	return ResponseStreamRecord_UPDATEOP
}

func (m *ResponseStreamRecord) GetLogOp() *utils.LogOperation {
	if m != nil {
		return m.LogOp
	}
	return nil
}

type QueryRequest struct {
	//Timestamp is part of the attributes
	//Desclared explicitly here for easier parsing
	Clock                *utils.SnapshotTimePredicate `protobuf:"bytes,1,opt,name=clock,proto3" json:"clock,omitempty"`
	Predicate            []*utils.AttributePredicate  `protobuf:"bytes,2,rep,name=predicate,proto3" json:"predicate,omitempty"`
	Metadata             map[string]string            `protobuf:"bytes,3,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Sync                 bool                         `protobuf:"varint,4,opt,name=sync,proto3" json:"sync,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                     `json:"-"`
	XXX_unrecognized     []byte                       `json:"-"`
	XXX_sizecache        int32                        `json:"-"`
}

func (m *QueryRequest) Reset()         { *m = QueryRequest{} }
func (m *QueryRequest) String() string { return proto.CompactTextString(m) }
func (*QueryRequest) ProtoMessage()    {}
func (*QueryRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{2}
}

func (m *QueryRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryRequest.Unmarshal(m, b)
}
func (m *QueryRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryRequest.Marshal(b, m, deterministic)
}
func (m *QueryRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryRequest.Merge(m, src)
}
func (m *QueryRequest) XXX_Size() int {
	return xxx_messageInfo_QueryRequest.Size(m)
}
func (m *QueryRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryRequest.DiscardUnknown(m)
}

var xxx_messageInfo_QueryRequest proto.InternalMessageInfo

func (m *QueryRequest) GetClock() *utils.SnapshotTimePredicate {
	if m != nil {
		return m.Clock
	}
	return nil
}

func (m *QueryRequest) GetPredicate() []*utils.AttributePredicate {
	if m != nil {
		return m.Predicate
	}
	return nil
}

func (m *QueryRequest) GetMetadata() map[string]string {
	if m != nil {
		return m.Metadata
	}
	return nil
}

func (m *QueryRequest) GetSync() bool {
	if m != nil {
		return m.Sync
	}
	return false
}

type AckMsg struct {
	SequenceId           int64    `protobuf:"varint,1,opt,name=sequence_id,json=sequenceId,proto3" json:"sequence_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AckMsg) Reset()         { *m = AckMsg{} }
func (m *AckMsg) String() string { return proto.CompactTextString(m) }
func (*AckMsg) ProtoMessage()    {}
func (*AckMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{3}
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

func (m *AckMsg) GetSequenceId() int64 {
	if m != nil {
		return m.SequenceId
	}
	return 0
}

type ConfigRequest struct {
	Clock                *utils.SnapshotTimePredicate `protobuf:"bytes,1,opt,name=clock,proto3" json:"clock,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                     `json:"-"`
	XXX_unrecognized     []byte                       `json:"-"`
	XXX_sizecache        int32                        `json:"-"`
}

func (m *ConfigRequest) Reset()         { *m = ConfigRequest{} }
func (m *ConfigRequest) String() string { return proto.CompactTextString(m) }
func (*ConfigRequest) ProtoMessage()    {}
func (*ConfigRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{4}
}

func (m *ConfigRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ConfigRequest.Unmarshal(m, b)
}
func (m *ConfigRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ConfigRequest.Marshal(b, m, deterministic)
}
func (m *ConfigRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConfigRequest.Merge(m, src)
}
func (m *ConfigRequest) XXX_Size() int {
	return xxx_messageInfo_ConfigRequest.Size(m)
}
func (m *ConfigRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ConfigRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ConfigRequest proto.InternalMessageInfo

func (m *ConfigRequest) GetClock() *utils.SnapshotTimePredicate {
	if m != nil {
		return m.Clock
	}
	return nil
}

type ConfigResponse struct {
	QpuType              ConfigResponse_QPUType      `protobuf:"varint,1,opt,name=qpu_type,json=qpuType,proto3,enum=qpu.ConfigResponse_QPUType" json:"qpu_type,omitempty"`
	SupportedQueries     []*utils.AttributePredicate `protobuf:"bytes,2,rep,name=supportedQueries,proto3" json:"supportedQueries,omitempty"`
	Dataset              *DataSet                    `protobuf:"bytes,3,opt,name=dataset,proto3" json:"dataset,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                    `json:"-"`
	XXX_unrecognized     []byte                      `json:"-"`
	XXX_sizecache        int32                       `json:"-"`
}

func (m *ConfigResponse) Reset()         { *m = ConfigResponse{} }
func (m *ConfigResponse) String() string { return proto.CompactTextString(m) }
func (*ConfigResponse) ProtoMessage()    {}
func (*ConfigResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{5}
}

func (m *ConfigResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ConfigResponse.Unmarshal(m, b)
}
func (m *ConfigResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ConfigResponse.Marshal(b, m, deterministic)
}
func (m *ConfigResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConfigResponse.Merge(m, src)
}
func (m *ConfigResponse) XXX_Size() int {
	return xxx_messageInfo_ConfigResponse.Size(m)
}
func (m *ConfigResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ConfigResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ConfigResponse proto.InternalMessageInfo

func (m *ConfigResponse) GetQpuType() ConfigResponse_QPUType {
	if m != nil {
		return m.QpuType
	}
	return ConfigResponse_DBDRIVER
}

func (m *ConfigResponse) GetSupportedQueries() []*utils.AttributePredicate {
	if m != nil {
		return m.SupportedQueries
	}
	return nil
}

func (m *ConfigResponse) GetDataset() *DataSet {
	if m != nil {
		return m.Dataset
	}
	return nil
}

type DataSet struct {
	Databases            map[string]*DataSet_DB `protobuf:"bytes,1,rep,name=databases,proto3" json:"databases,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}               `json:"-"`
	XXX_unrecognized     []byte                 `json:"-"`
	XXX_sizecache        int32                  `json:"-"`
}

func (m *DataSet) Reset()         { *m = DataSet{} }
func (m *DataSet) String() string { return proto.CompactTextString(m) }
func (*DataSet) ProtoMessage()    {}
func (*DataSet) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{6}
}

func (m *DataSet) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DataSet.Unmarshal(m, b)
}
func (m *DataSet) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DataSet.Marshal(b, m, deterministic)
}
func (m *DataSet) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DataSet.Merge(m, src)
}
func (m *DataSet) XXX_Size() int {
	return xxx_messageInfo_DataSet.Size(m)
}
func (m *DataSet) XXX_DiscardUnknown() {
	xxx_messageInfo_DataSet.DiscardUnknown(m)
}

var xxx_messageInfo_DataSet proto.InternalMessageInfo

func (m *DataSet) GetDatabases() map[string]*DataSet_DB {
	if m != nil {
		return m.Databases
	}
	return nil
}

type DataSet_DB struct {
	Datacenters          map[string]*DataSet_DC `protobuf:"bytes,1,rep,name=datacenters,proto3" json:"datacenters,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}               `json:"-"`
	XXX_unrecognized     []byte                 `json:"-"`
	XXX_sizecache        int32                  `json:"-"`
}

func (m *DataSet_DB) Reset()         { *m = DataSet_DB{} }
func (m *DataSet_DB) String() string { return proto.CompactTextString(m) }
func (*DataSet_DB) ProtoMessage()    {}
func (*DataSet_DB) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{6, 0}
}

func (m *DataSet_DB) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DataSet_DB.Unmarshal(m, b)
}
func (m *DataSet_DB) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DataSet_DB.Marshal(b, m, deterministic)
}
func (m *DataSet_DB) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DataSet_DB.Merge(m, src)
}
func (m *DataSet_DB) XXX_Size() int {
	return xxx_messageInfo_DataSet_DB.Size(m)
}
func (m *DataSet_DB) XXX_DiscardUnknown() {
	xxx_messageInfo_DataSet_DB.DiscardUnknown(m)
}

var xxx_messageInfo_DataSet_DB proto.InternalMessageInfo

func (m *DataSet_DB) GetDatacenters() map[string]*DataSet_DC {
	if m != nil {
		return m.Datacenters
	}
	return nil
}

type DataSet_DC struct {
	Shards               []string `protobuf:"bytes,1,rep,name=shards,proto3" json:"shards,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DataSet_DC) Reset()         { *m = DataSet_DC{} }
func (m *DataSet_DC) String() string { return proto.CompactTextString(m) }
func (*DataSet_DC) ProtoMessage()    {}
func (*DataSet_DC) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{6, 1}
}

func (m *DataSet_DC) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DataSet_DC.Unmarshal(m, b)
}
func (m *DataSet_DC) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DataSet_DC.Marshal(b, m, deterministic)
}
func (m *DataSet_DC) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DataSet_DC.Merge(m, src)
}
func (m *DataSet_DC) XXX_Size() int {
	return xxx_messageInfo_DataSet_DC.Size(m)
}
func (m *DataSet_DC) XXX_DiscardUnknown() {
	xxx_messageInfo_DataSet_DC.DiscardUnknown(m)
}

var xxx_messageInfo_DataSet_DC proto.InternalMessageInfo

func (m *DataSet_DC) GetShards() []string {
	if m != nil {
		return m.Shards
	}
	return nil
}

type PingMsg struct {
	SeqId                int64    `protobuf:"varint,1,opt,name=seq_id,json=seqId,proto3" json:"seq_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PingMsg) Reset()         { *m = PingMsg{} }
func (m *PingMsg) String() string { return proto.CompactTextString(m) }
func (*PingMsg) ProtoMessage()    {}
func (*PingMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_0be19c925dafeffa, []int{7}
}

func (m *PingMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PingMsg.Unmarshal(m, b)
}
func (m *PingMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PingMsg.Marshal(b, m, deterministic)
}
func (m *PingMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PingMsg.Merge(m, src)
}
func (m *PingMsg) XXX_Size() int {
	return xxx_messageInfo_PingMsg.Size(m)
}
func (m *PingMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_PingMsg.DiscardUnknown(m)
}

var xxx_messageInfo_PingMsg proto.InternalMessageInfo

func (m *PingMsg) GetSeqId() int64 {
	if m != nil {
		return m.SeqId
	}
	return 0
}

func init() {
	proto.RegisterEnum("qpu.ResponseStreamRecord_StreamRecordType", ResponseStreamRecord_StreamRecordType_name, ResponseStreamRecord_StreamRecordType_value)
	proto.RegisterEnum("qpu.ConfigResponse_QPUType", ConfigResponse_QPUType_name, ConfigResponse_QPUType_value)
	proto.RegisterType((*RequestStream)(nil), "qpu.RequestStream")
	proto.RegisterType((*ResponseStreamRecord)(nil), "qpu.ResponseStreamRecord")
	proto.RegisterType((*QueryRequest)(nil), "qpu.QueryRequest")
	proto.RegisterMapType((map[string]string)(nil), "qpu.QueryRequest.MetadataEntry")
	proto.RegisterType((*AckMsg)(nil), "qpu.AckMsg")
	proto.RegisterType((*ConfigRequest)(nil), "qpu.ConfigRequest")
	proto.RegisterType((*ConfigResponse)(nil), "qpu.ConfigResponse")
	proto.RegisterType((*DataSet)(nil), "qpu.DataSet")
	proto.RegisterMapType((map[string]*DataSet_DB)(nil), "qpu.DataSet.DatabasesEntry")
	proto.RegisterType((*DataSet_DB)(nil), "qpu.DataSet.DB")
	proto.RegisterMapType((map[string]*DataSet_DC)(nil), "qpu.DataSet.DB.DatacentersEntry")
	proto.RegisterType((*DataSet_DC)(nil), "qpu.DataSet.DC")
	proto.RegisterType((*PingMsg)(nil), "qpu.PingMsg")
}

func init() { proto.RegisterFile("qpu.proto", fileDescriptor_0be19c925dafeffa) }

var fileDescriptor_0be19c925dafeffa = []byte{
	// 901 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x55, 0xdd, 0x6f, 0xe3, 0x44,
	0x10, 0x8f, 0xed, 0x7c, 0x79, 0xd2, 0xf4, 0x7c, 0x7b, 0x57, 0x94, 0xe6, 0x4e, 0x6a, 0x64, 0x09,
	0xd4, 0x3b, 0x89, 0x14, 0x05, 0xe9, 0xf8, 0x38, 0x84, 0x64, 0xc7, 0x5b, 0x12, 0x91, 0x0f, 0x77,
	0xe3, 0x02, 0xe2, 0x81, 0xc8, 0xb5, 0x97, 0xd4, 0x6a, 0x6a, 0x3b, 0xde, 0xf5, 0x49, 0x11, 0x6f,
	0xbc, 0xf1, 0xc2, 0x03, 0x6f, 0xfc, 0x4f, 0xf0, 0x3f, 0x21, 0xaf, 0x1d, 0xea, 0xdc, 0x15, 0x55,
	0xe8, 0x9e, 0x3c, 0x3b, 0xfb, 0x9b, 0xdf, 0xce, 0xce, 0xfe, 0x66, 0x0c, 0xea, 0x26, 0x4e, 0xfb,
	0x71, 0x12, 0xf1, 0x08, 0x29, 0x9b, 0x38, 0xed, 0xb6, 0x52, 0x1e, 0xac, 0x59, 0xee, 0xd1, 0x7f,
	0x97, 0xa0, 0x4d, 0xe8, 0x26, 0xa5, 0x8c, 0x2f, 0x78, 0x42, 0xdd, 0x5b, 0xf4, 0x31, 0x34, 0x92,
	0xdc, 0xd1, 0x91, 0x7a, 0xd2, 0x69, 0x6b, 0xf0, 0xb8, 0x9f, 0x11, 0x5c, 0xa4, 0x34, 0xd9, 0x16,
	0xc8, 0x51, 0x85, 0xec, 0x30, 0xe8, 0x04, 0x14, 0xd7, 0xbb, 0xe9, 0xc8, 0x02, 0xda, 0x12, 0x50,
	0xc3, 0xbb, 0x99, 0xb2, 0xd5, 0xa8, 0x42, 0xb2, 0x1d, 0xa4, 0x43, 0x35, 0x0e, 0xc2, 0x55, 0x47,
	0x11, 0x88, 0x03, 0x81, 0xb0, 0x83, 0x70, 0x95, 0x43, 0xc4, 0x9e, 0xa9, 0x42, 0x23, 0x76, 0xb7,
	0xeb, 0xc8, 0xf5, 0xf5, 0x5f, 0x65, 0x78, 0x4a, 0x28, 0x8b, 0xa3, 0x90, 0xd1, 0x3c, 0x23, 0x42,
	0xbd, 0x28, 0xf1, 0xd1, 0x09, 0xb4, 0x58, 0x76, 0x66, 0xe8, 0xd1, 0x65, 0xe0, 0x8b, 0xdc, 0x14,
	0x02, 0x3b, 0xd7, 0xd8, 0x47, 0x5f, 0x43, 0x95, 0x6f, 0x63, 0x2a, 0x52, 0x39, 0x1c, 0xbc, 0x14,
	0x07, 0xdd, 0xc7, 0xd4, 0x2f, 0x2f, 0x9c, 0x6d, 0x4c, 0x89, 0x88, 0x43, 0x2f, 0xa0, 0xb6, 0x8e,
	0x56, 0xf3, 0xb8, 0xc8, 0xf4, 0x49, 0x3f, 0xaf, 0xd3, 0x24, 0xf3, 0xd1, 0xc4, 0xe5, 0x41, 0x14,
	0x92, 0x1c, 0xa1, 0xff, 0x04, 0xda, 0xdb, 0x24, 0xe8, 0x00, 0x9a, 0x97, 0xb6, 0x65, 0x38, 0x78,
	0x6e, 0x6b, 0x15, 0xf4, 0x08, 0x5a, 0xf9, 0xca, 0xc2, 0x13, 0xc7, 0xd0, 0x24, 0xa4, 0x42, 0x6d,
	0xe1, 0x18, 0x0e, 0xd6, 0x64, 0xd4, 0x06, 0x75, 0x84, 0x0d, 0xe2, 0x98, 0xd8, 0x70, 0x34, 0x05,
	0x3d, 0x86, 0x36, 0x9e, 0x59, 0xcb, 0xf9, 0xf9, 0x72, 0xe1, 0x10, 0x6c, 0x4c, 0xb5, 0xaa, 0xfe,
	0x9b, 0x0c, 0x07, 0xe5, 0x82, 0xa3, 0x01, 0xd4, 0xbc, 0x75, 0xe4, 0xdd, 0x14, 0x4f, 0xf2, 0xbc,
	0xc8, 0x6d, 0x11, 0xba, 0x31, 0xbb, 0x8e, 0xb8, 0x13, 0xdc, 0x52, 0x3b, 0xa1, 0x7e, 0xe0, 0xb9,
	0x9c, 0x92, 0x1c, 0x8a, 0x3e, 0x03, 0x35, 0xde, 0xf9, 0x3a, 0x72, 0x4f, 0x39, 0x6d, 0x0d, 0x8e,
	0x8b, 0x38, 0x83, 0xf3, 0x24, 0xb8, 0x4a, 0x79, 0x29, 0xe8, 0x0e, 0x8b, 0x5e, 0x43, 0xf3, 0x96,
	0x72, 0xd7, 0x77, 0xb9, 0xdb, 0x51, 0x44, 0xdc, 0xc9, 0x3b, 0x12, 0xe8, 0x4f, 0x0b, 0x04, 0x0e,
	0x79, 0xb2, 0x25, 0xff, 0x06, 0x20, 0x04, 0x55, 0xb6, 0x0d, 0xbd, 0x4e, 0xb5, 0x27, 0x9d, 0x36,
	0x89, 0xb0, 0xbb, 0xaf, 0xa1, 0xbd, 0x07, 0x47, 0x1a, 0x28, 0x37, 0x74, 0x2b, 0x2e, 0xa3, 0x92,
	0xcc, 0x44, 0x4f, 0xa1, 0xf6, 0xc6, 0x5d, 0xa7, 0xf9, 0xeb, 0xa9, 0x24, 0x5f, 0x7c, 0x29, 0x7f,
	0x2e, 0xe9, 0x2f, 0xa0, 0x9e, 0x0b, 0xea, 0x41, 0x05, 0xe8, 0x43, 0x68, 0x0f, 0xa3, 0xf0, 0xe7,
	0x60, 0xf5, 0x1e, 0x65, 0xd3, 0xff, 0x92, 0xe1, 0x70, 0xc7, 0x92, 0x8b, 0x07, 0xbd, 0x82, 0xe6,
	0x26, 0x4e, 0x97, 0x42, 0x5d, 0x92, 0x50, 0xd7, 0x33, 0x51, 0x90, 0x7d, 0x58, 0xff, 0xc2, 0xbe,
	0x14, 0x72, 0x6a, 0x6c, 0xe2, 0x54, 0x48, 0x02, 0x83, 0xc6, 0xd2, 0x38, 0x8e, 0x12, 0x4e, 0xfd,
	0xac, 0x78, 0x01, 0x65, 0x0f, 0x3f, 0xc4, 0x3b, 0x21, 0xe8, 0x23, 0x68, 0x64, 0xa5, 0x63, 0x94,
	0xef, 0x35, 0x91, 0xe5, 0x72, 0x77, 0x41, 0x39, 0xd9, 0x6d, 0xea, 0x7f, 0x48, 0xd0, 0x28, 0x72,
	0xc8, 0xd4, 0x68, 0x99, 0x16, 0x19, 0x7f, 0x87, 0x89, 0x56, 0x41, 0x00, 0xf5, 0xf3, 0xf1, 0xc4,
	0xc1, 0x24, 0x17, 0xe2, 0x78, 0x66, 0xe1, 0x1f, 0x34, 0x39, 0x33, 0x87, 0xc6, 0x70, 0x84, 0x35,
	0x05, 0x1d, 0xc3, 0xd1, 0x39, 0xb6, 0x30, 0x31, 0x9c, 0xf1, 0x7c, 0xb6, 0xb4, 0xc6, 0x0b, 0xdb,
	0x70, 0x86, 0x23, 0x4c, 0xb4, 0x6a, 0xa6, 0xcf, 0xc9, 0xdc, 0xb0, 0x96, 0xa6, 0x31, 0x31, 0x66,
	0x43, 0x4c, 0xb4, 0x5a, 0xc6, 0x37, 0x31, 0xa6, 0xa6, 0x65, 0x68, 0x75, 0xd4, 0x82, 0xc6, 0x0c,
	0x3b, 0xdf, 0xcf, 0xc9, 0xb7, 0x5a, 0x03, 0x69, 0x70, 0x30, 0x9e, 0x39, 0x98, 0x2c, 0xf0, 0x30,
	0x23, 0xd2, 0x9a, 0xfa, 0xdf, 0x32, 0x34, 0x8a, 0x4c, 0xd1, 0x17, 0xa0, 0x66, 0xb9, 0x5e, 0xb9,
	0x8c, 0xb2, 0x8e, 0x24, 0x0a, 0xf1, 0xac, 0x7c, 0x15, 0xf1, 0x15, 0xbb, 0xb9, 0xaa, 0xee, 0xd0,
	0xdd, 0x3f, 0x25, 0x90, 0x2d, 0x13, 0x99, 0xd0, 0xca, 0x7c, 0x1e, 0x0d, 0x39, 0x4d, 0x76, 0x1c,
	0xbd, 0x7d, 0x0e, 0x53, 0x98, 0x05, 0x24, 0x27, 0x2a, 0x07, 0x75, 0xe7, 0xa0, 0xbd, 0x0d, 0xb8,
	0x47, 0x90, 0x1f, 0x96, 0x05, 0xd9, 0x1a, 0x3c, 0xda, 0x3f, 0x63, 0x58, 0x52, 0x68, 0xf7, 0x39,
	0xc8, 0xd6, 0x10, 0x7d, 0x00, 0x75, 0x76, 0xed, 0x26, 0x7e, 0x9e, 0x95, 0x4a, 0x8a, 0x55, 0x77,
	0x0a, 0x87, 0xfb, 0xd7, 0xfa, 0xdf, 0x87, 0x99, 0xe5, 0x76, 0xe8, 0x41, 0xa3, 0x98, 0x9e, 0xe8,
	0x08, 0xea, 0x8c, 0x6e, 0xee, 0x5a, 0xa1, 0xc6, 0xe8, 0x66, 0xec, 0x0f, 0x7e, 0x01, 0xe5, 0xc2,
	0xbe, 0x44, 0x5f, 0x41, 0x4d, 0x34, 0x2c, 0x42, 0xc5, 0x24, 0x2c, 0x0d, 0xf9, 0xee, 0xf1, 0x7f,
	0x4e, 0x47, 0xbd, 0x72, 0x2a, 0x7d, 0x22, 0xa1, 0x57, 0xa0, 0x7e, 0x43, 0x79, 0x2e, 0xf0, 0x82,
	0x61, 0xaf, 0xb5, 0xba, 0x4f, 0xee, 0xe9, 0x00, 0xbd, 0x62, 0xce, 0xe1, 0x28, 0x88, 0xfa, 0xab,
	0x24, 0xf6, 0xc4, 0x0f, 0x86, 0xa6, 0xcc, 0x5b, 0x07, 0x34, 0xe4, 0x66, 0xf3, 0xc2, 0xbe, 0xb4,
	0xb3, 0x5f, 0x8e, 0x2d, 0xfd, 0xf8, 0x72, 0x15, 0xf0, 0xeb, 0xf4, 0xaa, 0xef, 0x45, 0xb7, 0x67,
	0xfe, 0x1b, 0x97, 0x05, 0x6b, 0x97, 0x9d, 0x15, 0xf0, 0x33, 0x96, 0x78, 0xc2, 0x8e, 0xd8, 0xd9,
	0x26, 0x4e, 0xaf, 0xea, 0xc2, 0xfe, 0xf4, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x92, 0x4b, 0xb9,
	0x90, 0xc6, 0x06, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// QPUClient is the client API for QPU service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type QPUClient interface {
	Query(ctx context.Context, opts ...grpc.CallOption) (QPU_QueryClient, error)
	GetConfig(ctx context.Context, in *ConfigRequest, opts ...grpc.CallOption) (*ConfigResponse, error)
}

type qPUClient struct {
	cc *grpc.ClientConn
}

func NewQPUClient(cc *grpc.ClientConn) QPUClient {
	return &qPUClient{cc}
}

func (c *qPUClient) Query(ctx context.Context, opts ...grpc.CallOption) (QPU_QueryClient, error) {
	stream, err := c.cc.NewStream(ctx, &_QPU_serviceDesc.Streams[0], "/qpu.QPU/Query", opts...)
	if err != nil {
		return nil, err
	}
	x := &qPUQueryClient{stream}
	return x, nil
}

type QPU_QueryClient interface {
	Send(*RequestStream) error
	Recv() (*ResponseStreamRecord, error)
	grpc.ClientStream
}

type qPUQueryClient struct {
	grpc.ClientStream
}

func (x *qPUQueryClient) Send(m *RequestStream) error {
	return x.ClientStream.SendMsg(m)
}

func (x *qPUQueryClient) Recv() (*ResponseStreamRecord, error) {
	m := new(ResponseStreamRecord)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *qPUClient) GetConfig(ctx context.Context, in *ConfigRequest, opts ...grpc.CallOption) (*ConfigResponse, error) {
	out := new(ConfigResponse)
	err := c.cc.Invoke(ctx, "/qpu.QPU/GetConfig", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QPUServer is the server API for QPU service.
type QPUServer interface {
	Query(QPU_QueryServer) error
	GetConfig(context.Context, *ConfigRequest) (*ConfigResponse, error)
}

func RegisterQPUServer(s *grpc.Server, srv QPUServer) {
	s.RegisterService(&_QPU_serviceDesc, srv)
}

func _QPU_Query_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(QPUServer).Query(&qPUQueryServer{stream})
}

type QPU_QueryServer interface {
	Send(*ResponseStreamRecord) error
	Recv() (*RequestStream, error)
	grpc.ServerStream
}

type qPUQueryServer struct {
	grpc.ServerStream
}

func (x *qPUQueryServer) Send(m *ResponseStreamRecord) error {
	return x.ServerStream.SendMsg(m)
}

func (x *qPUQueryServer) Recv() (*RequestStream, error) {
	m := new(RequestStream)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _QPU_GetConfig_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConfigRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QPUServer).GetConfig(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/qpu.QPU/GetConfig",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QPUServer).GetConfig(ctx, req.(*ConfigRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _QPU_serviceDesc = grpc.ServiceDesc{
	ServiceName: "qpu.QPU",
	HandlerType: (*QPUServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetConfig",
			Handler:    _QPU_GetConfig_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Query",
			Handler:       _QPU_Query_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "qpu.proto",
}
