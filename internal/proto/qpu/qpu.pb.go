// Code generated by protoc-gen-go. DO NOT EDIT.
// source: api/protobuf-spec/qpu.proto

package qpu

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
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

type AttributePredicate_PredicateType int32

const (
	// Used to implement the SQL 'IS NULL'.
	AttributePredicate_ISNULL AttributePredicate_PredicateType = 0
	// Used to implement the SQL 'IS NOT NULL'
	AttributePredicate_ISNOTNULL AttributePredicate_PredicateType = 1
	// Implements the SQL WHERE attr >= lbound AND attr < ubound
	AttributePredicate_RANGE AttributePredicate_PredicateType = 2
)

var AttributePredicate_PredicateType_name = map[int32]string{
	0: "ISNULL",
	1: "ISNOTNULL",
	2: "RANGE",
}

var AttributePredicate_PredicateType_value = map[string]int32{
	"ISNULL":    0,
	"ISNOTNULL": 1,
	"RANGE":     2,
}

func (x AttributePredicate_PredicateType) String() string {
	return proto.EnumName(AttributePredicate_PredicateType_name, int32(x))
}

func (AttributePredicate_PredicateType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{5, 0}
}

type SnapshotTime_SnapshotTimeType int32

const (
	// Keyword used to refer to for the most recent snapshot if we don't know
	// the actual timestamp value.
	SnapshotTime_LATEST SnapshotTime_SnapshotTimeType = 0
	// Keyword used to refer to the earliest snapshot in the system.
	SnapshotTime_ZERO SnapshotTime_SnapshotTimeType = 1
	// Keyword used to refer to a timestamp that is always greater than any
	// other timestamp
	// Used to implement long-lived queries.
	// Results in a stream that stays open indefinitely.
	SnapshotTime_INF SnapshotTime_SnapshotTimeType = 2
	// Keyword used to signify that this SnapshotTime has an actual value.
	SnapshotTime_VECTORCLOCK SnapshotTime_SnapshotTimeType = 3
)

var SnapshotTime_SnapshotTimeType_name = map[int32]string{
	0: "LATEST",
	1: "ZERO",
	2: "INF",
	3: "VECTORCLOCK",
}

var SnapshotTime_SnapshotTimeType_value = map[string]int32{
	"LATEST":      0,
	"ZERO":        1,
	"INF":         2,
	"VECTORCLOCK": 3,
}

func (x SnapshotTime_SnapshotTimeType) String() string {
	return proto.EnumName(SnapshotTime_SnapshotTimeType_name, int32(x))
}

func (SnapshotTime_SnapshotTimeType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{7, 0}
}

// The important part of every stream record.
type LogOperation struct {
	ObjectId string `protobuf:"bytes,1,opt,name=object_id,json=objectId,proto3" json:"object_id,omitempty"`
	// Bucket/Table
	// TODO: use a single term through the codebase
	Bucket               string       `protobuf:"bytes,2,opt,name=bucket,proto3" json:"bucket,omitempty"`
	Timestamp            *Vectorclock `protobuf:"bytes,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Payload              *Payload     `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *LogOperation) Reset()         { *m = LogOperation{} }
func (m *LogOperation) String() string { return proto.CompactTextString(m) }
func (*LogOperation) ProtoMessage()    {}
func (*LogOperation) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{0}
}

func (m *LogOperation) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_LogOperation.Unmarshal(m, b)
}
func (m *LogOperation) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_LogOperation.Marshal(b, m, deterministic)
}
func (m *LogOperation) XXX_Merge(src proto.Message) {
	xxx_messageInfo_LogOperation.Merge(m, src)
}
func (m *LogOperation) XXX_Size() int {
	return xxx_messageInfo_LogOperation.Size(m)
}
func (m *LogOperation) XXX_DiscardUnknown() {
	xxx_messageInfo_LogOperation.DiscardUnknown(m)
}

var xxx_messageInfo_LogOperation proto.InternalMessageInfo

func (m *LogOperation) GetObjectId() string {
	if m != nil {
		return m.ObjectId
	}
	return ""
}

func (m *LogOperation) GetBucket() string {
	if m != nil {
		return m.Bucket
	}
	return ""
}

func (m *LogOperation) GetTimestamp() *Vectorclock {
	if m != nil {
		return m.Timestamp
	}
	return nil
}

func (m *LogOperation) GetPayload() *Payload {
	if m != nil {
		return m.Payload
	}
	return nil
}

type Payload struct {
	// Types that are valid to be assigned to Val:
	//	*Payload_State
	//	*Payload_Delta
	Val                  isPayload_Val `protobuf_oneof:"val"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *Payload) Reset()         { *m = Payload{} }
func (m *Payload) String() string { return proto.CompactTextString(m) }
func (*Payload) ProtoMessage()    {}
func (*Payload) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{1}
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

type isPayload_Val interface {
	isPayload_Val()
}

type Payload_State struct {
	State *ObjectState `protobuf:"bytes,1,opt,name=state,proto3,oneof"`
}

type Payload_Delta struct {
	Delta *Payload_StateDelta `protobuf:"bytes,2,opt,name=delta,proto3,oneof"`
}

func (*Payload_State) isPayload_Val() {}

func (*Payload_Delta) isPayload_Val() {}

func (m *Payload) GetVal() isPayload_Val {
	if m != nil {
		return m.Val
	}
	return nil
}

func (m *Payload) GetState() *ObjectState {
	if x, ok := m.GetVal().(*Payload_State); ok {
		return x.State
	}
	return nil
}

func (m *Payload) GetDelta() *Payload_StateDelta {
	if x, ok := m.GetVal().(*Payload_Delta); ok {
		return x.Delta
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Payload) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Payload_State)(nil),
		(*Payload_Delta)(nil),
	}
}

type Payload_StateDelta struct {
	Old                  *ObjectState `protobuf:"bytes,1,opt,name=old,proto3" json:"old,omitempty"`
	New                  *ObjectState `protobuf:"bytes,2,opt,name=new,proto3" json:"new,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *Payload_StateDelta) Reset()         { *m = Payload_StateDelta{} }
func (m *Payload_StateDelta) String() string { return proto.CompactTextString(m) }
func (*Payload_StateDelta) ProtoMessage()    {}
func (*Payload_StateDelta) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{1, 0}
}

func (m *Payload_StateDelta) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Payload_StateDelta.Unmarshal(m, b)
}
func (m *Payload_StateDelta) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Payload_StateDelta.Marshal(b, m, deterministic)
}
func (m *Payload_StateDelta) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Payload_StateDelta.Merge(m, src)
}
func (m *Payload_StateDelta) XXX_Size() int {
	return xxx_messageInfo_Payload_StateDelta.Size(m)
}
func (m *Payload_StateDelta) XXX_DiscardUnknown() {
	xxx_messageInfo_Payload_StateDelta.DiscardUnknown(m)
}

var xxx_messageInfo_Payload_StateDelta proto.InternalMessageInfo

func (m *Payload_StateDelta) GetOld() *ObjectState {
	if m != nil {
		return m.Old
	}
	return nil
}

func (m *Payload_StateDelta) GetNew() *ObjectState {
	if m != nil {
		return m.New
	}
	return nil
}

type ObjectState struct {
	// Data model: a data item is a primary key (object_id) + a set of attributes.
	// An attribute is a key value pair (attrKey, attrValue)
	Attributes           map[string]*Value `protobuf:"bytes,1,rep,name=attributes,proto3" json:"attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *ObjectState) Reset()         { *m = ObjectState{} }
func (m *ObjectState) String() string { return proto.CompactTextString(m) }
func (*ObjectState) ProtoMessage()    {}
func (*ObjectState) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{2}
}

func (m *ObjectState) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ObjectState.Unmarshal(m, b)
}
func (m *ObjectState) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ObjectState.Marshal(b, m, deterministic)
}
func (m *ObjectState) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ObjectState.Merge(m, src)
}
func (m *ObjectState) XXX_Size() int {
	return xxx_messageInfo_ObjectState.Size(m)
}
func (m *ObjectState) XXX_DiscardUnknown() {
	xxx_messageInfo_ObjectState.DiscardUnknown(m)
}

var xxx_messageInfo_ObjectState proto.InternalMessageInfo

func (m *ObjectState) GetAttributes() map[string]*Value {
	if m != nil {
		return m.Attributes
	}
	return nil
}

type Attribute struct {
	AttrKey              string   `protobuf:"bytes,1,opt,name=attr_key,json=attrKey,proto3" json:"attr_key,omitempty"`
	Value                *Value   `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Attribute) Reset()         { *m = Attribute{} }
func (m *Attribute) String() string { return proto.CompactTextString(m) }
func (*Attribute) ProtoMessage()    {}
func (*Attribute) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{3}
}

func (m *Attribute) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Attribute.Unmarshal(m, b)
}
func (m *Attribute) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Attribute.Marshal(b, m, deterministic)
}
func (m *Attribute) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Attribute.Merge(m, src)
}
func (m *Attribute) XXX_Size() int {
	return xxx_messageInfo_Attribute.Size(m)
}
func (m *Attribute) XXX_DiscardUnknown() {
	xxx_messageInfo_Attribute.DiscardUnknown(m)
}

var xxx_messageInfo_Attribute proto.InternalMessageInfo

func (m *Attribute) GetAttrKey() string {
	if m != nil {
		return m.AttrKey
	}
	return ""
}

func (m *Attribute) GetValue() *Value {
	if m != nil {
		return m.Value
	}
	return nil
}

type Value struct {
	// Types that are valid to be assigned to Val:
	//	*Value_Str
	//	*Value_Int
	//	*Value_Flt
	Val                  isValue_Val `protobuf_oneof:"val"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *Value) Reset()         { *m = Value{} }
func (m *Value) String() string { return proto.CompactTextString(m) }
func (*Value) ProtoMessage()    {}
func (*Value) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{4}
}

func (m *Value) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Value.Unmarshal(m, b)
}
func (m *Value) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Value.Marshal(b, m, deterministic)
}
func (m *Value) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Value.Merge(m, src)
}
func (m *Value) XXX_Size() int {
	return xxx_messageInfo_Value.Size(m)
}
func (m *Value) XXX_DiscardUnknown() {
	xxx_messageInfo_Value.DiscardUnknown(m)
}

var xxx_messageInfo_Value proto.InternalMessageInfo

type isValue_Val interface {
	isValue_Val()
}

type Value_Str struct {
	Str string `protobuf:"bytes,1,opt,name=str,proto3,oneof"`
}

type Value_Int struct {
	Int int64 `protobuf:"varint,2,opt,name=int,proto3,oneof"`
}

type Value_Flt struct {
	Flt float64 `protobuf:"fixed64,3,opt,name=flt,proto3,oneof"`
}

func (*Value_Str) isValue_Val() {}

func (*Value_Int) isValue_Val() {}

func (*Value_Flt) isValue_Val() {}

func (m *Value) GetVal() isValue_Val {
	if m != nil {
		return m.Val
	}
	return nil
}

func (m *Value) GetStr() string {
	if x, ok := m.GetVal().(*Value_Str); ok {
		return x.Str
	}
	return ""
}

func (m *Value) GetInt() int64 {
	if x, ok := m.GetVal().(*Value_Int); ok {
		return x.Int
	}
	return 0
}

func (m *Value) GetFlt() float64 {
	if x, ok := m.GetVal().(*Value_Flt); ok {
		return x.Flt
	}
	return 0
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Value) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Value_Str)(nil),
		(*Value_Int)(nil),
		(*Value_Flt)(nil),
	}
}

type AttributePredicate struct {
	Attr *Attribute                       `protobuf:"bytes,1,opt,name=attr,proto3" json:"attr,omitempty"`
	Type AttributePredicate_PredicateType `protobuf:"varint,2,opt,name=type,proto3,enum=qpu.AttributePredicate_PredicateType" json:"type,omitempty"`
	// Lower bound.
	Lbound *Value `protobuf:"bytes,3,opt,name=lbound,proto3" json:"lbound,omitempty"`
	// Upper bound.
	Ubound               *Value   `protobuf:"bytes,4,opt,name=ubound,proto3" json:"ubound,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AttributePredicate) Reset()         { *m = AttributePredicate{} }
func (m *AttributePredicate) String() string { return proto.CompactTextString(m) }
func (*AttributePredicate) ProtoMessage()    {}
func (*AttributePredicate) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{5}
}

func (m *AttributePredicate) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AttributePredicate.Unmarshal(m, b)
}
func (m *AttributePredicate) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AttributePredicate.Marshal(b, m, deterministic)
}
func (m *AttributePredicate) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AttributePredicate.Merge(m, src)
}
func (m *AttributePredicate) XXX_Size() int {
	return xxx_messageInfo_AttributePredicate.Size(m)
}
func (m *AttributePredicate) XXX_DiscardUnknown() {
	xxx_messageInfo_AttributePredicate.DiscardUnknown(m)
}

var xxx_messageInfo_AttributePredicate proto.InternalMessageInfo

func (m *AttributePredicate) GetAttr() *Attribute {
	if m != nil {
		return m.Attr
	}
	return nil
}

func (m *AttributePredicate) GetType() AttributePredicate_PredicateType {
	if m != nil {
		return m.Type
	}
	return AttributePredicate_ISNULL
}

func (m *AttributePredicate) GetLbound() *Value {
	if m != nil {
		return m.Lbound
	}
	return nil
}

func (m *AttributePredicate) GetUbound() *Value {
	if m != nil {
		return m.Ubound
	}
	return nil
}

type SnapshotTimePredicate struct {
	Lbound               *SnapshotTime `protobuf:"bytes,1,opt,name=lbound,proto3" json:"lbound,omitempty"`
	Ubound               *SnapshotTime `protobuf:"bytes,2,opt,name=ubound,proto3" json:"ubound,omitempty"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *SnapshotTimePredicate) Reset()         { *m = SnapshotTimePredicate{} }
func (m *SnapshotTimePredicate) String() string { return proto.CompactTextString(m) }
func (*SnapshotTimePredicate) ProtoMessage()    {}
func (*SnapshotTimePredicate) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{6}
}

func (m *SnapshotTimePredicate) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SnapshotTimePredicate.Unmarshal(m, b)
}
func (m *SnapshotTimePredicate) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SnapshotTimePredicate.Marshal(b, m, deterministic)
}
func (m *SnapshotTimePredicate) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SnapshotTimePredicate.Merge(m, src)
}
func (m *SnapshotTimePredicate) XXX_Size() int {
	return xxx_messageInfo_SnapshotTimePredicate.Size(m)
}
func (m *SnapshotTimePredicate) XXX_DiscardUnknown() {
	xxx_messageInfo_SnapshotTimePredicate.DiscardUnknown(m)
}

var xxx_messageInfo_SnapshotTimePredicate proto.InternalMessageInfo

func (m *SnapshotTimePredicate) GetLbound() *SnapshotTime {
	if m != nil {
		return m.Lbound
	}
	return nil
}

func (m *SnapshotTimePredicate) GetUbound() *SnapshotTime {
	if m != nil {
		return m.Ubound
	}
	return nil
}

type SnapshotTime struct {
	Type  SnapshotTime_SnapshotTimeType `protobuf:"varint,1,opt,name=type,proto3,enum=qpu.SnapshotTime_SnapshotTimeType" json:"type,omitempty"`
	Value *Vectorclock                  `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	// SnapshotTime is always used in ranges.
	// isClosed is used to signify if a range is open or closed.
	// E.g. (isClosed=true, isClosed=false) -> [SnapshotTime1, SnapshotTime2).
	IsClosed             bool     `protobuf:"varint,3,opt,name=isClosed,proto3" json:"isClosed,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SnapshotTime) Reset()         { *m = SnapshotTime{} }
func (m *SnapshotTime) String() string { return proto.CompactTextString(m) }
func (*SnapshotTime) ProtoMessage()    {}
func (*SnapshotTime) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{7}
}

func (m *SnapshotTime) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SnapshotTime.Unmarshal(m, b)
}
func (m *SnapshotTime) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SnapshotTime.Marshal(b, m, deterministic)
}
func (m *SnapshotTime) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SnapshotTime.Merge(m, src)
}
func (m *SnapshotTime) XXX_Size() int {
	return xxx_messageInfo_SnapshotTime.Size(m)
}
func (m *SnapshotTime) XXX_DiscardUnknown() {
	xxx_messageInfo_SnapshotTime.DiscardUnknown(m)
}

var xxx_messageInfo_SnapshotTime proto.InternalMessageInfo

func (m *SnapshotTime) GetType() SnapshotTime_SnapshotTimeType {
	if m != nil {
		return m.Type
	}
	return SnapshotTime_LATEST
}

func (m *SnapshotTime) GetValue() *Vectorclock {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *SnapshotTime) GetIsClosed() bool {
	if m != nil {
		return m.IsClosed
	}
	return false
}

type Vectorclock struct {
	Vc                   map[string]*timestamp.Timestamp `protobuf:"bytes,1,rep,name=vc,proto3" json:"vc,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}                        `json:"-"`
	XXX_unrecognized     []byte                          `json:"-"`
	XXX_sizecache        int32                           `json:"-"`
}

func (m *Vectorclock) Reset()         { *m = Vectorclock{} }
func (m *Vectorclock) String() string { return proto.CompactTextString(m) }
func (*Vectorclock) ProtoMessage()    {}
func (*Vectorclock) Descriptor() ([]byte, []int) {
	return fileDescriptor_87cc5ddaa298b2e3, []int{8}
}

func (m *Vectorclock) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Vectorclock.Unmarshal(m, b)
}
func (m *Vectorclock) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Vectorclock.Marshal(b, m, deterministic)
}
func (m *Vectorclock) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Vectorclock.Merge(m, src)
}
func (m *Vectorclock) XXX_Size() int {
	return xxx_messageInfo_Vectorclock.Size(m)
}
func (m *Vectorclock) XXX_DiscardUnknown() {
	xxx_messageInfo_Vectorclock.DiscardUnknown(m)
}

var xxx_messageInfo_Vectorclock proto.InternalMessageInfo

func (m *Vectorclock) GetVc() map[string]*timestamp.Timestamp {
	if m != nil {
		return m.Vc
	}
	return nil
}

func init() {
	proto.RegisterEnum("qpu.AttributePredicate_PredicateType", AttributePredicate_PredicateType_name, AttributePredicate_PredicateType_value)
	proto.RegisterEnum("qpu.SnapshotTime_SnapshotTimeType", SnapshotTime_SnapshotTimeType_name, SnapshotTime_SnapshotTimeType_value)
	proto.RegisterType((*LogOperation)(nil), "qpu.LogOperation")
	proto.RegisterType((*Payload)(nil), "qpu.Payload")
	proto.RegisterType((*Payload_StateDelta)(nil), "qpu.Payload.StateDelta")
	proto.RegisterType((*ObjectState)(nil), "qpu.ObjectState")
	proto.RegisterMapType((map[string]*Value)(nil), "qpu.ObjectState.AttributesEntry")
	proto.RegisterType((*Attribute)(nil), "qpu.Attribute")
	proto.RegisterType((*Value)(nil), "qpu.Value")
	proto.RegisterType((*AttributePredicate)(nil), "qpu.AttributePredicate")
	proto.RegisterType((*SnapshotTimePredicate)(nil), "qpu.SnapshotTimePredicate")
	proto.RegisterType((*SnapshotTime)(nil), "qpu.SnapshotTime")
	proto.RegisterType((*Vectorclock)(nil), "qpu.Vectorclock")
	proto.RegisterMapType((map[string]*timestamp.Timestamp)(nil), "qpu.Vectorclock.VcEntry")
}

func init() { proto.RegisterFile("api/protobuf-spec/qpu.proto", fileDescriptor_87cc5ddaa298b2e3) }

var fileDescriptor_87cc5ddaa298b2e3 = []byte{
	// 728 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x54, 0x4b, 0x6e, 0xdb, 0x3a,
	0x14, 0x35, 0x2d, 0x7f, 0xaf, 0xf3, 0xd1, 0x23, 0xf0, 0xde, 0xf3, 0x73, 0x06, 0xcf, 0x10, 0xd0,
	0xc0, 0x19, 0x54, 0x0e, 0x5c, 0xb4, 0x68, 0x3b, 0x6a, 0xe2, 0xba, 0x8d, 0x1b, 0xc3, 0x4e, 0x65,
	0x35, 0x83, 0x4c, 0x02, 0x7d, 0x18, 0x47, 0x8d, 0x2c, 0x2a, 0x12, 0xe5, 0xc0, 0x5b, 0xe8, 0x12,
	0xda, 0xd5, 0x74, 0x03, 0x5d, 0x4c, 0x57, 0x50, 0x90, 0x94, 0x15, 0xd9, 0x49, 0x66, 0xba, 0xe7,
	0x1c, 0x1e, 0x1e, 0x5e, 0x8a, 0x17, 0xf6, 0xac, 0xd0, 0xeb, 0x86, 0x11, 0x65, 0xd4, 0x4e, 0xae,
	0x9e, 0xc7, 0x21, 0x71, 0xba, 0xb7, 0x61, 0xa2, 0x0b, 0x04, 0x2b, 0xb7, 0x61, 0xd2, 0xfa, 0x7f,
	0x46, 0xe9, 0xcc, 0x27, 0x99, 0xa8, 0xcb, 0xbc, 0x39, 0x89, 0x99, 0x35, 0x0f, 0xa5, 0x4a, 0xfb,
	0x81, 0x60, 0x6b, 0x44, 0x67, 0x93, 0x90, 0x44, 0x16, 0xf3, 0x68, 0x80, 0xf7, 0xa0, 0x4e, 0xed,
	0xaf, 0xc4, 0x61, 0x97, 0x9e, 0xdb, 0x44, 0x6d, 0xd4, 0xa9, 0x1b, 0x35, 0x09, 0x0c, 0x5d, 0xfc,
	0x0f, 0x54, 0xec, 0xc4, 0xb9, 0x21, 0xac, 0x59, 0x14, 0x4c, 0x5a, 0x61, 0x1d, 0xea, 0x99, 0x71,
	0x53, 0x69, 0xa3, 0x4e, 0xa3, 0xa7, 0xea, 0x3c, 0xca, 0x39, 0x71, 0x18, 0x8d, 0x1c, 0x9f, 0x3a,
	0x37, 0xc6, 0xbd, 0x04, 0xef, 0x43, 0x35, 0xb4, 0x96, 0x3e, 0xb5, 0xdc, 0x66, 0x49, 0xa8, 0xb7,
	0x84, 0xfa, 0x4c, 0x62, 0xc6, 0x8a, 0xd4, 0x7e, 0x22, 0xa8, 0xa6, 0x20, 0xee, 0x40, 0x39, 0x66,
	0x16, 0x23, 0x22, 0xd4, 0xca, 0x7f, 0x22, 0x92, 0x4d, 0x39, 0x7e, 0x52, 0x30, 0xa4, 0x00, 0x77,
	0xa1, 0xec, 0x12, 0x9f, 0x59, 0x22, 0x64, 0xa3, 0xf7, 0x6f, 0xde, 0x5b, 0x17, 0xda, 0xf7, 0x9c,
	0xe6, 0x0b, 0x84, 0xae, 0x65, 0x02, 0xdc, 0xc3, 0x58, 0x03, 0x85, 0xfa, 0xee, 0x53, 0xdb, 0x18,
	0x9c, 0xe4, 0x9a, 0x80, 0xdc, 0xa5, 0x1b, 0x3c, 0xa2, 0x09, 0xc8, 0xdd, 0x71, 0x19, 0x94, 0x85,
	0xe5, 0x6b, 0xdf, 0x11, 0x34, 0x72, 0x1c, 0x7e, 0x07, 0x60, 0x31, 0x16, 0x79, 0x76, 0xc2, 0x48,
	0xdc, 0x44, 0x6d, 0xa5, 0xd3, 0xe8, 0xb5, 0x37, 0x1d, 0xf4, 0xa3, 0x4c, 0x32, 0x08, 0x58, 0xb4,
	0x34, 0x72, 0x6b, 0x5a, 0x43, 0xd8, 0xdd, 0xa0, 0xb1, 0x0a, 0xca, 0x0d, 0x59, 0xa6, 0xf7, 0xc5,
	0x3f, 0x71, 0x1b, 0xca, 0x0b, 0xcb, 0x4f, 0x48, 0x9a, 0x11, 0xe4, 0x75, 0x70, 0xc4, 0x90, 0xc4,
	0xdb, 0xe2, 0x6b, 0xa4, 0x9d, 0x40, 0x3d, 0xb3, 0xc2, 0xff, 0x41, 0x8d, 0xef, 0x72, 0x79, 0xef,
	0x54, 0xe5, 0xf5, 0x69, 0xde, 0x4d, 0x79, 0xc2, 0x4d, 0xfb, 0x04, 0x65, 0x51, 0x63, 0x0c, 0x4a,
	0xcc, 0x22, 0x69, 0x70, 0x52, 0x30, 0x78, 0xc1, 0x31, 0x2f, 0x90, 0x3f, 0x8d, 0xc2, 0x31, 0x2f,
	0x60, 0x1c, 0xbb, 0xf2, 0x99, 0x30, 0x44, 0x1c, 0xbb, 0xf2, 0xd9, 0xaa, 0x65, 0xbf, 0x11, 0xe0,
	0x2c, 0xd6, 0x59, 0x44, 0x5c, 0xcf, 0xe1, 0x9d, 0xd3, 0xa0, 0xc4, 0xf3, 0xa4, 0x37, 0xb3, 0x23,
	0x32, 0x64, 0x32, 0x43, 0x70, 0xf8, 0x0d, 0x94, 0xd8, 0x32, 0x94, 0xa7, 0xde, 0xe9, 0x3d, 0x5b,
	0xd7, 0x64, 0x56, 0x7a, 0xf6, 0x65, 0x2e, 0x43, 0x62, 0x88, 0x25, 0x58, 0x83, 0x8a, 0x6f, 0xd3,
	0x24, 0x70, 0x1f, 0x39, 0x64, 0xca, 0x70, 0x4d, 0x22, 0x35, 0xa5, 0x87, 0x1a, 0xc9, 0x68, 0x2f,
	0x61, 0x7b, 0xcd, 0x1e, 0x03, 0x54, 0x86, 0xd3, 0xf1, 0x97, 0xd1, 0x48, 0x2d, 0xe0, 0x6d, 0xa8,
	0x0f, 0xa7, 0xe3, 0x89, 0x29, 0x4a, 0x84, 0xeb, 0x50, 0x36, 0x8e, 0xc6, 0x1f, 0x07, 0x6a, 0x51,
	0x9b, 0xc3, 0xdf, 0xd3, 0xc0, 0x0a, 0xe3, 0x6b, 0xca, 0x4c, 0x6f, 0x9e, 0x3b, 0xf6, 0x41, 0x96,
	0x4b, 0x1e, 0xfc, 0x2f, 0xb1, 0x67, 0x5e, 0x9b, 0xc5, 0x3b, 0xc8, 0xe2, 0x15, 0x9f, 0x94, 0xa6,
	0x29, 0x7f, 0x21, 0xd8, 0xca, 0x13, 0xf8, 0x55, 0xda, 0x39, 0x24, 0x3a, 0xa7, 0x3d, 0x58, 0xb9,
	0x56, 0xe4, 0xda, 0xb6, 0xbf, 0xfe, 0xa3, 0x3d, 0x7c, 0xf7, 0x92, 0xc6, 0x2d, 0xa8, 0x79, 0x71,
	0xdf, 0xa7, 0x31, 0x91, 0x0d, 0xae, 0x19, 0x59, 0xad, 0x1d, 0x83, 0xba, 0xe9, 0xce, 0xbb, 0x36,
	0x3a, 0x32, 0x07, 0x53, 0x53, 0x2d, 0xe0, 0x1a, 0x94, 0x2e, 0x06, 0xc6, 0x44, 0x45, 0xb8, 0x0a,
	0xca, 0x70, 0xfc, 0x41, 0x2d, 0xe2, 0x5d, 0x68, 0x9c, 0x0f, 0xfa, 0xe6, 0xc4, 0xe8, 0x8f, 0x26,
	0xfd, 0x53, 0x55, 0xd1, 0xbe, 0x21, 0x68, 0xe4, 0xb6, 0xc5, 0x1d, 0x28, 0x2e, 0x9c, 0xf4, 0x7d,
	0x35, 0x37, 0x43, 0xe9, 0xe7, 0x8e, 0x7c, 0x57, 0xc5, 0x85, 0xd3, 0xfa, 0x0c, 0xd5, 0xb4, 0x7c,
	0xe4, 0x1d, 0x1d, 0xae, 0x1f, 0xaf, 0xa5, 0xcb, 0x89, 0xaa, 0xaf, 0x26, 0xaa, 0x6e, 0xae, 0xa6,
	0x5a, 0xee, 0x5d, 0x1d, 0x1f, 0x5e, 0xe8, 0x33, 0x8f, 0x5d, 0x27, 0xb6, 0xee, 0xd0, 0x79, 0xd7,
	0x5d, 0x58, 0xb1, 0xe7, 0x5b, 0xb1, 0x18, 0xc3, 0x24, 0x89, 0xbb, 0x5e, 0xc0, 0x48, 0x14, 0x58,
	0xbe, 0x9c, 0xcb, 0x7c, 0x68, 0xdb, 0x15, 0xf1, 0xf9, 0xe2, 0x4f, 0x00, 0x00, 0x00, 0xff, 0xff,
	0x56, 0x03, 0xcd, 0x89, 0xd4, 0x05, 0x00, 0x00,
}
