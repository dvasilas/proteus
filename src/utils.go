package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"runtime/debug"

	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	cli "github.com/dvasilas/proteus/src/qpu/client"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// QPU ...
type QPU struct {
	Client               cli.Client
	Conns                []*QPU
	Dataset              *pbQPU.DataSet
	QueryingCapabilities []*pbUtils.AttributePredicate
	Config               *config.Config
}

//---------------- API Functions -------------------

// ConnectToQPUGraph ...
func ConnectToQPUGraph(q *QPU) error {
	conns := make([]*QPU, len(q.Config.Connections))
	for i, conn := range q.Config.Connections {
		c, err := cli.NewClient(conn.Address)
		if err != nil {
			return err
		}
		retries := 0
		connConf, err := c.GetConfig()
		for err != nil || retries > 10 {
			ReportError(err)
			connConf, err = c.GetConfig()
			if err == nil {
				break
			}
			retries++
		}
		if err != nil {
			ReportError(err)
			return err
		}
		conns[i] = &QPU{
			Client:               c,
			QueryingCapabilities: connConf.GetSupportedQueries(),
			Dataset:              connConf.GetDataset(),
			Config: &config.Config{
				QpuType: connConf.QpuType,
				Port:    conn.Address,
			},
		}
	}
	q.Conns = conns
	calcQueryingCapabilities(q, conns)
	calcDataset(q, conns)
	return nil
}

//----------------- ObjectState --------------------

// ObjectState ...
type ObjectState struct {
	ObjectID   string
	ObjectType pbUtils.LogOperation_ObjectType
	Bucket     string
	State      pbUtils.ObjectState
	Timestamp  pbUtils.Vectorclock
}

// Marshal ...
func (o *ObjectState) Marshal() ([]byte, error) {
	marshalledState, err := marshalState(&o.State)
	if err != nil {
		return nil, err
	}
	marshalledVC, err := MarshalVectorClock(&o.Timestamp)
	if err != nil {
		return nil, err
	}
	marshalledParts := make([][]byte, 0)
	marshalledParts = append(marshalledParts, []byte(o.ObjectID))
	marshalledParts = append(marshalledParts, marshalObjectType(o.ObjectType))
	marshalledParts = append(marshalledParts, []byte(o.Bucket))
	marshalledParts = append(marshalledParts, marshalledState)
	marshalledParts = append(marshalledParts, marshalledVC)
	marshalledObjetState := bytes.Join(marshalledParts, []byte{'|'})
	return marshalledObjetState, nil
}

func marshalObjectType(t pbUtils.LogOperation_ObjectType) []byte {
	return []byte(pbUtils.LogOperation_ObjectType_name[int32(t)])
}

func marshalState(objectState *pbUtils.ObjectState) ([]byte, error) {
	return proto.Marshal(objectState)
}

func unmarshalObjectType(encodedType []byte) pbUtils.LogOperation_ObjectType {
	return pbUtils.LogOperation_ObjectType(pbUtils.LogOperation_ObjectType_value[string(encodedType)])
}

// MarshalVectorClock ...
func MarshalVectorClock(vc *pbUtils.Vectorclock) ([]byte, error) {
	return proto.Marshal(vc)
}

func unmarshalState(encodedObjectState []byte) (pbUtils.ObjectState, error) {
	var objectState pbUtils.ObjectState
	err := proto.Unmarshal(encodedObjectState, &objectState)
	return objectState, err
}

// UnmarshalVectorClock ...
func UnmarshalVectorClock(encodedVC []byte) (pbUtils.Vectorclock, error) {
	var vc pbUtils.Vectorclock
	err := proto.Unmarshal(encodedVC, &vc)
	return vc, err
}

// UnmarshalObject ...
func UnmarshalObject(data []byte) (ObjectState, error) {
	marshalledParts := bytes.Split(data, []byte{'|'})
	state, err := unmarshalState(marshalledParts[3])
	if err != nil {
		return ObjectState{}, err
	}
	vectorclock, err := UnmarshalVectorClock(marshalledParts[4])
	if err != nil {
		return ObjectState{}, err
	}
	objectState := ObjectState{
		ObjectID:   string(marshalledParts[0]),
		ObjectType: unmarshalObjectType(marshalledParts[1]),
		Bucket:     string(marshalledParts[2]),
		State:      state,
		Timestamp:  vectorclock,
	}
	return objectState, nil
}

// GetMessageSize ...
func GetMessageSize(streamRec *pbQPU.ResponseStreamRecord) (int, error) {
	buff, err := proto.Marshal(streamRec)
	if err != nil {
		return -1, err
	}
	bytesBuff := bytes.NewBuffer(buff)
	return bytesBuff.Len(), nil
}

// SubQuery ...
type SubQuery struct {
	SubQuery []*pbUtils.AttributePredicate
	Endpoint *QPU
}

// ObjectStateJSON ...
type ObjectStateJSON struct {
	ObjectID   string
	ObjectType string
	Bucket     string
	State      []struct {
		AttrKey   string
		AttrType  string
		AttrValue string
	}
	Timestamp map[string]uint64
}

// ValueToString converts an attribute value to a string
func ValueToString(val *pbUtils.Value) string {
	switch val.Val.(type) {
	case *pbUtils.Value_Int:
		return strconv.Itoa(int(val.GetInt()))
	case *pbUtils.Value_Flt:
		return fmt.Sprintf("%f", val.GetFlt())
	case *pbUtils.Value_Str:
		return val.GetStr()
	default:
		return ""
	}
}

// StringToValue ...
func StringToValue(t pbUtils.Attribute_AttributeType, str string) (*pbUtils.Value, error) {
	var val *pbUtils.Value
	switch t {
	case pbUtils.Attribute_S3TAGSTR, pbUtils.Attribute_CRDTLWWREG:
		val = protoutils.ValueStr(str)
	case pbUtils.Attribute_S3TAGINT, pbUtils.Attribute_CRDTCOUNTER:
		i, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		val = protoutils.ValueInt(i)
	case pbUtils.Attribute_S3TAGFLT:
		f, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, err
		}
		val = protoutils.ValueFlt(f)
	}
	return val, nil
}

// CanRespondToQuery ...
func CanRespondToQuery(predicate []*pbUtils.AttributePredicate, capabilities []*pbUtils.AttributePredicate) (bool, error) {
	if len(capabilities) == 0 {
		return true, nil
	}
	for _, p := range predicate {
		matchesCapabilities := false
		for _, c := range capabilities {
			if p.GetAttr().GetAttrKey() == c.GetAttr().GetAttrKey() && p.GetAttr().GetAttrType() == c.GetAttr().GetAttrType() {
				lb, err := Compare(p.GetLbound(), c.GetLbound())
				if err != nil {
					return false, err
				}
				ub, err := Compare(p.GetUbound(), c.GetUbound())
				if err != nil {
					return false, err
				}
				if lb >= 0 && ub <= 0 {
					matchesCapabilities = true
					break
				}
			}
		}
		if !matchesCapabilities {
			return false, nil
		}
	}
	return true, nil
}

// Compare ...
func Compare(a, b *pbUtils.Value) (int, error) {
	if valueType(a) != valueType(b) {
		return 0, errors.New("cannot compare different types of Value")
	}
	const TOLERANCE = 0.000001
	switch a.GetVal().(type) {
	case *pbUtils.Value_Flt:
		diff := a.GetFlt() - b.GetFlt()
		if diff := math.Abs(diff); diff < TOLERANCE {
			return 0, nil
		}
		if diff < 0 {
			return -1, nil
		}
		return 1, nil
	case *pbUtils.Value_Int:
		return int(a.GetInt() - b.GetInt()), nil
	case *pbUtils.Value_Str:
		return strings.Compare(a.GetStr(), b.GetStr()), nil
	}
	return 0, errors.New("unknown Value type")
}

// AttrMatchesPredicate checks if an object attribute matches a given predicate.
func AttrMatchesPredicate(predicate *pbUtils.AttributePredicate, attr *pbUtils.Attribute) (bool, error) {
	if keyMatch(predicate.GetAttr().GetAttrKey(), attr) && typeMatch(predicate.GetAttr().GetAttrType(), attr) {
		return rangeMatch(predicate, attr)
	}
	return false, nil
}

func keyMatch(objectName string, attr *pbUtils.Attribute) bool {
	if objectName == attr.GetAttrKey() {
		return true
	}
	return false
}

func typeMatch(t pbUtils.Attribute_AttributeType, attr *pbUtils.Attribute) bool {
	if t == attr.GetAttrType() {
		return true
	}
	return false
}

// within the range [greaterOrEqual, lessThan)
func rangeMatch(pred *pbUtils.AttributePredicate, attr *pbUtils.Attribute) (bool, error) {
	lb, err := Compare(attr.GetValue(), pred.GetLbound())
	if err != nil {
		return false, err
	}
	ub, err := Compare(attr.GetValue(), pred.GetUbound())
	if err != nil {
		return false, err
	}
	if lb >= 0 && ub < 0 {
		return true, nil
	}
	return false, nil
}

// Ping ...
func Ping(stream pbQPU.QPU_QueryServer, msg *pbQPU.PingMsg) error {
	seqID := msg.GetSeqId()
	for {
		seqID++
		if err := stream.Send(protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_HEARTBEAT, nil)); err != nil {
			return err
		}
		p, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		seqID = p.GetPing().GetSeqId()
		fmt.Println(seqID)
	}
	return nil
}

//----------- Stream Consumer Functions ------------

//QueryResponseConsumer receives a QueryResponseStream, iteratively reads from the stream, and processes each input element based on a given function
func QueryResponseConsumer(pred []*pbUtils.AttributePredicate, streamIn pbQPU.QPU_QueryClient, streamOut pbQPU.QPU_QueryServer, process func([]*pbUtils.AttributePredicate, *pbQPU.ResponseStreamRecord, pbQPU.QPU_QueryServer, *int64) error, errChan chan error) {
	seqID := int64(0)
	go func() {
		for {
			streamRec, err := streamIn.Recv()
			if err == io.EOF {
				if streamOut != nil {
					errChan <- streamOut.Send(
						protoutils.ResponseStreamRecord(
							seqID,
							pbQPU.ResponseStreamRecord_END_OF_STREAM,
							&pbUtils.LogOperation{},
						),
					)
				}
				errChan <- err
				return
			} else if err != nil {
				errChan <- err
				return
			}
			if err = process(pred, streamRec, streamOut, &seqID); err != nil {
				errChan <- err
				return
			}
		}
	}()
}

//---------------- Internal Functions --------------

func mergeDatasets(a, b *pbQPU.DataSet) {
	for databaseID := range b.GetDatabases() {
		if db, ok := a.GetDatabases()[databaseID]; ok {
			for datacenterID := range b.GetDatabases()[databaseID].GetDatacenters() {
				if dc, ok := db.GetDatacenters()[datacenterID]; ok {
					dc.Shards = append(dc.GetShards(), b.GetDatabases()[databaseID].GetDatacenters()[datacenterID].GetShards()...)
				} else {
					db.GetDatacenters()[datacenterID] = b.GetDatabases()[databaseID].GetDatacenters()[datacenterID]
				}
			}
		} else {
			a.GetDatabases()[databaseID] = b.GetDatabases()[databaseID]
		}
	}
}

func calcQueryingCapabilities(q *QPU, conns []*QPU) {
	for _, c := range conns {
		q.QueryingCapabilities = append(q.QueryingCapabilities, c.QueryingCapabilities...)
	}
}

func calcDataset(q *QPU, conns []*QPU) {
	q.Dataset = conns[0].Dataset
	for _, c := range conns[:1] {
		mergeDatasets(q.Dataset, c.Dataset)
	}
}

func valueType(v *pbUtils.Value) int {
	switch v.GetVal().(type) {
	case *pbUtils.Value_Flt:
		return 0
	case *pbUtils.Value_Int:
		return 1
	case *pbUtils.Value_Str:
		return 2
	}
	rand.Seed(time.Now().UTC().UnixNano())
	return rand.Int()
}

//----------------- responseTime -------------------

// ResponseTime ...
type ResponseTime []time.Duration

func (t ResponseTime) Len() int {
	return len(t)
}
func (t ResponseTime) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t ResponseTime) Less(i, j int) bool {
	return t[i].Nanoseconds() < t[j].Nanoseconds()
}

//---------------- error handling ------------------

// ReportError prints the given error and the stack trace returned by runtime.Stack.
func ReportError(e error) {
	log.WithFields(log.Fields{"error": e}).Warn("error")
	debug.PrintStack()
}

// Warn prints the given error
func Warn(e error) {
	log.WithFields(log.Fields{"error": e}).Warn("warning")
	debug.PrintStack()
}

//----------- query metadata parameters ------------

// MaxResponseCount ..
func MaxResponseCount(metadata map[string]string) (int64, error) {
	maxResponseCount := int64(-1)
	if metadata != nil {
		if val, ok := metadata["maxResponseCount"]; ok {
			mdMaxResponseCountVal, err := strconv.ParseInt(val, 10, 0)
			if err != nil {
				return maxResponseCount, err
			}
			maxResponseCount = mdMaxResponseCountVal
		}
	}
	return maxResponseCount, nil
}
