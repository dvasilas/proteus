package utils

import (
	"encoding/json"
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

// ObjectState ...
type ObjectState struct {
	ObjectID   string
	ObjectType pbUtils.LogOperation_ObjectType
	Bucket     string
	State      pbUtils.ObjectState
	Timestamp  pbUtils.Vectorclock
}

// SubQuery ...
type SubQuery struct {
	SubQuery []*pbUtils.AttributePredicate
	Endpoint *QPU
}

//---------------- API Functions -------------------

// ConnectToQPUGraph ...
func ConnectToQPUGraph(q *QPU) error {
	conns := make([]*QPU, len(q.Config.Connections))
	for i, conn := range q.Config.Connections {
		c, err := cli.NewClient(conn)
		if err != nil {
			return err
		}
		connConf, err := c.GetConfig()
		if err != nil {
			return err
		}
		conns[i] = &QPU{
			Client:               c,
			QueryingCapabilities: connConf.GetSupportedQueries(),
			Dataset:              connConf.GetDataset(),
			Config: &config.Config{
				QpuType: connConf.QpuType,
				Port:    conn,
			},
		}
	}
	q.Conns = conns
	calcQueryingCapabilities(q, conns)
	calcDataset(q, conns)
	return nil
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

// Marshal ...
func (o *ObjectState) Marshal() ([]byte, error) {
	objJ := ObjectStateJSON{
		ObjectID:   o.ObjectID,
		ObjectType: o.ObjectType.String(),
		Bucket:     o.Bucket,
		Timestamp:  o.Timestamp.GetVc(),
	}
	state := make([]struct {
		AttrKey   string
		AttrType  string
		AttrValue string
	}, len(o.State.Attrs))
	for i, attr := range o.State.Attrs {
		state[i].AttrKey = attr.GetAttrKey()
		state[i].AttrType = attr.GetAttrType().String()
		state[i].AttrValue = ValueToString(attr.GetValue())
	}
	objJ.State = state
	return json.Marshal(objJ)
}

// UnMarshal ...
func (o *ObjectState) UnMarshal(data []byte) error {
	var objJ ObjectStateJSON
	err := json.Unmarshal(data, &objJ)
	if err != nil {
		return err
	}
	o.ObjectID = objJ.ObjectID
	o.ObjectType = pbUtils.LogOperation_ObjectType(pbUtils.LogOperation_ObjectType_value[objJ.ObjectType])
	o.Bucket = objJ.Bucket
	o.Timestamp = *protoutils.Vectorclock(objJ.Timestamp)

	attrs := make([]*pbUtils.Attribute, len(objJ.State))
	for i, attr := range objJ.State {
		t := pbUtils.Attribute_AttributeType(pbUtils.Attribute_AttributeType_value[attr.AttrType])
		val, err := StringToValue(t, attr.AttrValue)
		if err != nil {
			return err
		}
		attrs[i] = protoutils.Attribute(attr.AttrKey, t, val)
	}
	o.State = *protoutils.ObjectState(attrs)
	return nil
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

//----------------- responseTime -------------------
//---------------- error handling ------------------

// ReportError prints the given error and the stack trace returned by runtime.Stack.
func ReportError(e error) {
	log.WithFields(log.Fields{"error": e}).Warn("error")
	debug.PrintStack()
}
