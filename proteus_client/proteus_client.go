package proteusclient

import (
	"errors"
	"strconv"

	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/client"
)

// Client represents a connection to Proteus.
type Client struct {
	client client.Client
}

// Host represents a QPU server.
type Host struct {
	Name string
	Port int
}

// AttributeType ...
type AttributeType int

const (
	S3TAGSTR    = iota
	S3TAGINT    = iota
	S3TAGFLT    = iota
	CRDTCOUNTER = iota
	CRDTLWWREG  = iota
)

type AttributePredicate struct {
	AttrName string
	AttrType AttributeType
	Lbound   interface{}
	Ubound   interface{}
}

// QueryType ...
type QueryType int

const (
	LATEST_SNAPSHOT = iota
	NOTIFY          = iota
)

type ObjectType int

const (
	S3OBJECT = iota
	MAPCRDT  = iota
)

type ResponseRecord struct {
	SequenceID int64
	ObjectID   string
	ObjectType ObjectType
	Bucket     string
	State      ObjectState
	Timestamp  Vectorclock
}

type ObjectState []Attribute

type Attribute struct {
	AttrName string
	AttrType AttributeType
	Value    interface{}
}

type Vectorclock map[string]uint64

// NewClient creates a new Proteus client connected to the given QPU server.
func NewClient(host Host) (*Client, error) {
	c, err := client.NewClient(host.Name + ":" + strconv.Itoa(host.Port))
	if err != nil {
		return nil, err
	}
	return &Client{
		client: c,
	}, nil
}

// Close closes the connection to Proteus.
func (c *Client) Close() {
	c.client.CloseConnection()
}

// Query ...
func (c *Client) Query(AttrPredicate []AttributePredicate, TsPredicate QueryType) (<-chan ResponseRecord, <-chan error, error) {
	pred, err := inputToAttributePredicate(AttrPredicate)
	if err != nil {
		return nil, nil, err
	}
	var tsPred *pbUtils.SnapshotTimePredicate
	switch TsPredicate {
	case LATEST_SNAPSHOT:
		tsPred = protoutils.SnapshotTimePredicate(
			protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
			protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
		)
	case NOTIFY:
		tsPred = protoutils.SnapshotTimePredicate(
			protoutils.SnapshotTime(pbUtils.SnapshotTime_INF, nil),
			protoutils.SnapshotTime(pbUtils.SnapshotTime_INF, nil),
		)
	}
	stream, _, err := c.client.Query(pred, tsPred, false, false)
	if err != nil {
		return nil, nil, err
	}
	respChan := make(chan ResponseRecord)
	errChan := make(chan error)
	go func() {
		seqID := int64(0)
		for {
			streamRec, err := stream.Recv()
			if err != nil {
				errChan <- err
				close(respChan)
				close(errChan)
				return
			}
			respChan <- ResponseRecord{
				SequenceID: seqID,
				ObjectID:   streamRec.GetLogOp().GetObjectId(),
				ObjectType: getObjectType(streamRec),
				Bucket:     streamRec.GetLogOp().GetBucket(),
				State:      logOpToObjectState(streamRec.GetLogOp()),
				Timestamp:  streamRec.GetLogOp().GetTimestamp().GetVc(),
			}
			seqID++
		}
	}()
	return respChan, errChan, nil
}

func logOpToObjectState(logOp *pbUtils.LogOperation) ObjectState {
	attrs := logOp.GetPayload().GetState().GetAttrs()
	state := make([]Attribute, len(attrs))
	for i, attr := range attrs {
		state[i] = Attribute{
			AttrName: attr.GetAttrKey(),
			AttrType: getAttrType(attr.GetAttrType()),
			Value:    attr.GetValue(),
		}
	}
	return state
}

func inputToAttributePredicate(predicate []AttributePredicate) ([]*pbUtils.AttributePredicate, error) {
	pred := make([]*pbUtils.AttributePredicate, len(predicate))
	for i, p := range predicate {
		switch p.AttrType {
		case S3TAGSTR:
			switch p.Lbound.(type) {
			case string:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			switch p.Ubound.(type) {
			case string:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			pred[i] = protoutils.AttributePredicate(protoutils.Attribute(p.AttrName, pbUtils.Attribute_S3TAGSTR, nil),
				protoutils.ValueStr(p.Lbound.(string)),
				protoutils.ValueStr(p.Ubound.(string)))
		case S3TAGINT:
			switch p.Lbound.(type) {
			case int64:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			switch p.Ubound.(type) {
			case int64:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			pred[i] = protoutils.AttributePredicate(protoutils.Attribute(p.AttrName, pbUtils.Attribute_S3TAGINT, nil),
				protoutils.ValueInt(p.Lbound.(int64)),
				protoutils.ValueInt(p.Ubound.(int64)))
		case S3TAGFLT:
			switch p.Lbound.(type) {
			case float64:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			switch p.Ubound.(type) {
			case float64:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			pred[i] = protoutils.AttributePredicate(protoutils.Attribute(p.AttrName, pbUtils.Attribute_S3TAGFLT, nil),
				protoutils.ValueFlt(p.Lbound.(float64)),
				protoutils.ValueFlt(p.Ubound.(float64)))
		case CRDTCOUNTER:
			switch p.Lbound.(type) {
			case int64:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			switch p.Ubound.(type) {
			case int64:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			pred[i] = protoutils.AttributePredicate(protoutils.Attribute(p.AttrName, pbUtils.Attribute_CRDTCOUNTER, nil),
				protoutils.ValueInt(p.Lbound.(int64)),
				protoutils.ValueInt(p.Ubound.(int64)))
		case CRDTLWWREG:
			switch p.Lbound.(type) {
			case string:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			switch p.Ubound.(type) {
			case string:
			default:
				return nil, errors.New("attribute datatype and bound type missmatch")
			}
			pred[i] = protoutils.AttributePredicate(protoutils.Attribute(p.AttrName, pbUtils.Attribute_S3TAGSTR, nil),
				protoutils.ValueStr(p.Lbound.(string)),
				protoutils.ValueStr(p.Ubound.(string)))
		default:
			return nil, errors.New("unspported datatype in attribute predicate")
		}
	}
	return pred, nil
}

func getObjectType(strRecord *pbQPU.ResponseStreamRecord) ObjectType {
	switch strRecord.GetLogOp().GetObjectType() {
	case pbUtils.LogOperation_S3OBJECT:
		return S3OBJECT
	default: //pbUtils.LogOperation_MAPCRDT:
		return MAPCRDT
	}
}

func getAttrType(attrType pbUtils.Attribute_AttributeType) AttributeType {
	switch attrType {
	case pbUtils.Attribute_S3TAGSTR:
		return S3TAGSTR
	case pbUtils.Attribute_S3TAGINT:
		return S3TAGINT
	case pbUtils.Attribute_S3TAGFLT:
		return S3TAGFLT
	case pbUtils.Attribute_CRDTCOUNTER:
		return CRDTCOUNTER
	default: //pbUtils.Attribute_CRDTLWWREG:
		return CRDTLWWREG
	}
}
