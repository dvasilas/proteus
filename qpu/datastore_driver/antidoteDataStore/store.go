package store

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	antidote "github.com/AntidoteDB/antidote-go-client"
	"github.com/dvasilas/proteus/protos"
	pbAnt "github.com/dvasilas/proteus/protos/antidote"
	pbUtils "github.com/dvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//AntidoteDataStore ...
type AntidoteDataStore struct {
	logStreamEndpoint string
	endpoint          string
	antidoteCli       *antidote.Client
	bucketName        string
}

//---------------- API Functions -------------------

//New creates and initializes an instance of AntidoteDataStore
func New(endp, logPropEndp, buck string) AntidoteDataStore {
	s := AntidoteDataStore{
		endpoint:          endp,
		logStreamEndpoint: logPropEndp,
		bucketName:        buck,
	}
	return s
}

//SubscribeOps subscribes to updates from AntidoteDB (supports only async mode)
// each time an update is received, it is formated as a pbUtils.LogOperation
// and sent to datastoredriver via a channel
func (ds AntidoteDataStore) SubscribeOps(msg chan *pbUtils.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error) {
	errs := make(chan error, 1)

	if sync {
		errs <- errors.New("SubscribeOps sync not supported for AntidoteDataStore")
		return nil, errs
	}

	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		errs <- err
		return nil, errs
	}
	client := pbAnt.NewServiceClient(conn)
	ctx := context.Background()
	stream, err := client.WatchAsync(ctx, &pbAnt.SubRequest{Timestamp: 0})
	if err != nil {
		errs <- err
		return conn, errs
	}
	ds.opConsumer(stream, msg, errs)
	return conn, errs
}

//GetSnapshot reads a snapshot of all objects stored in an Antidotedb bucket,
// not yet implemented
func (ds AntidoteDataStore) GetSnapshot(msg chan *pbUtils.LogOperation) <-chan error {
	errs := make(chan error, 1)
	ds.readSnapshot(msg, errs, ds.processAndForwardObject)
	return errs
}

//----------- Stream Consumer Functions ------------

//opConsumer creates a goroutine that receives a stream of updates from AntidoteDB,
// each time an update is received, it is parsed to a pbUtils.LogOperation object
// which is then sent to the datastoredriver via a channel
func (ds AntidoteDataStore) opConsumer(stream pbAnt.Service_WatchAsyncClient, msg chan *pbUtils.LogOperation, errs chan error) {
	go func() {
		for {
			op, err := stream.Recv()
			if err == io.EOF {
				errs <- errors.New("antidoteDataStore:opConsumer received EOF")
				break
			} else if err != nil {
				errs <- err
				break
			} else {
			}
			msg <- ds.formatOperation(op)
		}
		close(msg)
		close(errs)
	}()
}

//---------------- Internal Functions --------------

//readSnapshot retrieves all objects stored in the given bucket
// and for each object calls the processObj function
func (ds AntidoteDataStore) readSnapshot(msg chan *pbUtils.LogOperation, errs chan error, processObj func(string, *antidote.MapReadResult, []antidote.MapEntryKey, chan *pbUtils.LogOperation, chan error)) {
	endpoint := strings.Split(ds.endpoint, ":")
	port, err := strconv.ParseInt(endpoint[1], 10, 64)
	if err != nil {
		errs <- err
		close(errs)
	}
	c, err := antidote.NewClient(antidote.Host{Name: endpoint[0], Port: int(port)})
	if err != nil {
		errs <- err
		close(errs)
	}
	ds.antidoteCli = c
	bucket := antidote.Bucket{Bucket: []byte(ds.bucketName)}
	tx := ds.antidoteCli.CreateStaticTransaction()
	pIndex, err := bucket.ReadSet(tx, antidote.Key([]byte("."+ds.bucketName)))
	if err != nil {
		errs <- err
		close(errs)
	}
	go func() {
		for _, obj := range pIndex {
			objVal, err := bucket.ReadMap(tx, antidote.Key([]byte(obj)))
			if err != nil {
				errs <- err
				close(errs)
			}
			entries := objVal.ListMapKeys()
			if err != nil {
				errs <- err
				close(errs)
			}
			processObj(string(obj), objVal, entries, msg, errs)

		}
		close(msg)
	}()

}

//processAndForwardObject reads the content of an object (map crdt)
// creates a *pbUtils.LogOperation and sends it to the datastoredriver to datastoredriver via a channel
func (ds AntidoteDataStore) processAndForwardObject(key string, val *antidote.MapReadResult, entries []antidote.MapEntryKey, msg chan *pbUtils.LogOperation, errs chan error) {
	attrs := make([]*pbUtils.Attribute, len(entries))
	for i, e := range entries {
		switch e.CrdtType {
		case antidote.CRDTType_LWWREG:
			r, err := val.Reg(e.Key)
			if err != nil {
				errs <- err
				close(errs)
			}
			attrs[i] = protoutils.Attribute(string(e.Key), pbUtils.Attribute_CRDTLWWREG, protoutils.ValueStr(string(r)))
		case antidote.CRDTType_COUNTER:
			c, err := val.Counter(e.Key)
			if err != nil {
				errs <- err
				close(errs)
			}
			attrs[i] = protoutils.Attribute(string(e.Key), pbUtils.Attribute_CRDTCOUNTER, protoutils.ValueInt(int64(c)))
		case antidote.CRDTType_ORSET:
		case antidote.CRDTType_RRMAP:
		case antidote.CRDTType_MVREG:
		}
	}
	state := protoutils.ObjectState(attrs)
	obj := protoutils.LogOperation(
		string(key),
		ds.bucketName,
		pbUtils.LogOperation_MAPCRDT,
		nil,
		protoutils.PayloadState(state),
	)
	log.WithFields(log.Fields{
		"object": obj,
	}).Debug("antidoteDataStore: snapshot")
	msg <- obj
}

func (ds AntidoteDataStore) formatOperation(logOp *pbAnt.LogOperation) *pbUtils.LogOperation {
	var payload *pbUtils.Payload
	switch logOp.GetPayload().GetVal().(type) {
	case *pbAnt.LogOperation_Payload_Op:
		ops := logOp.GetPayload().GetOp().GetOp()
		attrs := make([]*pbUtils.Attribute, len(ops))
		updates := make([]*pbUtils.Operation_Update, len(ops))
		for i := range ops {
			var typ pbUtils.Attribute_AttributeType
			if ops[i].GetObject().GetType() == "antidote_crdt_counter_pn" {
				typ = pbUtils.Attribute_CRDTCOUNTER
			} else if ops[i].GetObject().GetType() == "antidote_crdt_register_lww" {
				typ = pbUtils.Attribute_CRDTLWWREG
			}
			attrs[i] = protoutils.Attribute(ops[i].GetObject().GetKey(), typ, nil)
			updates[i] = protoutils.Update(ops[i].GetUpdate().GetOpType(), crdtValToValue(ops[i].GetUpdate().GetValue()))
		}
		payload = protoutils.PayloadOp(attrs, updates)
	case *pbAnt.LogOperation_Payload_Delta:
		oldState := mapCrdtStateToObjectState(logOp.GetPayload().GetDelta().GetOld().GetState())
		newState := mapCrdtStateToObjectState(logOp.GetPayload().GetDelta().GetNew().GetState())
		payload = protoutils.PayloadDelta(oldState, newState)
	}
	op := protoutils.LogOperation(
		logOp.GetKey(),
		logOp.GetBucket(),
		pbUtils.LogOperation_MAPCRDT,
		//TODO antidote should return vector clock
		protoutils.Vectorclock(map[string]uint64{"antidote": uint64(logOp.GetCommitTime())}),
		payload,
	)
	return op
}

func mapCrdtStateToObjectState(crdtSt []*pbAnt.CrdtMapState_MapState) *pbUtils.ObjectState {
	attrs := make([]*pbUtils.Attribute, len(crdtSt))
	for i := range crdtSt {
		var typ pbUtils.Attribute_AttributeType
		if crdtSt[i].GetObject().GetType() == "antidote_crdt_counter_pn" {
			typ = pbUtils.Attribute_CRDTCOUNTER
		} else if crdtSt[i].GetObject().GetType() == "antidote_crdt_register_lww" {
			typ = pbUtils.Attribute_CRDTLWWREG
		}
		attrs[i] = protoutils.Attribute(crdtSt[i].GetObject().GetKey(), typ, crdtValToValue(crdtSt[i].GetValue()))
	}
	return protoutils.ObjectState(attrs)
}

func crdtValToValue(val *pbAnt.CrdtValue) *pbUtils.Value {
	value := &pbUtils.Value{}
	switch val.GetVal().(type) {
	case *pbAnt.CrdtValue_Str:
		value.Val = &pbUtils.Value_Str{
			Str: val.GetStr(),
		}
	case *pbAnt.CrdtValue_Int:
		value.Val = &pbUtils.Value_Int{
			Int: val.GetInt(),
		}
	}
	return value
}
