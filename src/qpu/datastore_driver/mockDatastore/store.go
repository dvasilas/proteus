package store

import (
	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/proto"
	"github.com/dvasilas/proteus/src/proto/qpu"
	"google.golang.org/grpc"
)

//Datastore ...
type Datastore struct {
	opChan chan *qpu.LogOperation
	data   map[string]utils.ObjectState
}

//---------------- API Functions -------------------

//New creates and initializes an instance of AntidoteDataStore
func New() Datastore {
	return Datastore{
		data: make(map[string]utils.ObjectState),
	}
}

//SubscribeOps ..
func (ds Datastore) SubscribeOps(msg chan *qpu.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error) {
	ds.opChan = make(chan *qpu.LogOperation)
	for op := range ds.opChan {
		msg <- op
	}
	return nil, nil
}

//GetSnapshot reads a snapshot of all objects stored in an Antidotedb bucket,
// not yet implemented
func (ds Datastore) GetSnapshot(bucket string, msg chan *qpu.LogOperation) <-chan error {
	for _, item := range ds.data {
		payload := protoutils.PayloadState(&item.State)
		msg <- protoutils.LogOperation(item.ObjectID, item.Bucket, qpu.LogOperation_S3OBJECT, &item.Timestamp, payload)
	}
	return nil
}

// Op ...
func (ds Datastore) Op(op *qpu.LogOperation) {
	if ds.opChan != nil {
		ds.opChan <- op
	}
	ds.data[op.GetObjectId()] = utils.ObjectState{
		ObjectID:   op.GetObjectId(),
		ObjectType: op.GetObjectType(),
		Bucket:     op.GetBucket(),
		State:      *op.GetPayload().GetDelta().GetNew(),
		Timestamp:  *op.GetTimestamp(),
	}
}

//----------- Stream Consumer Functions ------------

//---------------- Internal Functions --------------
