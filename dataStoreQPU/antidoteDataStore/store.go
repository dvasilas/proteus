package store

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/dimitriosvasilas/proteus/protos/antidote"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type listBucketResult struct {
	Name        string
	MaxKeys     int
	IsTruncated bool
	Contents    []struct {
		Key          string
		LastModified string
		Size         int64
	}
}

//AntidoteDataStore ...
type AntidoteDataStore struct {
	interDCEndpoint string
}

//New ...
func New() AntidoteDataStore {
	s := AntidoteDataStore{
		interDCEndpoint: "127.0.0.1:10001",
	}
	return s
}

//GetSnapshot ...
func (ds AntidoteDataStore) GetSnapshot(msg chan *pbQPU.Object) chan error {
	errCh := make(chan error)

	close(msg)
	errCh <- nil

	return errCh
}

//SubscribeOpsSync ...
func (ds AntidoteDataStore) SubscribeOpsSync(msg chan *pbQPU.Operation, ack chan bool) (*grpc.ClientConn, chan error) {
	errCh := make(chan error)
	errCh <- nil
	return nil, errCh
}

func (ds AntidoteDataStore) formatOperation(crdtOp *pb.Operation) (*pbQPU.Operation, error) {
	op := &pbQPU.Operation{}
	op.Key = crdtOp.GetCrdtType() + "__" + crdtOp.GetKey()
	op.Bucket = crdtOp.GetBucket()
	op.OpPayload = &pbQPU.OperationPayload{
		Payload: &pbQPU.OperationPayload_Op{
			Op: &pbQPU.Op{
				AttrKey:  crdtOp.GetOp().GetMapKey(),
				AttrType: crdtOp.GetOp().GetMapType(),
				Payload:  crdtOp.GetOp().GetMapVal(),
			},
		},
	}
	op.DataSet = &pbQPU.DataSet{
		Db:    "Antidote",
		Dc:    crdtOp.GetDcID(),
		Shard: crdtOp.GetPartitionID(),
	}
	return op, nil
}

func (ds AntidoteDataStore) getOperations(stream pb.AntidoteDataStore_WatchAsyncClient, msg chan *pbQPU.Operation) error {
	for {
		crdtOp, err := stream.Recv()
		if err == io.EOF {
			return errors.New("Antidote: WatchAsync stream returned EOF")
		}
		if err != nil {
			return err
		}
		log.WithFields(log.Fields{
			"operation": crdtOp,
		}).Debug("Antidote data store received operation")
		op, err := ds.formatOperation(crdtOp)
		if err != nil {
			return err
		}
		msg <- op
	}
}

//SubscribeOpsAsync ...
func (ds AntidoteDataStore) SubscribeOpsAsync(msg chan *pbQPU.Operation) (*grpc.ClientConn, chan error) {
	errCh := make(chan error)

	conn, err := grpc.Dial(ds.interDCEndpoint, grpc.WithInsecure())
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	ctx := context.Background()
	client := pb.NewAntidoteDataStoreClient(conn)
	stream, err := client.WatchAsync(ctx, &pb.SubRequest{Timestamp: time.Now().UnixNano()})
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	err = ds.getOperations(stream, msg)
	errCh <- err
	return conn, errCh
}
