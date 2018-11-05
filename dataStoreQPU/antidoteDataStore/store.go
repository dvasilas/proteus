package store

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/dimitriosvasilas/modqp/protos/antidote"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
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
func (ds AntidoteDataStore) GetSnapshot(msg chan *pbQPU.Object, done chan bool, errs chan error) {
	done <- true
	errs <- nil
}

//SubscribeOpsSync ...
func (ds AntidoteDataStore) SubscribeOpsSync(msg chan *pbQPU.Operation, done chan bool, ack chan bool, errs chan error) {
	done <- true
	errs <- nil
}

func (ds AntidoteDataStore) formatOperation(crdtOp *pb.Operation) (*pbQPU.Operation, error) {
	op := &pbQPU.Operation{}
	op.Key = crdtOp.GetCrdtType() + "__" + crdtOp.GetKey()
	op.Bucket = crdtOp.GetBucket()
	op.Op = &pbQPU.Op{
		AttrKey:   crdtOp.GetOp().GetMapKey(),
		AttrType:  crdtOp.GetOp().GetMapType(),
		OpPayload: crdtOp.GetOp().GetMapVal(),
	}
	op.DataSet = &pbQPU.DataSet{
		Db:    "Antidote",
		Dc:    crdtOp.GetDcID(),
		Shard: crdtOp.GetPartitionID(),
	}
	return op, nil
}

func (ds AntidoteDataStore) getOperations(stream pb.AntidoteDataStore_WatchAsyncClient, msg chan *pbQPU.Operation, done chan bool) error {
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
		done <- false
		msg <- op
	}
}

//SubscribeOpsAsync ...
func (ds AntidoteDataStore) SubscribeOpsAsync(msg chan *pbQPU.Operation, done chan bool, errs chan error) {
	conn, err := grpc.Dial(ds.interDCEndpoint, grpc.WithInsecure())
	if err != nil {
		done <- true
		errs <- err
	}
	defer conn.Close()
	ctx := context.Background()
	client := pb.NewAntidoteDataStoreClient(conn)
	stream, err := client.WatchAsync(ctx, &pb.SubRequest{Timestamp: time.Now().UnixNano()})
	if err != nil {
		done <- true
		errs <- err
	}
	err = ds.getOperations(stream, msg, done)
	done <- true
	errs <- err
}
