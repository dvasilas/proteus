package store

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus/protos"
	pb "github.com/dvasilas/proteus/protos/antidote"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
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

func (ds AntidoteDataStore) formatOperation(crdtOp *pb.Operation) (*pbQPU.Operation, error) {
	op := protoutils.OperationOp(
		"fixThis",
		crdtOp.GetCrdtType()+"__"+crdtOp.GetKey(),
		crdtOp.GetBucket(),
		crdtOp.GetOp().GetMapKey(),
		crdtOp.GetOp().GetMapType(),
		crdtOp.GetOp().GetMapVal(),
		protoutils.DataSet("Antidote", crdtOp.GetDcID(), crdtOp.GetPartitionID()),
	)
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

//SubscribeOps ...
func (ds AntidoteDataStore) SubscribeOps(msg chan *pbQPU.Operation, ack chan bool, sync bool) (*grpc.ClientConn, chan error) {
	errCh := make(chan error)

	if sync {
		errCh <- nil
		return nil, errCh
	}
	conn, err := grpc.Dial(ds.interDCEndpoint, grpc.WithInsecure())
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	ctx := context.Background()
	client := pb.NewAntidoteDataStoreClient(conn)
	stream, err := client.WatchAsync(ctx, protoutils.SubRequestAntidote(time.Now().UnixNano()))
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	err = ds.getOperations(stream, msg)
	errCh <- err
	return conn, errCh
}
