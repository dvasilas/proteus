package datastoredriver

import (
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	antDS "github.com/dvasilas/proteus/src/qpu/datastore_driver/antidoteDataStore"
	mockDS "github.com/dvasilas/proteus/src/qpu/datastore_driver/mockDatastore"
	s3DS "github.com/dvasilas/proteus/src/qpu/datastore_driver/s3DataStore"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//DriverQPU implements a data store QPU
type DriverQPU struct {
	qpu *utils.QPU
	ds  dataStore
}

type dataStore interface {
	GetSnapshot(chan *pbUtils.LogOperation) <-chan error
	SubscribeOps(msg chan *pbUtils.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error)
	Op(op *pbUtils.LogOperation)
}

//---------------- API Functions -------------------

//QPU creates a data store QPU
func QPU(conf *config.Config) (*DriverQPU, error) {
	q := &DriverQPU{
		qpu: &utils.QPU{
			Dataset:              conf.DatastoreConfig.Dataset,
			QueryingCapabilities: []*pbUtils.AttributePredicate{},
			Config:               conf,
		},
	}

	switch conf.DatastoreConfig.Type {
	case config.S3:
		q.ds = s3DS.New(
			conf.DatastoreConfig.ΑwsAccessKeyID,
			conf.DatastoreConfig.AwsSecretAccessKey,
			conf.DatastoreConfig.Endpoint,
			conf.DatastoreConfig.Bucket,
			conf.DatastoreConfig.LogStreamEndpoint,
		)
	case config.ANTIDOTE:
		q.ds = antDS.New(
			conf.DatastoreConfig.Endpoint,
			conf.DatastoreConfig.LogStreamEndpoint,
			conf.DatastoreConfig.Bucket)

	case config.MOCK:
		q.ds = mockDS.New()
	}
	return q, nil
}

//Query implements the Query API for the data store QPU
func (q *DriverQPU) Query(streamOut pbQPU.QPU_QueryServer) error {
	log.Debug("Query")
	msg, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}
	req := msg.GetRequest()
	log.WithFields(log.Fields{
		"req": req,
	}).Debug("Query request")

	if req.GetOps() {
		if req.GetClock().GetLbound().GetType() != pbUtils.SnapshotTime_LATEST || req.GetClock().GetUbound().GetType() != pbUtils.SnapshotTime_INF {
			return errors.New("not supported")
		}

		opCh := make(chan *pbUtils.LogOperation)
		var conn *grpc.ClientConn
		ack := make(chan bool)

		errsConsm := q.opConsumer(streamOut, opCh, ack, req.GetSync())
		conn, errsSub := q.ds.SubscribeOps(opCh, ack, req.GetSync())

		select {
		case err := <-errsConsm:
			conn.Close()
			return err
		case err := <-errsSub:
			return err
		}
	} else {
		if req.GetClock().GetLbound().GetType() != pbUtils.SnapshotTime_ZERO || req.GetClock().GetUbound().GetType() != pbUtils.SnapshotTime_LATEST {
			return errors.New("not supported")
		}

		streamCh, errsConsm := q.snapshotConsumer(streamOut)
		errsGetSn := q.ds.GetSnapshot(streamCh)

		select {
		case err := <-errsGetSn:
			if err != nil {
				return err
			}
		case err := <-errsConsm:
			return err
		}
	}
	return nil
}

func (q *DriverQPU) Op(op *pbUtils.LogOperation) {
	q.ds.Op(op)
}

//GetConfig implements the GetConfig API for the datastoredriver QPU
func (q *DriverQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

//Cleanup is called when the process receives a SIGTERM signcal
func (q *DriverQPU) Cleanup() {
	log.Info("data store QPU cleanup")
}

//----------- Stream Consumer Functions ------------

func (q *DriverQPU) snapshotConsumer(stream pbQPU.QPU_QueryServer) (chan *pbUtils.LogOperation, chan error) {
	var seqID int64
	errChan := make(chan error)
	streamChan := make(chan *pbUtils.LogOperation)

	go func() {
		for streamRec := range streamChan {
			log.WithFields(log.Fields{"object": streamRec}).Debug("dataStoreQPU: received object")

			if err := stream.Send(protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_STATE, streamRec)); err != nil {
				errChan <- err
				close(errChan)
			}
			seqID++
		}
		errChan <- nil
		close(errChan)
	}()
	return streamChan, errChan
}

func (q *DriverQPU) opConsumer(stream pbQPU.QPU_QueryServer, opChan chan *pbUtils.LogOperation, ack chan bool, sync bool) <-chan error {
	var seqID int64
	errs := make(chan error, 1)

	heartbeat(int64(0), stream, errs)

	go func() {
		for op := range opChan {
			if err := stream.Send(protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_UPDATEDELTA, op)); err != nil {
				errs <- err
				break
			}
			seqID++
			if sync {
				log.Debug("DataStoreQPU:opConsumer waiting for ACK..")
				ackMsg, err := stream.Recv()
				if err == io.EOF {
					errs <- errors.New("DataStoreQPU:opConsumer reveived nil")
					break
				}
				if err != nil {
					errs <- err
					break
				}
				log.WithFields(log.Fields{
					"message": ackMsg,
				}).Debug("DataStoreQPU:opConsumer received ACK, forwarding")
				ack <- true
			}
		}
		close(errs)
	}()
	return errs
}

//---------------- Internal Functions --------------

func heartbeat(seqID int64, stream pbQPU.QPU_QueryServer, errs chan error) {
	beat := protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_HEARTBEAT, nil)
	if err := stream.Send(beat); err != nil {
		log.Debug("failed to send heartbeat - Connection closed")
		errs <- err
		return
	}
	f := newHeartbeat(seqID+1, stream, errs)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(seqID int64, stream pbQPU.QPU_QueryServer, errCh chan error) func() {
	return func() {
		heartbeat(seqID, stream, errCh)
	}
}
