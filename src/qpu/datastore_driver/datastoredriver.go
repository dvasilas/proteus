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
	mysqlDS "github.com/dvasilas/proteus/src/qpu/datastore_driver/mySQL"
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
	GetSnapshot(string, chan *pbUtils.LogOperation) <-chan error
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
	var err error
	switch conf.DatastoreConfig.Type {
	case config.S3:
		q.ds = s3DS.New(
			conf.DatastoreConfig.ΑwsAccessKeyID,
			conf.DatastoreConfig.AwsSecretAccessKey,
			conf.DatastoreConfig.Endpoint,
			conf.DatastoreConfig.LogStreamEndpoint,
		)
	case config.ANTIDOTE:
		q.ds = antDS.New(
			conf.DatastoreConfig.Endpoint,
			conf.DatastoreConfig.LogStreamEndpoint,
		)
	case config.MYSQL:
		q.ds, err = mysqlDS.New(
			conf.DatastoreConfig,
		)
	case config.MOCK:
		q.ds = mockDS.New()
	}
	return q, err
}

//Query implements the Query API for the data store QPU
func (q *DriverQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	if requestRec.GetPing() != nil {
		return utils.Ping(streamOut, requestRec.GetPing())
	}
	request := requestRec.GetRequest()
	if request.GetClock().GetUbound().GetType() < request.GetClock().GetUbound().GetType() {
		return errors.New("lower bound of timestamp attribute cannot be greater than the upper bound")
	}
	if request.GetClock().GetLbound().GetType() != pbUtils.SnapshotTime_LATEST &&
		request.GetClock().GetLbound().GetType() != pbUtils.SnapshotTime_INF &&
		request.GetClock().GetUbound().GetType() != pbUtils.SnapshotTime_LATEST &&
		request.GetClock().GetUbound().GetType() != pbUtils.SnapshotTime_INF {
		return errors.New("not supported")
	}
	if request.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_INF || request.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_INF {
		opCh := make(chan *pbUtils.LogOperation)
		ack := make(chan bool)

		errsConsm := q.opConsumer(streamOut, opCh, ack, request.GetSync())
		conn, errsSub := q.ds.SubscribeOps(opCh, ack, request.GetSync())
		for {
			select {
			case err, ok := <-errsConsm:
				if !ok {
					errsConsm = nil
				} else {
					conn.Close()
					return err
				}
			case err, ok := <-errsSub:
				if !ok {
					errsSub = nil
				} else {
					return err
				}
			}
			if errsConsm == nil && errsSub == nil {
				break
			}
		}
	}
	if request.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_LATEST || request.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_LATEST {
		streamCh, errsConsm := q.snapshotConsumer(streamOut)
		errsGetSn := q.ds.GetSnapshot(request.GetBucket(), streamCh)
		for {
			select {
			case err, ok := <-errsGetSn:
				if !ok {
					errsConsm = nil
				} else {
					return err
				}
			case err, ok := <-errsConsm:
				if !ok {
					errsConsm = nil
				} else {
					return err
				}
			}
			if errsGetSn == nil && errsConsm == nil {
				break
			}
		}
	}
	return nil
}

// Op ...
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

// GetDataTransfer ...
func (q *DriverQPU) GetDataTransfer() float32 {
	return 0
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
			if err := stream.Send(protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_STATE, streamRec)); err != nil {
				utils.Warn(err)
				close(errChan)
				return
			}
			seqID++
		}
		if err := stream.Send(protoutils.ResponseStreamRecord(
			seqID,
			pbQPU.ResponseStreamRecord_END_OF_STREAM,
			&pbUtils.LogOperation{},
		)); err != nil {
			utils.Warn(err)
			close(errChan)
			return
		}
		errChan <- nil
		close(errChan)
	}()
	return streamChan, errChan
}

func (q *DriverQPU) opConsumer(stream pbQPU.QPU_QueryServer, opChan chan *pbUtils.LogOperation, ack chan bool, sync bool) <-chan error {
	var seqID int64
	errs := make(chan error, 1)

	// heartbeat(int64(0), stream, errs)

	go func() {
		for op := range opChan {
			if err := stream.Send(protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_UPDATEDELTA, op)); err != nil {
				utils.Warn(err)
				break
			}
			seqID++
			if sync {
				log.Debug("DataStoreQPU:opConsumer waiting for ACK..")
				ackMsg, err := stream.Recv()
				if err == io.EOF {
					utils.ReportError(errors.New("DataStoreQPU:opConsumer received nil"))
					errs <- errors.New("DataStoreQPU:opConsumer received nil")
					break
				}
				if err != nil {
					utils.ReportError(err)
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
		utils.ReportError(err)
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
