package datastoredriver

import (
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	mysqlDS "github.com/dvasilas/proteus/internal/qpu/datastore_driver/mysql"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//DriverQPU implements a data store QPU
type DriverQPU struct {
	qpu *utils.QPU
	ds  dataStore
}

type dataStore interface {
	GetSnapshot(string, chan *qpu.LogOperation) <-chan error
	SubscribeOps(msg chan *qpu.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error)
	Op(op *qpu.LogOperation)
}

//---------------- API Functions -------------------

//QPU creates a data store QPU
func QPU(conf *config.Config) (*DriverQPU, error) {
	q := &DriverQPU{
		qpu: &utils.QPU{
			Dataset:              conf.DatastoreConfig.Dataset,
			QueryingCapabilities: []*qpu.AttributePredicate{},
			Config:               conf,
		},
	}
	var err error
	switch conf.DatastoreConfig.Type {
	case config.MYSQL:
		q.ds, err = mysqlDS.New(
			conf,
		)
	}
	return q, err
}

//Query implements the Query API for the data store QPU
func (q *DriverQPU) Query(streamOut qpu_api.QPU_QueryServer, query *qpu_api.QueryInternalQuery, metadata map[string]string, block bool) error {
	if query.GetClock().GetUbound().GetType() < query.GetClock().GetUbound().GetType() {
		return errors.New("lower bound of timestamp attribute cannot be greater than the upper bound")
	}
	if query.GetClock().GetLbound().GetType() != qpu.SnapshotTime_LATEST &&
		query.GetClock().GetLbound().GetType() != qpu.SnapshotTime_INF &&
		query.GetClock().GetUbound().GetType() != qpu.SnapshotTime_LATEST &&
		query.GetClock().GetUbound().GetType() != qpu.SnapshotTime_INF {
		return errors.New("not supported")
	}
	if query.GetClock().GetLbound().GetType() == qpu.SnapshotTime_INF || query.GetClock().GetUbound().GetType() == qpu.SnapshotTime_INF {
		opCh := make(chan *qpu.LogOperation)
		ack := make(chan bool)

		errsConsm := q.opConsumer(streamOut, opCh, ack, block)
		conn, errsSub := q.ds.SubscribeOps(opCh, ack, block)
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
	if query.GetClock().GetLbound().GetType() == qpu.SnapshotTime_LATEST || query.GetClock().GetUbound().GetType() == qpu.SnapshotTime_LATEST {
		streamCh, errsConsm := q.snapshotConsumer(streamOut)
		errsGetSn := q.ds.GetSnapshot(query.GetBucket(), streamCh)
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
func (q *DriverQPU) Op(op *qpu.LogOperation) {
	q.ds.Op(op)
}

//GetConfig implements the GetConfig API for the datastoredriver QPU
func (q *DriverQPU) GetConfig() (*qpu_api.ConfigResponse, error) {
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

func (q *DriverQPU) snapshotConsumer(stream qpu_api.QPU_QueryServer) (chan *qpu.LogOperation, chan error) {
	var seqID int64
	errChan := make(chan error)
	streamChan := make(chan *qpu.LogOperation)

	go func() {
		for streamRec := range streamChan {
			if err := stream.Send(protoutils.ResponseStreamRecord(seqID, qpu_api.ResponseStreamRecord_STATE, streamRec)); err != nil {
				utils.Warn(err)
				close(errChan)
				return
			}
			seqID++
		}
		if err := stream.Send(protoutils.ResponseStreamRecord(
			seqID,
			qpu_api.ResponseStreamRecord_END_OF_STREAM,
			&qpu.LogOperation{},
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

func (q *DriverQPU) opConsumer(stream qpu_api.QPU_QueryServer, opChan chan *qpu.LogOperation, ack chan bool, sync bool) <-chan error {
	var seqID int64
	errs := make(chan error, 1)

	// heartbeat(int64(0), stream, errs)

	go func() {
		for op := range opChan {
			if err := stream.Send(protoutils.ResponseStreamRecord(seqID, qpu_api.ResponseStreamRecord_UPDATEDELTA, op)); err != nil {
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

func heartbeat(seqID int64, stream qpu_api.QPU_QueryServer, errs chan error) {
	beat := protoutils.ResponseStreamRecord(seqID, qpu_api.ResponseStreamRecord_HEARTBEAT, nil)
	if err := stream.Send(beat); err != nil {
		utils.ReportError(err)
		errs <- err
		return
	}
	f := newHeartbeat(seqID+1, stream, errs)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(seqID int64, stream qpu_api.QPU_QueryServer, errCh chan error) func() {
	return func() {
		heartbeat(seqID, stream, errCh)
	}
}
