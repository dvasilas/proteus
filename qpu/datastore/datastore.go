package datastore

import (
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus"
	"github.com/dvasilas/proteus/config"
	"github.com/dvasilas/proteus/protos"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	ant "github.com/dvasilas/proteus/qpu/datastore/antidoteDataStore"
	fS "github.com/dvasilas/proteus/qpu/datastore/fsDataStore"
	s3ds "github.com/dvasilas/proteus/qpu/datastore/s3DataStore"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

//DsQPU implements a data store QPU
type DsQPU struct {
	ds     dataStore
	config config.QPUConfig
}

type dataStore interface {
	GetSnapshot(msg chan *pbQPU.Object) chan error
	SubscribeOps(msg chan *pbQPU.Operation, ack chan bool, sync bool) (*grpc.ClientConn, chan error)
}

//---------------- API Functions -------------------

//QPU creates a data store QPU
func QPU(conf config.QPUConfig) (*DsQPU, error) {
	qpu := DsQPU{
		config: conf,
	}
	if conf.Connections[0].DataStoreConfig.Type == "fs" {
		qpu.ds = fS.New(viper.Get("HOME").(string) + conf.Connections[0].DataStoreConfig.DataDir)
	} else if conf.Connections[0].DataStoreConfig.Type == "s3" {
		qpu.ds = s3ds.New(conf.Connections[0].DataStoreConfig.ΑwsAccessKeyID, conf.Connections[0].DataStoreConfig.AwsSecretAccessKey, conf.Connections[0].EndPoint, conf.Connections[0].DataStoreConfig.BucketName, conf.Connections[0].DataStoreConfig.LogStreamEndpoint)
	} else if conf.Connections[0].DataStoreConfig.Type == "antidote" {
		qpu.ds = ant.New()
	} else {
		return nil, errors.New("Unknown dataStore type")
	}
	return &qpu, nil
}

//Query implements the Query API for the data store QPU
func (q *DsQPU) Query(streamOut pb.QPU_QueryServer, conns utils.DownwardConns) error {
	msg, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("SubscribeOpsAsync received EOF")
	}
	if err != nil {
		return err
	}
	req := msg.GetRequest()
	log.WithFields(log.Fields{
		"req": req,
	}).Debug("Query request")

	if req.GetOps() {
		opCh := make(chan *pbQPU.Operation)
		errsConsm := make(chan error)
		errsSub := make(chan error)
		var conn *grpc.ClientConn
		ack := make(chan bool)

		errsConsm = q.opsConsumer(streamOut, opCh, ack, req.GetSync())
		conn, errsSub = q.ds.SubscribeOps(opCh, ack, req.GetSync())

		select {
		case err := <-errsConsm:
			close(errsConsm)
			conn.Close()
			return err
		case err := <-errsSub:
			return err
		}
	} else {
		streamCh, errsConsm := q.snapshotConsumer(streamOut)
		errsGetSn := q.ds.GetSnapshot(streamCh)

		err := <-errsGetSn
		if err != nil {
			close(errsConsm)
			return err
		}
		err = <-errsConsm
		return err
	}
}

//Cleanup ...
func (q *DsQPU) Cleanup() {
	log.Info("data store QPU cleanup")
}

//----------- Stream Consumer Functions ------------

func (q *DsQPU) snapshotConsumer(stream pb.QPU_QueryServer) (chan *pbQPU.Object, chan error) {
	errCh := make(chan error)
	streamCh := make(chan *pbQPU.Object)

	go func() {
		for obj := range streamCh {

			log.WithFields(log.Fields{
				"object": obj,
			}).Debug("dataStoreQPU: received object")

			toSend := protoutils.QueryResponseStreamState(
				obj,
				protoutils.DataSet(q.config.Connections[0].DataSet.DB, q.config.Connections[0].DataSet.DC, q.config.Connections[0].DataSet.Shard),
			)

			if err := stream.Send(toSend); err != nil {
				log.WithFields(log.Fields{
					"err": err,
				}).Debug("dataStoreQPU: strea.Send()")

				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	return streamCh, errCh
}

func (q *DsQPU) opsConsumer(stream pb.QPU_QueryServer, opCh chan *pbQPU.Operation, ack chan bool, sync bool) chan error {
	errCh := make(chan error)

	heartbeat(stream, errCh)

	go func() {
		for op := range opCh {
			op.DataSet = protoutils.DataSet(q.config.Connections[0].DataSet.DB, q.config.Connections[0].DataSet.DC, q.config.Connections[0].DataSet.Shard)
			log.Debug("DataStoreQPU:opsConsumer received op, forwarding")
			if err := stream.Send(protoutils.QueryResponseStreamOperation(op)); err != nil {
				errCh <- err
				return
			}
			if sync {
				log.Debug("DataStoreQPU:opsConsumer waiting for ACK, ")
				ackMsg, err := stream.Recv()
				if err == io.EOF {
					errCh <- errors.New("DataStoreQPU:opsConsumer reveived nil")
					return
				}
				if err != nil {
					errCh <- err
					return
				}
				log.WithFields(log.Fields{
					"message": ackMsg,
				}).Debug("DataStoreQPU:opsConsumer received ACK, forwarding")
				ack <- true
			}
		}
	}()
	return errCh
}

//---------------- Internal Functions --------------

func heartbeat(stream pb.QPU_QueryServer, errCh chan error) {
	op := protoutils.OperationOp("no_op", "", "", "", "", "", nil)
	if err := stream.Send(protoutils.QueryResponseStreamOperation(op)); err != nil {
		log.Debug("heartbeat:stream.Send error - Connection closed")
		errCh <- err
		return
	}
	f := newHeartbeat(stream, errCh)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(stream pb.QPU_QueryServer, errCh chan error) func() {
	return func() {
		heartbeat(stream, errCh)
	}
}
