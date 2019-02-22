package datastore

import (
	"errors"
	"io"
	"time"

	"github.com/dimitriosvasilas/proteus"
	"github.com/dimitriosvasilas/proteus/config"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	ant "github.com/dimitriosvasilas/proteus/qpu/datastore/antidoteDataStore"
	fS "github.com/dimitriosvasilas/proteus/qpu/datastore/fsDataStore"
	s3ds "github.com/dimitriosvasilas/proteus/qpu/datastore/s3DataStore"
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
	SubscribeOpsAsync(msg chan *pbQPU.Operation) (*grpc.ClientConn, chan error)
	SubscribeOpsSync(msg chan *pbQPU.Operation, ack chan bool) (*grpc.ClientConn, chan error)
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

//Find implements the Find API for the data store QPU
func (q *DsQPU) Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error {
	return errors.New("data Store QPU does not support Find()")
}

//GetSnapshot ...
func (q *DsQPU) GetSnapshot(in *pb.SubRequest, stream pb.QPU_GetSnapshotServer) error {
	streamCh, errsConsm := q.snapshotConsumer(stream)
	errsGetSn := q.ds.GetSnapshot(streamCh)

	select {
	case err := <-errsConsm:
		return err
	case err := <-errsGetSn:
		return err
	}
}

//SubscribeOpsAsync ...
func (q *DsQPU) SubscribeOpsAsync(in *pb.SubRequest, stream pb.QPU_SubscribeOpsAsyncServer) error {
	opCh := make(chan *pbQPU.Operation)

	errsConsm := q.opsConsumerAsync(stream, opCh)
	conn, errsSub := q.ds.SubscribeOpsAsync(opCh)

	select {
	case err := <-errsConsm:
		close(errsConsm)
		conn.Close()
		return err
	case err := <-errsSub:
		return err
	}
}

//SubscribeOpsSync ...
func (q *DsQPU) SubscribeOpsSync(stream pb.QPU_SubscribeOpsSyncServer) error {
	opCh := make(chan *pbQPU.Operation)
	ack := make(chan bool)

	errsConsm := q.opsConsumerSync(stream, opCh, ack)
	conn, errsSub := q.ds.SubscribeOpsSync(opCh, ack)

	select {
	case err := <-errsConsm:
		close(errsConsm)
		conn.Close()
		return err
	case err := <-errsSub:
		return err
	}
}

//Cleanup ...
func (q *DsQPU) Cleanup() {
	log.Info("data store QPU cleanup")
}

//----------- Stream Consumer Functions ------------

func (q *DsQPU) snapshotConsumer(stream pb.QPU_GetSnapshotServer) (chan *pbQPU.Object, chan error) {
	errCh := make(chan error)
	streamCh := make(chan *pbQPU.Object)

	go func() {
		for obj := range streamCh {
			toSend := &pb.StateStream{
				Object: obj,
				Dataset: &pbQPU.DataSet{
					Db:    q.config.Connections[0].DataSet.DB,
					Dc:    q.config.Connections[0].DataSet.DC,
					Shard: q.config.Connections[0].DataSet.Shard,
				},
			}
			if err := stream.Send(toSend); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	return streamCh, errCh
}

func (q *DsQPU) opsConsumerAsync(stream pb.QPU_SubscribeOpsAsyncServer, opCh chan *pbQPU.Operation) chan error {
	errCh := make(chan error)

	heartbeat(stream, errCh)

	go func() {
		for op := range opCh {
			log.WithFields(log.Fields{
				"operation": op,
			}).Debug("datastore QPU received operation")

			if q.config.Connections[0].DataStoreConfig.Type == "s3" {
				ds := &pbQPU.DataSet{
					Db:    q.config.Connections[0].DataSet.DB,
					Dc:    q.config.Connections[0].DataSet.DC,
					Shard: q.config.Connections[0].DataSet.Shard,
				}
				op.DataSet = ds
			}
			if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
				log.Debug("SubscribeOpsAsync client closed connection")
				errCh <- err
				return
			}
		}
	}()
	return errCh
}

func (q *DsQPU) opsConsumerSync(stream pb.QPU_SubscribeOpsSyncServer, opCh chan *pbQPU.Operation, ack chan bool) chan error {
	errCh := make(chan error)

	heartbeat(stream, errCh)

	go func() {
		for op := range opCh {
			ds := &pbQPU.DataSet{
				Db:    q.config.Connections[0].DataSet.DB,
				Dc:    q.config.Connections[0].DataSet.DC,
				Shard: q.config.Connections[0].DataSet.Shard,
			}
			op.DataSet = ds
			log.Debug("DataStoreQPU:opsConsumerSync received op, sending to indexQPU")
			if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
				errCh <- err
				return
			}
			log.Debug("DataStoreQPU:opsConsumerSync waiting for ACK, ..")
			ackMsg, err := stream.Recv()
			if err == io.EOF {
				errCh <- errors.New("opsConsumerSync reveived nil")
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			log.WithFields(log.Fields{
				"message": ackMsg,
			}).Debug("S3DataStore:watchSync received ACK, forwarding ACK")
			ack <- true
		}
	}()
	return errCh
}

//---------------- Internal Functions --------------

func heartbeat(stream pb.QPU_SubscribeOpsAsyncServer, errCh chan error) {
	opID := &pbQPU.Operation{OpId: "no_op"}
	if err := stream.Send(&pb.OpStream{Operation: opID}); err != nil {
		log.Debug("SubscribeOpsAsync client closed connection")
		errCh <- err
		return
	}
	f := newHeartbeat(stream, errCh)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(stream pb.QPU_SubscribeOpsAsyncServer, errCh chan error) func() {
	return func() {
		heartbeat(stream, errCh)
	}
}
