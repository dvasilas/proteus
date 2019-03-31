package datastore

import (
	"errors"
	"io"
	"time"

	"github.com/dvasilas/proteus"
	"github.com/dvasilas/proteus/config"
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

//SubscribeOps ...
func (q *DsQPU) SubscribeOps(stream pb.QPU_SubscribeOpsServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		return errors.New("SubscribeOpsAsync received EOF")
	}
	if err != nil {
		return err
	}
	req := msg.GetRequest()
	log.WithFields(log.Fields{
		"req": req,
	}).Debug("SubscribeOpsAsync request")

	opCh := make(chan *pbQPU.Operation)
	errsConsm := make(chan error)
	errsSub := make(chan error)
	var conn *grpc.ClientConn
	ack := make(chan bool)

	errsConsm = q.opsConsumer(stream, opCh, ack, req.GetSync())
	conn, errsSub = q.ds.SubscribeOps(opCh, ack, req.GetSync())

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

func (q *DsQPU) opsConsumer(stream pb.QPU_SubscribeOpsServer, opCh chan *pbQPU.Operation, ack chan bool, sync bool) chan error {
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
			log.Debug("DataStoreQPU:opsConsumer received op, forwarding")
			if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
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

func heartbeat(stream pb.QPU_SubscribeOpsServer, errCh chan error) {
	opID := &pbQPU.Operation{OpId: "no_op"}
	if err := stream.Send(&pb.OpStream{Operation: opID}); err != nil {
		log.Debug("heartbeat:stream.Send error - Connection closed")
		errCh <- err
		return
	}
	f := newHeartbeat(stream, errCh)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(stream pb.QPU_SubscribeOpsServer, errCh chan error) func() {
	return func() {
		heartbeat(stream, errCh)
	}
}
