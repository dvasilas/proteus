package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"time"

	config "github.com/dimitriosvasilas/proteus/config"
	ant "github.com/dimitriosvasilas/proteus/dataStoreQPU/antidoteDataStore"
	fS "github.com/dimitriosvasilas/proteus/dataStoreQPU/fsDataStore"
	s3 "github.com/dimitriosvasilas/proteus/dataStoreQPU/s3DataStore"
	pb "github.com/dimitriosvasilas/proteus/protos/datastore"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dataStore interface {
	GetSnapshot(msg chan *pbQPU.Object, done chan bool, errs chan error)
	SubscribeOpsAsync(msg chan *pbQPU.Operation, done chan bool, errs chan error)
	SubscribeOpsSync(msg chan *pbQPU.Operation, done chan bool, ack chan bool, errs chan error)
}

//Server ...
type Server struct {
	ds     dataStore
	config config.DSQPUConfig
}

//ΝewServer ...
func ΝewServer(confFile string) error {
	log.SetLevel(log.DebugLevel)

	var conf config.DSQPUConfig
	err := config.GetConfig(confFile, &conf)
	if err != nil {
		return err
	}

	var server Server
	if conf.DataStore.Type == "fs" {
		server = Server{ds: fS.New(viper.Get("HOME").(string) + conf.DataStore.DataDir), config: conf}
	} else if conf.DataStore.Type == "s3" {
		server = Server{ds: s3.New(conf.DataStore.ΑwsAccessKeyID, conf.DataStore.AwsSecretAccessKey, conf.DataStore.Endpoint, conf.DataStore.BucketName, conf.DataStore.LogStreamEndpoint), config: conf}
	} else if conf.DataStore.Type == "antidote" {
		server = Server{ds: ant.New(), config: conf}
	} else {
		return errors.New("Unknown dataStore type")
	}

	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"port": conf.Port,
	}).Info("listening")

	s := grpc.NewServer()

	pb.RegisterDataStoreServer(s, &server)
	reflection.Register(s)
	return s.Serve(lis)
}

func (s *Server) snapshotConsumer(stream pb.DataStore_SubscribeStatesServer, msg chan *pbQPU.Object, done chan bool, errsFrom chan error, errs chan error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errsFrom
			errs <- err
			return
		}
		obj := <-msg
		toSend := &pb.StateStream{
			Object: obj,
			Dataset: &pbQPU.DataSet{
				Db:    s.config.DataStore.DataSet.DB,
				Dc:    s.config.DataStore.DataSet.DC,
				Shard: s.config.DataStore.DataSet.Shard,
			},
		}
		if err := stream.Send(toSend); err != nil {
			errs <- err
			return
		}
	}
}

func heartbeat(stream pb.DataStore_SubscribeOpsAsyncServer) {
	opID := &pbQPU.Operation{OpId: "no_op"}
	if err := stream.Send(&pb.OpStream{Operation: opID}); err != nil {
		return
	}
	f := newHeartbeat(stream)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(stream pb.DataStore_SubscribeOpsAsyncServer) func() {
	return func() {
		heartbeat(stream)
	}
}

func (s *Server) opsConsumerAsync(stream pb.DataStore_SubscribeOpsAsyncServer, msg chan *pbQPU.Operation, done chan bool, errsFrom chan error, errs chan error) {
	heartbeat(stream)
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errsFrom
			errs <- err
			return
		}
		op := <-msg
		log.WithFields(log.Fields{
			"operation": op,
		}).Debug("datastore QPU received operation")

		if s.config.DataStore.Type == "s3" {
			ds := &pbQPU.DataSet{
				Db:    s.config.DataStore.DataSet.DB,
				Dc:    s.config.DataStore.DataSet.DC,
				Shard: s.config.DataStore.DataSet.Shard,
			}
			op.DataSet = ds
		}
		if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
			errs <- err
			return
		}
	}
}

func (s *Server) opsConsumerSync(stream pb.DataStore_SubscribeOpsSyncServer, msg chan *pbQPU.Operation, done chan bool, ack chan bool, errsFrom chan error, errs chan error) {
	heartbeat(stream)
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errsFrom
			errs <- err
			return
		}
		op := <-msg

		ds := &pbQPU.DataSet{
			Db:    s.config.DataStore.DataSet.DB,
			Dc:    s.config.DataStore.DataSet.DC,
			Shard: s.config.DataStore.DataSet.Shard,
		}
		op.DataSet = ds
		log.Debug("DataStoreQPU:opsConsumerSync received op, sending to indexQPU")
		if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
			errs <- err
			return
		}
		log.Debug("DataStoreQPU:opsConsumerSync waiting for ACK, ..")
		ackMsg, err := stream.Recv()
		if err == io.EOF {
			errs <- errors.New("opsConsumerSync reveived nil")
			return
		}
		if err != nil {
			errs <- err
			return
		}
		log.WithFields(log.Fields{
			"message": ackMsg,
		}).Debug("S3DataStore:watchSync received ACK, forwarding ACK")
		ack <- true
	}
}

//SubscribeStates ...
func (s *Server) SubscribeStates(in *pb.SubRequest, stream pb.DataStore_SubscribeStatesServer) error {
	return nil
}

//SubscribeOpsAsync ...
func (s *Server) SubscribeOpsAsync(in *pb.SubRequest, stream pb.DataStore_SubscribeOpsAsyncServer) error {
	msg := make(chan *pbQPU.Operation)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)

	go s.opsConsumerAsync(stream, msg, done, errs, errs1)
	go s.ds.SubscribeOpsAsync(msg, done, errs)

	err := <-errs1
	return err
}

//SubscribeOpsSync ...
func (s *Server) SubscribeOpsSync(stream pb.DataStore_SubscribeOpsSyncServer) error {
	msg := make(chan *pbQPU.Operation)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)
	ack := make(chan bool)

	go s.opsConsumerSync(stream, msg, done, ack, errs, errs1)
	go s.ds.SubscribeOpsSync(msg, done, ack, errs)

	err := <-errs1
	return err
}

//GetSnapshot ...
func (s *Server) GetSnapshot(in *pb.SubRequest, stream pb.DataStore_GetSnapshotServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)
	go s.snapshotConsumer(stream, msg, done, errs, errs1)
	go s.ds.GetSnapshot(msg, done, errs)

	err := <-errs1
	return err
}

//GetConfig ...
func (s *Server) GetConfig(ctx context.Context, in *pb.ConfigRequest) (*pb.ConfigResponse, error) {
	return &pb.ConfigResponse{
		Dataset: &pbQPU.DataSet{
			Db:    s.config.DataStore.DataSet.DB,
			Dc:    s.config.DataStore.DataSet.DC,
			Shard: s.config.DataStore.DataSet.Shard,
		}}, nil

}

func main() {
	var confFile string
	flag.StringVar(&confFile, "conf", "noArg", "configuration file to be used")
	flag.Parse()
	err := ΝewServer(confFile)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("dataStoreQPU server failed")
	}
}
