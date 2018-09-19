package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net"
	"path/filepath"
	"runtime"
	"time"

	fS "github.com/dimitriosvasilas/modqp/dataStoreQPU/fsDataStore"
	s3 "github.com/dimitriosvasilas/modqp/dataStoreQPU/s3DataStore"
	pb "github.com/dimitriosvasilas/modqp/protos/datastore"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dataStore interface {
	GetSnapshot(msg chan *pbQPU.Object, done chan bool, errs chan error)
	SubscribeOps(msg chan *pbQPU.Operation, done chan bool, errs chan error)
}

//Config ...
type Config struct {
	Port      string
	DataStore struct {
		DataSet struct {
			DB    int
			DC    int
			Shard int
		}
		Type               string
		DataDir            string
		ΑwsAccessKeyID     string
		AwsSecretAccessKey string
		Endpoint           string
		BucketName         string
		LogStreamEndpoint  string
	}
}

//Server ...
type Server struct {
	ds     dataStore
	config Config
}

func getConfig(confFArg string) (Config, error) {
	var conf Config
	viper.AutomaticEnv()
	err := viper.BindEnv("HOME")
	if err != nil {
		return conf, err
	}
	var confFile string
	if confFArg == "noArg" {
		confF := viper.Get("DS_CONFIG_FILE")
		if confF == nil {
			return conf, errors.New("QPU config file not specified")
		}
		confFile = confF.(string)
	} else {
		confFile = confFArg
	}

	_, f, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(f)
	viper.SetConfigName(confFile)
	viper.AddConfigPath(basepath + "/../../conf")
	viper.AddConfigPath(basepath + "/../../conf/local")
	viper.AddConfigPath(basepath + "/../../conf/dockerCompose")
	viper.SetConfigType("json")
	if err = viper.ReadInConfig(); err != nil {
		return conf, err
	}
	if err = viper.Unmarshal(&conf); err != nil {
		return conf, err
	}

	confJSON, err := json.Marshal(conf)
	if err != nil {
		return conf, err
	}
	log.WithFields(log.Fields{
		"configuration": string(confJSON),
	}).Info("read configuration")
	return conf, nil
}

//ΝewServer ...
func ΝewServer(confFile string) error {
	conf, err := getConfig(confFile)
	if err != nil {
		return err
	}

	var server Server
	if conf.DataStore.Type == "fs" {
		server = Server{ds: fS.New(viper.Get("HOME").(string) + conf.DataStore.DataDir), config: conf}
	} else if conf.DataStore.Type == "s3" {
		server = Server{ds: s3.New(conf.DataStore.ΑwsAccessKeyID, conf.DataStore.AwsSecretAccessKey, conf.DataStore.Endpoint, conf.DataStore.BucketName, conf.DataStore.LogStreamEndpoint), config: conf}
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
				Db:    int64(s.config.DataStore.DataSet.DB),
				Dc:    int64(s.config.DataStore.DataSet.DC),
				Shard: int64(s.config.DataStore.DataSet.Shard),
			},
		}
		if err := stream.Send(toSend); err != nil {
			errs <- err
			return
		}
	}
}

func heartbeat(stream pb.DataStore_SubscribeOpsServer) {
	op := &pbQPU.Operation{Op: "no_op"}
	if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
		return
	}
	f := newHeartbeat(stream)
	time.AfterFunc(10*time.Second, f)
}

func newHeartbeat(stream pb.DataStore_SubscribeOpsServer) func() {
	return func() {
		heartbeat(stream)
	}
}

func (s *Server) opsConsumer(stream pb.DataStore_SubscribeOpsServer, msg chan *pbQPU.Operation, done chan bool, errsFrom chan error, errs chan error) {
	heartbeat(stream)
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errsFrom
			errs <- err
			return
		}
		op := <-msg
		ds := &pbQPU.DataSet{
			Db:    int64(s.config.DataStore.DataSet.DB),
			Dc:    int64(s.config.DataStore.DataSet.DC),
			Shard: int64(s.config.DataStore.DataSet.Shard),
		}
		op.DataSet = ds
		if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
			errs <- err
			return
		}
	}
}

//SubscribeStates ...
func (s *Server) SubscribeStates(in *pb.SubRequest, stream pb.DataStore_SubscribeStatesServer) error {
	return nil
}

//SubscribeOps ...
func (s *Server) SubscribeOps(in *pb.SubRequest, stream pb.DataStore_SubscribeOpsServer) error {
	msg := make(chan *pbQPU.Operation)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)

	go s.opsConsumer(stream, msg, done, errs, errs1)
	go s.ds.SubscribeOps(msg, done, errs)

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
			Db:    int64(s.config.DataStore.DataSet.DB),
			Dc:    int64(s.config.DataStore.DataSet.DC),
			Shard: int64(s.config.DataStore.DataSet.Shard),
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
		}).Fatalf("dataStoreQPU server failed")
	}
}
