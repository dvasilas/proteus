package main

import (
	"encoding/json"
	"errors"
	"net"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	fS "github.com/dimitriosvasilas/modqp/dataStoreQPU/fsDataStore"
	s3 "github.com/dimitriosvasilas/modqp/dataStoreQPU/s3DataStore"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
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
	Hostname  string
	Port      string
	DataStore struct {
		Type               string
		DataDir            string
		ΑwsAccessKeyID     string
		AwsSecretAccessKey string
		Endpoint           string
		BucketName         string
	}
}

//Server ...
type Server struct {
	ds dataStore
}

func getConfig() (Config, error) {
	var conf Config
	viper.AutomaticEnv()
	err := viper.BindEnv("HOME")
	if err != nil {
		return conf, err
	}
	viper.SetConfigName("dataStore")
	viper.AddConfigPath("../../conf")
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
func ΝewServer() error {
	conf, err := getConfig()
	if err != nil {
		return err
	}

	var server Server
	if conf.DataStore.Type == "fs" {
		server = Server{ds: fS.New(viper.Get("HOME").(string) + conf.DataStore.DataDir)}
	} else if conf.DataStore.Type == "s3" {
		server = Server{ds: s3.New(conf.DataStore.ΑwsAccessKeyID, conf.DataStore.AwsSecretAccessKey, conf.DataStore.Endpoint, conf.DataStore.BucketName)}
	} else {
		return errors.New("Unknown dataStore type")
	}

	lis, err := net.Listen("tcp", conf.Hostname+":"+conf.Port)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"port": conf.Port,
	}).Info("listening")

	s := grpc.NewServer()

	pb.RegisterDataStoreQPUServer(s, &server)
	reflection.Register(s)
	return s.Serve(lis)
}

func snapshotConsumer(stream pb.DataStoreQPU_SubscribeStatesServer, msg chan *pbQPU.Object, done chan bool, errsFrom chan error, errs chan error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errsFrom
			errs <- err
			return
		}
		obj := <-msg
		if err := stream.Send(&pb.StateStream{Object: obj}); err != nil {
			errs <- err
			return
		}
	}
}

func opsConsumer(stream pb.DataStoreQPU_SubscribeOpsServer, msg chan *pbQPU.Operation, done chan bool, errsFrom chan error, errs chan error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errsFrom
			errs <- err
			return
		}
		op := <-msg
		if err := stream.Send(&pb.OpStream{Operation: op}); err != nil {
			errs <- err
			return
		}
	}
}

//SubscribeStates ...
func (s *Server) SubscribeStates(in *pb.SubRequest, stream pb.DataStoreQPU_SubscribeStatesServer) error {
	return nil
}

//SubscribeOps ...
func (s *Server) SubscribeOps(in *pb.SubRequest, stream pb.DataStoreQPU_SubscribeOpsServer) error {
	msg := make(chan *pbQPU.Operation)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)

	go opsConsumer(stream, msg, done, errs, errs1)
	go s.ds.SubscribeOps(msg, done, errs)

	err := <-errs1
	return err
}

//GetSnapshot ...
func (s *Server) GetSnapshot(in *pb.SubRequest, stream pb.DataStoreQPU_GetSnapshotServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)

	go snapshotConsumer(stream, msg, done, errs, errs1)
	go s.ds.GetSnapshot(msg, done, errs)

	err := <-errs1
	return err
}

func main() {
	err := ΝewServer()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatalf("dataStoreQPU server failed")
	}
}
