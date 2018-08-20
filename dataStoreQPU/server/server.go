package main

import (
	"log"
	"net"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	fS "github.com/dimitriosvasilas/modqp/dataStoreQPU/fsDataStore"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dataStore interface {
	GetSnapshot(msg chan *pbQPU.Object, done chan bool) error
	SubscribeOps(msg chan *pbQPU.Operation, done chan bool) error
	GetPath() string
}

//Config ...
type Config struct {
	Hostname  string
	Port      string
	DataStore struct {
		Type    string
		DataDir string
	}
}

//Server ...
type Server struct {
	ds dataStore
}

func getConfig() (Config, error) {
	viper.AutomaticEnv()
	viper.BindEnv("HOME")
	viper.SetConfigName("dataStore")
	viper.AddConfigPath("../../conf")
	viper.SetConfigType("json")
	var conf Config
	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}
	if err := viper.Unmarshal(&conf); err != nil {
		return conf, err
	}
	return conf, nil
}

//ΝewServer ...
func ΝewServer() error {
	conf, err := getConfig()
	if err != nil {
		return err
	}
	server := Server{ds: fS.New(viper.Get("HOME").(string) + conf.DataStore.DataDir)}

	lis, err := net.Listen("tcp", conf.Hostname+":"+conf.Port)
	if err != nil {
		return err
	}
	s := grpc.NewServer()

	pb.RegisterDataStoreQPUServer(s, &server)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}

func getSnapshotConsumer(stream pb.DataStoreQPU_SubscribeStatesServer, msg chan *pbQPU.Object, done chan bool, exit chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		obj := <-msg
		stream.Send(&pb.StateStream{Object: obj})
	}
}

func subscribeOpsConsumer(stream pb.DataStoreQPU_SubscribeOpsServer, msg chan *pbQPU.Operation, done chan bool, exit chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		op := <-msg
		stream.Send(&pb.OpStream{Operation: op})
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
	exit := make(chan bool)

	go subscribeOpsConsumer(stream, msg, done, exit)
	go s.ds.SubscribeOps(msg, done)
	<-exit

	return nil
}

//GetSnapshot ...
func (s *Server) GetSnapshot(in *pb.SubRequest, stream pb.DataStoreQPU_GetSnapshotServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	go getSnapshotConsumer(stream, msg, done, exit)
	go s.ds.GetSnapshot(msg, done)
	<-exit

	return nil
}

func main() {
	err := ΝewServer()
	if err != nil {
		log.Fatalf("dataStoreQPU server failed %v", err)
	}
}
