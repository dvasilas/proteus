package main

import (
	"log"
	"net"

	fS "github.com/dimitriosvasilas/modqp/dataStoreQPU/fsDataStore"
	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/protos"
	pbQPU "github.com/dimitriosvasilas/modqp/protos"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dataStore interface {
	GetSnapshot(msg chan *pbQPU.Object, done chan bool) error
}

type server struct {
	ds dataStore
}

func newServer() server {
	return server{ds: fS.FSDataStore{}}
}

type config struct {
	hostname string
	port     string
}

func getConfig() (config, error) {
	var conf config

	viper.SetConfigName("config")
	viper.AddConfigPath("../")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Couldn't read configuration file: %v", err)
	}

	conf.hostname = viper.GetString("hostname")
	conf.port = viper.GetString("port")

	return conf, nil
}
func getSnapshotConsumer(stream pb.DataStoreQPU_SubscribeStatesServer, msg chan *pbQPU.Object, done chan bool, exit chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		Obj := <-msg
		stream.Send(&pb.StateStream{Object: Obj})
	}
}

func (s *server) SubscribeStates(in *pb.SubRequest, stream pb.DataStoreQPU_SubscribeStatesServer) error {
	return nil
}

func (s *server) SubscribeOps(in *pb.SubRequest, stream pb.DataStoreQPU_SubscribeOpsServer) error {
	return nil
}

func (s *server) GetSnapshot(in *pb.SubRequest, stream pb.DataStoreQPU_GetSnapshotServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	go getSnapshotConsumer(stream, msg, done, exit)
	go s.ds.GetSnapshot(msg, done)
	<-exit

	return nil
}

func main() {
	conf, err := getConfig()
	if err != nil {
		log.Fatalf("failed read configuration: %v", err)
	}

	lis, err := net.Listen("tcp", conf.hostname+":"+conf.port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	server := newServer()
	pb.RegisterDataStoreQPUServer(s, &server)

	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
