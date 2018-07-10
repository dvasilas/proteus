package main

import (
	"log"
	"net"

	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	pbQPU "github.com/dimitriosvasilas/modqp/qpupb"
	"github.com/dimitriosvasilas/modqp/scanQPU/filter"
	pb "github.com/dimitriosvasilas/modqp/scanQPU/sqpupb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type config struct {
	hostname string
	port     string
	conn     struct {
		hostname string
		port     string
	}
}

//Server ...
type Server struct {
	connClient dSQPUcli.Client
}

func getConfig() (config, error) {
	var conf config

	viper.SetConfigName("config")
	viper.AddConfigPath("../")
	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}
	conf.hostname = viper.GetString("hostname")
	conf.port = viper.GetString("port")

	conf.conn.hostname = viper.GetString("conn.hostname")
	conf.conn.port = viper.GetString("conn.port")

	return conf, nil
}

//NewServer ...
func NewServer() error {
	conf, err := getConfig()

	c, _, err := dSQPUcli.NewClient(conf.conn.hostname + ":" + conf.conn.port)
	if err != nil {
		return err
	}
	server := Server{connClient: c}
	s := grpc.NewServer()
	pb.RegisterScanQPUServer(s, &server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+conf.port)
	if err != nil {
		return err
	}
	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}

func snapshotConsumer(stream pb.ScanQPU_FindServer, msg chan *pbQPU.Object, done chan bool, exit chan bool, fn func(*pbQPU.Object) bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		streamMsg := <-msg
		if fn(streamMsg) {
			stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: streamMsg.Key, Attributes: streamMsg.Attributes, Timestamp: streamMsg.Timestamp}})
		}
	}
}

//Find ...
func (s *Server) Find(in *pb.FindRequest, stream pb.ScanQPU_FindServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	filter := func(obj *pbQPU.Object) bool {
		f, _ := filter.Filter(obj, in.Predicate)
		return f
	}

	go snapshotConsumer(stream, msg, done, exit, filter)
	go s.connClient.GetSnapshot(in.Timestamp, msg, done)
	<-exit

	return nil
}

func main() {
	err := NewServer()
	if err != nil {
		log.Fatalf("Scan QPU server failed: %v", err)
	}
}
