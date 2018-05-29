package main

import (
	"log"
	"net"

	dScli "github.com/dimitriosvasilas/modqp/dataStore/client"
	pbDS "github.com/dimitriosvasilas/modqp/dataStore/datastore"
	pbDSQPU "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpu"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port      = ":50052"
	dSAddress = "localhost:50051"
)

type server struct {
	port             string
	dataStoreAddress string
	conn             dScli.Client
}

func newDSQPUServer(port string) server {
	c, _ := dScli.NewDSClient(dSAddress)
	return server{port: port, dataStoreAddress: dSAddress, conn: c}
}

func _subscribeConsumer(stream pbDSQPU.DataStoreQPU_SubscribeStatesServer, msg chan *pbDS.StateStream, done chan bool, exit chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		streamMsg := <-msg
		stream.Send(&pbDSQPU.StateStream{State: &pbDSQPU.ObjectMD{Key: streamMsg.State.Key, State: streamMsg.State.State, Timestamp: streamMsg.State.Timestamp}})
	}
}

func (s *server) SubscribeStates(in *pbDSQPU.QueryRequest, stream pbDSQPU.DataStoreQPU_SubscribeStatesServer) error {
	msg := make(chan *pbDS.StateStream)
	done := make(chan bool)
	exit := make(chan bool)

	go _subscribeConsumer(stream, msg, done, exit)
	go s.conn.SubscribeStates(in.Timestamp, msg, done)
	<-exit
	return nil
}

func (s *server) SubscribeOps(in *pbDSQPU.QueryRequest, stream pbDSQPU.DataStoreQPU_SubscribeOpsServer) error {
	return nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	server := newDSQPUServer(port)
	pbDSQPU.RegisterDataStoreQPUServer(s, &server)

	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
