package main

import (
	"log"
	"net"

	dScli "github.com/dimitriosvasilas/modqp/dataStore/client"
	pbDS "github.com/dimitriosvasilas/modqp/dataStore/datastore"
	pbDSQPU "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpu"
	"github.com/dimitriosvasilas/modqp/dataStoreQPU/filter"
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

func getSnashotConsumer(stream pbDSQPU.DataStoreQPU_SubscribeStatesServer, msg chan *pbDS.StateStream, done chan bool, exit chan bool, fn func(*pbDS.ObjectMD) bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		StateStreamMsg := <-msg
		if fn(StateStreamMsg.State) {
			stream.Send(&pbDSQPU.StateStream{State: &pbDSQPU.ObjectMD{Key: StateStreamMsg.State.Key, State: StateStreamMsg.State.State, Timestamp: StateStreamMsg.State.Timestamp}})
		}
	}
}

func (s *server) SubscribeStates(in *pbDSQPU.SubRequest, stream pbDSQPU.DataStoreQPU_SubscribeStatesServer) error {
	return nil
}

func (s *server) SubscribeOps(in *pbDSQPU.SubRequest, stream pbDSQPU.DataStoreQPU_SubscribeOpsServer) error {
	return nil
}

func (s *server) GetSnapshot(in *pbDSQPU.SubRequest, stream pbDSQPU.DataStoreQPU_GetSnapshotServer) error {
	msg := make(chan *pbDS.StateStream)
	done := make(chan bool)
	exit := make(chan bool)

	go getSnashotConsumer(stream, msg, done, exit, func(obj *pbDS.ObjectMD) bool { return true })
	go s.conn.GetSnapshot(in.Timestamp, msg, done)
	<-exit

	return nil
}

func (s *server) Query(in *pbDSQPU.QueryRequest, stream pbDSQPU.DataStoreQPU_QueryServer) error {
	msg := make(chan *pbDS.StateStream)
	done := make(chan bool)
	exit := make(chan bool)

	filter := func(obj *pbDS.ObjectMD) bool {
		f, _ := filter.Filter(obj, in.Predicate)
		return f
	}

	go getSnashotConsumer(stream, msg, done, exit, filter)
	go s.conn.GetSnapshot(in.Timestamp, msg, done)
	<-exit

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
