package main

import (
	"log"
	"net"

	dScli "github.com/dimitriosvasilas/modqp/dataStore/client"
	pbDS "github.com/dimitriosvasilas/modqp/dataStore/protos"
	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/protos"
	pbQPU "github.com/dimitriosvasilas/modqp/protos"
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
	c, _ := dScli.NewClient(dSAddress)
	return server{port: port, dataStoreAddress: dSAddress, conn: c}
}

func getSnapshotConsumer(stream pb.DataStoreQPU_SubscribeStatesServer, msg chan *pbDS.StateStream, done chan bool, exit chan bool, fn func(*pbQPU.Object) bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		StateStreamMsg := <-msg
		if fn(StateStreamMsg.Object) {
			stream.Send(&pb.StateStream{Object: &pbQPU.Object{Key: StateStreamMsg.Object.Key, Attributes: StateStreamMsg.Object.Attributes, Timestamp: StateStreamMsg.Object.Timestamp}})
		}
	}
}

func (s *server) SubscribeStates(in *pb.SubRequest, stream pb.DataStoreQPU_SubscribeStatesServer) error {
	return nil
}

func (s *server) SubscribeOps(in *pb.SubRequest, stream pb.DataStoreQPU_SubscribeOpsServer) error {
	return nil
}

func (s *server) GetSnapshot(in *pb.SubRequest, stream pb.DataStoreQPU_GetSnapshotServer) error {
	msg := make(chan *pbDS.StateStream)
	done := make(chan bool)
	exit := make(chan bool)

	go getSnapshotConsumer(stream, msg, done, exit, func(obj *pbQPU.Object) bool { return true })
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
	pb.RegisterDataStoreQPUServer(s, &server)

	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
