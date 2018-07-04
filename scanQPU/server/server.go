package main

import (
	"log"
	"net"

	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	pbQPU "github.com/dimitriosvasilas/modqp/protos"
	"github.com/dimitriosvasilas/modqp/scanQPU/filter"
	pb "github.com/dimitriosvasilas/modqp/scanQPU/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port     = ":50053"
	connAddr = "localhost:50052"
)

type server struct {
	port     string
	connAddr string
	conn     dSQPUcli.Client
}

func newServer(port string) server {
	c, _ := dSQPUcli.NewClient(connAddr)
	return server{port: port, connAddr: connAddr, conn: c}
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

func (s *server) Find(in *pb.FindRequest, stream pb.ScanQPU_FindServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	filter := func(obj *pbQPU.Object) bool {
		f, _ := filter.Filter(obj, in.Predicate)
		return f
	}

	go snapshotConsumer(stream, msg, done, exit, filter)
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
	server := newServer(port)
	pb.RegisterScanQPUServer(s, &server)

	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
