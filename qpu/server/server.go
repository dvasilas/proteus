package main

import (
	"log"
	"net"
	"os"
	"time"

	utils "github.com/dimitriosvasilas/modqp"
	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	"github.com/dimitriosvasilas/modqp/qpu/cache"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	"github.com/dimitriosvasilas/modqp/qpu/dispatch"
	"github.com/dimitriosvasilas/modqp/qpu/filter"
	"github.com/dimitriosvasilas/modqp/qpu/index"
	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//Server ...
type Server struct {
	config   utils.QPUConfig
	dConn    []utils.DownwardConn
	dsClient dSQPUcli.Client
	cache    *cache.Cache
	index    index.Index
}

func getConfig(confFile string) (utils.QPUConfig, error) {
	viper.SetConfigName(confFile)
	viper.AddConfigPath("../../conf")
	viper.SetConfigType("json")
	var conf utils.QPUConfig
	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}
	if err := viper.Unmarshal(&conf); err != nil {
		return conf, err
	}
	return conf, nil
}

//NewServer ...
func NewServer(confFile string) error {
	conf, err := getConfig(confFile)

	var server Server
	if conf.QpuType == "scan" {
		c, _, err := dSQPUcli.NewClient(conf.Conns[0].Hostname + ":" + conf.Conns[0].Port)
		if err != nil {
			return err
		}
		server = Server{config: conf, dsClient: c}
	} else if conf.QpuType == "cache" {
		c, _, err := cli.NewClient(conf.Conns[0].Hostname + ":" + conf.Conns[0].Port)
		if err != nil {
			return err
		}
		server = Server{config: conf, dConn: utils.NewDConnClient(c), cache: cache.New(10)}
	} else if conf.QpuType == "index" {
		c, _, err := dSQPUcli.NewClient(conf.Conns[0].Hostname + ":" + conf.Conns[0].Port)
		if err != nil {
			return err
		}
		msg := make(chan *pbQPU.Operation)
		done := make(chan bool)

		server = Server{config: conf, dsClient: c, index: index.New("size", 0, 2048)}

		go server.opConsumer(msg, done)
		_, _ = c.SubscribeOps(time.Now().UnixNano(), msg, done)

		server.indexCatchUp()
	} else if conf.QpuType == "dispatch" {
		downwardsConns, err := utils.NewDConn(conf)
		if err != nil {
			return err
		}
		server = Server{config: conf, dConn: downwardsConns}
	}

	s := grpc.NewServer()
	pb.RegisterQPUServer(s, &server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		return err
	}
	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}

func (s *Server) snapshotConsumer(pred []*pbQPU.Predicate, stream pb.QPU_FindServer, msg chan *pbQPU.Object, done chan bool, exit chan bool, process func(*pbQPU.Object, []*pbQPU.Predicate, pb.QPU_FindServer) error) error {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return nil
		}
		streamMsg := <-msg
		err := process(streamMsg, pred, stream)
		if err != nil {
			exit <- true
			return err
		}
	}
}

func (s *Server) opConsumer(msg chan *pbQPU.Operation, done chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			return
		}
		op := <-msg
		s.index.Update(op)
	}
}

func (s *Server) catchUpConsumer(msg chan *pbQPU.Object, done chan bool, exit chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		obj := <-msg
		op := &pbQPU.Operation{
			Key:    obj.Key,
			Op:     "catchUp",
			Object: obj,
		}
		s.index.Update(op)
	}
}

func (s *Server) indexCatchUp() {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	go s.catchUpConsumer(msg, done, exit)
	go s.dsClient.GetSnapshot(time.Now().UnixNano(), msg, done)
	<-exit
}

//Find ...
func (s *Server) Find(in *pb.FindRequest, stream pb.QPU_FindServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	if s.config.QpuType == "scan" {
		go s.snapshotConsumer(in.Predicate, stream, msg, done, exit, filter.Forward)
		go s.dsClient.GetSnapshot(in.Timestamp, msg, done)
		<-exit
	} else if s.config.QpuType == "cache" {
		cachedResult, hit, err := s.cache.Get(in.Predicate)
		if err != nil {
			return err
		}
		if hit {
			for _, item := range cachedResult {
				stream.Send(&pb.QueryResultStream{Object: &item})
			}
		} else {
			go s.snapshotConsumer(in.Predicate, stream, msg, done, exit, s.cache.StoreInCache)

			pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: [2]*pbQPU.Value{in.Predicate[0].Lbound, in.Predicate[0].Ubound}}
			go s.dConn[0].Client.Find(in.Timestamp, pred, msg, done)
			<-exit
		}
	} else if s.config.QpuType == "index" {
		indexResult, found, _ := s.index.Get(in.Predicate)
		if found {
			for _, item := range indexResult {
				stream.Send(&pb.QueryResultStream{Object: &item})
			}
		}
	} else if s.config.QpuType == "dispatch" {
		go s.snapshotConsumer(in.Predicate, stream, msg, done, exit, dispatch.ForwardResponse)

		pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: [2]*pbQPU.Value{in.Predicate[0].Lbound, in.Predicate[0].Ubound}}
		client, err := dispatch.ForwardQuery(s.dConn, *in.Predicate[0])
		if err != nil {
			return err
		}
		go client.Find(in.Timestamp, pred, msg, done)
		<-exit
		return nil
	}
	return nil
}

func main() {
	err := NewServer(os.Args[1])
	if err != nil {
		log.Fatalf("Scan QPU server failed: %v", err)
	}
}
