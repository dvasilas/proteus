package main

import (
	"log"
	"net"
	"os"
	"time"

	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	"github.com/dimitriosvasilas/modqp/qpu/cache"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	"github.com/dimitriosvasilas/modqp/qpu/filter"
	"github.com/dimitriosvasilas/modqp/qpu/index"
	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type config struct {
	qpuType  string
	hostname string
	port     string
	conn     struct {
		hostname string
		port     string
	}
}

//Server ...
type Server struct {
	config    config
	dsClient  dSQPUcli.Client
	qpuClient cli.Client
	cache     *cache.Cache
	index     index.Index
}

func getConfig(confFile string) (config, error) {
	var conf config
	viper.SetConfigName(confFile)
	viper.AddConfigPath("../")
	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}
	conf.qpuType = viper.GetString("qpuType")
	conf.hostname = viper.GetString("hostname")
	conf.port = viper.GetString("port")

	conf.conn.hostname = viper.GetString("conn.hostname")
	conf.conn.port = viper.GetString("conn.port")

	return conf, nil
}

//NewServer ...
func NewServer(confFile string) error {
	conf, err := getConfig(confFile)

	var server Server
	if conf.qpuType == "scan" {
		c, _, err := dSQPUcli.NewClient(conf.conn.hostname + ":" + conf.conn.port)
		if err != nil {
			return err
		}
		server = Server{config: conf, dsClient: c}
	} else if conf.qpuType == "cache" {
		c, _, err := cli.NewClient(conf.conn.hostname + ":" + conf.conn.port)
		if err != nil {
			return err
		}
		server = Server{config: conf, qpuClient: c, cache: cache.New(10)}
	} else if conf.qpuType == "index" {
		c, _, err := dSQPUcli.NewClient(conf.conn.hostname + ":" + conf.conn.port)
		if err != nil {
			return err
		}
		msg := make(chan *pbQPU.Operation)
		done := make(chan bool)

		server = Server{config: conf, dsClient: c, index: index.New("size", 0, 2048)}

		go server.opConsumer(msg, done)
		_, _ = c.SubscribeOps(time.Now().UnixNano(), msg, done)
	}

	s := grpc.NewServer()
	pb.RegisterQPUServer(s, &server)
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

func (s *Server) snapshotConsumer(pred []*pbQPU.Predicate, stream pb.QPU_FindServer, msg chan *pbQPU.Object, done chan bool, exit chan bool, fn func(*pbQPU.Object, []*pbQPU.Predicate) bool) {
	for {
		if doneMsg := <-done; doneMsg {
			exit <- true
			return
		}
		streamMsg := <-msg
		if fn(streamMsg, pred) {
			stream.Send(&pb.QueryResultStream{Object: &pbQPU.Object{Key: streamMsg.Key, Attributes: streamMsg.Attributes, Timestamp: streamMsg.Timestamp}})
		}
	}
}

func (s *Server) opConsumer(msg chan *pbQPU.Operation, done chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			return
		}
		op := <-msg
		s.updateIndex(op)
	}
}

func (s *Server) updateIndex(op *pbQPU.Operation) {
	if s.index.FilterIndexable(op) {
		s.index.Put(op)
	}
}

func (s *Server) storeInCache(obj *pbQPU.Object, in []*pbQPU.Predicate) bool {
	s.cache.Put(in, *obj)
	return true
}

//Find ...
func (s *Server) Find(in *pb.FindRequest, stream pb.QPU_FindServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	exit := make(chan bool)

	if s.config.qpuType == "scan" {
		filter := func(obj *pbQPU.Object, pred []*pbQPU.Predicate) bool {
			f, _ := filter.Filter(obj, pred)
			return f
		}
		go s.snapshotConsumer(in.Predicate, stream, msg, done, exit, filter)
		go s.dsClient.GetSnapshot(in.Timestamp, msg, done)
		<-exit
	} else if s.config.qpuType == "cache" {
		cachedResult, hit, err := s.cache.Get(in.Predicate)
		if err != nil {
			return err
		}
		if hit {
			for _, item := range cachedResult {
				stream.Send(&pb.QueryResultStream{Object: &item})
			}
		} else {
			go s.snapshotConsumer(in.Predicate, stream, msg, done, exit, s.storeInCache)

			pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: [2]*pbQPU.Value{in.Predicate[0].Lbound, in.Predicate[0].Ubound}}
			go s.qpuClient.Find(in.Timestamp, pred, msg, done)
			<-exit
		}
	} else if s.config.qpuType == "index" {
		indexResult, found, _ := s.index.Get(in.Predicate)
		if found {
			for _, item := range indexResult {
				stream.Send(&pb.QueryResultStream{Object: &item})
			}
		}
	}
	return nil
}

func main() {
	err := NewServer(os.Args[1])
	if err != nil {
		log.Fatalf("Scan QPU server failed: %v", err)
	}
}
