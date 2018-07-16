package main

import (
	"fmt"
	"log"
	"net"
	"os"

	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	"github.com/dimitriosvasilas/modqp/qpu/cache"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	"github.com/dimitriosvasilas/modqp/qpu/filter"
	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const n = 10

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
	config     config
	connClient dSQPUcli.Client
	client     cli.Client
	cache      *cache.Cache
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
		server = Server{config: conf, connClient: c}
	} else if conf.qpuType == "cache" {
		c, _, err := cli.NewClient(conf.conn.hostname + ":" + conf.conn.port)
		if err != nil {
			return err
		}
		server = Server{config: conf, client: c, cache: cache.New(10)}
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

func snapshotConsumer(pred []*pbQPU.Predicate, stream pb.QPU_FindServer, msg chan *pbQPU.Object, done chan bool, exit chan bool, fn func(*pbQPU.Object, []*pbQPU.Predicate) bool) {
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

func (s *Server) storeInCache(obj *pbQPU.Object, in []*pbQPU.Predicate) bool {
	s.cache.Put(in, *obj)
	//
	s.cache.Print()
	//
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
		go snapshotConsumer(in.Predicate, stream, msg, done, exit, filter)
		go s.connClient.GetSnapshot(in.Timestamp, msg, done)
		<-exit
	} else if s.config.qpuType == "cache" {
		cachedResult, hit, err := s.cache.Get(in.Predicate)
		if err != nil {
			return err
		}
		if hit {
			fmt.Println("cache hit")
			for _, item := range cachedResult {
				stream.Send(&pb.QueryResultStream{Object: &item})
			}
		} else {
			fmt.Println("cache miss")
			go snapshotConsumer(in.Predicate, stream, msg, done, exit, s.storeInCache)

			pred := map[string][2]int64{in.Predicate[0].Attribute: [2]int64{in.Predicate[0].Lbound, in.Predicate[0].Ubound}}
			go s.client.Find(in.Timestamp, pred, msg, done)
			<-exit
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
