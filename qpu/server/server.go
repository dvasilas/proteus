package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	utils "github.com/dimitriosvasilas/modqp"
	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	pbDsQPU "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	"github.com/dimitriosvasilas/modqp/qpu/cache"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	"github.com/dimitriosvasilas/modqp/qpu/dispatch"
	"github.com/dimitriosvasilas/modqp/qpu/filter"
	"github.com/dimitriosvasilas/modqp/qpu/index"
	pb "github.com/dimitriosvasilas/modqp/qpu/qpupb"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	log "github.com/sirupsen/logrus"
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

func getConfig() (utils.QPUConfig, error) {
	var conf utils.QPUConfig
	viper.AutomaticEnv()
	err := viper.BindEnv("QPU_CONFIG_FILE")
	if err != nil {
		return conf, err
	}
	confFile := viper.Get("QPU_CONFIG_FILE")
	viper.SetConfigName(confFile.(string))
	viper.AddConfigPath("../../conf")
	viper.SetConfigType("json")

	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}
	if err := viper.Unmarshal(&conf); err != nil {
		return conf, err
	}

	confJSON, err := json.Marshal(conf)
	if err != nil {
		return conf, err
	}
	log.WithFields(log.Fields{
		"configuration": string(confJSON),
	}).Info("read configuration")
	return conf, nil
}

//NewServer ...
func NewServer() error {
	conf, err := getConfig()
	if err != nil {
		return err
	}

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
		server = Server{config: conf, dsClient: c, index: index.New("size", 0, 2048)}

		stream, cancel, err := c.SubscribeOps(time.Now().UnixNano())
		if err != nil {
			cancel()
			return err
		}
		go server.opConsumer(stream, cancel)

		if err := server.indexCatchUp(); err != nil {
			return err
		}
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
	log.WithFields(log.Fields{
		"port": conf.Port,
	}).Info("listening")

	return s.Serve(lis)
}

func (s *Server) findResultConsumer(pred []*pbQPU.Predicate, stream pb.QPU_FindServer, msg chan *pbQPU.Object, done chan bool, errFind chan error, errs chan error, process func(*pbQPU.Object, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errFind
			errs <- err
		}
		streamMsg := <-msg
		if err := process(streamMsg, pred, stream); err != nil {
			errs <- err
		}
	}
}

//TODO: Find a way to handle an error here
func (s *Server) opConsumer(stream pbDsQPU.DataStoreQPU_SubscribeOpsClient, cancel context.CancelFunc) {
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			log.Warnf("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatalf("opConsumer failed")
			return
		}
		if err := s.index.Update(streamMsg.Operation); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatalf("opConsumer failed")
			return
		}
	}
}

func (s *Server) snapshotConsumer(pred []*pbQPU.Predicate, streamFrom pbDsQPU.DataStoreQPU_GetSnapshotClient, streamTo pb.QPU_FindServer, errs chan error, process func(*pbQPU.Object, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
	for {
		streamMsg, err := streamFrom.Recv()
		if err == io.EOF {
			errs <- nil
			return
		} else if err != nil {
			errs <- err
			return
		}
		if err = process(streamMsg.Object, pred, streamTo); err != nil {
			errs <- err
			return
		}
	}
}

func (s *Server) catchUpConsumer(streamFrom pbDsQPU.DataStoreQPU_GetSnapshotClient, errs chan error) {
	for {
		streamMsg, err := streamFrom.Recv()
		if err == io.EOF {
			errs <- nil
			return
		} else if err != nil {
			errs <- err
			return
		}
		op := &pbQPU.Operation{
			Key:    streamMsg.Object.Key,
			Op:     "catchUp",
			Object: streamMsg.Object,
		}
		if err := s.index.Update(op); err != nil {
			errs <- err
			return
		}
	}
}

func (s *Server) indexCatchUp() error {
	errs := make(chan error)

	streamFrom, cancel, err := s.dsClient.GetSnapshot(time.Now().UnixNano())
	defer cancel()
	if err != nil {
		return err
	}
	go s.catchUpConsumer(streamFrom, errs)
	err = <-errs
	return err
}

//Find ...
func (s *Server) Find(in *pb.FindRequest, streamTo pb.QPU_FindServer) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	errs := make(chan error)

	if s.config.QpuType == "scan" {
		streamFrom, cancel, err := s.dsClient.GetSnapshot(in.Timestamp)
		defer cancel()
		if err != nil {
			return err
		}
		go s.snapshotConsumer(in.Predicate, streamFrom, streamTo, errs, filter.Forward)
		err = <-errs
		return err
	} else if s.config.QpuType == "cache" {
		cachedResult, hit := s.cache.Get(in.Predicate)
		if hit {
			for _, item := range cachedResult {
				if err := streamTo.Send(&pb.QueryResultStream{Object: &item}); err != nil {
					return err
				}
			}
			return nil
		}
		errs1 := make(chan error)
		pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: {in.Predicate[0].Lbound, in.Predicate[0].Ubound}}

		go s.findResultConsumer(in.Predicate, streamTo, msg, done, errs1, errs, s.cache.StoreAndRespond)
		go s.dConn[0].Client.Find(in.Timestamp, pred, msg, done, errs1)

		err := <-errs
		return err
	} else if s.config.QpuType == "index" {
		indexResult, found, err := s.index.Get(in.Predicate)
		if err != nil {
			return err
		}
		if found {
			for _, item := range indexResult {
				if err := streamTo.Send(&pb.QueryResultStream{Object: &item}); err != nil {
					return err
				}
			}
		}
		return nil
	} else if s.config.QpuType == "dispatch" {
		client, err := dispatch.ForwardQuery(s.dConn, *in.Predicate[0])
		if err != nil {
			return err
		}
		errs1 := make(chan error)
		pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: {in.Predicate[0].Lbound, in.Predicate[0].Ubound}}

		go s.findResultConsumer(in.Predicate, streamTo, msg, done, errs1, errs, dispatch.ForwardResponse)
		go client.Find(in.Timestamp, pred, msg, done, errs1)
		<-errs
		return nil
	}
	return errors.New("QPU Type not known")
}

func main() {
	err := NewServer()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatalf("QPU server failed")
	}
}
