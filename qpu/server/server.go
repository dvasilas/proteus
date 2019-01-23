package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"time"

	utils "github.com/dimitriosvasilas/modqp"
	dSQPUcli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	pbDsQPU "github.com/dimitriosvasilas/modqp/protos/datastore"
	pb "github.com/dimitriosvasilas/modqp/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/dimitriosvasilas/modqp/qpu/cache"
	"github.com/dimitriosvasilas/modqp/qpu/dispatch"
	"github.com/dimitriosvasilas/modqp/qpu/filter"
	"github.com/dimitriosvasilas/modqp/qpu/index"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//Server ...
type Server struct {
	config       utils.QPUConfig
	dispatchConn utils.DownwardConns
	dsClient     []dSQPUcli.Client
	cache        *cache.Cache
	index        *index.Index
}

func getConfig(qType string) (utils.QPUConfig, error) {
	var conf utils.QPUConfig
	viper.AutomaticEnv()
	err := viper.BindEnv("QPU_CONFIG_FILE")
	if err != nil {
		return conf, err
	}
	var confFile string
	if qType == "noType" {
		confF := viper.Get("QPU_CONFIG_FILE")
		if confF == nil {
			return conf, errors.New("QPU config file not specified")
		}
		confFile = confF.(string)
	} else {
		confFile = qType
	}
	viper.SetConfigName(confFile)
	_, f, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(f)
	viper.AddConfigPath(basepath + "/../../conf")
	viper.AddConfigPath(basepath + "/../../conf/local")
	viper.AddConfigPath(basepath + "/../../conf/dockerCompose")
	viper.SetConfigType("json")

	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}
	if err := viper.Unmarshal(&conf); err != nil {
		return conf, err
	}

	return conf, nil
}

//NewServer ...
func NewServer(qType string) error {
	conf, err := getConfig(qType)
	if err != nil {
		return err
	}
	log.SetLevel(log.DebugLevel)

	var server Server
	if conf.QpuType == "scan" {
		c, _, err := dSQPUcli.NewClient(conf.Conns[0].EndPoint)
		if err != nil {
			return err
		}
		server = Server{config: conf, dsClient: []dSQPUcli.Client{c}}
		dSConfig, err := server.dsClient[0].GetConfig()
		if err != nil {
			return err
		}
		server.config.Conns[0].DataSet.DB = dSConfig.Dataset.Db
		server.config.Conns[0].DataSet.DC = dSConfig.Dataset.Dc
		server.config.Conns[0].DataSet.Shard = dSConfig.Dataset.Shard
		server.config.IndexConfig.DataType = "any"
		server.config.IndexConfig.Attribute = "any"
		server.config.IndexConfig.LBound = "any"
		server.config.IndexConfig.UBound = "any"

	} else if conf.QpuType == "cache" {
		downwardsConn, err := utils.NewDConn(conf)
		if err != nil {
			return err
		}
		server = Server{config: conf, dispatchConn: downwardsConn, cache: cache.New(10)}
	} else if conf.QpuType == "index" {
		var dsConns []dSQPUcli.Client
		for _, conn := range conf.Conns {
			c, _, err := dSQPUcli.NewClient(conn.EndPoint)
			if err != nil {
				return err
			}
			dsConns = append(dsConns, c)
		}
		index, err := index.New(conf.IndexConfig.DataType, conf.IndexConfig.Attribute, conf.IndexConfig.LBound, conf.IndexConfig.UBound)
		if err != nil {
			return err
		}
		server = Server{config: conf, dsClient: dsConns, index: index}
		for i, c := range server.dsClient {
			dSConfig, err := c.GetConfig()
			if err != nil {
				return err
			}
			server.config.Conns[i].DataSet.DB = dSConfig.Dataset.Db
			server.config.Conns[i].DataSet.DC = dSConfig.Dataset.Dc
			server.config.Conns[i].DataSet.Shard = dSConfig.Dataset.Shard

			if conf.IndexConfig.ConsLevel == "async" {
				stream, cancel, err := c.SubscribeOpsAsync(time.Now().UnixNano())
				if err != nil {
					cancel()
					return err
				}
				go server.opConsumerAsync(stream, cancel)
			} else if conf.IndexConfig.ConsLevel == "sync" {
				stream, cancel, err := c.SubscribeOpsSync(time.Now().UnixNano())
				if err != nil {
					cancel()
					return err
				}
				go server.opConsumerSync(stream, cancel)
			} else {
				return errors.New("ConsLevel in IndexConfig can be sync/async")
			}
		}

		if err := server.indexCatchUp(); err != nil {
			return err
		}
	} else if conf.QpuType == "dispatch" {
		time.Sleep(5 * time.Second)
		downwardsConns, err := utils.NewDConn(conf)
		if err != nil {
			return err
		}
		server = Server{config: conf, dispatchConn: downwardsConns}
	}

	confJSON, err := json.Marshal(server.config)
	if err != nil {
		return err
	}
	dispatchConfJSON, err := json.Marshal(server.dispatchConn)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"configuration":  string(confJSON),
		"dispatchConfig": string(dispatchConfJSON),
	}).Info("QPU initialization")

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

func (s *Server) findResultConsumer(pred []*pbQPU.Predicate, stream pb.QPU_FindServer, msg chan *pb.QueryResultStream, done chan bool, errFind chan error, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errFind
			errs <- err
		}
		streamMsg := <-msg
		if err := process(streamMsg.GetObject(), streamMsg.GetDataset(), pred, stream); err != nil {
			errs <- err
		}
	}
}

//TODO: Find a way to handle an error here
func (s *Server) opConsumerAsync(stream pbDsQPU.DataStore_SubscribeOpsAsyncClient, cancel context.CancelFunc) {
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("opConsumer: stream.Recv() error")
			return
		} else {
			if streamMsg.Operation.OpId == "no_op" {
				continue
			}
			log.WithFields(log.Fields{
				"operation": streamMsg.GetOperation(),
			}).Debug("index QPU received operation")

			if err := index.Update(s.index, streamMsg.GetOperation()); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"op":    streamMsg.Operation,
				}).Fatal("opConsumer: index Update failed")
				return
			}
		}
	}
}

func (s *Server) opConsumerSync(stream pbDsQPU.DataStore_SubscribeOpsSyncClient, cancel context.CancelFunc) {
	for {
		streamMsg, err := stream.Recv()
		if err == io.EOF {
			log.Fatal("opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("opConsumer: stream.Recv() error")
			return
		} else {
			if streamMsg.Operation.OpId == "no_op" {
				continue
			}
			log.WithFields(log.Fields{
				"op": streamMsg.GetOperation(),
			}).Debug("QPUServer:opConsumerSync:  received op")
			if err := index.Update(s.index, streamMsg.Operation); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"op":    streamMsg.Operation,
				}).Fatal("opConsumer: index Update failed")
				return
			}
			log.Debug("QPUServer:index updated, sending ACK")
			if err := stream.Send(&pbDsQPU.OpAckStream{Msg: "ack", OpId: streamMsg.GetOperation().GetOpId()}); err != nil {
				log.Fatal("opConsumerSync stream.Send failed")
				return
			}
		}
	}
}

func (s *Server) snapshotConsumer(pred []*pbQPU.Predicate, streamFrom pbDsQPU.DataStore_GetSnapshotClient, streamTo pb.QPU_FindServer, errs chan error, process func(*pbQPU.Object, *pbQPU.DataSet, []*pbQPU.Predicate, pb.QPU_FindServer) error) {
	for {
		streamMsg, err := streamFrom.Recv()
		if err == io.EOF {
			errs <- nil
			return
		} else if err != nil {
			errs <- err
			return
		}
		if err = process(streamMsg.GetObject(), streamMsg.GetDataset(), pred, streamTo); err != nil {
			errs <- err
			return
		}
	}
}

func (s *Server) catchUpConsumer(streamFrom pbDsQPU.DataStore_GetSnapshotClient, errs chan error) {
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
			OpId:      "catchUp",
			OpPayload: &pbQPU.OperationPayload{Payload: &pbQPU.OperationPayload_State{State: streamMsg.GetObject()}},
			DataSet:   streamMsg.GetDataset(),
		}
		if err := index.Update(s.index, op); err != nil {
			errs <- err
			return
		}
	}
}

func (s *Server) indexCatchUp() error {
	errs := make([]chan error, len(s.dsClient))
	for i := range s.dsClient {
		errs[i] = make(chan error)
	}

	for i, c := range s.dsClient {
		streamFrom, cancel, err := c.GetSnapshot(time.Now().UnixNano())
		defer cancel()
		if err != nil {
			return err
		}
		go s.catchUpConsumer(streamFrom, errs[i])
	}
	for i := range s.dsClient {
		err := <-errs[i]
		if err != nil {
			return err
		}
	}
	return nil
}

//Find ...
func (s *Server) Find(in *pb.FindRequest, streamTo pb.QPU_FindServer) error {
	msg := make(chan *pb.QueryResultStream)
	done := make(chan bool)
	errs := make(chan error)

	if s.config.QpuType == "scan" {
		streamFrom, cancel, err := s.dsClient[0].GetSnapshot(in.Timestamp)
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
			log.WithFields(log.Fields{
				"cache entry": cachedResult,
			}).Info("cache hit, responding")
			for _, item := range cachedResult {
				if err := streamTo.Send(&pb.QueryResultStream{Object: &item.Object, Dataset: &item.Dataset}); err != nil {
					return err
				}
			}
			return nil
		}
		errs1 := make(chan error)
		pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: {in.Predicate[0].Lbound, in.Predicate[0].Ubound}}

		clients, err := dispatch.ForwardQuery(s.dispatchConn, *in.Predicate[0])
		if err != nil {
			return err
		}
		go s.findResultConsumer(in.Predicate, streamTo, msg, done, errs1, errs, s.cache.StoreAndRespond)
		go clients[0].Find(in.Timestamp, pred, msg, done, errs1)

		err = <-errs
		return err
	} else if s.config.QpuType == "index" {
		log.WithFields(log.Fields{
			"query": in.Predicate,
		}).Info("Index lookup")
		indexResult, found, err := s.index.Get(in.Predicate)
		if err != nil {
			return err
		}
		if found {
			for _, item := range indexResult {
				if err := streamTo.Send(&pb.QueryResultStream{Object: &item.Object, Dataset: &item.Dataset}); err != nil {
					return err
				}
			}
		}
		return nil
	} else if s.config.QpuType == "dispatch" {
		log.WithFields(log.Fields{
			"query": *in.Predicate[0],
		}).Info("Received query")
		clients, err := dispatch.ForwardQuery(s.dispatchConn, *in.Predicate[0])
		if err != nil {
			return err
		}

		pred := map[string][2]*pbQPU.Value{in.Predicate[0].Attribute: {in.Predicate[0].Lbound, in.Predicate[0].Ubound}}

		done := make([]chan bool, len(clients))
		errs := make([]chan error, len(clients))
		errs1 := make([]chan error, len(clients))
		for i := range clients {
			done[i] = make(chan bool)
			errs[i] = make(chan error)
			errs1[i] = make(chan error)
		}
		for i, c := range clients {
			go s.findResultConsumer(in.Predicate, streamTo, msg, done[i], errs1[i], errs[i], dispatch.ForwardResponse)
			go c.Find(in.Timestamp, pred, msg, done[i], errs1[i])
			time.Sleep(time.Millisecond * 100)
		}
		for _, e := range errs {
			err = <-e
			if err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("QPU Type not known")
}

//GetConfig ...
func (s *Server) GetConfig(ctx context.Context, in *pb.ConfigRequest) (*pb.ConfigResponse, error) {
	resp := new(pb.ConfigResponse)
	resp.QPUType = s.config.QpuType
	if s.config.QpuType == "scan" || s.config.QpuType == "index" {
		lb, ub, err := utils.AttrBoundStrToVal(s.config.IndexConfig.DataType, s.config.IndexConfig.LBound, s.config.IndexConfig.UBound)
		if err != nil {
			return nil, err
		}
		resp.SupportedQueries = append(resp.SupportedQueries, &pbQPU.Predicate{
			Datatype:  s.config.IndexConfig.DataType,
			Attribute: s.config.IndexConfig.Attribute,
			Lbound:    lb,
			Ubound:    ub,
		})
		for _, c := range s.config.Conns {
			resp.Dataset = append(resp.Dataset, &pbQPU.DataSet{
				Db:    c.DataSet.DB,
				Dc:    c.DataSet.DC,
				Shard: c.DataSet.Shard,
			})
		}
	} else {
		for dbID, db := range s.dispatchConn.DBs {
			for rID, r := range db.DCs {
				for shID, sh := range r.Shards {
					resp.Dataset = append(resp.Dataset, &pbQPU.DataSet{
						Db:    dbID,
						Dc:    rID,
						Shard: shID,
					})
					for _, q := range sh.QPUs {
						resp.SupportedQueries = append(resp.SupportedQueries, &pbQPU.Predicate{
							Datatype:  q.DataType,
							Attribute: q.Attribute,
							Lbound:    q.Lbound,
							Ubound:    q.Ubound,
						})
					}
				}
			}
		}
	}
	return resp, nil
}

func main() {
	var qType string
	flag.StringVar(&qType, "qpu", "noType", "the QPU type")
	flag.Parse()
	err := NewServer(qType)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("QPU server failed")
	}
}
