package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	utils "github.com/dimitriosvasilas/proteus"
	attribute "github.com/dimitriosvasilas/proteus/attributes"
	"github.com/dimitriosvasilas/proteus/config"
	dSQPUcli "github.com/dimitriosvasilas/proteus/dataStoreQPU/client"
	pb "github.com/dimitriosvasilas/proteus/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	"github.com/dimitriosvasilas/proteus/qpu/cache"
	"github.com/dimitriosvasilas/proteus/qpu/filter"
	"github.com/dimitriosvasilas/proteus/qpu/index"
	partitionManager "github.com/dimitriosvasilas/proteus/qpu/partition_manager"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//QPU specifies the API of a QPU
type QPU interface {
	Find(in *pb.FindRequest, streamOut pb.QPU_FindServer, conns utils.DownwardConns) error
	Cleanup()
}

//Server implements a generic QPU server
type Server struct {
	qpu           QPU
	config        config.QPUConfig
	downwardConns utils.DownwardConns
}

func main() {
	var qType string
	flag.StringVar(&qType, "conf", "noArg", "configuration file to be used")
	flag.Parse()

	err := server(qType)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("QPU server failed")
	}
}

//---------------- API Functions -------------------

//Find implements the Find method of a generic QPU
//Calls specific implementations of the Find method
func (s *Server) Find(in *pb.FindRequest, streamTo pb.QPU_FindServer) error {
	return s.qpu.Find(in, streamTo, s.downwardConns)
}

//GetConfig constructs and returns a structure describing the configuration of a QPU
func (s *Server) GetConfig(ctx context.Context, in *pb.ConfigRequest) (*pb.ConfigResponse, error) {
	resp := pb.ConfigResponse{QPUType: s.config.QpuType}

	switch s.config.QpuType {
	case "filter":
		resp.SupportedQueries = append(resp.SupportedQueries, &pbQPU.Predicate{
			Attribute: "any",
			Lbound:    utils.ValStr("any"),
			Ubound:    utils.ValStr("any"),
		})
		for _, c := range s.config.Conns {
			resp.Dataset = append(resp.Dataset, &pbQPU.DataSet{
				Db:    c.DataSet.DB,
				Dc:    c.DataSet.DC,
				Shard: c.DataSet.Shard,
			})
		}
	case "index":
		attr, _, err := attribute.Attr(s.config.IndexConfig.Attribute, nil)
		if err != nil {
			return nil, err
		}
		lb, ub, err := attr.BoundStrToVal(s.config.IndexConfig.LBound, s.config.IndexConfig.UBound)
		if err != nil {
			return nil, err
		}
		resp.SupportedQueries = append(resp.SupportedQueries, &pbQPU.Predicate{
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
	default:
		for dbID, db := range s.downwardConns.DBs {
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
	return &resp, nil
}

//---------------- Internal Functions --------------

func server(confArg string) error {
	initDebug()

	var conf config.QPUConfig
	err := config.GetConfig(confArg, &conf)
	if err != nil {
		return err
	}

	var server Server
	switch conf.QpuType {
	case "filter":
		c, _, err := dSQPUcli.NewClient(conf.Conns[0].EndPoint)
		if err != nil {
			return err
		}
		var dConns utils.DownwardConns
		dConns.DsConn = []dSQPUcli.Client{c}
		qpu, err := filter.QPU()
		if err != nil {
			return err
		}
		server = Server{config: conf, downwardConns: dConns, qpu: qpu}
		dSConfig, err := server.downwardConns.DsConn[0].GetConfig()
		if err != nil {
			return err
		}
		server.config.Conns[0].DataSet.DB = dSConfig.Dataset.Db
		server.config.Conns[0].DataSet.DC = dSConfig.Dataset.Dc
		server.config.Conns[0].DataSet.Shard = dSConfig.Dataset.Shard
		server.config.IndexConfig.Attribute = "any"
		server.config.IndexConfig.IndexType = "any"
		server.config.IndexConfig.LBound = "any"
		server.config.IndexConfig.UBound = "any"
	case "cache":
		downwardsConn, err := utils.NewDConn(conf)
		if err != nil {
			return err
		}
		qpu, err := cache.QPU()
		if err != nil {
			return err
		}
		server = Server{config: conf, downwardConns: downwardsConn, qpu: qpu}
	case "index":
		var dsConns []dSQPUcli.Client
		for _, conn := range conf.Conns {
			c, _, err := dSQPUcli.NewClient(conn.EndPoint)
			if err != nil {
				return err
			}
			dsConns = append(dsConns, c)
		}
		var dConns utils.DownwardConns
		dConns.DsConn = dsConns

		server = Server{config: conf, downwardConns: dConns}
		for i, c := range server.downwardConns.DsConn {
			dSConfig, err := c.GetConfig()
			if err != nil {
				return err
			}
			server.config.Conns[i].DataSet.DB = dSConfig.Dataset.Db
			server.config.Conns[i].DataSet.DC = dSConfig.Dataset.Dc
			server.config.Conns[i].DataSet.Shard = dSConfig.Dataset.Shard
		}
		qpu, err := index.QPU(conf.IndexConfig, server.downwardConns)
		if err != nil {
			return err
		}
		server.qpu = qpu
	case "partition_manager":
		time.Sleep(5 * time.Second)
		downwardsConns, err := utils.NewDConn(conf)
		if err != nil {
			return err
		}
		qpu, err := partitionManager.QPU(downwardsConns)
		if err != nil {
			return err
		}
		server = Server{config: conf, downwardConns: downwardsConns, qpu: qpu}
	}

	setCleanup(server)

	confJSON, err := json.Marshal(server.config)
	if err != nil {
		return err
	}
	dispatchConfJSON, err := json.Marshal(server.downwardConns)
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

func (s *Server) cleanup() {
	log.Info("QPU server received SIGTERM")
	s.qpu.Cleanup()
	switch s.config.QpuType {
	case "index":
		for _, conn := range s.downwardConns.DsConn {
			conn.CloseConnection()
		}
	default:
	}
}

//---------------- Auxiliary Functions -------------

func initDebug() error {
	err := viper.BindEnv("DEBUG")
	if err != nil {
		return errors.New("BindEnv DEBUG failed")
	}
	debug := viper.GetBool("DEBUG")
	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	return nil
}

//setCleanup set a cleanup() fucntion that will be called
//in case the QPU server process receives a SIGTERM signal
func setCleanup(server Server) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		server.cleanup()
		os.Exit(0)
	}()
}
