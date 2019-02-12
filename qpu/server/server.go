package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net"
	"path/filepath"
	"runtime"
	"time"

	utils "github.com/dimitriosvasilas/proteus"
	attribute "github.com/dimitriosvasilas/proteus/attributes"
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
}

//Server implements a generic QPU server
type Server struct {
	qpu           QPU
	config        utils.QPUConfig
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
		attr, err := attribute.Attr(s.config.IndexConfig.Attribute, nil)
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
	conf, err := config(confArg)
	if err != nil {
		return err
	}
	initDebug()

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
		qpu, err := index.QPU(conf, server.downwardConns)
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
		qpu, err := partitionManager.QPU()
		if err != nil {
			return err
		}
		server = Server{config: conf, downwardConns: downwardsConns, qpu: qpu}
	}
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

//---------------- Auxiliary Functions -------------

func config(confArg string) (utils.QPUConfig, error) {
	var conf utils.QPUConfig
	viper.AutomaticEnv()
	err := viper.BindEnv("QPU_CONFIG_FILE")
	if err != nil {
		return conf, err
	}
	var confFile string
	if confArg == "noArg" {
		confF := viper.Get("QPU_CONFIG_FILE")
		if confF == nil {
			return conf, errors.New("QPU config file not specified")
		}
		confFile = confF.(string)
	} else {
		confFile = confArg
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
