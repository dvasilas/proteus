package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dvasilas/proteus/src/config"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	"github.com/dvasilas/proteus/src/qpu/cache"
	"github.com/dvasilas/proteus/src/qpu/datastore_driver"
	"github.com/dvasilas/proteus/src/qpu/federation_dispatcher"
	"github.com/dvasilas/proteus/src/qpu/filter"
	"github.com/dvasilas/proteus/src/qpu/index"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//QPUServer implements a generic QPU server
// api:
// config:
type QPUServer struct {
	api    QPUAPI
	config *config.Config
}

//QPUAPI specifies the API of a QPU
type QPUAPI interface {
	Query(stream pbQPU.QPU_QueryServer) error
	GetConfig() (*pbQPU.ConfigResponse, error)
	Cleanup()
}

//---------------- API Functions -------------------

//Query implements the Query method of a generic QPU
//Calls specific implementations of the Query method
func (s *QPUServer) Query(stream pbQPU.QPU_QueryServer) error {
	return s.api.Query(stream)
}

//GetConfig constructs and returns a structure describing the configuration of a QPU
func (s *QPUServer) GetConfig(ctx context.Context, in *pbQPU.ConfigRequest) (*pbQPU.ConfigResponse, error) {
	return s.api.GetConfig()
}

//---------------- Internal Functions --------------

func server(confArg string) error {
	initDebug()

	conf, err := config.GetConfig(confArg)
	if err != nil {
		return err
	}

	var server QPUServer
	var api QPUAPI
	switch conf.QpuType {
	case pbQPU.ConfigResponse_DBDRIVER:
		api, err = datastoredriver.QPU(conf)
		if err != nil {
			return err
		}
	case pbQPU.ConfigResponse_FILTER:
		api, err = filter.QPU(conf)
		if err != nil {
			return err
		}
	case pbQPU.ConfigResponse_CACHE:
		api, err = cache.QPU(conf)
		if err != nil {
			return err
		}
	case pbQPU.ConfigResponse_INDEX:
		api, err = index.QPU(conf)
		if err != nil {
			return err
		}
	case pbQPU.ConfigResponse_FEDERATION_DISPATCHER:
		api, err = federation.QPU(conf)
		if err != nil {
			return err
		}
	}
	server = QPUServer{config: conf, api: api}

	setCleanup(server)

	s := grpc.NewServer()
	pbQPU.RegisterQPUServer(s, &server)
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

func (s *QPUServer) cleanup() {
	log.Info("QPU server received SIGTERM")
	s.api.Cleanup()
}

//---------------- Auxiliary Functions -------------

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

//setCleanup set a cleanup() function that will be called
//in case the QPU server process receives a SIGTERM signal
func setCleanup(server QPUServer) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		server.cleanup()
		os.Exit(0)
	}()
}
