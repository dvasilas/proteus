package server

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/dvasilas/proteus/internal/qpu/cache"
	"github.com/dvasilas/proteus/internal/qpu/datastore_driver"
	"github.com/dvasilas/proteus/internal/qpu/federation_dispatcher"
	"github.com/dvasilas/proteus/internal/qpu/filter"
	"github.com/dvasilas/proteus/internal/qpu/index"
	"github.com/dvasilas/proteus/internal/qpu/intersection"
	"github.com/dvasilas/proteus/internal/qpu/lambda"
	"github.com/dvasilas/proteus/internal/qpu/load_balancer"
	"github.com/dvasilas/proteus/internal/qpu/network"
	"github.com/dvasilas/proteus/internal/sqlparser"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//QPUServer implements a generic QPU server
// api:
// config:
type QPUServer struct {
	Server QPUAPI
	Config *config.Config
}

//QPUAPI specifies the API of a QPU
type QPUAPI interface {
	Query(qpu_api.QPU_QueryServer, *qpu_api.QueryInternalQuery, map[string]string, bool) error
	GetConfig() (*qpu_api.ConfigResponse, error)
	Cleanup()
	GetDataTransfer() float32
}

//---------------- API Functions -------------------

//Query implements the Query method of a generic QPU
//Calls specific implementations of the Query method
func (s *QPUServer) Query(stream qpu_api.QPU_QueryServer) error {
	requestRec, err := stream.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}

	switch requestRec.GetPayload().(type) {
	case *qpu_api.RequestStream_Request:
		query := requestRec.GetRequest().GetQuery()
		metadata := requestRec.GetRequest().GetMetadata()
		sync := requestRec.GetRequest().GetSync()
		switch query.GetVal().(type) {
		case *qpu_api.Query_QueryI:
			return s.Server.Query(stream, query.GetQueryI(), metadata, sync)
		case *qpu_api.Query_QuerySql:
			parsedquery, err := sqlparser.Parse(query.GetQuerySql().GetQueryStr())
			if err != nil {
				return err
			}
			return s.Server.Query(stream, parsedquery.GetQueryI(), metadata, sync)
		default:
			return errors.New("should not have reached here")
		}
	case *qpu_api.RequestStream_Ping:
		return errors.New("not expexted RequestStream_Ping")
	case *qpu_api.RequestStream_Ack:
		return errors.New("not expexted RequestStream_Ack")
	default:
		return errors.New("should not have reached here")
	}
}

//GetConfig constructs and returns a structure describing the configuration of a QPU
func (s *QPUServer) GetConfig(ctx context.Context, in *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error) {
	return s.Server.GetConfig()
}

// GetDataTransfer ...
func (s *QPUServer) GetDataTransfer(ctx context.Context, in *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error) {
	datatransferredCount := s.Server.GetDataTransfer()
	return &qpu_api.DataTransferResponse{
		KBytesTranferred: datatransferredCount,
	}, nil
}

//---------------- Internal Functions --------------

// Server ...
func Server(confArg config.ConfJSON) error {
	conf, err := config.GetConfig(confArg)
	if err != nil {
		return err
	}

	var server QPUServer
	var api QPUAPI
	switch conf.QpuType {
	case qpu_api.ConfigResponse_DBDRIVER:
		api, err = datastoredriver.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_FILTER:
		api, err = filter.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_CACHE:
		api, err = cache.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_INDEX:
		api, err = index.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_FEDERATION_DISPATCHER:
		api, err = federation.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_NETWORK:
		api, err = network.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_LOAD_BALANCER:
		api, err = loadbalancer.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_LAMBDA:
		api, err = lambda.QPU(conf)
		if err != nil {
			return err
		}
	case qpu_api.ConfigResponse_INTERSECTION:
		api, err = intersection.QPU(conf)
		if err != nil {
			return err
		}
	}
	server = QPUServer{Config: conf, Server: api}

	setCleanup(server)

	s := grpc.NewServer()
	qpu_api.RegisterQPUServer(s, &server)
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
	s.Server.Cleanup()
}

//---------------- Auxiliary Functions -------------

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
