package qpu

import (
	"github.com/dvasilas/proteus/internal/apiprocessor"
	"github.com/dvasilas/proteus/internal/config"
	grpcserver "github.com/dvasilas/proteus/internal/grpc_server"
	"github.com/dvasilas/proteus/internal/libqpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	mysqlbackend "github.com/dvasilas/proteus/internal/qpustate/mysql_backend"
	log "github.com/sirupsen/logrus"
)

// This package is responsible for providing the QPU service functionalities.
// It provides an API for creating/initializing a new service, and for starting
//  and shuting down a given service.

// Service represents a QPU service
type Service struct {
	qpu       *libqpu.QPU
	rpcServer *grpcserver.Server
}

// NewQPUService is responsible for initializing all QPU components and
// starting the QPU service.
// This includes:
// - Reading the configuration file
// - Requesting the capabilities of adjacent QPUs in the graph
// - Initializing the state backend store (if any)
// - Initializing the class - specific query processing and
//   response record handling logic (apiserver)
// - Starting the API server for receiving requests
func NewQPUService(configFile string) (libqpu.QPUService, error) {
	qpu := &libqpu.QPU{}

	if err := config.GetQPUConfig(configFile, qpu); err != nil {
		return nil, err
	}

	if err := qpugraph.ConnectToGraph(qpu); err != nil {
		return nil, err
	}

	state, err := mysqlbackend.NewStateBackend(qpu.Config)
	if err != nil {
		return nil, err
	}
	qpu.State = state

	apiProcessor, err := apiprocessor.NewProcessor(qpu)
	if err != nil {
		return nil, err
	}

	rpcServer, err := grpcserver.NewServer(qpu.Config.Port, apiProcessor)
	if err != nil {
		return nil, err
	}

	return Service{
		qpu:       qpu,
		rpcServer: rpcServer,
	}, nil
}

// Start starts the QPU's rpcServer.
// After this point the QPU can receive requests.
func (s Service) Start() error {
	return s.rpcServer.Serve()
}

// Shutdown prepares the QPU service for graceful shutdown
func (s Service) Shutdown() {
	log.Info("Stopping the QPU service ..")
	if s.qpu.State != nil {
		s.qpu.State.Cleanup()
	}
	log.Info("Stopped")
}
