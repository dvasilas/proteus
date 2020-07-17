package qpu

import (
	"github.com/dvasilas/proteus/internal/apiprocessor"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/libqpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	mysqlbackend "github.com/dvasilas/proteus/internal/qpustate/mysql_backend"
	rpcserver "github.com/dvasilas/proteus/internal/rpc_server"
	log "github.com/sirupsen/logrus"
)

// This package is responsible for providing the QPU service functionalities.
// It provides an API for creating/initializing a new service, and for starting
//  and shuting down a given service.

// Service represents a QPU service
type Service struct {
	qpu         *libqpu.QPU
	rpcServer   *rpcserver.Server
	catchUpDone chan int
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

	catchUpDoneCh := make(chan int)
	apiProcessor, err := apiprocessor.NewProcessor(qpu, catchUpDoneCh)
	if err != nil {
		return nil, err
	}

	// qpu.State temporarily added as the last argument for testing
	rpcServer, err := rpcserver.NewServer(qpu.Config.Port, qpu.Config.Evaluation.Tracing, apiProcessor, qpu.State, qpu.Config)
	if err != nil {
		return nil, err
	}

	return Service{
		qpu:         qpu,
		rpcServer:   rpcServer,
		catchUpDone: catchUpDoneCh,
	}, nil
}

// Start starts the QPU's rpcServer.
// After this point the QPU can receive requests.
func (s Service) Start() error {
	<-s.catchUpDone
	close(s.catchUpDone)

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
