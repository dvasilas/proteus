package deployment

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/dvasilas/proteus/src/config"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	"github.com/dvasilas/proteus/src/qpu/cache"
	"github.com/dvasilas/proteus/src/qpu/datastore_driver"
	"github.com/dvasilas/proteus/src/qpu/filter"
	"github.com/dvasilas/proteus/src/qpu/index"
	"github.com/dvasilas/proteus/src/qpu/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// DeployedQPU ...
type DeployedQPU struct {
	Qpu    server.QPUAPI
	Server *grpc.Server
}

// Deploy well, deploys a QPU process locally with the given configuration
// then waits for the QPU grpc server to start listening to the given port
func Deploy(configuration config.ConfJSON) (DeployedQPU, error) {
	conf, err := config.GetConfig(configuration)
	if err != nil {
		return DeployedQPU{}, err
	}
	qpu, err := createQPU(conf)
	if err != nil {
		return DeployedQPU{}, err
	}
	_, f, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(f)
	return qpu, execCmd(exec.Command(basepath+"/wait-for-it.sh", "--host=127.0.0.1", "--port="+conf.Port, "--timeout=0"), true)
}

// TearDown stops the grpc servers and calls cleanup() for a given list of
// deployed QPUs
func TearDown(qpus []DeployedQPU) {
	for _, q := range qpus {
		q.Qpu.Cleanup()
		q.Server.Stop()
	}
}

func createQPU(conf *config.Config) (DeployedQPU, error) {
	var qpu server.QPUAPI
	var err error
	switch conf.QpuType {
	case pbQPU.ConfigResponse_DBDRIVER:
		qpu, err = datastoredriver.QPU(conf)
		if err != nil {
			return DeployedQPU{}, err
		}
	case pbQPU.ConfigResponse_FILTER:
		qpu, err = filter.QPU(conf)
		if err != nil {
			return DeployedQPU{}, err
		}
	case pbQPU.ConfigResponse_INDEX:
		qpu, err = index.QPU(conf)
		if err != nil {
			return DeployedQPU{}, err
		}
	case pbQPU.ConfigResponse_CACHE:
		qpu, err = cache.QPU(conf)
		if err != nil {
			return DeployedQPU{}, err
		}
	default:
		return DeployedQPU{}, errors.New("Not implemented")
	}
	server, err := startServer(conf, qpu)
	return DeployedQPU{Qpu: qpu, Server: server}, err
}

func startServer(conf *config.Config, qpu server.QPUAPI) (*grpc.Server, error) {
	server := server.QPUServer{Config: conf, Server: qpu}
	s := grpc.NewServer()
	pbQPU.RegisterQPUServer(s, &server)
	reflection.Register(s)
	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		return nil, err
	}
	go s.Serve(lis)
	return s, nil
}

func execCmd(cmd *exec.Cmd, wait bool) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if wait {
		return cmd.Run()
	}
	return cmd.Start()
}
