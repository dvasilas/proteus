package libqpu

import (
	"context"

	"github.com/dvasilas/proteus/internal/proto/qpu_api"
)

// QPUService ...
type QPUService interface {
	Start() error
	Shutdown()
}

// QPU ...
type QPU struct {
	Class        QPUClass
	Config       *QPUConfig
	Schema       Schema
	AdjacentQPUs []*AdjacentQPU
	State        QPUState
}

// QPUClass ...
type QPUClass interface {
	ProcessQuerySnapshot(InternalQuery, RequestStream, map[string]string, bool) (<-chan LogOperation, <-chan error)
	ProcessQuerySubscribe(InternalQuery, RequestStream, map[string]string, bool) (<-chan LogOperation, <-chan error)
}

// AdjacentQPU ...
type AdjacentQPU struct {
	APIClient APIClient
}

// APIProcessor ...
type APIProcessor interface {
	Query(QueryRequest, RequestStream) error
	GetConfig(context.Context, *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error)
	GetDataTransfer(context.Context, *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error)
}

// QPUState ...
type QPUState interface {
	Init(string, string, string) error
	Insert(string, string, string, ...interface{}) error
	Update(string, string, string, ...interface{}) error
	Get(string, string, string, ...interface{}) (interface{}, error)
	Scan(string, []string) (<-chan map[string]string, error)
	Cleanup()
}

// APIClient ...
type APIClient interface {
	Query(QueryRequest) (ResponseStream, error)
	QuerySQL(string, map[string]string, bool) (ResponseStream, error)
	CloseConnection() error
}

// QPUConfig specifies the configuration structure of a QPU
type QPUConfig struct {
	QpuType      qpu_api.ConfigResponse_QPUType
	Port         string
	Connections  []QPUConnection
	StateBackend struct {
		Endpoint    string
		Credentials struct {
			AccessKeyID     string
			SecretAccessKey string
		}
	}
	DatastoreConfig struct {
		Type              DatastoreType
		Endpoint          string
		LogStreamEndpoint string
		DBName            string
		Credentials       struct {
			AccessKeyID     string
			SecretAccessKey string
		}
	}
	SumConfig struct {
		SourceTable       string
		RecordIDAttribute []string
		AttributeToSum    string
		Query             struct {
			Projection []string
			IsNull     []string
		}
	}
}

// QPUConnection ...
type QPUConnection struct {
	Address string
	Local   bool
}

// DatastoreType ...
type DatastoreType int

const (
	// MYSQL is the enum value for an MySQL backend data store
	MYSQL DatastoreType = iota
)
