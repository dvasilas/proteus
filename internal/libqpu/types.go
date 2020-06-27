package libqpu

import (
	"context"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"
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

// APIProcessor ...
type APIProcessor interface {
	Query(QueryRequest, RequestStream) error
	QueryUnary(QueryRequest, opentracing.Span) ([]*LogOperation, error)
	GetConfig(context.Context, *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error)
	GetDataTransfer(context.Context, *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error)
}

// QPUClass ...
type QPUClass interface {
	ProcessQuerySnapshot(InternalQuery, map[string]string, bool, opentracing.Span) (<-chan LogOperation, <-chan error)
	ProcessQuerySubscribe(InternalQuery, map[string]string, bool) (int, <-chan LogOperation, <-chan error)
	RemovePersistentQuery(string, int)
}

// AdjacentQPU ...
type AdjacentQPU struct {
	APIClient APIClient
}

// APIClient ...
type APIClient interface {
	Query(QueryRequest) (ResponseStream, error)
	QueryUnary(QueryRequest) (*qpu_api.QueryResponse, error)
	QuerySQL(string, map[string]string, bool) (ResponseStream, error)
	CloseConnection() error
}

// QPUState ...
type QPUState interface {
	Init(string, string, string) error
	Insert(string, map[string]interface{}, map[string]*timestamp.Timestamp) error
	Update(string, map[string]interface{}, map[string]interface{}, map[string]*timestamp.Timestamp) error
	Get(string, string, map[string]*qpu.Value) (interface{}, error)
	Scan(string, []string, int64, opentracing.Span) (<-chan map[string]string, error)
	Cleanup()
}

// QPUConfig specifies the configuration structure of a QPU
type QPUConfig struct {
	QpuType      qpu_api.ConfigResponse_QPUType
	Port         string
	Tracing      bool
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
			IsNotNull  []string
		}
	}
	JoinConfig struct {
		Source []struct {
			Table      string
			Projection []string
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
