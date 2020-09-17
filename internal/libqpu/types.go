package libqpu

import (
	"context"
	"time"

	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
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
	InputSchema  Schema
	AdjacentQPUs []*AdjacentQPU
	State        QPUState
}

// APIProcessor ...
type APIProcessor interface {
	Query(QueryRequest, RequestStream) error
	QueryUnary(QueryRequest, opentracing.Span) (*pb.QueryResp, error)
	GetConfig(context.Context, *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error)
	// GetDataTransfer(context.Context, *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error)
}

// QPUClass ...
type QPUClass interface {
	ClientQuery(ASTQuery, opentracing.Span) (*pb.QueryResp, error)
	ProcessQuerySnapshot(ASTQuery, map[string]string, bool, opentracing.Span) (<-chan LogOperation, <-chan error)
	ProcessQuerySubscribe(ASTQuery, map[string]string, bool) (int, <-chan LogOperation, <-chan error)
	RemovePersistentQuery(string, int)
	GetConfig() *qpu_api.ConfigResponse
}

// AdjacentQPU ...
type AdjacentQPU struct {
	APIClient    APIClient
	OutputSchema []string
}

// APIClient ...
type APIClient interface {
	Query(QueryRequest) (ResponseStream, error)
	QueryUnary(QueryRequest) (*qpu_api.QueryResponse, error)
	QuerySQL(string, map[string]string, bool) (ResponseStream, error)
	CloseConnection() error
	GetConfig() (*qpu_api.ConfigResponse, error)
}

// QPUState ...
type QPUState interface {
	Init(string, string, string) error
	Insert(string, map[string]interface{}, map[string]*timestamp.Timestamp, interface{}) error
	Update(string, map[string]interface{}, map[string]interface{}, map[string]*timestamp.Timestamp, interface{}) error
	Get(string, []string, map[string]interface{}, string, int64, bool, opentracing.Span) (<-chan map[string]interface{}, error)
	SeparateTS(string) error
	LogQuery(string, time.Time, []*pb.QueryRespRecord) error
	Cleanup()
}

// OperatorType ...
type OperatorType int

const (
	// DBDriver ...
	DBDriver OperatorType = iota
	// Aggregation ...
	Aggregation OperatorType = iota
	// Join ...
	Join OperatorType = iota
	// Router ...
	Router OperatorType = iota
)

// StateType ...
type StateType int

const (
	// Stateless ...
	Stateless StateType = iota
	// MaterializedView ...
	MaterializedView StateType = iota
)

// QPUConfig specifies the configuration structure of a QPU
type QPUConfig struct {
	Operator     OperatorType
	State        StateType
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
	AggregationConfig struct {
		AggregationFunc      AggregationType
		GroupBy              string
		AggregationAttribute string
	}
	JoinConfig struct {
		OutputTableAlias     string
		JoinedAttributeAlias string
		JoinAttribute        map[string]string
	}
	Evaluation struct {
		Tracing       bool
		LogTimestamps bool
	}
	MaxWorkers  int
	MaxJobQueue int
}

// QPUConnection ...
type QPUConnection struct {
	Address string
	Local   bool
}

// DatastoreType ...
type DatastoreType int

const (
	// MYSQL is the enum value for a MySQL storage tier
	MYSQL DatastoreType = iota
	// S3 is the enum value for a S3 storage tier
	S3 DatastoreType = iota
)

// AggregationType ...
type AggregationType int

const (
	// Sum ...
	Sum AggregationType = iota
)
