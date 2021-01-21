package libqpu

import (
	"context"
	"database/sql"
	"time"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
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
	QueryUnary(QueryRequest, opentracing.Span) (*qpuextapi.QueryResp, error)
	GetConfig(context.Context, *qpuapi.ConfigRequest) (*qpuapi.ConfigResponse, error)
	GetMetrics(context.Context, *qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error)
	// GetDataTransfer(context.Context, *qpuapi.GetDataRequest) (*qpuapi.DataTransferResponse, error)
}

// QPUClass ...
type QPUClass interface {
	ClientQuery(ASTQuery, string, opentracing.Span) (*qpuextapi.QueryResp, error)
	ProcessQuerySnapshot(ASTQuery, map[string]string, bool, opentracing.Span) (<-chan LogOperation, <-chan error)
	ProcessQuerySubscribe(ASTQuery, map[string]string, bool) (int, <-chan LogOperation, <-chan error)
	RemovePersistentQuery(string, int)
	GetConfig() *qpuapi.ConfigResponse
	GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error)
}

// AdjacentQPU ...
type AdjacentQPU struct {
	APIClient           APIClient
	OutputSchema        []string
	MeasureDataTransfer bool
}

// APIClient ...
type APIClient interface {
	Query(QueryRequest) (ResponseStream, error)
	QueryUnary(string) (*qpuextapi.QueryResp, error)
	QuerySQL(string, map[string]string, bool) (ResponseStream, error)
	CloseConnection() error
	GetConfig() (*qpuapi.ConfigResponse, error)
}

// QPUState ...
type QPUState interface {
	Init(string, string, string) error
	Insert(string, map[string]interface{}, map[string]*timestamp.Timestamp) error
	Update(string, map[string]interface{}, map[string]interface{}, map[string]*timestamp.Timestamp) error
	Get(string, []string, []string, string, int64, opentracing.Span) (<-chan map[string]interface{}, error)
	GetRow(string, []string, []string, opentracing.Span) *sql.Row
	Cleanup()
	LobstersFrontpage() (*qpuextapi.LobFrontpageResp, error)
	LobstersStoryVote(*qpuextapi.LobStoryVoteReq) error
	LobstersStoryVoteInsert(*qpuextapi.LobStoryVoteReq) error
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
	// Index ...
	Index OperatorType = iota
	// Cache ...
	Cache OperatorType = iota
	// Router ...
	Router OperatorType = iota
	// Lobsters ...
	Lobsters OperatorType = iota
)

// StateType ...
type StateType int

const (
	// Stateless ...
	Stateless StateType = iota
	// MaterializedView ...
	MaterializedView StateType = iota
	// Cache ...
	CacheState StateType = iota
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
	CacheConfig struct {
		Size int
		Ttl  int
	}
	Evaluation struct {
		Tracing                    bool
		LogTimestamps              bool
		MeasureNotificationLatency bool
		MeasureDataTransfer        bool
	}
	ProcessingConfig struct {
		API struct {
			MaxWorkers  int
			MaxJobQueue int
		}
		Input struct {
			MaxWorkers  int
			MaxJobQueue int
		}
		GrpcServers int
	}
}

// QPUConnection ...
type QPUConnection struct {
	Address             string
	MeasureDataTransfer bool
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

// WriteLogEntry ...
type WriteLogEntry struct {
	RowID int64
	T0    time.Time
	T1    time.Time
}

// QueryLogEntry ...
type QueryLogEntry struct {
	RowIDs []string
	Ts     time.Time
}
