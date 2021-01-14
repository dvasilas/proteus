package apiprocessor

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sync"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpuapi"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	datastoredriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver"
	joinqpu "github.com/dvasilas/proteus/internal/qpu_classes/join"
	lobsters "github.com/dvasilas/proteus/internal/qpu_classes/lobsters"
	router "github.com/dvasilas/proteus/internal/qpu_classes/router"
	sumqpu "github.com/dvasilas/proteus/internal/qpu_classes/sum"
	"github.com/dvasilas/proteus/internal/queries"
	"github.com/dvasilas/proteus/internal/sqlparser"
	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
)

// This package is responsible for implementing the libqpu.APIProcessor.
// It provides top-level, class-independent processing of calls to the QPU's API.
// The core processing logic is delegated to libqpu.QPUClass methods.

// APIProcessor implements the libqpu.APIProcessor interface.
// It provides access to the methods implemented by libqpu.QPUClass.
type APIProcessor struct {
	qpuClass                   libqpu.QPUClass
	sqlCache                   *sqlToASTCache
	measureNotificationLatency bool
	processingLatencyM         metrics.LatencyM
	measureDataTransfer        bool
	dataTransfer               dataSent
	dataTransferCh             chan int64
}

type dataSent struct {
	count int64
}

// ---------------- API Functions -------------------

// NewProcessor creates an instance of APIProcessor.
// It initiates the libqpu.QPUClass corresponding to the QPU's class.
func NewProcessor(qpu *libqpu.QPU, catchUpDoneCh chan int) (*APIProcessor, error) {
	qpuClass, err := getQPUClass(qpu, catchUpDoneCh)
	if err != nil {
		return nil, utils.Error(err)
	}

	var processingLatencyM metrics.LatencyM
	if qpu.Config.Evaluation.MeasureNotificationLatency {
		processingLatencyM = metrics.NewLatencyM()
	}

	s := &APIProcessor{
		qpuClass:                   qpuClass,
		sqlCache:                   newSQLToASTCache(),
		measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		measureDataTransfer:        qpu.Config.Evaluation.MeasureDataTransfer,
		processingLatencyM:         processingLatencyM,
		dataTransferCh:             make(chan int64, 10000),
	}

	go func() {
		for respSize := range s.dataTransferCh {
			s.dataTransfer.count += respSize
		}
	}()

	return s, nil
}

// Query is responsible for the top-level processing of invocation of the Query API.
func (s *APIProcessor) Query(queryReq libqpu.QueryRequest, stream libqpu.RequestStream) error {
	// utils.Trace("query received", map[string]interface{}{"queryReq": queryReq})

	var astQuery libqpu.ASTQuery
	switch queryReq.QueryType() {
	case libqpu.ASTQueryT:
		astQuery = queryReq.GetQueryI()
	case libqpu.SQLQueryT:
		var err error
		astQuery, err = sqlparser.Parse(queryReq.GetSQLStr())
		if err != nil {
			return utils.Error(err)
		}
	case libqpu.UnknownQueryT:
		return utils.Error(errors.New("apiProcessor:Query:UnknownQueryT"))
	default:
		return utils.Error(errors.New("apiProcessor:Query:default"))
	}

	var logOpSubscribeCh, logOpSnapshotCh <-chan libqpu.LogOperation
	var errSubscribeCh, errSnapshotCh <-chan error
	queryID := -1
	snapshotStream := false

	isSnapshot, isSubscribe := queries.QueryType(astQuery)
	if !isSubscribe && !isSnapshot {
		return utils.Error(errors.New("invalid query"))
	}

	if isSubscribe {
		queryID, logOpSubscribeCh, errSubscribeCh = s.qpuClass.ProcessQuerySubscribe(astQuery, queryReq.GetMetadata(), queryReq.GetSync())
	}
	if isSnapshot {
		snapshotStream = true
		logOpSnapshotCh, errSnapshotCh = s.qpuClass.ProcessQuerySnapshot(astQuery, queryReq.GetMetadata(), queryReq.GetSync(), nil)
	}

	var seqID int64

	for {
		select {
		case logOp, ok := <-logOpSubscribeCh:
			if !ok {
				logOpSubscribeCh = nil
			} else {
				// utils.Trace("api processor received", map[string]interface{}{"logOp": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, astQuery)
				// utils.Trace("SatisfiesPredicate", map[string]interface{}{"ok": ok})
				if err != nil {
					return utils.Error(err)
				}
				if ok {

					if s.measureNotificationLatency {
						if err = s.processingLatencyM.AddFromTs(logOp.InTs); err != nil {
							log.Fatal(err)
						}
					}
					go func() {
						if s.measureDataTransfer && queryReq.GetMeasureDataTransfer() {
							logOpSize, err := getLogOperationSize(logOp)
							if err != nil {
								utils.Error(err)
							}

							s.dataTransferCh <- logOpSize
						}

						if err := stream.Send(seqID, libqpu.Delta, logOp); err != nil {
							utils.Warn(err)
							s.qpuClass.RemovePersistentQuery(astQuery.GetTable(), queryID)
						}
					}()
					seqID++
				}
			}
		case logOp, ok := <-logOpSnapshotCh:
			if !ok {
				logOpSnapshotCh = nil
			} else {
				// utils.Trace("api processor received", map[string]interface{}{"logOp": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, astQuery)
				// utils.Trace("SatisfiesPredicate", map[string]interface{}{"ok": ok})
				if err != nil {
					return utils.Error(err)
				}
				if ok {
					if err := stream.Send(seqID, libqpu.State, logOp); err != nil {
						return utils.Error(err)
					}
					seqID++
				}
			}
		case err, ok := <-errSubscribeCh:
			if !ok {
				errSubscribeCh = nil
			} else {
				utils.Trace("api processor received error", map[string]interface{}{"error": err})
				// 			if cancel != nil {
				// 				cancel()
				// 			}
				return err
			}
		case err, ok := <-errSnapshotCh:
			if !ok {
				errSnapshotCh = nil
			} else {
				// 			if cancel != nil {
				// 				cancel()
				// 			}
				return err
			}
		}
		if logOpSnapshotCh == nil && errSnapshotCh == nil && snapshotStream {
			snapshotStream = false
			err := stream.Send(
				seqID,
				libqpu.EndOfStream,
				libqpu.LogOperation{},
			)
			if err != nil {
				return utils.Error(err)
			}
		}
		if logOpSubscribeCh == nil && errSubscribeCh == nil && logOpSnapshotCh == nil && errSnapshotCh == nil {
			return nil
		}
	}
}

// QueryUnary ...
func (s *APIProcessor) QueryUnary(req libqpu.QueryRequest, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {
	astQuery, found := s.sqlCache.get(req.GetSQLStr())
	if !found {
		var err error
		astQuery, err = sqlparser.Parse(req.GetSQLStr())
		if err != nil {
			return nil, err
		}
		s.sqlCache.put(req.GetSQLStr(), astQuery)
	}

	resp, err := s.qpuClass.ClientQuery(astQuery, parentSpan)

	if s.measureDataTransfer {
		respSize, err := getQueryRespSize(resp)
		if err != nil {
			return nil, err
		}
		s.dataTransferCh <- respSize
	}

	return resp, err
}

// GetConfig is responsible for the top-level processing of invocation of the GetConfig API.
func (s *APIProcessor) GetConfig(ctx context.Context, in *qpuapi.ConfigRequest) (*qpuapi.ConfigResponse, error) {
	return s.qpuClass.GetConfig(), nil
}

// GetMetrics ...
func (s *APIProcessor) GetMetrics(ctx context.Context, req *qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	resp, err := s.qpuClass.GetMetrics(req)
	if err != nil {
		return nil, err
	}

	var p50, p90, p95, p99 float64
	p50, p90, p95, p99 = -1, -1, -1, -1

	if s.measureNotificationLatency {
		p50, p90, p95, p99 = s.processingLatencyM.GetMetrics()
	}
	resp.ProcessingLatencyP50 = p50
	resp.ProcessingLatencyP90 = p90
	resp.ProcessingLatencyP95 = p95
	resp.ProcessingLatencyP99 = p99

	if s.measureDataTransfer {
		resp.KBytesSent = float64(s.dataTransfer.count) / float64(1024)
	}

	return resp, nil
}

// ---------------- Internal Functions --------------

func getQPUClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (libqpu.QPUClass, error) {
	switch qpu.Config.Operator {
	case libqpu.DBDriver:
		return datastoredriver.InitClass(qpu, catchUpDoneCh)
	case libqpu.Aggregation:
		return sumqpu.InitClass(qpu, catchUpDoneCh)
	case libqpu.Join:
		return joinqpu.InitClass(qpu, catchUpDoneCh)
	case libqpu.Router:
		return router.InitClass(qpu, catchUpDoneCh)
	case libqpu.Lobsters:
		return lobsters.InitClass(qpu, catchUpDoneCh)
	default:
		return nil, utils.Error(errors.New("Unknown QPU class"))
	}
}

type sqlToASTCache struct {
	sync.RWMutex
	m map[string]libqpu.ASTQuery
}

func newSQLToASTCache() *sqlToASTCache {
	return &sqlToASTCache{
		m: make(map[string]libqpu.ASTQuery),
	}
}

func (c *sqlToASTCache) put(sqlStmt string, ast libqpu.ASTQuery) {
	c.Lock()
	if _, ok := c.m[sqlStmt]; ok {
		c.Unlock()
		return
	}
	c.m[sqlStmt] = ast
	c.Unlock()
}

func (c *sqlToASTCache) get(sqlStmt string) (libqpu.ASTQuery, bool) {
	c.RLock()
	ast, found := c.m[sqlStmt]
	c.RUnlock()

	return ast, found
}

func getQueryRespSize(resp *qpuextapi.QueryResp) (int64, error) {
	buff, err := proto.Marshal(resp)
	if err != nil {
		return -1, err
	}
	bytesBuff := bytes.NewBuffer(buff)
	return int64(bytesBuff.Len()), nil
}

func getLogOperationSize(logOp libqpu.LogOperation) (int64, error) {
	buff, err := proto.Marshal(logOp.Op)
	if err != nil {
		return -1, err
	}
	bytesBuff := bytes.NewBuffer(buff)
	return int64(bytesBuff.Len()), nil
}
