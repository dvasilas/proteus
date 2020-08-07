package apiprocessor

import (
	"context"
	"errors"
	"sync"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	datastoredriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver"
	joinqpu "github.com/dvasilas/proteus/internal/qpu_classes/join"
	sumqpu "github.com/dvasilas/proteus/internal/qpu_classes/sum"
	"github.com/dvasilas/proteus/internal/queries"
	"github.com/dvasilas/proteus/internal/sqlparser"
	"github.com/opentracing/opentracing-go"
)

// This package is responsible for implementing the libqpu.APIProcessor.
// It provides top-level, class-independent processing of calls to the QPU's API.
// The core processing logic is delegated to libqpu.QPUClass methods.

// APIProcessor implements the libqpu.APIProcessor interface.
// It provides access to the methods implemented by libqpu.QPUClass.
type APIProcessor struct {
	qpuClass libqpu.QPUClass
	sqlCache *sqlToASTCache
}

type sqlToASTCache struct {
	sync.RWMutex
	cache map[string]libqpu.ASTQuery
}

func newSQLToASTCache() *sqlToASTCache {
	return &sqlToASTCache{
		cache: make(map[string]libqpu.ASTQuery),
	}
}

// ---------------- API Functions -------------------

// NewProcessor creates an instance of APIProcessor.
// It initiates the libqpu.QPUClass corresponding to the QPU's class.
func NewProcessor(qpu *libqpu.QPU, catchUpDoneCh chan int) (*APIProcessor, error) {
	qpuClass, err := getQPUClass(qpu, catchUpDoneCh)
	if err != nil {
		return nil, utils.Error(err)
	}

	return &APIProcessor{
		qpuClass: qpuClass,
		sqlCache: newSQLToASTCache(),
	}, nil
}

// Query is responsible for the top-level processing of invocation of the Query API.
func (s *APIProcessor) Query(queryReq libqpu.QueryRequest, stream libqpu.RequestStream) error {
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

	utils.Trace("internalQuery received", map[string]interface{}{"internalQuery": astQuery})
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
				utils.Trace("api processor received", map[string]interface{}{"logOp": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, astQuery)
				if err != nil {
					return utils.Error(err)
				}
				// utils.Trace("SatisfiesPredicate", map[string]interface{}{"ok": ok})
				if ok {
					if err := stream.Send(seqID, libqpu.Delta, logOp); err != nil {
						utils.Warn(err)
						s.qpuClass.RemovePersistentQuery(astQuery.GetTable(), queryID)
						return nil
					}
					seqID++
				}
			}
		case logOp, ok := <-logOpSnapshotCh:
			if !ok {
				logOpSnapshotCh = nil
			} else {
				utils.Trace("api processor received", map[string]interface{}{"logOp": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, astQuery)
				if err != nil {
					return utils.Error(err)
				}
				// utils.Trace("SatisfiesPredicate", map[string]interface{}{"ok": ok})
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
				utils.Trace("api processor received error", map[string]interface{}{"error": err})
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
func (s *APIProcessor) QueryUnary(req libqpu.QueryRequest, parentSpan opentracing.Span) (*qpu_api.QueryResp, error) {
	// var astQuery libqpu.ASTQuery
	// var err error
	// var ok bool

	// switch req.QueryType() {
	// case libqpu.ASTQueryT:
	// 	astQuery = req.GetQueryI()
	// case libqpu.SQLQueryT:
	// 	s.sqlCache.RLock()
	// 	astQuery, ok = s.sqlCache.cache[req.GetSQLStr()]
	// 	s.sqlCache.RUnlock()

	// 	if !ok {
	// 		astQuery, err = sqlparser.Parse(req.GetSQLStr())
	// 		if err != nil {
	// 			utils.Error(err)
	// 			return nil, err
	// 		}
	// 		s.sqlCache.Lock()
	// 		s.sqlCache.cache[req.GetSQLStr()] = astQuery
	// 		s.sqlCache.Unlock()
	// 	}
	// }

	// return s.qpuClass.ClientQuery(astQuery, parentSpan)
	astQuery, err := sqlparser.Parse(req.GetSQLStr())
	if err != nil {
		return nil, err
	}

	return s.qpuClass.ClientQuery(astQuery, parentSpan)
}

// GetConfig is responsible for the top-level processing of invocation of the GetConfig API.
func (s *APIProcessor) GetConfig(ctx context.Context, in *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error) {
	return nil, utils.Error(errors.New("not implemented"))
}

// ---------------- Internal Functions --------------

func getQPUClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (libqpu.QPUClass, error) {
	switch qpu.Config.QpuType {
	case qpu_api.ConfigResponse_DATASTORE_DRIVER:
		return datastoredriver.InitClass(qpu, catchUpDoneCh)
	case qpu_api.ConfigResponse_SUM:
		return sumqpu.InitClass(qpu, catchUpDoneCh)
	case qpu_api.ConfigResponse_JOIN:
		return joinqpu.InitClass(qpu, catchUpDoneCh)
	default:
		return nil, utils.Error(errors.New("Unknown QPU class"))
	}
}
