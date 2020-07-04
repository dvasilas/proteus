package apiprocessor

import (
	"context"
	"errors"

	"github.com/dvasilas/proteus/internal/libqpu"
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
}

// ---------------- API Functions -------------------

// NewProcessor creates an instance of APIProcessor.
// It initiates the libqpu.QPUClass corresponding to the QPU's class.
func NewProcessor(qpu *libqpu.QPU, catchUpDoneCh chan int) (APIProcessor, error) {
	qpuClass, err := getQPUClass(qpu, catchUpDoneCh)
	if err != nil {
		libqpu.Error(err)
		return APIProcessor{}, err
	}

	return APIProcessor{qpuClass: qpuClass}, nil
}

// Query is responsible for the top-level processing of invocation of the Query API.
func (s APIProcessor) Query(queryReq libqpu.QueryRequest, stream libqpu.RequestStream) error {
	var internalQuery libqpu.InternalQuery
	switch queryReq.QueryType() {
	case libqpu.InternalQueryType:
		internalQuery = queryReq.GetQueryI()
	case libqpu.SQLQueryType:
		var err error
		internalQuery, err = sqlparser.Parse(queryReq.GetSQLStr())
		if err != nil {
			libqpu.Error(err)
			return err
		}
	case libqpu.UnknownType:
		return libqpu.Error(errors.New("UnknownType"))
	default:
		return libqpu.Error(errors.New("default"))
	}

	libqpu.Trace("internalQuery received", map[string]interface{}{"internalQuery": internalQuery})
	var logOpSubscribeCh, logOpSnapshotCh <-chan libqpu.LogOperation
	var errSubscribeCh, errSnapshotCh <-chan error
	queryID := -1
	snapshotStream := false

	isSnapshot, isSubscribe := queries.QueryType(internalQuery)
	if !isSubscribe && !isSnapshot {
		return libqpu.Error(errors.New("invalid query"))
	}

	if isSubscribe {
		queryID, logOpSubscribeCh, errSubscribeCh = s.qpuClass.ProcessQuerySubscribe(internalQuery, queryReq.GetMetadata(), queryReq.GetSync())
	}
	if isSnapshot {
		snapshotStream = true
		logOpSnapshotCh, errSnapshotCh = s.qpuClass.ProcessQuerySnapshot(internalQuery, queryReq.GetMetadata(), queryReq.GetSync(), nil)
	}

	var seqID int64

	for {
		select {
		case logOp, ok := <-logOpSubscribeCh:
			if !ok {
				logOpSubscribeCh = nil
			} else {
				libqpu.Trace("api processor received", map[string]interface{}{"logOp": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, internalQuery)
				if err != nil {
					libqpu.Error(err)
					return err
				}
				// libqpu.Trace("SatisfiesPredicate", map[string]interface{}{"ok": ok})
				if ok {
					if err := stream.Send(seqID, libqpu.Delta, logOp); err != nil {
						libqpu.Error(err)
						s.qpuClass.RemovePersistentQuery(internalQuery.GetTable(), queryID)
						return nil
					}
					seqID++
				}
			}
		case logOp, ok := <-logOpSnapshotCh:
			if !ok {
				logOpSnapshotCh = nil
			} else {
				libqpu.Trace("api processor received", map[string]interface{}{"logOp": logOp})
				ok, err := queries.SatisfiesPredicate(logOp, internalQuery)
				if err != nil {
					libqpu.Error(err)
					return err
				}
				// libqpu.Trace("SatisfiesPredicate", map[string]interface{}{"ok": ok})
				if ok {
					if err := stream.Send(seqID, libqpu.State, logOp); err != nil {
						libqpu.Error(err)
						s.qpuClass.RemovePersistentQuery(internalQuery.GetTable(), queryID)
					}
					seqID++
				}
			}
		case err, ok := <-errSubscribeCh:
			if !ok {
				errSubscribeCh = nil
			} else {
				libqpu.Trace("api processor received error", map[string]interface{}{"error": err})
				// 			if cancel != nil {
				// 				cancel()
				// 			}
				return err
			}
		case err, ok := <-errSnapshotCh:
			if !ok {
				errSnapshotCh = nil
			} else {
				libqpu.Trace("api processor received error", map[string]interface{}{"error": err})
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
				libqpu.Error(err)
				return err
			}
		}
		if logOpSubscribeCh == nil && errSubscribeCh == nil && logOpSnapshotCh == nil && errSnapshotCh == nil {
			return nil
		}
	}

	// libqpu.QPUClass.ProcessQuery provides the actual query processing functionality
	// return s.qpuClass.ProcessQuery(internalQuery, stream, queryReq.GetMetadata(), queryReq.GetSync())
}

// QueryUnary ...
func (s APIProcessor) QueryUnary(req libqpu.QueryRequest, parentSpan opentracing.Span) (*qpu_api.QueryResp, error) {
	return s.qpuClass.ClientQuery(libqpu.InternalQuery{}, parentSpan)
	// result := make([]*libqpu.LogOperation, 0)
	// logOpCh, errCh := s.qpuClass.ProcessQuerySnapshot(req.GetQueryI(), req.GetMetadata(), req.GetSync(), parentSpan)
	// for {
	// 	select {
	// 	case logOp, ok := <-logOpCh:
	// 		if !ok {
	// 			logOpCh = nil
	// 		} else {
	// 			libqpu.Trace("api processor received", map[string]interface{}{"logOp": logOp})
	// 			result = append(result, &logOp)
	// 		}
	// 	case err, ok := <-errCh:
	// 		if !ok {
	// 			errCh = nil
	// 		} else {
	// 			libqpu.Trace("api processor received error", map[string]interface{}{"error": err})
	// 			// 			if cancel != nil {
	// 			// 				cancel()
	// 			// 			}
	// 			return nil, err
	// 		}
	// 	}
	// 	if logOpCh == nil && errCh == nil {
	// 		return result, nil
	// 	}
	// }
}

// GetConfig is responsible for the top-level processing of invocation of the GetConfig API.
func (s APIProcessor) GetConfig(ctx context.Context, in *qpu_api.ConfigRequest) (*qpu_api.ConfigResponse, error) {
	return nil, libqpu.Error(errors.New("not implemented"))
}

// GetDataTransfer is responsible for the top-level processing of invocation of the GetDataTransfer API.
// func (s APIProcessor) GetDataTransfer(ctx context.Context, in *qpu_api.GetDataRequest) (*qpu_api.DataTransferResponse, error) {
// 	return nil, libqpu.Error(errors.New("not implemented"))
// }

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
		return nil, libqpu.Error(errors.New("Unknown QPU class"))
	}
}
