package cache

import (
	"errors"
	"io"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/cache/lruCache"
	log "github.com/sirupsen/logrus"
)

// CQPU implements a cache QPU.
type CQPU struct {
	qpu    *utils.QPU
	cache  cacheImplementation
	config *config.Config
}

// Describes the interface that any cache implementation needs to expose
// to work with this module.
type cacheImplementation interface {
	Put(predicate []*pbUtils.AttributePredicate, obj utils.ObjectState) error
	Get(p []*pbUtils.AttributePredicate) ([]utils.ObjectState, bool)
}

//---------------- API Functions -------------------

// QPU creates a cache QPU
func QPU(conf *config.Config) (*CQPU, error) {
	q := &CQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
		cache: lrucache.New(conf),
	}
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	if len(q.qpu.Conns) > 1 {
		return nil, errors.New("cache QPUs support a single connection")
	}
	return q, nil
}

// Query implements the Query API for the cache QPU
func (q *CQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	request := requestRec.GetRequest()
	log.WithFields(log.Fields{"request": request}).Debug("query request received")
	maxResponseCount, err := utils.MaxResponseCount(request.GetMetadata())
	if err != nil {
		return nil
	}
	if request.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_LATEST || request.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_LATEST {
		cachedResult, hit := q.cache.Get(request.GetPredicate())
		if hit {
			seqID := int64(0)
			for _, item := range cachedResult {
				logOp := protoutils.LogOperation(
					item.ObjectID,
					item.Bucket,
					item.ObjectType,
					&item.Timestamp,
					protoutils.PayloadState(&item.State),
				)
				if err := streamOut.Send(protoutils.ResponseStreamRecord(seqID, pbQPU.ResponseStreamRecord_STATE, logOp)); err != nil {
					return err
				}
				seqID++
				if maxResponseCount > 0 && seqID >= maxResponseCount {
					return streamOut.Send(
						protoutils.ResponseStreamRecord(
							seqID,
							pbQPU.ResponseStreamRecord_END_OF_STREAM,
							&pbUtils.LogOperation{},
						))
				}
			}
			return nil
		}
		streamIn, _, err := q.qpu.Conns[0].Client.Query(request.GetPredicate(), request.GetClock(), nil, false)
		if err != nil {
			return err
		}
		respond := true
		seqID := int64(0)
		for {
			streamRec, err := streamIn.Recv()
			if err == io.EOF {
				if respond {
					if err := streamOut.Send(
						protoutils.ResponseStreamRecord(
							seqID,
							pbQPU.ResponseStreamRecord_END_OF_STREAM,
							&pbUtils.LogOperation{},
						),
					); err != nil {
						return nil
					}
				}
				return err
			} else if err != nil {
				return err
			}
			if err = q.storeAndRespond(request.GetPredicate(), streamRec, streamOut, &seqID, respond); err != nil {
				return err
			}
			if maxResponseCount > 0 && seqID >= maxResponseCount {
				if respond {
					if err := streamOut.Send(
						protoutils.ResponseStreamRecord(
							seqID,
							pbQPU.ResponseStreamRecord_END_OF_STREAM,
							&pbUtils.LogOperation{},
						)); err != nil {
						return err
					}
				}
				respond = false
			}
		}
	}
	return errors.New("not supported")
}

// GetConfig implements the GetConfig API for the cache QPU
func (q *CQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup is called when the process receives a SIGTERM signcal
func (q *CQPU) Cleanup() {
	log.Info("cache QPU cleanup")
}

//---------------- Internal Functions --------------

// Stores an object that is part of a query response in the cache
// and forwards to the response stream
func (q *CQPU) storeAndRespond(predicate []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64, respond bool) error {
	if streamRec.GetType() == pbQPU.ResponseStreamRecord_STATE {
		obj := utils.ObjectState{
			ObjectID:   streamRec.GetLogOp().GetObjectId(),
			ObjectType: streamRec.GetLogOp().GetObjectType(),
			Bucket:     streamRec.GetLogOp().GetBucket(),
			State:      *streamRec.GetLogOp().GetPayload().GetState(),
			Timestamp:  *streamRec.GetLogOp().GetTimestamp(),
		}
		if err := q.cache.Put(predicate, obj); err != nil {
			return err
		}
	}
	if respond {
		err := streamOut.Send(
			protoutils.ResponseStreamRecord(
				*seqID,
				streamRec.GetType(),
				streamRec.GetLogOp(),
			))
		(*seqID)++
		return err
	}
	return nil
}
