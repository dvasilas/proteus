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
func (q *CQPU) Query(streamOut pbQPU.QPU_QueryServer) error {
	request, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}
	req := request.GetRequest()
	log.WithFields(log.Fields{"req": req}).Debug("Query request")

	if req.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_LATEST || req.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_LATEST {
		cachedResult, hit := q.cache.Get(req.GetPredicate())
		if hit {
			log.WithFields(log.Fields{
				"cache entry": cachedResult,
			}).Debug("cache hit, responding")

			var seqID int64
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
			}
			return nil
		}
		log.WithFields(log.Fields{}).Debug("cache miss")

		errChan := make(chan error)
		streamIn, _, err := q.qpu.Conns[0].Client.Query(req.GetPredicate(), req.GetClock(), false, false)
		if err != nil {
			return err
		}
		utils.QueryResponseConsumer(req.GetPredicate(), streamIn, streamOut, q.storeAndRespond, errChan)
		err = <-errChan
		if err != io.EOF {
			return err
		}
		return nil
	} else {
		return errors.New("not supported")
	}
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
func (q *CQPU) storeAndRespond(predicate []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer) error {
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
	return streamOut.Send(streamRec)
}
