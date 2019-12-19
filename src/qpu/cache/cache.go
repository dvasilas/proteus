package cache

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/cache/lruCache"
	"github.com/dvasilas/proteus/src/qpu/client"
	log "github.com/sirupsen/logrus"
)

// CQPU implements a cache QPU.
type CQPU struct {
	qpu                 *utils.QPU
	cache               cacheImplementation
	config              *config.Config
	dataTransferCount   float32
	dataTransferMutex   sync.Mutex
	connectionIsLocal   bool
	measureDataTransfer bool
}

// Describes the interface that any cache implementation needs to expose
// to work with this module.
type cacheImplementation interface {
	Put(predicate []*pbUtils.AttributePredicate, objects []utils.ObjectState, size int, client client.Client) error
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
	q.measureDataTransfer = os.Getenv("MEASURE_DATA_TRANSFER") == "true"
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	if len(q.qpu.Conns) > 1 {
		return nil, errors.New("cache QPUs support a single connection")
	}
	q.connectionIsLocal = q.qpu.Config.Connections[0].Local
	return q, nil
}

// Query implements the Query API for the cache QPU
func (q *CQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	request := requestRec.GetRequest()
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
		tempCacheEntry := make([]utils.ObjectState, 0)
		tempCacheEntrySize := 0
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
				break
			} else if err != nil {
				return err
			}
			if streamRec.GetType() == pbQPU.ResponseStreamRecord_STATE {
				object := responseStreamRecordToObjectState(streamRec)
				tempCacheEntry = append(tempCacheEntry, object)
				tempCacheEntrySize += len(streamRec.GetLogOp().GetPayload().GetState().GetAttrs()) + 1
			}
			if q.measureDataTransfer && !q.connectionIsLocal {
				size, err := utils.GetMessageSize(streamRec)
				if err != nil {
					log.Fatal(err)
				}
				q.dataTransferMutex.Lock()
				q.dataTransferCount += float32(size) / 1024.0
				q.dataTransferMutex.Unlock()
			}
			if err = q.forward(request.GetPredicate(), streamRec, streamOut, &seqID, respond); err != nil {
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
		if err := q.cache.Put(request.GetPredicate(), tempCacheEntry, tempCacheEntrySize, q.qpu.Conns[0].Client); err != nil {
			return err
		}
		return nil
	}
	if request.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_INF || request.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_INF {
		subQueryResponseRecordCh := make(chan *pbQPU.ResponseStreamRecord)
		errCh := make(chan error)
		seqID := int64(0)
		streamIn, _, err := q.qpu.Conns[0].Client.Query(request.GetPredicate(), protoutils.SnapshotTimePredicate(request.GetClock().GetLbound(), request.GetClock().GetUbound()), request.GetMetadata(), false)
		if err != nil {
			return err
		}
		go func() {
			for {
				streamRec, err := streamIn.Recv()
				if err == io.EOF {
					close(subQueryResponseRecordCh)
					close(errCh)
					return
				} else if err != nil {
					errCh <- err
					return
				}
				if q.measureDataTransfer && !q.connectionIsLocal {
					size, err := utils.GetMessageSize(streamRec)
					if err != nil {
						log.Fatal(err)
					}
					q.dataTransferMutex.Lock()
					q.dataTransferCount += float32(size) / 1024.0
					q.dataTransferMutex.Unlock()
				}
				subQueryResponseRecordCh <- streamRec
			}
		}()
		for {
			select {
			case err, ok := <-errCh:
				if !ok {
					errCh = nil
				} else {
					return err
				}
			case streamRec, ok := <-subQueryResponseRecordCh:
				if !ok {
					subQueryResponseRecordCh = nil
				} else {
					if streamRec.GetType() == pbQPU.ResponseStreamRecord_END_OF_STREAM {
					} else {
						if err := streamOut.Send(
							protoutils.ResponseStreamRecord(
								seqID,
								streamRec.GetType(),
								streamRec.GetLogOp(),
							)); err != nil {
							return err
						}
						seqID++
					}
				}
			}
			if errCh == nil && subQueryResponseRecordCh == nil {
				return streamOut.Send(
					protoutils.ResponseStreamRecord(
						seqID,
						pbQPU.ResponseStreamRecord_END_OF_STREAM,
						&pbUtils.LogOperation{},
					))
			}
		}
	}
	return nil
}

// GetConfig implements the GetConfig API for the cache QPU
func (q *CQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// GetDataTransfer ...
func (q *CQPU) GetDataTransfer() float32 {
	q.dataTransferMutex.Lock()
	res := q.dataTransferCount
	q.dataTransferMutex.Unlock()
	return res
}

// Cleanup is called when the process receives a SIGTERM signcal
func (q *CQPU) Cleanup() {
	log.Info("cache QPU cleanup")
}

//---------------- Internal Functions --------------

// Stores an object that is part of a query response in the cache
// and forwards to the response stream
func (q *CQPU) forward(predicate []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64, respond bool) error {
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

func responseStreamRecordToObjectState(streamRec *pbQPU.ResponseStreamRecord) utils.ObjectState {
	return utils.ObjectState{
		ObjectID:   streamRec.GetLogOp().GetObjectId(),
		ObjectType: streamRec.GetLogOp().GetObjectType(),
		Bucket:     streamRec.GetLogOp().GetBucket(),
		State:      *streamRec.GetLogOp().GetPayload().GetState(),
		Timestamp:  *streamRec.GetLogOp().GetTimestamp(),
	}
}
