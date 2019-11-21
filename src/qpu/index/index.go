package index

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/filter"
	"github.com/dvasilas/proteus/src/qpu/index/antidote"
	"github.com/dvasilas/proteus/src/qpu/index/inMem"
	log "github.com/sirupsen/logrus"
)

// IQPU implements an index QPU
type IQPU struct {
	qpu          *utils.QPU
	index        indexStore
	cancelFuncs  []context.CancelFunc
	persistentQs map[int]persistentQuery
	mutex        sync.RWMutex
}

type persistentQuery struct {
	predicate []*pbUtils.AttributePredicate
	stream    pbQPU.QPU_QueryServer
}

// indexStore describes the interface that any index implementation needs to expose
// to work with this module.
type indexStore interface {
	Update(*pbUtils.Attribute, *pbUtils.Attribute, utils.ObjectState, pbUtils.Vectorclock) error
	UpdateCatchUp(*pbUtils.Attribute, utils.ObjectState, pbUtils.Vectorclock) error
	Lookup(*pbUtils.AttributePredicate, *pbUtils.SnapshotTimePredicate, chan utils.ObjectState, chan error)
}

//---------------- API Functions -------------------

// QPU creates an index QPU
func QPU(conf *config.Config) (*IQPU, error) {
	rand.Seed(time.Now().UnixNano())
	q := &IQPU{
		qpu: &utils.QPU{
			Config:               conf,
			QueryingCapabilities: conf.IndexConfig.IndexingConfig,
		},
		persistentQs: make(map[int]persistentQuery),
	}

	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	var index indexStore
	var err error
	switch q.qpu.Config.IndexConfig.IndexStore.Store {
	case config.INMEM:
		index, err = inmemindex.New(
			q.qpu.Config.IndexConfig.IndexingConfig[0].GetAttr().GetAttrKey(),
			q.qpu.Config.IndexConfig.IndexingConfig[0].GetAttr().GetAttrType())
		if err != nil {
			return &IQPU{}, err
		}
	case config.ANT:
		index, err = antidoteindex.New(conf)
		if err != nil {
			return &IQPU{}, err
		}
	}
	q.index = index

	var sync bool
	switch conf.IndexConfig.ConsLevel {
	case "sync":
		sync = true
	case "async":
		sync = false
	default:
		return nil, errors.New("unknown index consistency level")
	}
	pred := []*pbUtils.AttributePredicate{}
	q.cancelFuncs = make([]context.CancelFunc, len(q.qpu.Conns))
	for i, conn := range q.qpu.Conns {
		streamIn, cancel, err := conn.Client.Query(
			pred,
			protoutils.SnapshotTimePredicate(
				protoutils.SnapshotTime(pbUtils.SnapshotTime_INF, nil),
				protoutils.SnapshotTime(pbUtils.SnapshotTime_INF, nil),
			),
			nil,
			sync,
		)
		if err != nil {
			cancel()
			return nil, err
		}
		q.cancelFuncs[i] = cancel
		go q.opConsumer(streamIn, cancel, sync)
	}

	if err := q.catchUp(); err != nil {
		return nil, err
	}
	return q, nil
}

// Query implements the Query API for the index QPU
func (q *IQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	request := requestRec.GetRequest()
	log.WithFields(log.Fields{"request": request}).Debug("query request received")
	if request.GetClock().GetUbound().GetType() < request.GetClock().GetUbound().GetType() {
		return errors.New("lower bound of timestamp attribute cannot be greater than the upper bound")
	}
	if request.GetClock().GetLbound().GetType() != pbUtils.SnapshotTime_LATEST &&
		request.GetClock().GetLbound().GetType() != pbUtils.SnapshotTime_INF &&
		request.GetClock().GetUbound().GetType() != pbUtils.SnapshotTime_LATEST &&
		request.GetClock().GetUbound().GetType() != pbUtils.SnapshotTime_INF {
		return errors.New("not supported")
	}
	maxResponseCount, err := utils.MaxResponseCount(request.GetMetadata())
	if err != nil {
		return nil
	}
	if request.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_LATEST || request.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_LATEST {
		lookupResCh := make(chan utils.ObjectState)
		errCh := make(chan error)
		go q.index.Lookup(request.GetPredicate()[0], request.GetClock(), lookupResCh, errCh)
		seqID := int64(0)
		for {
			select {
			case err, ok := <-errCh:
				if !ok {
					errCh = nil
				} else if err == io.EOF {
					errCh = nil
				} else {
					utils.ReportError(err)
					return err
				}
			case item, ok := <-lookupResCh:
				if !ok {
					lookupResCh = nil
				} else {
					logOp := protoutils.LogOperation(
						item.ObjectID,
						item.Bucket,
						item.ObjectType,
						&item.Timestamp,
						protoutils.PayloadState(&item.State),
					)
					if err := streamOut.Send(protoutils.ResponseStreamRecord(
						seqID,
						pbQPU.ResponseStreamRecord_STATE,
						logOp,
					)); err != nil {
						fmt.Println("err from Send(): ", err)
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
			}
			if errCh == nil && lookupResCh == nil {
				break
			}
		}
	}
	if request.GetClock().GetLbound().GetType() == pbUtils.SnapshotTime_INF || request.GetClock().GetUbound().GetType() == pbUtils.SnapshotTime_INF {
		chID := rand.Int()
		q.mutex.Lock()
		q.persistentQs[chID] = persistentQuery{
			predicate: request.GetPredicate(),
			stream:    streamOut,
		}
		q.mutex.Unlock()
		heartbeatCh := make(chan int)
		go q.heartbeat(heartbeatCh)
		heartbeatCh <- 0
		for i := range heartbeatCh {
			err := streamOut.Send(protoutils.ResponseStreamRecord(int64(0), pbQPU.ResponseStreamRecord_HEARTBEAT, nil))
			if err != nil {
				q.mutex.Lock()
				delete(q.persistentQs, chID)
				q.mutex.Unlock()
				heartbeatCh <- 1
				break
			}
			heartbeatCh <- i
		}
	}
	return nil
}

func (q *IQPU) heartbeat(heartbeatCh chan int) {
	checkIfClosed := <-heartbeatCh
	if checkIfClosed == 1 {
		return
	}
	heartbeatCh <- 0
	f := q.newHeartbeat(heartbeatCh)
	time.AfterFunc(100*time.Millisecond, f)
}

func (q *IQPU) newHeartbeat(heartbeatCh chan int) func() {
	return func() {
		q.heartbeat(heartbeatCh)
	}
}

// GetConfig implements the GetConfig API for the index QPU
func (q *IQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup ...
func (q *IQPU) Cleanup() {
	log.Info("index QPU cleanup")
	for _, cFunc := range q.cancelFuncs {
		cFunc()
	}
}

//----------- Stream Consumer Functions ------------

func (q *IQPU) forward(record *pbQPU.ResponseStreamRecord) error {
	q.mutex.RLock()
	for _, query := range q.persistentQs {
		match, err := filter.Filter(query.predicate, record)
		if err != nil {
			q.mutex.RUnlock()
			return err
		}
		if match {
			if err := query.stream.Send(record); err != nil {
				q.mutex.RUnlock()
				return nil
			}
		}
	}
	q.mutex.RUnlock()
	return nil
}

// Receives an stream of update operations
// Updates the index for each operation
// TODO: Query a way to handle an error here
func (q *IQPU) opConsumer(stream pbQPU.QPU_QueryClient, cancel context.CancelFunc, sync bool) {
	for {
		streamRec, err := stream.Recv()
		if err == io.EOF {
			// TODO: see datastoredriver to fix this
			log.Fatal("indexQPU:opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.Fatal("opConsumer err", err)
			return
		} else {
			if streamRec.GetType() == pbQPU.ResponseStreamRecord_UPDATEDELTA {
				go func() {
					if err := q.updateIndex(streamRec); err != nil {
						utils.ReportError(err)
						return
					}
					if sync {
						log.Debug("QPUServer:index updated, sending ACK")
						if err := stream.Send(protoutils.RequestStreamAck(streamRec.GetSequenceId())); err != nil {
							log.Fatal("opConsumer stream.Send failed")
							return
						}
					}
					if err := q.forward(streamRec); err != nil {
						log.Fatal("forwards err", err)
						return
					}
				}()
			}
		}
	}
}

//---------------- Internal Functions --------------

// Given an operation sent from the data store, updates the index
func (q *IQPU) updateIndex(rec *pbQPU.ResponseStreamRecord) error {
	state := utils.ObjectState{
		ObjectID:   rec.GetLogOp().GetObjectId(),
		ObjectType: rec.GetLogOp().GetObjectType(),
		Bucket:     rec.GetLogOp().GetBucket(),
		Timestamp:  *rec.GetLogOp().GetTimestamp(),
	}
	if rec.GetType() == pbQPU.ResponseStreamRecord_UPDATEDELTA {
		if rec.GetLogOp().GetPayload().GetDelta().GetNew() != nil {
			state.State = *rec.GetLogOp().GetPayload().GetDelta().GetNew()
		} else if rec.GetLogOp().GetPayload().GetDelta().GetOld() != nil {
			state.State = *rec.GetLogOp().GetPayload().GetDelta().GetOld()
		}
	} else if rec.GetType() == pbQPU.ResponseStreamRecord_STATE {
		state.State = *rec.GetLogOp().GetPayload().GetState()
	}
	for _, attr := range state.State.GetAttrs() {
		toIndex := true
		for _, pred := range q.qpu.QueryingCapabilities {
			match, err := utils.AttrMatchesPredicate(pred, attr)
			if err != nil {
				return err
			}
			if !match {
				toIndex = false
				break
			}
		}
		if toIndex {
			if rec.GetType() == pbQPU.ResponseStreamRecord_UPDATEDELTA {
				if rec.GetLogOp().GetPayload().GetDelta().GetNew() != nil {
					for _, attrOld := range rec.GetLogOp().GetPayload().GetDelta().GetOld().GetAttrs() {
						if attr.GetAttrKey() == attrOld.GetAttrKey() && attr.GetAttrType() == attrOld.GetAttrType() {
							return q.index.Update(attrOld, attr, state, *rec.GetLogOp().GetTimestamp())
						}
					}
					return q.index.Update(nil, attr, state, *rec.GetLogOp().GetTimestamp())
				}
				return q.index.Update(attr, nil, state, *rec.GetLogOp().GetTimestamp())
			} else if rec.GetType() == pbQPU.ResponseStreamRecord_STATE {
				return q.index.UpdateCatchUp(attr, state, *rec.GetLogOp().GetTimestamp())
			}
		}
	}
	return nil
}

// catchUp performs an index catch-up operation.
// It reads the latest snapshot for the underlying data store, and builds an index.
func (q *IQPU) catchUp() error {
	errChan := make(chan error)
	for _, conn := range q.qpu.Conns {
		pred := make([]*pbUtils.AttributePredicate, 0)
		stream, cancel, err := conn.Client.Query(
			pred,
			protoutils.SnapshotTimePredicate(
				protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
				protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
			),
			nil,
			false,
		)
		defer cancel()
		if err != nil {
			return err
		}
		go utils.QueryResponseConsumer(pred, stream, nil, q.updateIndexCatchUp, errChan)
	}
	streamCnt := len(q.qpu.Conns)
	for streamCnt > 0 {
		select {
		case err := <-errChan:
			if err == io.EOF {
				streamCnt--
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *IQPU) updateIndexCatchUp(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	return q.updateIndex(streamRec)
}
