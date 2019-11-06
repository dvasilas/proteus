package loadbalancer

import (
	"errors"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// LBQPU implements a load balancer QPU
type LBQPU struct {
	qpu     *utils.QPU
	config  *config.Config
	rRIndex int
	mutex   sync.Mutex
}

//---------------- API Functions -------------------

// QPU creates a fault injection QPU
func QPU(conf *config.Config) (*LBQPU, error) {
	rand.Seed(time.Now().UnixNano())
	q := &LBQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
		mutex: sync.Mutex{},
	}
	if len(conf.Connections) < 2 {
		return nil, errors.New("load balancer QPUs need to have support at least 2 connections")
	}
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	return q, nil
}

// Query implements the Query API for the fault injection QPU
func (q *LBQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	request := requestRec.GetRequest()
	log.WithFields(log.Fields{"req": request}).Debug("Query request")
	forwardTo, err := q.generateSubQueries()
	if err != nil {
		return err
	}

	errChan := make(chan error)
	for _, frwTo := range forwardTo {
		streamIn, _, err := frwTo.Client.Query(request.GetPredicate(), protoutils.SnapshotTimePredicate(request.GetClock().GetLbound(), request.GetClock().GetUbound()), nil, false)
		if err != nil {
			return err
		}
		go utils.QueryResponseConsumer(request.GetPredicate(), streamIn, streamOut, q.forward, errChan)
	}
	streamCnt := len(forwardTo)
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

// GetConfig implements the GetConfig API for the fault injection QPU
func (q *LBQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *LBQPU) Cleanup() {
	log.Info("load balancer QPU cleanup")
}

//---------------- Internal Functions --------------

func (q *LBQPU) generateSubQueries() ([]*utils.QPU, error) {
	forwardTo := make([]*utils.QPU, 0)
	q.mutex.Lock()
	forwardTo = append(forwardTo, q.qpu.Conns[q.rRIndex%len(q.qpu.Conns)])
	q.rRIndex++
	q.mutex.Unlock()
	return forwardTo, nil
}

func (q *LBQPU) forward(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	log.WithFields(log.Fields{
		"record": streamRec,
		"pred":   pred,
	}).Debug("LBQPU: received input stream record")

	return streamOut.Send(streamRec)
}
