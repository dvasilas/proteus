package faultinjection

import (
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// FIQPU implements a fault injection QPU
type FIQPU struct {
	qpu    *utils.QPU
	config *config.Config
}

//---------------- API Functions -------------------

// QPU creates a fault injection QPU
func QPU(conf *config.Config) (*FIQPU, error) {
	rand.Seed(time.Now().UnixNano())
	q := &FIQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
	}
	if len(conf.Connections) > 1 {
		return nil, errors.New("filter QPUs support a single connection")
	}
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	return q, nil
}

// Query implements the Query API for the fault injection QPU
func (q *FIQPU) Query(streamOut pbQPU.QPU_QueryServer) error {
	request, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}
	req := request.GetRequest()
	log.WithFields(log.Fields{"req": req}).Debug("Query request")

	errChan := make(chan error)
	streamIn, _, err := q.qpu.Conns[0].Client.Query(req.GetPredicate(), req.GetClock(), req.GetSync())
	utils.QueryResponseConsumer(req.GetPredicate(), streamIn, streamOut, q.forward, errChan)
	err = <-errChan
	if err != io.EOF {
		return err
	}
	return nil
}

// GetConfig implements the GetConfig API for the fault injection QPU
func (q *FIQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *FIQPU) Cleanup() {
	log.Info("fault injection QPU cleanup")
}

//---------------- Internal Functions --------------

func (q *FIQPU) forward(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	log.WithFields(log.Fields{
		"record": streamRec,
		"pred":   pred,
	}).Debug("FIQPU: received input stream record")

	if q.qpu.Config.FaultInjectionConfig.Function == "drop" {
		return drop(q.qpu.Config.FaultInjectionConfig.Rate, streamRec, streamOut)
	} else {
		return streamOut.Send(streamRec)
	}
}

func drop(rate float32, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer) error {
	if rate < rand.Float32() {
		return streamOut.Send(streamRec)
	}
	return nil
}
