package filter

import (
	"errors"
	"io"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// FQPU implements a filter QPU
type FQPU struct {
	qpu    *utils.QPU
	config *config.Config
}

//---------------- API Functions -------------------

// QPU creates a filter QPU
func QPU(conf *config.Config) (*FQPU, error) {
	q := &FQPU{
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

// Query implements the Query API for the filter QPU
func (q *FQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	request := requestRec.GetRequest()
	log.WithFields(log.Fields{"request": request}).Debug("query request received")
	maxResponseCount, err := utils.MaxResponseCount(request.GetMetadata())
	if err != nil {
		return nil
	}
	streamIn, cancel, err := q.qpu.Conns[0].Client.Query(request.GetPredicate(), request.GetClock(), nil, request.GetSync())
	seqID := int64(0)
	for {
		streamRec, err := streamIn.Recv()
		if err == io.EOF {
			return streamOut.Send(
				protoutils.ResponseStreamRecord(
					seqID,
					pbQPU.ResponseStreamRecord_END_OF_STREAM,
					&pbUtils.LogOperation{},
				),
			)
		} else if err != nil {
			return err
		}
		if err = filterAndForward(request.GetPredicate(), streamRec, streamOut, &seqID); err != nil {
			return err
		}
		if maxResponseCount > 0 && seqID >= maxResponseCount {
			cancel()
			return streamOut.Send(
				protoutils.ResponseStreamRecord(
					seqID,
					pbQPU.ResponseStreamRecord_END_OF_STREAM,
					&pbUtils.LogOperation{},
				))
		}
	}
}

// GetConfig implements the GetConfig API for the filter QPU
func (q *FQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *FQPU) Cleanup() {
	log.Info("filter QPU cleanup")
}

//---------------- Internal Functions --------------

// filterAndForward checks if a given object matches a given predicate
// if yes, it sends it to a given stream
func filterAndForward(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	match, err := Filter(pred, streamRec)
	if err != nil {
		return err
	}
	if match {
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

// Filter ...
func Filter(predicate []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord) (bool, error) {
	if len(predicate) == 0 {
		return false, errors.New("empty Query AttributePredicate")
	}
	for _, pred := range predicate {
		var attrs []*pbUtils.Attribute
		switch streamRec.GetType() {
		case pbQPU.ResponseStreamRecord_STATE:
			attrs = streamRec.GetLogOp().GetPayload().GetState().GetAttrs()
		case pbQPU.ResponseStreamRecord_UPDATEDELTA:
			attrs = streamRec.GetLogOp().GetPayload().GetDelta().GetNew().GetAttrs()
		}
		for _, attr := range attrs {
			match, err := utils.AttrMatchesPredicate(pred, attr)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
	}
	return false, nil
}
