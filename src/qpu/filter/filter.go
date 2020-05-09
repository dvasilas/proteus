package filter

import (
	"errors"
	"io"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/proto"
	"github.com/dvasilas/proteus/src/proto/qpu"
	"github.com/dvasilas/proteus/src/proto/qpu_api"
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
func (q *FQPU) Query(streamOut qpu_api.QPU_QueryServer, query *qpu_api.QueryInternalQuery, metadata map[string]string, block bool) error {
	log.WithFields(log.Fields{"query": query, "QPU": "filter"}).Debug("query received")
	maxResponseCount, err := utils.MaxResponseCount(metadata)
	if err != nil {
		return nil
	}
	streamIn, cancel, err := q.qpu.Conns[0].Client.Query(query.GetBucket(), query.GetPredicate(), query.GetClock(), nil, block)
	seqID := int64(0)
	for {
		streamRec, err := streamIn.Recv()
		if err == io.EOF {
			return streamOut.Send(
				protoutils.ResponseStreamRecord(
					seqID,
					qpu_api.ResponseStreamRecord_END_OF_STREAM,
					&qpu.LogOperation{},
				),
			)
		} else if err != nil {
			return err
		}
		if err = filterAndForward(query.GetPredicate(), streamRec, streamOut, &seqID); err != nil {
			return err
		}
		if maxResponseCount > 0 && seqID >= maxResponseCount {
			cancel()
			return streamOut.Send(
				protoutils.ResponseStreamRecord(
					seqID,
					qpu_api.ResponseStreamRecord_END_OF_STREAM,
					&qpu.LogOperation{},
				))
		}
	}
}

// GetConfig implements the GetConfig API for the filter QPU
func (q *FQPU) GetConfig() (*qpu_api.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// GetDataTransfer ...
func (q *FQPU) GetDataTransfer() float32 {
	return 0
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *FQPU) Cleanup() {
	log.Info("filter QPU cleanup")
}

//---------------- Internal Functions --------------

// filterAndForward checks if a given object matches a given predicate
// if yes, it sends it to a given stream
func filterAndForward(pred []*qpu.AttributePredicate, streamRec *qpu_api.ResponseStreamRecord, streamOut qpu_api.QPU_QueryServer, seqID *int64) error {
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
func Filter(predicate []*qpu.AttributePredicate, streamRec *qpu_api.ResponseStreamRecord) (bool, error) {
	if len(predicate) == 0 {
		return false, errors.New("empty Query AttributePredicate")
	}
	for _, pred := range predicate {
		var attrs []*qpu.Attribute
		switch streamRec.GetType() {
		case qpu_api.ResponseStreamRecord_STATE:
			attrs = streamRec.GetLogOp().GetPayload().GetState().GetAttrs()
		case qpu_api.ResponseStreamRecord_UPDATEDELTA:
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
