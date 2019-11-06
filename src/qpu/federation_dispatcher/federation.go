package federation

import (
	"io"
	"sync"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// FQPU implements a federation dispatcher QPU
type FQPU struct {
	qpu    *utils.QPU
	config *config.Config
}

//---------------- API Functions -------------------

// QPU creates a federation dispatcher QPU
func QPU(conf *config.Config) (*FQPU, error) {
	q := &FQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
	}
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	return q, nil
}

// Query implements the Query API for the federation dispatcher QPU
func (q *FQPU) Query(streamOut pbQPU.QPU_QueryServer, requestRec *pbQPU.RequestStream) error {
	request := requestRec.GetRequest()
	log.WithFields(log.Fields{"request": request}).Debug("query request received")
	subQueries, err := q.generateSubQueries(request.GetPredicate())
	if err != nil {
		return err
	}
	subQueryCount := len(subQueries)
	var mutex sync.Mutex
	subQueryResponseRecordCh := make(chan *pbQPU.ResponseStreamRecord)
	errCh := make(chan error)
	seqID := int64(0)
	for _, subQ := range subQueries {
		streamIn, _, err := subQ.Client.Query(request.GetPredicate(), protoutils.SnapshotTimePredicate(request.GetClock().GetLbound(), request.GetClock().GetUbound()), request.GetMetadata(), false)
		if err != nil {
			return err
		}
		go func() {
			for {
				streamRec, err := streamIn.Recv()
				if err == io.EOF {
					mutex.Lock()
					subQueryCount--
					if subQueryCount == 0 {
						close(subQueryResponseRecordCh)
						close(errCh)
					}
					mutex.Unlock()
					return
				} else if err != nil {
					errCh <- err
					return
				}
				subQueryResponseRecordCh <- streamRec
			}
		}()
	}
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

// GetConfig implements the GetConfig API for the filter QPU
func (q *FQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup is called when the process receives a SIGTERM signcal
func (q *FQPU) Cleanup() {
	log.Info("federation dispatcher QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//---------------- Internal Functions --------------

func (q *FQPU) generateSubQueries(predicate []*pbUtils.AttributePredicate) ([]*utils.QPU, error) {
	forwardTo := make([]*utils.QPU, 0)
	for _, c := range q.qpu.Conns {
		capabl, err := utils.CanRespondToQuery(predicate, c.QueryingCapabilities)
		if err != nil {
			return nil, err
		}
		if capabl {
			forwardTo = append(forwardTo, c)
		}
	}
	return forwardTo, nil
}

func forward(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	log.WithFields(log.Fields{
		"record": streamRec,
	}).Debug("Federation QPU: received input stream record")

	//TODO fix sequenceIDs
	err := streamOut.Send(
		protoutils.ResponseStreamRecord(
			*seqID,
			streamRec.GetType(),
			streamRec.GetLogOp(),
		))
	(*seqID)++
	return err
}
