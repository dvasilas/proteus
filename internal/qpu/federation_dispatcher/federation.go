package federation

import (
	"io"
	"os"
	"sync"

	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	log "github.com/sirupsen/logrus"
)

// FQPU implements a federation dispatcher QPU
type FQPU struct {
	qpu                 *utils.QPU
	config              *config.Config
	dataTransferCount   float32
	dataTransferMutex   sync.Mutex
	measureDataTransfer bool
}

//---------------- API Functions -------------------

// QPU creates a federation dispatcher QPU
func QPU(conf *config.Config) (*FQPU, error) {
	q := &FQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
	}
	q.measureDataTransfer = os.Getenv("MEASURE_DATA_TRANSFER") == "true"
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}
	return q, nil
}

// Query implements the Query API for the federation dispatcher QPU
func (q *FQPU) Query(streamOut qpu_api.QPU_QueryServer, query *qpu_api.QueryInternalQuery, metadata map[string]string, block bool) error {
	log.WithFields(log.Fields{"query": query}).Debug("query received")
	subQueries, isLocal, err := q.generateSubQueries(query.GetPredicate())
	if err != nil {
		return err
	}
	subQueryCount := len(subQueries)
	var mutex sync.Mutex
	subQueryResponseRecordCh := make(chan *qpu_api.ResponseStreamRecord)
	errCh := make(chan error)
	seqID := int64(0)
	for i, subQ := range subQueries {
		streamIn, _, err := subQ.Client.Query(query.GetBucket(), query.GetPredicate(), protoutils.SnapshotTimePredicate(query.GetClock().GetLbound(), query.GetClock().GetUbound()), metadata, false)
		if err != nil {
			return err
		}
		go func(local bool) {
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
				if q.measureDataTransfer && !local {
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
		}(isLocal[i])
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
				if streamRec.GetType() == qpu_api.ResponseStreamRecord_END_OF_STREAM {
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
	q.dataTransferMutex.Lock()
	res := q.dataTransferCount
	q.dataTransferMutex.Unlock()
	return res
}

// Cleanup is called when the process receives a SIGTERM signcal
func (q *FQPU) Cleanup() {
	log.Info("federation dispatcher QPU cleanup")
}

//----------- Stream Consumer Functions ------------

//---------------- Internal Functions --------------

func (q *FQPU) generateSubQueries(predicate []*qpu.AttributePredicate) ([]*utils.QPU, []bool, error) {
	forwardTo := make([]*utils.QPU, 0)
	isLocal := make([]bool, 0)
	for i, c := range q.qpu.Conns {
		capabl, err := utils.CanRespondToQuery(predicate, c.QueryingCapabilities)
		if err != nil {
			return nil, nil, err
		}
		if capabl {
			forwardTo = append(forwardTo, c)
			isLocal = append(isLocal, q.qpu.Config.Connections[i].Local)
		}
	}
	return forwardTo, isLocal, nil
}

func forward(pred []*qpu.AttributePredicate, streamRec *qpu_api.ResponseStreamRecord, streamOut qpu_api.QPU_QueryServer, seqID *int64) error {
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
