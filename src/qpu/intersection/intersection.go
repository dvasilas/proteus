package intersection

import (
	"io"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// IQPU implements an intersection QPU
type IQPU struct {
	qpu    *utils.QPU
	config *config.Config
}

type response struct {
	streamID       string
	objectKey      string
	responseRecord *pbQPU.ResponseStreamRecord
}

//---------------- API Functions -------------------

// QPU creates a federation dispatcher QPU
func QPU(conf *config.Config) (*IQPU, error) {
	q := &IQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
	}
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}

	return q, nil
}

// Query implements the Query API for the intersection QPU
func (q *IQPU) Query(streamOut pbQPU.QPU_QueryServer, query *pbQPU.QueryInternalQuery, metadata map[string]string, block bool) error {
	log.WithFields(log.Fields{"query": query, "QPU": "intersection"}).Debug("query received")
	subQueries, err := q.generateSubQueries(query.GetPredicate())
	if err != nil {
		return err
	}

	for _, subQ := range subQueries {
		log.WithFields(log.Fields{"q": subQ.SubQuery, "c": subQ.Endpoint.Config.Port}).Debug("subQuery")
	}

	intersectionM := make(map[string]map[string]bool)
	responseCh := make(chan response)
	errCh := make(chan error)

	for _, subQ := range subQueries {
		streamIn, _, err := subQ.Endpoint.Client.Query(query.GetBucket(), subQ.SubQuery, protoutils.SnapshotTimePredicate(query.GetClock().GetLbound(), query.GetClock().GetUbound()), nil, false)
		if err != nil {
			return err
		}
		go queryResponseConsumer(subQ.Endpoint.Config.Port, streamIn, responseCh, errCh)
	}

	streamCnt := len(subQueries)
	seqID := int64(0)
	for streamCnt > 0 {
		select {
		case resp := <-responseCh:
			if intersectionM[resp.objectKey] == nil {
				intersectionM[resp.objectKey] = make(map[string]bool)
				for _, subQ := range subQueries {
					intersectionM[resp.objectKey][subQ.Endpoint.Config.Port] = false
				}
			}
			intersectionM[resp.objectKey][resp.streamID] = true
			f := true
			for _, v := range intersectionM[resp.objectKey] {
				f = f && v
			}
			if f {
				if err := streamOut.Send(
					protoutils.ResponseStreamRecord(
						seqID,
						resp.responseRecord.GetType(),
						resp.responseRecord.GetLogOp(),
					)); err != nil {
					return err
				}
				(seqID)++
			}
		case err := <-errCh:
			if err == io.EOF {
				streamCnt--
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

// GetConfig implements the GetConfig API for the filter QPU
func (q *IQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// GetDataTransfer ...
func (q *IQPU) GetDataTransfer() float32 {
	return 0
}

// Cleanup is called when the process receives a SIGTERM signcal
func (q *IQPU) Cleanup() {
	log.Info("intersection QPU cleanup")
}

//----------- Stream Consumer Functions ------------

func queryResponseConsumer(streamID string, streamIn pbQPU.QPU_QueryClient, responseCh chan response, errChan chan error) {
	for {
		streamRec, err := streamIn.Recv()
		if err == io.EOF {
			errChan <- err
			return
		} else if err != nil {
			errChan <- err
			return
		}
		responseCh <- response{
			streamID:       streamID,
			objectKey:      streamRec.GetLogOp().GetObjectId(),
			responseRecord: streamRec,
		}
	}
}

//---------------- Internal Functions --------------

func (q *IQPU) generateSubQueries(predicate []*pbUtils.AttributePredicate) ([]utils.SubQuery, error) {
	subQs := make([]utils.SubQuery, 0)
	endpoints := make(map[string]*utils.QPU)
	capabilities := make(map[string][]*pbUtils.AttributePredicate)
	for _, c := range q.qpu.Conns {
		endpoints[c.Config.Port] = c
		capabilities[c.Config.Port] = make([]*pbUtils.AttributePredicate, 0)
	}
	for _, p := range predicate {
		for _, c := range q.qpu.Conns {
			capabl, err := utils.CanRespondToQuery([]*pbUtils.AttributePredicate{p}, c.QueryingCapabilities)
			if err != nil {
				return nil, err
			}
			if capabl {
				capabilities[c.Config.Port] = append(capabilities[c.Config.Port], p)
			}
		}
	}
	for k, endP := range endpoints {
		if len(capabilities[k]) > 0 {
			subQs = append(subQs, utils.SubQuery{
				SubQuery: capabilities[k],
				Endpoint: endP,
			})
		}
	}
	return subQs, nil
}

func forward(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	log.WithFields(log.Fields{
		"record": streamRec,
	}).Debug("Intersection QPU: received input stream record")

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
