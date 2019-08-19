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
func (q *FQPU) Query(streamOut pbQPU.QPU_QueryServer) error {
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
	utils.QueryResponseConsumer(req.GetPredicate(), streamIn, streamOut, forward, errChan)
	err = <-errChan
	if err != io.EOF {
		return err
	}
	return nil
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

//forward checks if a given object matches a given predicate
// if yes, it sends it to a given stream
func forward(pred []*pbUtils.AttributePredicate, streamRec *pbQPU.ResponseStreamRecord, streamOut pbQPU.QPU_QueryServer, seqID *int64) error {
	log.WithFields(log.Fields{
		"record": streamRec,
		"pred":   pred,
	}).Debug("FQPU: received input stream record")

	match, err := Filter(pred, streamRec)
	if err != nil {
		return err
	}
	if match {
		log.WithFields(log.Fields{"Object": streamRec.GetLogOp()}).Debug("Object matches query")
		err := streamOut.Send(
			protoutils.ResponseStreamRecord(
				*seqID,
				streamRec.GetType(),
				streamRec.GetLogOp(),
			))
		(*seqID)++
		return err
	}
	log.WithFields(log.Fields{"Object": streamRec.GetLogOp()}).Debug("Object does not match query")
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
			match, err := AttrMatchesPredicate(pred, attr)
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

// AttrMatchesPredicate checks if an object attribute matches a given predicate.
func AttrMatchesPredicate(predicate *pbUtils.AttributePredicate, attr *pbUtils.Attribute) (bool, error) {
	if keyMatch(predicate.GetAttr().GetAttrKey(), attr) && typeMatch(predicate.GetAttr().GetAttrType(), attr) {
		return rangeMatch(predicate, attr)
	}
	return false, nil
}

func keyMatch(objectName string, attr *pbUtils.Attribute) bool {
	if objectName == attr.GetAttrKey() {
		return true
	}
	return false
}

func typeMatch(t pbUtils.Attribute_AttributeType, attr *pbUtils.Attribute) bool {
	if t == attr.GetAttrType() {
		return true
	}
	return false
}

// within the range [greaterOrEqual, lessThan)
func rangeMatch(pred *pbUtils.AttributePredicate, attr *pbUtils.Attribute) (bool, error) {
	lb, err := utils.Compare(attr.GetValue(), pred.GetLbound())
	if err != nil {
		return false, err
	}
	ub, err := utils.Compare(attr.GetValue(), pred.GetUbound())
	if err != nil {
		return false, err
	}
	if lb >= 0 && ub < 0 {
		return true, nil
	}
	return false, nil
}
