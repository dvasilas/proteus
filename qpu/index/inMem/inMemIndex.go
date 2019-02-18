package inmemindex

import (
	"errors"
	"strings"

	utils "github.com/dimitriosvasilas/proteus"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	btreeIndex "github.com/dimitriosvasilas/proteus/qpu/index/inMem/btree"
	log "github.com/sirupsen/logrus"
)

//InMemIntex ...
type InMemIntex struct {
	index Implementation
	state map[string]pbQPU.Object
}

//Implementation ...
type Implementation interface {
	Put(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet) error
	Get(p []*pbQPU.Predicate) (map[string]utils.Posting, bool, error)
	RemoveOldEntry(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, state map[string]pbQPU.Object) error
	FilterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string)
}

//New ...
func New(indexType string) (*InMemIntex, error) {
	i := &InMemIntex{
		state: make(map[string]pbQPU.Object),
	}
	var impl Implementation
	var err error
	indexT := strings.Split(indexType, "_")
	switch indexT[0] {
	case "btree":
		impl, err = btreeIndex.New(indexT[1])
		if err != nil {
			return &InMemIntex{}, errors.New("unknown in_mem index type")
		}
	default:
		return &InMemIntex{}, errors.New("unknown in_mem index type")
	}
	i.index = impl
	return i, nil
}

//Update ...
func (i *InMemIntex) Update(op *pbQPU.Operation, attribute string, lbound *pbQPU.Value, ubound *pbQPU.Value) error {
	switch op.GetOpPayload().Payload.(type) {
	case *pbQPU.OperationPayload_State:
		for k, v := range op.GetOpPayload().GetState().GetAttributes() {
			if indexable, k := i.index.FilterIndexable(k, v, lbound, ubound); indexable {
				if op.GetOpId() != "catchUp" {
					i.index.RemoveOldEntry(k, v, op.GetOpPayload().GetState(), i.state)
				}
				if err := i.index.Put(k, v, op.GetOpPayload().GetState(), op.GetDataSet()); err != nil {
					return err
				}
			}
		}
		i.state[op.GetOpPayload().GetState().GetKey()] = *op.GetOpPayload().GetState()
	case *pbQPU.OperationPayload_Op:
		log.Debug("index:Update: OperationPayload_Op")
	}

	return nil
}

//Lookup ...
func (i *InMemIntex) Lookup(p []*pbQPU.Predicate) (map[string]utils.Posting, bool, error) {
	return i.index.Get(p)
}
