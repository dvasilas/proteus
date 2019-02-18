package antidoteindex

import (
	antidote "github.com/AntidoteDB/antidote-go-client"
	utils "github.com/dimitriosvasilas/proteus"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	s3tafg "github.com/dimitriosvasilas/proteus/qpu/index/antidote/s3TagF"
	log "github.com/sirupsen/logrus"
)

//AntidoteIndex ...
type AntidoteIndex struct {
	antidoteCli *antidote.Client
	bucket      antidote.Bucket
	index       Implementation
	state       map[string]utils.Posting
}

//Implementation ...
type Implementation interface {
	Put(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet, buck antidote.Bucket, tx antidote.Transaction) error
	Get(p []*pbQPU.Predicate, buck antidote.Bucket, cli *antidote.Client) (map[string]utils.Posting, bool, error)
	RemoveOldEntry(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, state map[string]utils.Posting, buck antidote.Bucket, tx antidote.Transaction) error
	FilterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string)
}

//New ...
func New(indexType string, host string, port int, buck string) (*AntidoteIndex, error) {
	c, err := antidote.NewClient(antidote.Host{Name: host, Port: port})
	if err != nil {
		return &AntidoteIndex{}, err
	}
	bucket := antidote.Bucket{Bucket: []byte(buck)}
	return &AntidoteIndex{
		antidoteCli: c,
		index:       s3tafg.New(),
		bucket:      bucket,
		state:       make(map[string]utils.Posting),
	}, nil
}

//Update ...
func (i *AntidoteIndex) Update(op *pbQPU.Operation, attribute string, lbound *pbQPU.Value, ubound *pbQPU.Value) error {
	switch op.GetOpPayload().Payload.(type) {
	case *pbQPU.OperationPayload_State:
		for k, v := range op.GetOpPayload().GetState().GetAttributes() {
			if indexable, k := i.index.FilterIndexable(k, v, lbound, ubound); indexable {
				tx, err := i.antidoteCli.StartTransaction()
				if err != nil {
					return err
				}
				i.index.RemoveOldEntry(k, v, op.GetOpPayload().GetState(), i.state, i.bucket, tx)
				if err := i.index.Put(k, v, op.GetOpPayload().GetState(), op.GetDataSet(), i.bucket, tx); err != nil {
					return err
				}
				err = tx.Commit()
				if err != nil {
					return err
				}
			}
		}
		p := utils.Posting{
			Object:  *op.GetOpPayload().GetState(),
			Dataset: *op.GetDataSet(),
		}
		i.state[op.GetOpPayload().GetState().GetKey()] = p
	case *pbQPU.OperationPayload_Op:
		log.Debug("index:Update: OperationPayload_Op")
	}
	return nil
}

//Lookup ...
func (i *AntidoteIndex) Lookup(p []*pbQPU.Predicate) (map[string]utils.Posting, bool, error) {
	return i.index.Get(p, i.bucket, i.antidoteCli)
}
