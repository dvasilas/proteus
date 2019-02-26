package s3tagf

import (
	"strings"

	antidote "github.com/AntidoteDB/antidote-go-client"
	utils "github.com/dimitriosvasilas/proteus"
	attribute "github.com/dimitriosvasilas/proteus/attributes"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
)

//S3TagF ...
type S3TagF struct {
}

//New ...
func New() *S3TagF {
	return &S3TagF{}
}

//Put ...
func (i *S3TagF) Put(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet, buck antidote.Bucket, tx antidote.Transaction) error {
	log.WithFields(log.Fields{
		"attrKey": attrKey,
		"attrVal": attrVal,
		"obj":     obj,
		"ds":      ds,
	}).Debug("index Put")
	indexEntry, err := utils.EncodeIndexEntry(*obj, *ds)
	if err != nil {
		return err
	}
	return buck.Update(tx, antidote.MapUpdate(antidote.Key(attrKey), antidote.SetAdd(antidote.Key(utils.ValToString(attrVal)), indexEntry)))
}

//Get ...
func (i *S3TagF) Get(p []*pbQPU.AttributePredicate, buck antidote.Bucket, cli *antidote.Client) (map[string]utils.Posting, bool, error) {
	tx := cli.CreateStaticTransaction()
	mapVal, err := buck.ReadMap(tx, antidote.Key([]byte(p[0].GetAttribute())))
	if err != nil {
		return nil, true, err
	}
	setVal, err := mapVal.Set(antidote.Key(utils.ValToString(p[0].GetLbound())))
	if err != nil {
		return nil, false, err
	}
	res := make(map[string]utils.Posting)
	for _, x := range setVal {
		p, err := utils.DecodeIndexEntry(x)
		if err != nil {
			return nil, true, err
		}
		res[p.Object.GetKey()] = p
	}
	return res, true, nil
}

//RemoveOldEntry ...
func (i *S3TagF) RemoveOldEntry(attrKey string, attrVal *pbQPU.Value, obj *pbQPU.Object, state map[string]utils.Posting, buck antidote.Bucket, tx antidote.Transaction) error {
	log.WithFields(log.Fields{
		"attrKey":     attrKey,
		"new attrVal": attrVal,
		"obj":         obj,
		"state":       state,
	}).Debug("index RemoveOldEntry")
	if s, ok := state[obj.Key]; ok {
		_, olgAttrVal, err := attribute.Attr(attrKey, &s.Object)
		if err != nil {
			return err
		}
		indexEntry, err := utils.EncodeIndexEntry(s.Object, s.Dataset)
		if err != nil {
			return err
		}
		buck.Update(tx, antidote.MapUpdate(antidote.Key(attrKey), antidote.SetRemove(antidote.Key(utils.ValToString(olgAttrVal)), indexEntry)))
	}
	return nil
}

//FilterIndexable ...
func (i *S3TagF) FilterIndexable(attrKey string, attrVal *pbQPU.Value, lb *pbQPU.Value, ub *pbQPU.Value) (bool, string) {
	switch attrVal.Val.(type) {
	case *pbQPU.Value_Flt:
		if strings.HasPrefix(attrKey, "x-amz-meta-f-") {
			if attrVal.GetFlt() > lb.GetFlt() && attrVal.GetFlt() <= ub.GetFlt() {
				return true, strings.TrimPrefix(attrKey, "x-amz-meta-f-")
			}
		}
	default:
		return false, attrKey
	}
	return false, attrKey
}
