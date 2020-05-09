package antidoteindex

import (
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	antidote "github.com/AntidoteDB/antidote-go-client"
	utils "github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/proto"
	"github.com/dvasilas/proteus/src/proto/qpu"
	log "github.com/sirupsen/logrus"
)

const maxVersionCount = 10

// AntidoteIndex ...
type AntidoteIndex struct {
	attributeName string
	attributeType config.DatastoreAttributeType
	client        *antidote.Client
	bucket        antidote.Bucket
	mutex         sync.RWMutex
	config        *config.Config
}

//---------------- API Functions -------------------

// New ...
func New(conf *config.Config) (*AntidoteIndex, error) {
	rand.Seed(time.Now().UnixNano())
	endpoint := strings.Split(conf.IndexConfig.IndexStore.Endpoint, ":")
	port, err := strconv.ParseInt(endpoint[1], 10, 64)
	if err != nil {
		return &AntidoteIndex{}, err
	}
	c, err := antidote.NewClient(antidote.Host{Name: endpoint[0], Port: int(port)})
	if err != nil {
		return &AntidoteIndex{}, err
	}
	buckName := conf.IndexConfig.IndexStore.Bucket + string(genReference())
	bucket := antidote.Bucket{Bucket: []byte(buckName)}
	return &AntidoteIndex{
		attributeName: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrKey(),
		attributeType: conf.GetAttributeType(buckName, conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrKey()),
		client:        c,
		bucket:        bucket,
		config:        conf,
	}, nil
}

// UpdateCatchUp ...
func (i *AntidoteIndex) UpdateCatchUp(attr *qpu.Attribute, object utils.ObjectState, ts qpu.Vectorclock) error {
	indexStoreUpdates := make([]*antidote.CRDTUpdate, 0)
	objectEncoded, err := object.Marshal()
	if err != nil {
		return err
	}
	successful := false
	factor := 1
	for !successful {
		i.mutex.Lock()
		tx, err := i.client.StartTransaction()
		if err != nil {
			i.mutex.Unlock()
			return err
		}
		valueIndex, err := i.getValueIndex(attr, tx)
		if err != nil {
			i.mutex.Unlock()
			return err
		}
		_, versionIndex, err := i.getVersionIndexPoint(valueIndex, attr, tx)
		var postingListRef []byte
		if err != nil && strings.Contains(err.Error(), "not found") {
			var versionIndexUpdate *antidote.CRDTUpdate
			valueIndexUpdate, versionIndexRef, err := i.updateValueIndex(attr)
			if err != nil {
				i.mutex.Unlock()
				utils.ReportError(err)
				return err
			}
			marshalledVC, err := utils.MarshalVectorClock(&ts)
			if err != nil {
				i.mutex.Unlock()
				utils.ReportError(err)
				return err
			}
			versionIndexUpdate, postingListRef = appendNewVersion(versionIndexRef, marshalledVC, tx)
			indexStoreUpdates = append(indexStoreUpdates,
				valueIndexUpdate,
				versionIndexUpdate,
			)
		} else {
			postingListRef, err = getLatestPostingList(versionIndex)
			if err != nil {
				i.mutex.Unlock()
				return err
			}
		}
		indexStoreUpdates = append(indexStoreUpdates,
			antidote.SetAdd(antidote.Key([]byte(postingListRef)), []byte(object.ObjectID)),
			antidote.RegPut(antidote.Key([]byte(object.ObjectID)), objectEncoded),
		)
		if err := i.bucket.Update(tx, indexStoreUpdates...); err != nil {
			i.mutex.Unlock()
			return err
		}
		err = tx.Commit()
		if err == nil {
			successful = true
			i.mutex.Unlock()
		} else {
			i.mutex.Unlock()
			backoff := rand.Intn(10 * factor)
			log.WithFields(log.Fields{"error": err}).Info("indexUpdate: transaction commit error")
			log.WithFields(log.Fields{"error": err, "after": time.Millisecond * time.Duration((backoff))}).Info("indexUpdate: retrying")
			time.Sleep(time.Millisecond * time.Duration(backoff))
			factor *= 2
		}
	}
	return err
}

func (i *AntidoteIndex) updateOldIndexEntry(attrOld *qpu.Attribute, object utils.ObjectState, ts qpu.Vectorclock, tx *antidote.InteractiveTransaction, indexStoreUpdates []*antidote.CRDTUpdate, objectEncoded []byte) ([]*antidote.CRDTUpdate, bool, error) {
	valueIndex, err := i.getValueIndex(attrOld, tx)
	if err != nil {
		return indexStoreUpdates, true, err
	}
	versionIndexRef, versionIndex, err := i.getVersionIndexPoint(valueIndex, attrOld, tx)
	if err != nil {
		return indexStoreUpdates, true, err
	}
	postingListRefO, err := getLatestPostingList(versionIndex)
	if err != nil {
		return indexStoreUpdates, true, err
	}
	postingListO, err := i.getPostingList(postingListRefO, tx)
	if err != nil {
		return indexStoreUpdates, true, err
	}
	if len(versionIndex.ListMapKeys()) >= maxVersionCount {
		valueIndexUpdate, versionIndexRefN, err := i.updateValueIndex(attrOld)
		if err != nil {
			return indexStoreUpdates, false, err
		}
		indexStoreUpdates = append(indexStoreUpdates,
			antidote.MapUpdate(
				antidote.Key(versionIndexRefN),
				antidote.RegPut(antidote.Key([]byte("prev")), versionIndexRef),
			),
		)
		versionIndexRef = versionIndexRefN
		indexStoreUpdates = append(indexStoreUpdates,
			valueIndexUpdate,
		)
	}
	marshalledVC, err := utils.MarshalVectorClock(&ts)
	if err != nil {
		return indexStoreUpdates, false, err
	}
	versionIndexUpdate, postingListRefN := appendNewVersion(versionIndexRef, marshalledVC, tx)
	indexStoreUpdates = append(indexStoreUpdates,
		versionIndexUpdate,
		antidote.SetAdd(antidote.Key([]byte(postingListRefN)), postingListO...),
		antidote.SetRemove(antidote.Key([]byte(postingListRefN)), []byte(object.ObjectID)),
	)
	return indexStoreUpdates, false, nil
}

func (i *AntidoteIndex) updateNewIndexEntry(attrNew *qpu.Attribute, object utils.ObjectState, ts qpu.Vectorclock, tx *antidote.InteractiveTransaction, indexStoreUpdates []*antidote.CRDTUpdate, objectEncoded []byte) ([]*antidote.CRDTUpdate, bool, error) {
	valueIndex, err := i.getValueIndex(attrNew, tx)
	if err != nil {
		return indexStoreUpdates, true, err
	}
	versionIndexRef, versionIndex, err := i.getVersionIndexPoint(valueIndex, attrNew, tx)
	if err != nil && strings.Contains(err.Error(), "not found") {
		valueIndexUpdate, versionIndexRef, err := i.updateValueIndex(attrNew)
		if err != nil {
			return indexStoreUpdates, true, err
		}
		marshalledVC, err := utils.MarshalVectorClock(&ts)
		if err != nil {
			return indexStoreUpdates, true, err
		}
		versionIndexUpdate, postingListRef := appendNewVersion(versionIndexRef, marshalledVC, tx)
		indexStoreUpdates = append(indexStoreUpdates,
			valueIndexUpdate,
			versionIndexUpdate,
			antidote.SetAdd(antidote.Key([]byte(postingListRef)), []byte(object.ObjectID)),
			antidote.RegPut(antidote.Key([]byte(object.ObjectID)), objectEncoded),
		)
	} else {
		postingListRefO, err := getLatestPostingList(versionIndex)
		if err != nil {
			return indexStoreUpdates, true, err
		}
		postingListO, err := i.getPostingList(postingListRefO, tx)
		if err != nil {
			return indexStoreUpdates, true, err
		}
		if len(versionIndex.ListMapKeys()) >= maxVersionCount {
			valueIndexUpdate, versionIndexRefN, err := i.updateValueIndex(attrNew)
			if err != nil {
				return indexStoreUpdates, false, err
			}
			indexStoreUpdates = append(indexStoreUpdates,
				antidote.MapUpdate(
					antidote.Key(versionIndexRefN),
					antidote.RegPut(antidote.Key([]byte("prev")), versionIndexRef),
				),
			)
			versionIndexRef = versionIndexRefN
			indexStoreUpdates = append(indexStoreUpdates,
				valueIndexUpdate,
			)
		}
		marshalledVC, err := utils.MarshalVectorClock(&ts)
		if err != nil {
			return indexStoreUpdates, false, err
		}
		versionIndexUpdate, postingListRefNewN := appendNewVersion(versionIndexRef, marshalledVC, tx)
		indexStoreUpdates = append(indexStoreUpdates,
			versionIndexUpdate,
			antidote.SetAdd(antidote.Key([]byte(postingListRefNewN)), postingListO...),
			antidote.SetAdd(antidote.Key([]byte(postingListRefNewN)), []byte(object.ObjectID)),
			antidote.RegPut(antidote.Key([]byte(object.ObjectID)), objectEncoded),
		)
	}
	return indexStoreUpdates, false, nil
}

// Update ...
func (i *AntidoteIndex) Update(attrOld *qpu.Attribute, attrNew *qpu.Attribute, object utils.ObjectState, ts qpu.Vectorclock) error {
	indexStoreUpdates := make([]*antidote.CRDTUpdate, 0)
	objectEncoded, err := object.Marshal()
	if err != nil {
		return err
	}
	successful := false
	factor := 1
	for !successful {
		i.mutex.Lock()
		tx, err := i.client.StartTransaction()
		if err != nil {
			i.mutex.Unlock()
			return err
		}
		var okToFail bool
		if attrOld != nil {
			indexStoreUpdates, okToFail, err = i.updateOldIndexEntry(attrOld, object, ts, tx, indexStoreUpdates, objectEncoded)
			if err != nil {
				utils.ReportError(err)
				if !okToFail {
					i.mutex.Unlock()
					return err
				}
			}
		}
		if attrNew != nil {
			indexStoreUpdates, okToFail, err = i.updateNewIndexEntry(attrNew, object, ts, tx, indexStoreUpdates, objectEncoded)
			if err != nil {
				utils.ReportError(err)
				if !okToFail {
					i.mutex.Unlock()
					return err
				}
			}
		}
		if len(indexStoreUpdates) > 0 {
			if err := i.bucket.Update(tx, indexStoreUpdates...); err != nil {
				i.mutex.Unlock()
				return err
			}
		}
		err = tx.Commit()
		if err == nil {
			successful = true
			i.mutex.Unlock()
		} else {
			i.mutex.Unlock()
			log.WithFields(log.Fields{"error": err}).Info("indexUpdate: transaction commit error")
			log.WithFields(log.Fields{"error": err, "after": time.Millisecond * time.Duration((10 * factor))}).Info("indexUpdate: retrying")

			time.Sleep(time.Millisecond * time.Duration((10 * factor)))
			factor *= 2
		}
	}
	return err
}

// Lookup ...
func (i *AntidoteIndex) Lookup(attr *qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, lookupResCh chan utils.ObjectState, errCh chan error) {
	tx := i.client.CreateStaticTransaction()
	valueIndex, err := i.getValueIndex(attr.GetAttr(), tx)
	if err != nil {
		utils.ReportError(err)
		errCh <- err
		return
	}
	versionIndexArr, err := i.getVersionIndexRange(valueIndex, attr, tx)
	if err != nil {
		utils.ReportError(err)
		errCh <- err
		return
	}
	for _, vIndex := range versionIndexArr {
		lastVRef, err := getLatestPostingList(vIndex)
		if err != nil {
			utils.ReportError(err)
			errCh <- err
			return
		}
		postingList, err := i.getPostingList(lastVRef, tx)
		if err != nil {
			utils.ReportError(err)
			errCh <- err
			return
		}
		for _, objectID := range postingList {
			objEnc, err := i.bucket.ReadReg(tx, antidote.Key(objectID))
			if err != nil {
				utils.ReportError(err)
				errCh <- err
				return
			}
			obj, err := utils.UnmarshalObject(objEnc)
			if err != nil {
				utils.ReportError(err)
				errCh <- err
				return
			}
			lookupResCh <- obj
		}
	}
	close(lookupResCh)
	close(errCh)
}

//---------------- Internal Functions --------------

func (i *AntidoteIndex) getValueIndex(attr *qpu.Attribute, tx antidote.Transaction) (*antidote.MapReadResult, error) {
	attributeTypeStr, err := config.AttributeTypeToString(i.config.GetAttributeType(i.config.IndexConfig.Bucket, attr.GetAttrKey()))
	if err != nil {
		return nil, err
	}
	valueIndexRef := attr.GetAttrKey() + "_" + attributeTypeStr
	return i.bucket.ReadMap(tx, antidote.Key([]byte(valueIndexRef)))
}

func (i *AntidoteIndex) getVersionIndexPoint(valueIndex *antidote.MapReadResult, attr *qpu.Attribute, tx antidote.Transaction) ([]byte, *antidote.MapReadResult, error) {
	versionIndexRef, err := valueIndex.Reg(antidote.Key(utils.ValueToString(attr.GetValue())))
	if err != nil {
		return nil, nil, err
	}
	versionIndex, err := i.bucket.ReadMap(tx, antidote.Key(versionIndexRef))
	if err != nil {
		return nil, nil, err
	}
	return versionIndexRef, versionIndex, nil
}

func (i *AntidoteIndex) getVersionIndexRange(valueIndex *antidote.MapReadResult, predicate *qpu.AttributePredicate, tx antidote.Transaction) ([]*antidote.MapReadResult, error) {
	res := make([]*antidote.MapReadResult, 0)
	c, err := utils.Compare(predicate.GetLbound(), predicate.GetUbound())
	if err != nil {
		utils.ReportError(err)
		return nil, err
	}
	if c == 0 {
		versionIndexRef, err := valueIndex.Reg(antidote.Key(utils.ValueToString(predicate.GetLbound())))
		if err != nil && strings.Contains(err.Error(), "not found") {
			return res, nil
		}
		versionIndex, err := i.bucket.ReadMap(tx, antidote.Key(versionIndexRef))
		if err != nil {
			utils.ReportError(err)
			return nil, err
		}
		res = append(res, versionIndex)
	} else {
		vIndexEntryKeys := valueIndex.ListMapKeys()
		for _, vIndexEntryK := range vIndexEntryKeys {
			vIndexEntryKv, err := i.config.StringToValue(i.config.IndexConfig.Bucket, i.attributeName, string(vIndexEntryK.Key))
			if err != nil {
				utils.ReportError(err)
				return nil, err
			}
			attr := protoutils.Attribute(predicate.GetAttr().GetAttrKey(), vIndexEntryKv)
			match, err := utils.AttrMatchesPredicate(predicate, attr)
			if err != nil {
				utils.ReportError(err)
				return nil, err
			}
			if match {
				versionIndexRef, err := valueIndex.Reg(antidote.Key(vIndexEntryK.Key))
				if err != nil {
					utils.ReportError(err)
					return nil, err
				}
				versionIndex, err := i.bucket.ReadMap(tx, antidote.Key(versionIndexRef))
				if err != nil {
					utils.ReportError(err)
					return nil, err
				}
				res = append(res, versionIndex)
			}
		}
	}
	return res, nil
}

func (i *AntidoteIndex) getPostingList(postingListRef []byte, tx antidote.Transaction) ([][]byte, error) {
	return i.bucket.ReadSet(tx, antidote.Key(postingListRef))
}

func (i *AntidoteIndex) updateValueIndex(attr *qpu.Attribute) (*antidote.CRDTUpdate, []byte, error) {
	ref := genReference()
	attributeTypeStr, err := config.AttributeTypeToString(i.config.GetAttributeType(i.config.IndexConfig.Bucket, attr.GetAttrKey()))
	if err != nil {
		return nil, ref, err
	}
	valueIndexRef := attr.GetAttrKey() + "_" + attributeTypeStr
	return antidote.MapUpdate(
		antidote.Key(valueIndexRef),
		antidote.RegPut(antidote.Key(utils.ValueToString(attr.GetValue())), ref),
	), ref, nil
}

func appendNewVersion(versionIndexRef []byte, tsKey []byte, tx antidote.Transaction) (*antidote.CRDTUpdate, []byte) {
	ref := genReference()
	return antidote.MapUpdate(
		antidote.Key(versionIndexRef),
		antidote.RegPut(antidote.Key(tsKey), ref),
	), ref
}

func getLatestPostingList(versionIndex *antidote.MapReadResult) ([]byte, error) {
	versions := versionIndex.ListMapKeys()
	var latestVRef antidote.Key
	if len(versions) == 1 {
		latestVRef = antidote.Key(versions[0].Key)
	} else {
		versionsTs, err := decodeTimestamps(versions)
		if err != nil {
			return nil, err
		}
		latestVTs := lastVersion(versionsTs)
		marshalledVC, err := utils.MarshalVectorClock(latestVTs)
		if err != nil {
			return nil, err
		}
		latestVRef = antidote.Key(marshalledVC)
	}
	return versionIndex.Reg(latestVRef)
}

func decodeTimestamps(tsEncArr []antidote.MapEntryKey) ([]*qpu.Vectorclock, error) {
	res := make([]*qpu.Vectorclock, 0)
	for _, ts := range tsEncArr {
		if string(ts.Key) != "prev" {
			vc, err := utils.UnmarshalVectorClock(ts.Key)
			if err != nil {
				return nil, err
			}
			res = append(res, &vc)
		}
	}
	return res, nil
}

func genReference() []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 20)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func lastVersion(tsList []*qpu.Vectorclock) *qpu.Vectorclock {
	max := tsList[0]
	for _, ts := range tsList {
		if greater(ts, max) {
			max = ts
		}
	}
	return max
}

func greater(a, b *qpu.Vectorclock) bool {
	var greater bool
	bMap := b.GetVc()
	for k, ts := range a.GetVc() {
		if bMap[k] > ts {
			return false
		} else if bMap[k] < ts {
			greater = true
		}
	}
	return greater
}

func (i *AntidoteIndex) print() error {
	log.Debug("printing index")
	attributeTypeStr, err := config.AttributeTypeToString(i.attributeType)
	if err != nil {
		return err
	}
	valueIndexRef := i.attributeName + "_" + attributeTypeStr
	tx, err := i.client.StartTransaction()
	if err != nil {
		return err
	}
	valueIndex, err := i.bucket.ReadMap(tx, antidote.Key([]byte(valueIndexRef)))
	if err != nil {
		return err
	}
	values := valueIndex.ListMapKeys()
	for _, val := range values {
		versionIndexRef, err := valueIndex.Reg(antidote.Key(val.Key))
		if err != nil {
			return err
		}
		log.WithFields(log.Fields{"key": string(val.Key), "value": string(versionIndexRef)}).Debug("[index.print] value index entry")
		versionIndex, err := i.bucket.ReadMap(tx, antidote.Key((versionIndexRef)))
		if err != nil {
			return err
		}
		versions := versionIndex.ListMapKeys()
		for _, vers := range versions {
			postingListRef, err := versionIndex.Reg(antidote.Key(vers.Key))
			if err != nil {
				return err
			}
			vs, err := utils.UnmarshalVectorClock(vers.Key)
			if err != nil {
				return err
			}
			log.WithFields(log.Fields{"key": vs, "value": string(postingListRef)}).Debug("[index.print] version index entry")
			postingList, err := i.getPostingList(postingListRef, tx)
			if err != nil {
				return err
			}
			for _, objectID := range postingList {
				log.WithFields(log.Fields{"obj": string(objectID)}).Debug("[index.print][posting list]")
				objEnc, err := i.bucket.ReadReg(tx, antidote.Key(objectID))
				if err != nil {
					return err
				}
				object, err := utils.UnmarshalObject(objEnc)
				if err != nil {
					return err
				}
				log.WithFields(log.Fields{"obj": object}).Debug("[index.print][posting list][object]")
			}
		}
	}
	log.Debug("printing index done")
	return tx.Commit()
}
