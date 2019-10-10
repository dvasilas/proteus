package antidoteindex

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dvasilas/proteus/src/protos"

	antidote "github.com/AntidoteDB/antidote-go-client"
	utils "github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

const maxVersionCount = 10

// AntidoteIndex ...
type AntidoteIndex struct {
	attributeName string
	attributeType pbUtils.Attribute_AttributeType
	client        *antidote.Client
	bucket        antidote.Bucket
	mutex         sync.RWMutex
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
	log.Debug(buckName)
	bucket := antidote.Bucket{Bucket: []byte(buckName)}
	return &AntidoteIndex{
		attributeName: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrKey(),
		attributeType: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrType(),
		client:        c,
		bucket:        bucket,
	}, nil
}

// UpdateCatchUp ...
func (i *AntidoteIndex) UpdateCatchUp(attr *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	indexStoreUpdates := make([]*antidote.CRDTUpdate, 0)
	objectEncoded, err := encodeObject(object)
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
		_, versionIndex, err := i.getVersionIndex(valueIndex, attr, tx)
		var postingListRef []byte
		if err != nil && strings.Contains(err.Error(), "not found") {
			valueIndexUpdate, versionIndexRef := updateValueIndex(attr)
			versionIndexUpdate, postingListRef := appendNewVersion(versionIndexRef, genCatchUpTs(ts), tx)
			indexStoreUpdates = append(indexStoreUpdates,
				valueIndexUpdate,
				versionIndexUpdate,
				antidote.SetAdd(antidote.Key([]byte(postingListRef)), objectEncoded),
			)
		} else {
			postingListRef, err = getLatestPostingList(versionIndex)
			if err != nil {
				i.mutex.Unlock()
				return err
			}
		}
		indexStoreUpdates = append(indexStoreUpdates,
			antidote.SetAdd(antidote.Key([]byte(postingListRef)), objectEncoded),
		)
		if err := i.bucket.Update(tx, indexStoreUpdates...); err != nil {
			i.mutex.Unlock()
			return nil
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

// Update ...
func (i *AntidoteIndex) Update(attrOld *pbUtils.Attribute, attrNew *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	indexStoreUpdates := make([]*antidote.CRDTUpdate, 0)
	objectEncoded, err := encodeObject(object)
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
		if attrOld != nil {
			valueIndex, err := i.getValueIndex(attrOld, tx)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getAttrToValIndex #1")
				i.mutex.Unlock()
				return err
			}
			versionIndexRef, versionIndex, err := i.getVersionIndex(valueIndex, attrOld, tx)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getValtoTsIndex")
				i.mutex.Unlock()
				return err
			}
			postingListRefO, err := getLatestPostingList(versionIndex)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getLastVersionRef #1")
				i.mutex.Unlock()
				return err
			}
			postingListO, err := i.getPostingList(postingListRefO, tx)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getPostingList #1")
				i.mutex.Unlock()
				return err
			}
			if len(versionIndex.ListMapKeys()) >= maxVersionCount {
				valueIndexUpdate, versionIndexRefN := updateValueIndex(attrOld)
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
			versionIndexUpdate, postingListRefN := appendNewVersion(versionIndexRef, encodeTimestamp(ts.GetVc()), tx)
			indexStoreUpdates = append(indexStoreUpdates,
				versionIndexUpdate,
				antidote.SetAdd(antidote.Key([]byte(postingListRefN)), postingListO...),
				antidote.SetRemove(antidote.Key([]byte(postingListRefN)), objectEncoded),
			)
		}
		if attrNew != nil {
			valueIndex, err := i.getValueIndex(attrNew, tx)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getAttrToValIndex #2")
				i.mutex.Unlock()
				return err
			}
			versionIndexRef, versionIndex, err := i.getVersionIndex(valueIndex, attrNew, tx)
			if err != nil && strings.Contains(err.Error(), "not found") {
				valueIndexUpdate, versionIndexRef := updateValueIndex(attrNew)
				versionIndexUpdate, postingListRef := appendNewVersion(versionIndexRef, encodeTimestamp(ts.GetVc()), tx)
				indexStoreUpdates = append(indexStoreUpdates,
					valueIndexUpdate,
					versionIndexUpdate,
					antidote.SetAdd(antidote.Key([]byte(postingListRef)), objectEncoded),
				)
			} else {
				postingListRefO, err := getLatestPostingList(versionIndex)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getLastVersionRef #2")
					i.mutex.Unlock()
					return err
				}
				postingListO, err := i.getPostingList(postingListRefO, tx)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Info("indexUpdate: getPostingList #2")
					i.mutex.Unlock()
					return err
				}
				if len(versionIndex.ListMapKeys()) >= maxVersionCount {
					valueIndexUpdate, versionIndexRefN := updateValueIndex(attrNew)
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
				versionIndexUpdate, postingListRefNewN := appendNewVersion(versionIndexRef, encodeTimestamp(ts.GetVc()), tx)
				indexStoreUpdates = append(indexStoreUpdates,
					versionIndexUpdate,
					antidote.SetAdd(antidote.Key([]byte(postingListRefNewN)), postingListO...),
					antidote.SetAdd(antidote.Key([]byte(postingListRefNewN)), objectEncoded),
				)
			}
		}
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
			log.WithFields(log.Fields{"error": err}).Info("indexUpdate: transaction commit error")
			log.WithFields(log.Fields{"error": err, "after": time.Millisecond * time.Duration((10 * factor))}).Info("indexUpdate: retrying")
			time.Sleep(time.Millisecond * time.Duration((10 * factor)))
			factor *= 2
		}
	}
	return err
}

// Lookup ...
func (i *AntidoteIndex) Lookup(attr *pbUtils.AttributePredicate, ts *pbUtils.SnapshotTimePredicate) (map[string]utils.ObjectState, error) {
	//i.mutex.RLock()
	tx, err := i.client.StartTransaction()
	if err != nil {
		return nil, err
	}
	valueIndex, err := i.getValueIndex(attr.GetAttr(), tx)
	if err != nil {
		return nil, err
	}
	c, err := utils.Compare(attr.GetLbound(), attr.GetUbound())
	if err != nil {
		return nil, err
	}
	if c != 0 {
		return nil, errors.New("index supports only point queries")
	}
	res := make(map[string]utils.ObjectState)
	predAttr := attr.GetAttr()
	predAttr.Value = attr.GetLbound()
	_, versionIndex, err := i.getVersionIndex(valueIndex, predAttr, tx)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return res, nil
		}
		return nil, err
	}
	lastVRef, err := getLatestPostingList(versionIndex)
	if err != nil {
		return nil, err
	}
	postingList, err := i.getPostingList(lastVRef, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	//i.mutex.RUnlock()
	for _, objEnc := range postingList {
		obj, err := decodeObject(objEnc)
		if err != nil {
			return nil, err
		}
		res[obj.ObjectID] = obj
	}
	return res, nil
}

//---------------- Internal Functions --------------

func (i *AntidoteIndex) getValueIndex(attr *pbUtils.Attribute, tx *antidote.InteractiveTransaction) (*antidote.MapReadResult, error) {
	valueIndexRef := attr.GetAttrKey() + "_" + attr.GetAttrType().String()
	return i.bucket.ReadMap(tx, antidote.Key([]byte(valueIndexRef)))
}

func (i *AntidoteIndex) getVersionIndex(valueIndex *antidote.MapReadResult, attr *pbUtils.Attribute, tx *antidote.InteractiveTransaction) ([]byte, *antidote.MapReadResult, error) {
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

func (i *AntidoteIndex) getPostingList(postingListRef []byte, tx *antidote.InteractiveTransaction) ([][]byte, error) {
	return i.bucket.ReadSet(tx, antidote.Key(postingListRef))
}

func updateValueIndex(attr *pbUtils.Attribute) (*antidote.CRDTUpdate, []byte) {
	ref := genReference()
	valueIndexRef := attr.GetAttrKey() + "_" + attr.GetAttrType().String()
	return antidote.MapUpdate(
		antidote.Key(valueIndexRef),
		antidote.RegPut(antidote.Key(utils.ValueToString(attr.GetValue())), ref),
	), ref
}

func appendNewVersion(versionIndexRef []byte, tsKey []byte, tx *antidote.InteractiveTransaction) (*antidote.CRDTUpdate, []byte) {
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
		latestVRef = antidote.Key([]byte(encodeTimestamp(latestVTs.GetVc())))
	}
	return versionIndex.Reg(latestVRef)
}

func decodeTimestamps(tsEncArr []antidote.MapEntryKey) ([]*pbUtils.Vectorclock, error) {
	res := make([]*pbUtils.Vectorclock, 0)
	for _, ts := range tsEncArr {
		if string(ts.Key) != "prev" {
			var vcEntries []string
			if strings.Contains(string(ts.Key), "_") {
				vcEntries = strings.Split(string(ts.Key), "_")
			} else {
				vcEntries = []string{string(ts.Key)}
			}
			vcMap := make(map[string]uint64)
			for _, e := range vcEntries {
				vc := strings.Split(e, ":")
				t, err := strconv.ParseInt(vc[1], 10, 64)
				if err != nil {
					return nil, err
				}
				vcMap[vc[0]] = uint64(t)
			}
			res = append(res, protoutils.Vectorclock(vcMap))
		}
	}
	return res, nil
}

func genCatchUpTs(ts pbUtils.Vectorclock) []byte {
	zeroTs := make(map[string]uint64)
	for k := range ts.GetVc() {
		zeroTs[k] = 0
	}
	return encodeTimestamp(zeroTs)
}

func encodeTimestamp(ts map[string]uint64) []byte {
	enc := ""
	for k, v := range ts {
		enc += k + ":" + strconv.FormatInt(int64(v), 10) + "_"
	}
	return []byte(enc[:len(enc)-1])
}

func encodeObject(object utils.ObjectState) ([]byte, error) {
	return []byte(object.ObjectID), nil
}

func decodeObject(data []byte) (utils.ObjectState, error) {
	var object utils.ObjectState
	object.ObjectID = string(data)
	return object, nil
}

func genReference() []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 20)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func lastVersion(tsList []*pbUtils.Vectorclock) *pbUtils.Vectorclock {
	max := tsList[0]
	for _, ts := range tsList {
		if greater(ts, max) {
			max = ts
		}
	}
	return max
}

func greater(a, b *pbUtils.Vectorclock) bool {
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
	valueIndexRef := i.attributeName + "_" + i.attributeType.String()
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
			log.WithFields(log.Fields{"key": string(vers.Key), "value": string(postingListRef)}).Debug("[index.print] version index entry")
			postingList, err := i.getPostingList(postingListRef, tx)
			if err != nil {
				return err
			}
			for _, obj := range postingList {
				object, err := decodeObject(obj)
				if err != nil {
					return err
				}
				log.WithFields(log.Fields{"obj": object.ObjectID}).Debug("[index.print][posting list]")
			}
		}
	}
	return tx.Commit()
}
