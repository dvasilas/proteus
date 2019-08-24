package antidoteindex

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/dvasilas/proteus/src/protos"

	antidote "github.com/AntidoteDB/antidote-go-client"
	utils "github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// AntidoteIndex ...
type AntidoteIndex struct {
	attributeName string
	attributeType pbUtils.Attribute_AttributeType
	client        *antidote.Client
	bucket        antidote.Bucket
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

// Update ...
func (i *AntidoteIndex) Update(attrOld *pbUtils.Attribute, attrNew *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	indexStoreUpdates := make([]*antidote.CRDTUpdate, 0)
	objectEncoded, err := encodeObject(object)
	if err != nil {
		return err
	}
	tx, err := i.client.StartTransaction()
	if err != nil {
		return err
	}
	attrToValIndex, err := i.getAttrToValIndex(attrNew, tx)
	if err != nil {
		return err
	}
	// if attrOld != nil then the previous value has been indexed
	// the index entry of the previous value needs to be updated
	// by creating a new object list version without the given object
	if attrOld != nil {
		valToTsIndex, err := getValtoTsIndex(attrToValIndex, attrOld)
		if err != nil {
			return nil
		}
		lastVRef, err := getLastVersionRef(valToTsIndex)
		if err != nil {
			return nil
		}
		objList, err := i.bucket.ReadSet(tx, lastVRef)
		if err != nil {
			return err
		}
		newVUpdate, ref := putNewVersion(attrOld, encodeTimestamp(ts.GetVc()), tx)
		indexStoreUpdates = append(indexStoreUpdates,
			newVUpdate,
			antidote.SetAdd(antidote.Key([]byte(ref)), objList...),
			antidote.SetRemove(antidote.Key([]byte(ref)), objectEncoded),
		)
	}
	valToTsIndex, err := getValtoTsIndex(attrToValIndex, attrNew)
	// if valIndex == nil the is no entry for this value in the value-to-ts index
	// a new entry with the given value needs to be created
	if valToTsIndex == nil {
		newVUpdate, ref := putNewVersion(attrNew, encodeTimestamp(ts.GetVc()), tx)
		indexStoreUpdates = append(indexStoreUpdates,
			newVUpdate,
			antidote.SetAdd(antidote.Key([]byte(ref)), objectEncoded),
		)
	} else {
		// an index entry for this value exists
		// a new version for the entry needs to created, by added the given object
		lastVRef, err := getLastVersionRef(valToTsIndex)
		if err != nil {
			return nil
		}
		objList, err := i.bucket.ReadSet(tx, lastVRef)
		if err != nil {
			return err
		}
		newVUpdate, ref := putNewVersion(attrNew, encodeTimestamp(ts.GetVc()), tx)
		indexStoreUpdates = append(indexStoreUpdates,
			newVUpdate,
			antidote.SetAdd(antidote.Key([]byte(ref)), objList...),
			antidote.SetAdd(antidote.Key([]byte(ref)), objectEncoded),
		)
	}
	if err := i.bucket.Update(tx, indexStoreUpdates...); err != nil {
		return nil
	}
	return tx.Commit()
}

// UpdateCatchUp ...
func (i *AntidoteIndex) UpdateCatchUp(attr *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	indexStoreUpdates := make([]*antidote.CRDTUpdate, 0)
	objectEncoded, err := encodeObject(object)
	if err != nil {
		return err
	}

	tx, err := i.client.StartTransaction()
	if err != nil {
		return err
	}
	attrToValIndex, err := i.getAttrToValIndex(attr, tx)
	if err != nil {
		return err
	}
	valToTsIndex, err := getValtoTsIndex(attrToValIndex, attr)
	// if valIndex == nil the is no entry for this value in the value-to-ts index
	// a new entry with the given value needs to be created
	if valToTsIndex == nil {
		newVUpdate, ref := putNewVersion(attr, genCatchUpVersion(ts), tx)
		indexStoreUpdates = append(indexStoreUpdates,
			newVUpdate,
			antidote.SetAdd(antidote.Key([]byte(ref)), objectEncoded),
		)
	} else {
		catchUpVersion, err := valToTsIndex.Reg(antidote.Key(genCatchUpVersion(ts)))
		if err != nil {
			return err
		}
		indexStoreUpdates = append(indexStoreUpdates,
			antidote.SetAdd(catchUpVersion, objectEncoded),
		)
	}
	if err := i.bucket.Update(tx, indexStoreUpdates...); err != nil {
		return nil
	}
	return tx.Commit()
}

// Lookup ...
func (i *AntidoteIndex) Lookup(attr *pbUtils.AttributePredicate, ts *pbUtils.SnapshotTimePredicate) (map[string]utils.ObjectState, error) {
	tx, err := i.client.StartTransaction()
	if err != nil {
		return nil, err
	}
	attrToValIndex, err := i.getAttrToValIndex(attr.GetAttr(), tx)
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
	valToTsIndex, err := getValtoTsIndex(attrToValIndex, predAttr)
	//valIndex, err := attrToValIndex.Map(antidote.Key(utils.ValueToString(attr.GetLbound())))
	if valToTsIndex == nil || err != nil {
		return nil, err
	}
	lastVRef, err := getLastVersionRef(valToTsIndex)
	if err != nil {
		return nil, err
	}
	objList, err := i.bucket.ReadSet(tx, lastVRef)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	for _, objEnc := range objList {
		obj, err := decodeIndexEntry(objEnc)
		if err != nil {
			return nil, err
		}
		res[obj.ObjectID] = obj
	}
	return res, nil
}

//---------------- Internal Functions --------------

func (i *AntidoteIndex) getAttrToValIndex(attr *pbUtils.Attribute, tx *antidote.InteractiveTransaction) (*antidote.MapReadResult, error) {
	attributeKey := attr.GetAttrKey() + "_" + attr.GetAttrType().String()
	return i.bucket.ReadMap(tx, antidote.Key([]byte(attributeKey)))
}

func getValtoTsIndex(i *antidote.MapReadResult, attr *pbUtils.Attribute) (*antidote.MapReadResult, error) {
	return i.Map(antidote.Key(utils.ValueToString(attr.GetValue())))
}

func putNewVersion(attr *pbUtils.Attribute, tsKey []byte, tx *antidote.InteractiveTransaction) (*antidote.CRDTUpdate, []byte) {
	attrKey := attr.GetAttrKey() + "_" + attr.GetAttrType().String()
	ref := genReference()
	return antidote.MapUpdate(
		antidote.Key(attrKey),
		antidote.MapUpdate(
			antidote.Key(antidote.Key(utils.ValueToString(attr.GetValue()))),
			antidote.RegPut(antidote.Key(tsKey), []byte(ref)),
		),
	), ref
}

func genCatchUpVersion(ts pbUtils.Vectorclock) []byte {
	zeroTs := make(map[string]uint64)
	for k := range ts.GetVc() {
		zeroTs[k] = 0
	}
	return encodeTimestamp(zeroTs)
}

func encodeObject(object utils.ObjectState) ([]byte, error) {
	return []byte(object.ObjectID), nil
}

func decodeIndexEntry(data []byte) (utils.ObjectState, error) {
	var object utils.ObjectState
	object.ObjectID = string(data)
	//err := object.UnMarshal(data)
	return object, nil
}

func encodeTimestamp(ts map[string]uint64) []byte {
	enc := ""
	for k, v := range ts {
		enc += k + ":" + strconv.FormatInt(int64(v), 10) + "_"
	}
	return []byte(enc[:len(enc)-1])
}

func decodeTimestamps(tsEncArr []antidote.MapEntryKey) ([]*pbUtils.Vectorclock, error) {
	log.Debug("decodeTimestamps")
	res := make([]*pbUtils.Vectorclock, 0)
	for _, ts := range tsEncArr {
		log.Debug(string(ts.Key))
		var vcEntries []string
		if strings.Contains(string(ts.Key), "_") {
			vcEntries = strings.Split(string(ts.Key), "_")
		} else {
			vcEntries = []string{string(ts.Key)}
		}
		log.Debug(vcEntries)
		vcMap := make(map[string]uint64)
		for _, e := range vcEntries {
			log.Debug(e)
			vc := strings.Split(e, ":")
			t, err := strconv.ParseInt(vc[1], 10, 64)
			if err != nil {
				return nil, err
			}
			vcMap[vc[0]] = uint64(t)
		}
		res = append(res, protoutils.Vectorclock(vcMap))
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

func getLastVersionRef(versionIndex *antidote.MapReadResult) ([]byte, error) {
	versions := versionIndex.ListMapKeys()
	versionList, err := decodeTimestamps(versions)
	if err != nil {
		return nil, err
	}
	lastV := lastVersion(versionList)
	return versionIndex.Reg(antidote.Key([]byte(encodeTimestamp(lastV.GetVc()))))
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

func (i *AntidoteIndex) print(attr *pbUtils.Attribute) error {
	log.Debug("Printing index")
	attributeKey := attr.GetAttrKey() + "_" + attr.GetAttrType().String()

	tx, err := i.client.StartTransaction()
	if err != nil {
		return err
	}
	attrIndex, err := i.bucket.ReadMap(tx, antidote.Key([]byte(attributeKey)))
	if err != nil {
		return err
	}
	values := attrIndex.ListMapKeys()
	for _, val := range values {
		log.WithFields(log.Fields{"val": string(val.Key)}).Debug("value")
		tsIndex, err := attrIndex.Map(val.Key)
		if err != nil {
			return err
		}
		versions := tsIndex.ListMapKeys()
		for _, vers := range versions {
			log.WithFields(log.Fields{"timestamp": string(vers.Key)}).Debug("posting list version")
			ref, err := tsIndex.Reg(vers.Key)
			if err != nil {
				return err
			}
			objList, err := i.bucket.ReadSet(tx, ref)
			if err != nil {
				return err
			}
			for _, obj := range objList {
				object, err := decodeIndexEntry(obj)
				if err != nil {
					return err
				}
				log.Debug("-", object.ObjectID)
			}
		}
	}
	return tx.Commit()
}
