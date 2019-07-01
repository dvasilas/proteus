package mapindex

import (
	"fmt"
	"strconv"
	"strings"

	antidote "github.com/AntidoteDB/antidote-go-client"
	utils "github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
)

// MapIndex ...
type MapIndex struct {
	attributeName string
	attributeType pbUtils.Attribute_AttributeType
	client        *antidote.Client
	bucket        antidote.Bucket
}

//---------------- API Functions -------------------

// New ...
func New(conf *config.Config) (*MapIndex, error) {
	endpoint := strings.Split(conf.IndexConfig.IndexStore.Endpoint, ":")
	port, err := strconv.ParseInt(endpoint[1], 10, 64)
	if err != nil {
		return &MapIndex{}, err
	}
	c, err := antidote.NewClient(antidote.Host{Name: endpoint[0], Port: int(port)})
	if err != nil {
		return &MapIndex{}, err
	}
	bucket := antidote.Bucket{Bucket: []byte(conf.IndexConfig.IndexStore.Bucket)}
	return &MapIndex{
		attributeName: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrKey(),
		attributeType: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrType(),
		client:        c,
		bucket:        bucket,
	}, nil
}

// Put ...
func (i *MapIndex) Put(attribute *pbUtils.Attribute, object utils.ObjectState) error {
	entry, err := encodeIndexEntry(object)
	if err != nil {
		return err
	}
	entryKey := attribute.GetAttrKey() + "_" + attribute.GetAttrType().String()
	tx := i.client.CreateStaticTransaction()
	if err = i.bucket.Update(tx,
		antidote.MapUpdate(
			antidote.Key(entryKey),
			antidote.SetAdd(
				antidote.Key(utils.ValueToString(attribute.GetValue())),
				entry,
			),
		),
	); err != nil {
		return err
	}
	return err
}

//Get ...
func (i *MapIndex) Get(predicate *pbUtils.AttributePredicate) (map[string]utils.ObjectState, error) {
	entryKey := predicate.GetAttr().GetAttrKey() + "_" + predicate.GetAttr().GetAttrType().String()
	tx := i.client.CreateStaticTransaction()
	mapVal, err := i.bucket.ReadMap(tx, antidote.Key([]byte(entryKey)))
	if err != nil {
		return nil, err
	}
	setVal, err := mapVal.Set(antidote.Key(utils.ValueToString(predicate.GetLbound())))
	if err != nil {
		return nil, err
	}

	res := make(map[string]utils.ObjectState)
	for _, x := range setVal {
		object, err := decodeIndexEntry(x)
		if err != nil {
			return nil, err
		}
		res[object.ObjectID] = object
	}
	return res, nil
}

// RemoveOldEntry ...
func (i *MapIndex) RemoveOldEntry(attribute *pbUtils.Attribute, objectID string) error {
	entryKey := attribute.GetAttrKey() + "_" + attribute.GetAttrType().String()
	tx, err := i.client.StartTransaction()
	if err != nil {
		return err
	}
	mapVal, err := i.bucket.ReadMap(tx, antidote.Key([]byte(entryKey)))
	if err != nil {
		return err
	}
	setVal, err := mapVal.Set(antidote.Key(utils.ValueToString(attribute.GetValue())))
	if err != nil {
		return err
	}
	var entryToRmv []byte
	for _, x := range setVal {
		object, err := decodeIndexEntry(x)
		if err != nil {
			return nil
		}
		if object.ObjectID == objectID {
			entryToRmv = x
			break
		}
	}
	fmt.Println("entryToRmv", string(entryToRmv))

	if err := i.bucket.Update(tx,
		antidote.MapUpdate(
			antidote.Key(entryKey),
			antidote.SetRemove(
				antidote.Key(utils.ValueToString(attribute.GetValue())),
				entryToRmv),
		),
	); err != nil {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return nil
	}
	return nil
}

// Print ...
func (i *MapIndex) Print() {
}

//---------------- Internal Functions --------------

func encodeIndexEntry(object utils.ObjectState) ([]byte, error) {
	return object.Marshal()
}

func decodeIndexEntry(data []byte) (utils.ObjectState, error) {
	var object utils.ObjectState
	err := object.UnMarshal(data)
	return object, err
}
