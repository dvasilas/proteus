package mongoindex

import (
	"context"
	"fmt"
	"log"

	utils "github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoIndex ...
type MongoIndex struct {
	attributeName string
	attributeType pbUtils.Attribute_AttributeType
	client        *mongo.Client
	collection    *mongo.Collection
}

type Value struct {
	Key      string
	Type     pbUtils.Attribute_AttributeType
	ValueStr string  `json:"valueStr" bson:"valueStr"`
	ValueInt int64   `json:"valueInt" bson:"valueInt"`
	ValueFlt float64 `json:"valueFlt" bson:"valueFlt"`
}

type mongoObject struct {
	ObjectID   string                          `json:"objectID" bson:"objectID"`
	ObjectType pbUtils.LogOperation_ObjectType `json:"objectType" bson:"objectType"`
	Bucket     string                          `json:"bucket" bson:"bucket"`
	Timestamp  pbUtils.Vectorclock             `json:"timestamp" bson:"timestamp"`
	// State      pbUtils.ObjectState             `json:"state" bson:"state"`
	State []Value `json:"state" bson:"state"`
}

// New ...
func New(conf *config.Config) (*MongoIndex, error) {
	// "mongodb://localhost:27017"
	clientOptions := options.Client().ApplyURI(conf.IndexConfig.IndexStore.Endpoint)

	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return &MongoIndex{}, err
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	collection := client.Database("proteus").Collection("index")
	return &MongoIndex{
		attributeName: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrKey(),
		attributeType: conf.IndexConfig.IndexingConfig[0].GetAttr().GetAttrType(),
		client:        client,
		collection:    collection,
	}, nil
}

// UpdateCatchUp ...
func (i *MongoIndex) UpdateCatchUp(attr *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	fmt.Println("UpdateCatchUp", attr, object, ts)
	obj := mongoObject{
		ObjectID:   object.ObjectID,
		ObjectType: object.ObjectType,
		Bucket:     object.Bucket,
		Timestamp:  ts,
		State:      make([]Value, len(object.State.GetAttrs())),
	}
	fmt.Println(len(object.State.GetAttrs()))
	for i, attr := range object.State.GetAttrs() {
		fmt.Println(attr.GetAttrKey())
		switch attr.GetValue().Val.(type) {
		case *pbUtils.Value_Str:
			obj.State[i] = Value{
				Key:      attr.GetAttrKey(),
				Type:     attr.GetAttrType(),
				ValueStr: attr.GetValue().GetStr(),
			}
		case *pbUtils.Value_Int:
			obj.State[i] = Value{
				Key:      attr.GetAttrKey(),
				Type:     attr.GetAttrType(),
				ValueInt: attr.GetValue().GetInt(),
			}
		case *pbUtils.Value_Flt:
			obj.State[i] = Value{
				Key:      attr.GetAttrKey(),
				Type:     attr.GetAttrType(),
				ValueFlt: attr.GetValue().GetFlt(),
			}
		}
	}
	fmt.Println(obj.State)
	insertResult, err := i.collection.InsertOne(context.TODO(), obj)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Inserted : ", insertResult.InsertedID)
	var out mongoObject
	filter := bson.D{{Key: "_id", Value: insertResult.InsertedID}}

	// Read data from collection
	err = i.collection.FindOne(context.TODO(), filter).Decode(&out)
	if err != nil {
		log.Fatalf("failed to read data (id=%v) from collection <experiments.proto>: %#v", insertResult.InsertedID, err)
	}
	fmt.Println(out)
	return nil
}

// Update ...
func (i *MongoIndex) Update(attrOld *pbUtils.Attribute, attrNew *pbUtils.Attribute, object utils.ObjectState, ts pbUtils.Vectorclock) error {
	fmt.Println("Update", attrOld, attrNew, object)
	return nil
}

// Lookup ...
func (i *MongoIndex) Lookup(attr *pbUtils.AttributePredicate, ts *pbUtils.SnapshotTimePredicate, lookupResCh chan utils.ObjectState, errCh chan error) {
	fmt.Println("Lookup", attr)

	findOptions := options.Find()

	var results []*mongoObject

	cur, err := i.collection.Find(context.TODO(), bson.D{{}}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	if err = cur.All(context.TODO(), &results); err != nil {
		log.Fatal(err)
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Println(result)
	}
}
