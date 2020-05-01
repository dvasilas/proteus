package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pb "github.com/dvasilas/proteus/src/protos/datastore"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"google.golang.org/grpc"
)

// MySQLDataStore ...
type MySQLDataStore struct {
	subscriptionEndpoint string
	cli                  pb.PublishUpdatesClient
	conn                 *grpc.ClientConn
	schema               datastoreSchema
}

// MySQLUpdate ...
type MySQLUpdate struct {
	RecordID   string
	Table      string
	Attributes []struct {
		Key      string
		ValueOld string
		ValueNew string
	}
}

type datastoreSchema map[string]map[string]config.DatastoreAttributeType

//---------------- API Functions -------------------

// New ...
func New(conf config.DatastoreConfig) (MySQLDataStore, error) {
	conn, err := grpc.Dial(conf.LogStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		return MySQLDataStore{}, err
	}

	schema := make(datastoreSchema)
	for _, table := range conf.Schema {
		schema[table.Table] = make(map[string]config.DatastoreAttributeType)
		for _, attribute := range table.Attributes {
			schema[table.Table][attribute.Key] = attribute.Type
		}
	}

	s := MySQLDataStore{
		subscriptionEndpoint: conf.LogStreamEndpoint,
		cli:                  pb.NewPublishUpdatesClient(conn),
		conn:                 conn,
		schema:               schema,
	}
	fmt.Println("MySQLDataStore", s)
	return s, nil
}

// SubscribeOps ...
func (ds MySQLDataStore) SubscribeOps(msg chan *pbUtils.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error) {
	errs := make(chan error, 1)
	ctx := context.Background()
	stream, err := ds.cli.SubscribeToUpdates(ctx)
	if err != nil {
		errs <- err
		return ds.conn, errs
	}
	err = stream.Send(
		&pb.RequestStream{
			Val: &pb.RequestStream_Request{
				Request: &pb.SubRequest{
					Timestamp: 0,
					Sync:      false,
				},
			},
		},
	)
	go ds.opConsumer(stream, msg, ack, errs, sync)
	return ds.conn, errs
}

// GetSnapshot ...
func (ds MySQLDataStore) GetSnapshot(bucket string, msg chan *pbUtils.LogOperation) <-chan error {
	errs := make(chan error, 1)
	return errs
}

// Op ...
func (ds MySQLDataStore) Op(op *pbUtils.LogOperation) {}

//----------- Stream Consumer Functions ------------

func (ds MySQLDataStore) opConsumer(stream pb.PublishUpdates_SubscribeToUpdatesClient, msg chan *pbUtils.LogOperation, ack chan bool, errs chan error, sync bool) {
	for {
		op, err := stream.Recv()
		fmt.Println(op, err)
		if err == io.EOF {
			errs <- errors.New("s3Datastore:opConsumer received EOF")
			break
		}
		if err != nil {
			errs <- err
			break
		}
		formattedOp, err := ds.formatOperation(op)
		if err != nil {
			errs <- err
			break
		}
		msg <- formattedOp

		if sync {
		}
	}
	close(msg)
	close(errs)
}

//---------------- Internal Functions --------------

func (ds MySQLDataStore) attributeValue(table, attributeKey, valueStr string) (*pbUtils.Value, error) {
	switch ds.schema[table][attributeKey] {
	case config.STR:
		return protoutils.ValueStr(valueStr), nil
	case config.INT:
		val, err := strconv.ParseInt(valueStr, 10, 0)
		if err != nil {
			return protoutils.ValueStr(valueStr), err
		}
		return protoutils.ValueInt(val), nil
	case config.FLT:
		val, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, err
		}
		return protoutils.ValueFlt(val), nil
	}
	return protoutils.ValueStr(valueStr), errors.New("schema: attribute type conversion not implemented")
}

func (ds MySQLDataStore) formatOperation(notificationMsg *pb.NotificationStream) (*pbUtils.LogOperation, error) {
	var update MySQLUpdate
	updateStr := notificationMsg.GetPayload()
	if err := json.Unmarshal([]byte(updateStr), &update); err != nil {
		panic(err)
	}
	fmt.Println(update)
	var payload *pbUtils.Payload
	attributesNew := make([]*pbUtils.Attribute, 0)
	attributesOld := make([]*pbUtils.Attribute, 0)
	for _, attribute := range update.Attributes {
		if attribute.ValueOld != "" {
			value, err := ds.attributeValue(update.Table, attribute.Key, attribute.ValueOld)
			if err != nil {
				return nil, err
			}
			attributesOld = append(attributesOld,
				protoutils.Attribute(attribute.Key, pbUtils.Attribute_S3TAGSTR, value),
			)
		}
		value, err := ds.attributeValue(update.Table, attribute.Key, attribute.ValueNew)
		if err != nil {
			return nil, err
		}
		attributesNew = append(attributesNew,
			protoutils.Attribute(attribute.Key, pbUtils.Attribute_S3TAGSTR, value),
		)
	}

	stateOld := protoutils.ObjectState(attributesOld)
	stateNew := protoutils.ObjectState(attributesNew)

	payload = protoutils.PayloadDelta(stateOld, stateNew)
	return protoutils.LogOperation(
		update.RecordID,
		update.Table,
		pbUtils.LogOperation_S3OBJECT,
		protoutils.Vectorclock(map[string]uint64{ds.subscriptionEndpoint: uint64(time.Now().UnixNano())}),
		payload), nil
}
