package mongodriver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDataStore ...
type MongoDataStore struct {
	inputSchema libqpu.Schema
	database    *mongo.Database
	endpoint    string
}

//---------------- API Functions -------------------

// NewDatastore ...
func NewDatastore(config *libqpu.QPUConfig, inputSchema libqpu.Schema) (MongoDataStore, error) {

	clientOptions := options.MergeClientOptions(
		&options.ClientOptions{
			Auth: &options.Credential{
				Username: config.DatastoreConfig.Credentials.AccessKeyID,
				Password: config.DatastoreConfig.Credentials.SecretAccessKey,
			}}).ApplyURI(config.DatastoreConfig.Endpoint)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return MongoDataStore{}, err
	}

	for {
		err = client.Ping(context.Background(), nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting to: ", config.StateBackend.Endpoint)
		} else {
			break
		}
	}

	database := client.Database(config.DatastoreConfig.DBName)

	s := MongoDataStore{
		inputSchema: inputSchema,
		database:    database,
		endpoint:    config.DatastoreConfig.Endpoint,
	}

	return s, nil
}

// SubscribeOps ...
func (ds MongoDataStore) SubscribeOps(table string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())

	col := ds.database.Collection(table)

	matchStage := bson.D{{"$match", bson.D{{"$or",
		bson.A{
			bson.D{{"operationType", "insert"}},
			bson.D{{"operationType", "update"}},
		},
	}}}}
	cs, err := col.Watch(ctx, mongo.Pipeline{matchStage})
	if err != nil {
		errCh <- utils.Error(err)
		cancel()
		return logOpCh, nil, errCh
	}

	go func() {
		for cs.Next(ctx) {
			next := cs.Current

			opType := next.Lookup("operationType").String()

			switch opType {
			case "\"insert\"":
				formatted, err := ds.formatInsertRecord(table, next)
				if err != nil {
					errCh <- utils.Error(err)
					cancel()
					break
				}
				logOpCh <- formatted
			case "\"update\"":
				formatted, err := ds.formatUpdateRecord(table, next)
				if err != nil {
					errCh <- utils.Error(err)
					cancel()
					break
				}
				logOpCh <- formatted
			}

		}
	}()

	// stream, err := ds.cli.SubscribeNotifications(ctx)
	// if err != nil {
	// 	errCh <- utils.Error(err)
	// 	cancel()
	// 	return logOpCh, nil, errCh
	// }

	// if err := stream.Send(&pbS3.RequestStream{Val: &pbS3.RequestStream_Request{Request: &pbS3.SubRequest{Sync: false, Timestamp: time.Now().UnixNano()}}}); err != nil {
	// 	errCh <- utils.Error(err)
	// 	cancel()
	// 	return logOpCh, nil, errCh
	// }

	// go ds.opConsumer(stream, logOpCh, errCh)

	return logOpCh, cancel, errCh
}

// GetSnapshot ...
func (ds MongoDataStore) GetSnapshot(table string, projection, isNull, isNotNull []string) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	col := ds.database.Collection(table)

	var results []*map[string]interface{}

	cur, err := col.Find(context.Background(), bson.M{}, options.Find())
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}
	if err = cur.All(context.Background(), &results); err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	go func() {
		for _, result := range results {
			dataItemID, dataItemAttributes, err := ds.formatFindResult(*result)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}
			logOpCh <- libqpu.LogOperationState(
				// 			*object.Key,
				dataItemID,
				table,
				nil,
				// 			libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
				// 			attributes,
				dataItemAttributes,
			)
		}

		close(logOpCh)
		close(errCh)
	}()

	return logOpCh, errCh
}

// GetNotificationLanency ...
func (ds MongoDataStore) GetNotificationLanency() (float64, float64, float64, float64) {
	return -1, -1, -1, -1
}

// ---------------- Internal Functions --------------

//func (ds MongoDataStore) opConsumer(stream pbS3.NotificationService_SubscribeNotificationsClient, msg chan libqpu.LogOperation, errCh chan error) {
//	for {
//		op, err := stream.Recv()
//		if err == io.EOF {
//			errCh <- utils.Error(errors.New("opConsumer received EOF"))
//			break
//		}
//		if err != nil {
//			errCh <- err
//			break
//		}
//		formattedOp, err := ds.formatLogOpDelta(op)
//		if err != nil {
//			errCh <- err
//			break
//		}
//		msg <- formattedOp
//
//	}
//	close(msg)
//	close(errCh)
//}

func (ds MongoDataStore) formatInsertRecord(table string, record bson.Raw) (libqpu.LogOperation, error) {
	elems, err := record.Lookup("fullDocument").Document().Elements()
	if err != nil {
		return libqpu.LogOperation{}, err
	}

	id := record.Lookup("fullDocument").Document().Lookup("_id").String()

	id = id[1 : len(id)-1]

	attributesNew := make(map[string]*qpu.Value)
	for _, elem := range elems {
		if strings.HasPrefix(elem.Key(), "attribute") {
			attributesNew[elem.Key()] = libqpu.ValueInt(elem.Value().Int64())
		}
	}

	return libqpu.LogOperationDelta(
		id,
		table,
		nil,
		nil,
		attributesNew,
	), nil
}

func (ds MongoDataStore) formatUpdateRecord(table string, record bson.Raw) (libqpu.LogOperation, error) {
	elems, err := record.Lookup("updateDescription").Document().Elements()
	if err != nil {
		return libqpu.LogOperation{}, err
	}

	id := record.Lookup("documentKey").Document().Lookup("_id").String()
	id = id[1 : len(id)-1]

	attributesNew := make(map[string]*qpu.Value)
	for _, elem := range elems {
		if strings.HasPrefix(elem.Key(), "attribute") {
			attributesNew[elem.Key()] = libqpu.ValueInt(elem.Value().Int64())
		}
	}

	return libqpu.LogOperationDelta(
		id,
		table,
		nil,
		nil,
		attributesNew,
	), nil
}

func (ds MongoDataStore) formatFindResult(record map[string]interface{}) (string, map[string]*qpu.Value, error) {
	attrs := make(map[string]*qpu.Value)

	for k, v := range record {
		if k != "_id" {
			switch v.(type) {
			case int64:
				attrs[k] = libqpu.ValueInt(v.(int64))
			case int32:
				attrs[k] = libqpu.ValueInt(int64(v.(int32)))
			}
		}
	}

	return record["_id"].(string), attrs, nil
}
