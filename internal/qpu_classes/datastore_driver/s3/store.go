package s3driver

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	pbS3 "github.com/dvasilas/proteus/internal/proto/s3"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc"

	//
	_ "github.com/go-sql-driver/mysql"
)

// S3DataStore ...
type S3DataStore struct {
	inputSchema          libqpu.Schema
	conn                 *grpc.ClientConn
	cli                  pbS3.NotificationServiceClient
	accessKeyID          string
	secretAccessKey      string
	endpoint             string
	subscriptionEndpoint string

	// subscriptionEndpoint string
	// cli                  mysql.PublishUpdatesClient
	// inputSchema          libqpu.Schema
	// conn                 *grpc.ClientConn
	// db                   *sql.DB
}

// listBucketResult is used to unmarshal the body of an http GET response
// listing all objects in a S3 bucket
type listBucketResult struct {
	Name        string
	MaxKeys     int
	IsTruncated bool
	Contents    []struct {
		Key          string
		LastModified string
		Size         int64
	}
}

//---------------- API Functions -------------------

// NewDatastore ...
func NewDatastore(conf *libqpu.QPUConfig, inputSchema libqpu.Schema) (S3DataStore, error) {

	for {
		endpoint := conf.DatastoreConfig.Endpoint
		if strings.HasPrefix(strings.ToLower(endpoint), "http://") {
			endpoint = strings.Split(conf.DatastoreConfig.Endpoint, "http://")[1]
		}
		fmt.Println("endpoint", endpoint)

		c, err := net.DialTimeout("tcp", endpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retying connecting to: ", conf.DatastoreConfig.Endpoint, err)
		} else {
			c.Close()
			break
		}
	}

	for {
		c, err := net.DialTimeout("tcp", conf.DatastoreConfig.LogStreamEndpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retying connecting to: ", conf.DatastoreConfig.LogStreamEndpoint)
		} else {
			c.Close()
			break
		}
	}

	conn, err := grpc.Dial(conf.DatastoreConfig.LogStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		return S3DataStore{}, err
	}

	s := S3DataStore{
		inputSchema:          inputSchema,
		conn:                 conn,
		cli:                  pbS3.NewNotificationServiceClient(conn),
		accessKeyID:          conf.DatastoreConfig.Credentials.AccessKeyID,
		secretAccessKey:      conf.DatastoreConfig.Credentials.SecretAccessKey,
		endpoint:             conf.DatastoreConfig.Endpoint,
		subscriptionEndpoint: conf.DatastoreConfig.LogStreamEndpoint,
	}

	// err = errors.New("not tried yet")
	// for err != nil {
	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	_, err = s.cli.SubscribeNotifications(ctx)
	// 	time.Sleep(2 * time.Second)
	// 	cancel()
	// 	fmt.Println("retying a test query", err)
	// }

	// s := S3DataStore{
	// 	inputSchema:          inputSchema,
	// 	conn:                 conn,
	// 	accessKeyID:          conf.DatastoreConfig.Credentials.AccessKeyID,
	// 	secretAccessKey:      conf.DatastoreConfig.Credentials.SecretAccessKey,
	// 	endpoint:             conf.DatastoreConfig.Endpoint,
	// 	subscriptionEndpoint: conf.DatastoreConfig.LogStreamEndpoint,
	// }

	return s, nil
	// for {
	// 	c, err := net.DialTimeout("tcp", conf.DatastoreConfig.Endpoint, time.Duration(time.Second))
	// 	if err != nil {
	// 		time.Sleep(2 * time.Second)
	// 		fmt.Println("retying connecting to: ", conf.DatastoreConfig.Endpoint)
	// 	} else {
	// 		c.Close()
	// 		break
	// 	}
	// }

	// for {
	// 	c, err := net.DialTimeout("tcp", conf.DatastoreConfig.LogStreamEndpoint, time.Duration(time.Second))
	// 	if err != nil {
	// 		time.Sleep(2 * time.Second)
	// 		fmt.Println("retying connecting to: ", conf.DatastoreConfig.LogStreamEndpoint)
	// 	} else {
	// 		c.Close()
	// 		break
	// 	}
	// }

	// connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
	// 	conf.DatastoreConfig.Credentials.AccessKeyID,
	// 	conf.DatastoreConfig.Credentials.SecretAccessKey,
	// 	conf.DatastoreConfig.Endpoint,
	// 	conf.DatastoreConfig.DBName,
	// )
	// db, err := sql.Open("mysql", connStr)
	// if err != nil {
	// 	return MySQLDataStore{}, err
	// }

	// conn, err := grpc.Dial(conf.DatastoreConfig.LogStreamEndpoint, grpc.WithInsecure())
	// if err != nil {
	// 	return MySQLDataStore{}, err
	// }

	// s := MySQLDataStore{
	// 	subscriptionEndpoint: conf.DatastoreConfig.LogStreamEndpoint,
	// 	cli:                  mysql.NewPublishUpdatesClient(conn),
	// 	inputSchema:          inputSchema,
	// 	conn:                 conn,
	// 	db:                   db,
	// }

	// err = errors.New("not tried yet")
	// for err != nil {
	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	_, err = s.cli.SubscribeToUpdates(ctx)
	// 	time.Sleep(2 * time.Second)
	// 	cancel()
	// 	fmt.Println("retying a test query", err)
	// }

	// return s, nil
}

// SubscribeOps ...
func (ds S3DataStore) SubscribeOps(table string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := ds.cli.SubscribeNotifications(ctx)
	if err != nil {
		errCh <- utils.Error(err)
		cancel()
		return logOpCh, nil, errCh
	}

	if err := stream.Send(&pbS3.RequestStream{Val: &pbS3.RequestStream_Request{Request: &pbS3.SubRequest{Sync: false, Timestamp: time.Now().UnixNano()}}}); err != nil {
		errCh <- utils.Error(err)
		cancel()
		return logOpCh, nil, errCh
	}

	// err = stream.Send(
	// 	&mysql.Request{
	// 		Val: &mysql.Request_Request{
	// 			Request: &mysql.SubRequest{
	// 				// Timestamp: 0,
	// 				Sync:  false,
	// 				Table: table,
	// 			},
	// 		},
	// 	},
	// )

	// if err != nil {
	// 	errCh <- utils.Error(err)
	// 	cancel()
	// 	return logOpCh, nil, errCh
	// }

	go ds.opConsumer(stream, logOpCh, errCh)

	return logOpCh, cancel, errCh
}

// GetSnapshot ...
func (ds S3DataStore) GetSnapshot(table string, projection, isNull, isNotNull []string) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	// prepare request url
	requestURL := fmt.Sprintf("%s/%s", ds.endpoint, table)

	// buffer to store response in
	listBuff := bytes.NewBuffer([]byte{})

	// prepare http request
	request, err := http.NewRequest("GET", requestURL, listBuff)
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	// reader for reading response
	reader := bytes.NewReader(listBuff.Bytes())

	// connection credentials
	credentials := credentials.NewStaticCredentials(ds.accessKeyID, ds.secretAccessKey, "")
	signer := v4.NewSigner(credentials)
	signer.Sign(request, reader, "s3", "us-east-1", time.Now())

	// send request
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		resp.Body.Close()
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}
	defer resp.Body.Close()

	// get response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	// unmarshal into listBucketResult
	var res listBucketResult
	xml.Unmarshal(body, &res)

	go func() {
		// for each object in listBucketResult
		// to a HEAD get its metadata tags
		for _, r := range res.Contents {

			mdBuff := bytes.NewBuffer([]byte{})
			requestURL = fmt.Sprintf("%s/%s/%s", ds.endpoint, table, r.Key)
			request, err = http.NewRequest("HEAD", requestURL, mdBuff)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}

			reader := bytes.NewReader(mdBuff.Bytes())
			signer.Sign(request, reader, "s3", "us-east-1", time.Now())

			resp, err = client.Do(request)
			if err != nil {
				resp.Body.Close()
				errCh <- utils.Error(err)
				break
			}
			defer resp.Body.Close()

			attributes, ts, err := ds.formatLogOpState(table, r.Key, resp.Header)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}

			timestamp, err := ptypes.TimestampProto(ts)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}

			logOpCh <- libqpu.LogOperationState(
				r.Key,
				table,
				libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
				attributes,
			)
		}
	}()
	// 	defer rows.Close()
	// 	for rows.Next() {
	// 		err = rows.Scan(scanArgs...)
	// 		if err != nil {
	// 			errCh <- utils.Error(err)
	// 			break
	// 		}
	// 		var recordID string
	// 		attributes := make(map[string]*qpu.Value)

	// 		for i, col := range values {
	// 			if col != nil {
	// 				if columns[i] == "ts" {
	// 					ts = col.(time.Time)
	// 				} else {
	// 					value, err := ds.inputSchema.InterfaceToValue(table, columns[i], col)
	// 					if err != nil {
	// 						errCh <- err
	// 						break
	// 					}
	// 					attributes[projection[i]] = value
	// 				}
	// 			}
	// 		}

	// 		timestamp, err := ptypes.TimestampProto(ts)
	// 		if err != nil {
	// 			errCh <- err
	// 			break
	// 		}

	// 		logOpCh <- libqpu.LogOperationState(
	// 			recordID,
	// 			table,
	// 			libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
	// 			attributes)

	// 	}
	// 	close(logOpCh)
	// 	close(errCh)
	// }()

	return logOpCh, errCh
}

// ---------------- Internal Functions --------------

func (ds S3DataStore) opConsumer(stream pbS3.NotificationService_SubscribeNotificationsClient, msg chan libqpu.LogOperation, errCh chan error) {
	for {
		op, err := stream.Recv()
		fmt.Println("received", op, err)
		if err == io.EOF {
			errCh <- utils.Error(errors.New("opConsumer received EOF"))
			break
		}
		if err != nil {
			errCh <- err
			break
		}
		formattedOp, err := ds.formatLogOpDelta(op)
		if err != nil {
			errCh <- err
			break
		}
		msg <- formattedOp

	}
	close(msg)
	close(errCh)
}

func (ds S3DataStore) formatLogOpDelta(notificationMsg *pbS3.NotificationStream) (libqpu.LogOperation, error) {
	attributesOld := make(map[string]*qpu.Value)
	attributesNew := make(map[string]*qpu.Value)

	if notificationMsg.GetPayload().GetOldState() != nil {
		for attributeKey, val := range notificationMsg.GetPayload().GetOldState().GetAttributes() {
			value, err := ds.inputSchema.StrToValue(notificationMsg.GetPayload().GetBucket(), attributeKey, val)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesOld[attributeKey] = value
		}
	}

	if notificationMsg.GetPayload().GetNewState() != nil {
		for attributeKey, val := range notificationMsg.GetPayload().GetNewState().GetAttributes() {
			value, err := ds.inputSchema.StrToValue(notificationMsg.GetPayload().GetBucket(), attributeKey, val)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesNew[attributeKey] = value
		}
	}

	// for _, attribute := range notificationMsg.GetPayload(). {
	// 	if attribute.ValueOld != "" {
	// 		value, err := ds.inputSchema.StrToValue(notificationMsg.Table, attribute.Key, attribute.ValueOld)
	// 		if err != nil {
	// 			return libqpu.LogOperation{}, err
	// 		}
	// 		attributesOld[attribute.Key] = value
	// 	}
	// 	if attribute.ValueNew != "" {
	// 		value, err := ds.inputSchema.StrToValue(notificationMsg.Table, attribute.Key, attribute.ValueNew)
	// 		if err != nil {
	// 			return libqpu.LogOperation{}, err
	// 		}
	// 		attributesNew[attribute.Key] = value
	// 	}

	return libqpu.LogOperationDelta(
		notificationMsg.GetPayload().GetObjectId(),
		notificationMsg.GetPayload().GetBucket(),
		// libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: notificationMsg.Timestamp}),
		nil,
		attributesOld,
		attributesNew,
	), nil
}

func (ds S3DataStore) formatLogOpState(table, recordID string, attributes http.Header) (map[string]*qpu.Value, time.Time, error) {
	attrs := make(map[string]*qpu.Value)
	var ts time.Time

	// attributesNew := make(map[string]*qpu.Value)
	for k := range attributes {
		// fmt.Println(attributeKey, attributes[attributeKey])
		attributeKey := strings.ToLower(k)
		if strings.HasPrefix(attributeKey, "x-amz-meta-") && !strings.Contains(attributeKey, "s3cmd-attrs") {
			value, err := ds.inputSchema.StrToValue(table, attributeKey, attributes[k][0])
			if err != nil {
				return nil, time.Time{}, err
			}
			attrs[attributeKey] = value
		}
		if attributeKey == "last-modified" {
			fmt.Println(attributeKey, attributes[k])
			var err error
			ts, err = time.Parse("Mon, 02 Jan 2006 15:04:05 MST", attributes[k][0])
			if err != nil {
				log.Fatalf("time.Parse failed:%s", err)
				return nil, time.Time{}, err
			}
			fmt.Println(ts, err)

		}
	}
	// for _, attribute := range notificationMsg.Attributes {
	// 	if attribute.ValueOld != "" {
	// 		value, err := ds.inputSchema.StrToValue(notificationMsg.Table, attribute.Key, attribute.ValueOld)
	// 		if err != nil {
	// 			return libqpu.LogOperation{}, err
	// 		}
	// 		attributesOld[attribute.Key] = value
	// 	}
	// 	if attribute.ValueNew != "" {
	// 		value, err := ds.inputSchema.StrToValue(notificationMsg.Table, attribute.Key, attribute.ValueNew)
	// 		if err != nil {
	// 			return libqpu.LogOperation{}, err
	// 		}
	// 		attributesNew[attribute.Key] = value
	// 	}
	// }

	return attrs, ts, nil
}
