package s3driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
	s3                   *s3.S3

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
			fmt.Println("retrying connecting to: ", conf.DatastoreConfig.Endpoint, err)
		} else {
			c.Close()
			break
		}
	}

	for {
		c, err := net.DialTimeout("tcp", conf.DatastoreConfig.LogStreamEndpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting to: ", conf.DatastoreConfig.LogStreamEndpoint)
		} else {
			c.Close()
			break
		}
	}

	conn, err := grpc.Dial(conf.DatastoreConfig.LogStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		return S3DataStore{}, err
	}
	awsRegion := "us-east-1"
	s3ForcePathStyle := true
	awsConf := aws.Config{
		Endpoint:         aws.String("http://127.0.0.1:8001"),
		Region:           &awsRegion,
		S3ForcePathStyle: &s3ForcePathStyle,
		Credentials:      credentials.NewStaticCredentials("accessKey1", "verySecretKey1", ""),
	}
	sess := session.Must(session.NewSession(&awsConf))
	svc := s3.New(sess)

	s := S3DataStore{
		inputSchema:          inputSchema,
		conn:                 conn,
		cli:                  pbS3.NewNotificationServiceClient(conn),
		accessKeyID:          conf.DatastoreConfig.Credentials.AccessKeyID,
		secretAccessKey:      conf.DatastoreConfig.Credentials.SecretAccessKey,
		endpoint:             conf.DatastoreConfig.Endpoint,
		subscriptionEndpoint: conf.DatastoreConfig.LogStreamEndpoint,
		s3:                   svc,
	}

	return s, nil
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

	go ds.opConsumer(stream, logOpCh, errCh)

	return logOpCh, cancel, errCh
}

// GetSnapshot ...
func (ds S3DataStore) GetSnapshot(table string, projection, isNull, isNotNull []string) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	listObjectsIn := s3.ListObjectsV2Input{
		Bucket: aws.String(table),
	}

	listObjectsRes, err := ds.s3.ListObjectsV2(&listObjectsIn)
	if err != nil {
		errCh <- utils.Error(err)
		return logOpCh, errCh
	}

	go func() {
		// for each object in listObjectsRes
		// get object's metadata tags
		for _, object := range listObjectsRes.Contents {
			headObjectIn := s3.HeadObjectInput{
				Bucket: aws.String(table),
				Key:    object.Key,
			}

			headObjectRes, err := ds.s3.HeadObject(&headObjectIn)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}

			attributes, err := ds.formatLogOpState(table, headObjectRes.Metadata)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}

			timestamp, err := ptypes.TimestampProto(*headObjectRes.LastModified)
			if err != nil {
				errCh <- utils.Error(err)
				break
			}

			logOpCh <- libqpu.LogOperationState(
				*object.Key,
				table,
				libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
				attributes,
			)
		}

		close(logOpCh)
		close(errCh)
	}()

	return logOpCh, errCh
}

// GetNotificationLanency ...
func (ds S3DataStore) GetNotificationLanency() (float64, float64, float64, float64) {
	return -1, -1, -1, -1
}

// ---------------- Internal Functions --------------

func (ds S3DataStore) opConsumer(stream pbS3.NotificationService_SubscribeNotificationsClient, msg chan libqpu.LogOperation, errCh chan error) {
	for {
		op, err := stream.Recv()
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

	timestamp, err := ptypes.TimestampProto(time.Unix(0, notificationMsg.GetTimestamp()))
	if err != nil {
		return libqpu.LogOperation{}, err
	}

	for attributeKey, val := range notificationMsg.GetPayload().GetNewState().GetAttributes() {
		var k string
		if strings.HasPrefix(attributeKey, "x-amz-meta-") {
			attrK := strings.Split(attributeKey, "x-amz-meta-")
			k = attrK[1]
		} else {
			k = attributeKey
		}
		value, err := ds.inputSchema.StrToValue(notificationMsg.GetPayload().GetBucket(), k, val)
		if err != nil {
			return libqpu.LogOperation{}, err
		}
		attributesNew[k] = value
	}

	if notificationMsg.GetPayload().GetOldState() != nil {
		for attributeKey, val := range notificationMsg.GetPayload().GetOldState().GetAttributes() {
			var k string
			if strings.HasPrefix(attributeKey, "x-amz-meta-") {
				attrK := strings.Split(attributeKey, "x-amz-meta-")
				k = attrK[1]
			} else {
				k = attributeKey
			}
			value, err := ds.inputSchema.StrToValue(notificationMsg.GetPayload().GetBucket(), k, val)
			if err != nil {
				return libqpu.LogOperation{}, err
			}
			attributesOld[k] = value
		}

		return libqpu.LogOperationDelta(
			notificationMsg.GetPayload().GetObjectId(),
			notificationMsg.GetPayload().GetBucket(),
			libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
			attributesOld,
			attributesNew,
		), nil
	}

	return libqpu.LogOperationDelta(
		notificationMsg.GetPayload().GetObjectId(),
		notificationMsg.GetPayload().GetBucket(),
		libqpu.Vectorclock(map[string]*tspb.Timestamp{ds.subscriptionEndpoint: timestamp}),
		nil,
		attributesNew,
	), nil
}

func (ds S3DataStore) formatLogOpState(table string, attributes map[string]*string) (map[string]*qpu.Value, error) {
	attrs := make(map[string]*qpu.Value)

	for k := range attributes {
		attributeKey := strings.ToLower(k)
		if !strings.Contains(attributeKey, "s3cmd-attrs") {
			value, err := ds.inputSchema.StrToValue(table, attributeKey, *attributes[k])
			if err != nil {
				return nil, err
			}
			attrs[attributeKey] = value
		}
	}

	return attrs, nil
}
