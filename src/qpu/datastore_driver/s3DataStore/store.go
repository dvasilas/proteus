package store

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/dvasilas/proteus/src/proto"
	"github.com/dvasilas/proteus/src/proto/qpu"
	"github.com/dvasilas/proteus/src/proto/s3"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//S3DataStore represents a s3datastore object
type S3DataStore struct {
	awsAccessKeyID     string
	awsSecretAccessKey string
	endpoint           string
	logStreamEndpoint  string
}

//listBucketResult is used to unmarshal the body of an http GET response
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

//New creates and initializes an instance of S3S3DataStore
func New(aKeyID string, aSecretKey string, endP string, logSEndP string) S3DataStore {
	s := S3DataStore{
		awsAccessKeyID:     aKeyID,
		awsSecretAccessKey: aSecretKey,
		endpoint:           endP,
		logStreamEndpoint:  logSEndP,
	}
	return s
}

//SubscribeOps subscribes to updates from scality/cloudserver (supports sync and async mode)
// each time an update is received, it is formated as a qpu.LogOperation
// and sent to datastoredriver via a channel
func (ds S3DataStore) SubscribeOps(msg chan *qpu.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error) {
	errs := make(chan error, 1)
	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		errs <- err
		return nil, errs
	}
	client := s3.NewNotificationServiceClient(conn)
	ctx := context.Background()
	stream, err := client.SubscribeNotifications(ctx)
	if err != nil {
		errs <- err
		return conn, errs
	}
	if err := stream.Send(&s3.RequestStream{Val: &s3.RequestStream_Request{Request: &s3.SubRequest{Sync: sync, Timestamp: time.Now().UnixNano()}}}); err != nil {
		errs <- err
		return conn, errs
	}
	ds.opConsumer(stream, msg, ack, errs, sync)
	return conn, errs
}

//GetSnapshot reads a snapshot of all objects in the specified bucket, and their metadata attributes
// by performing a http GET request to list all objects in the bucket
// and the a http HEAD to read the metadata attributes of each object
func (ds S3DataStore) GetSnapshot(bucket string, msg chan *qpu.LogOperation) <-chan error {
	errs := make(chan error, 1)
	ds.readSnapshot(bucket, msg, errs, ds.processAndForwardObject)
	return errs
}

// Op ...
func (ds S3DataStore) Op(op *qpu.LogOperation) {}

//----------- Stream Consumer Functions ------------

//opConsumer creates a goroutine that receives a stream of updates from cloudserver,
// each time an update is received, it is parsed to a qpu.LogOperation object
// which is then sent to the datastoredriver via a channel
func (ds S3DataStore) opConsumer(stream s3.NotificationService_SubscribeNotificationsClient, msg chan *qpu.LogOperation, ack chan bool, errs chan error, sync bool) {
	go func() {
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
				<-ack

				log.WithFields(log.Fields{
					"timestamp": op.GetTimestamp(),
				}).Debug("S3DataStore:watchSync received ACK, sending ACK to data store")

				if err := stream.Send(&s3.RequestStream{Val: &s3.RequestStream_Ack{Ack: &s3.AckMsg{Timestamp: op.GetTimestamp()}}}); err != nil {
					errs <- err
					return
				}
			}
		}
		close(msg)
		close(errs)
	}()
}

//---------------- Internal Functions --------------

//readSnapshot sends a http GET request to cloudserver in order to retrieve
// all objects in the given bucket
// for each object it sends a http HEAD request to get the object's metadata attributes
// and calls the processObj function
func (ds S3DataStore) readSnapshot(bucket string, msg chan *qpu.LogOperation, errs chan error, processObj func(string, string, http.Header, chan *qpu.LogOperation, chan error)) {
	buff := bytes.NewBuffer([]byte{})
	requestURL := fmt.Sprintf("%s/%s", ds.endpoint, bucket)
	request, err := http.NewRequest("GET", requestURL, buff)
	if err != nil {
		errs <- err
		return
	}
	reader := bytes.NewReader(buff.Bytes())
	credentials := credentials.NewStaticCredentials(ds.awsAccessKeyID,
		ds.awsSecretAccessKey, "")
	signer := v4.NewSigner(credentials)
	signer.Sign(request, reader, "s3", "us-east-1", time.Now())
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		errs <- err
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errs <- err
		return
	}
	var res listBucketResult
	xml.Unmarshal(body, &res)
	go func() {
		for _, r := range res.Contents {
			buff := bytes.NewBuffer([]byte{})
			requestURL = fmt.Sprintf("%s/%s/%s", ds.endpoint, bucket, r.Key)
			request, err = http.NewRequest("HEAD", requestURL, buff)
			if err != nil {
				errs <- err
				close(errs)
				close(msg)
				break
			}
			reader := bytes.NewReader(buff.Bytes())
			signer.Sign(request, reader, "s3", "us-east-1", time.Now())

			resp, err = client.Do(request)
			if err != nil {
				errs <- err
				close(errs)
				close(msg)
				break
			}
			processObj(r.Key, bucket, resp.Header, msg, errs)
		}
		close(msg)
	}()
}

//processAndForwardObject receives the metadata attributes of an object
// in the form of an http header, it creates a *qpu.LogOperation
// and sends it to the datastoredriver to datastoredriver via a channel
func (ds S3DataStore) processAndForwardObject(key string, bucket string, head http.Header, msg chan *qpu.LogOperation, errs chan error) {
	var payload *qpu.Payload
	attrs := make([]*qpu.Attribute, 0)

	for k := range head {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") && !strings.Contains(strings.ToLower(k), "s3cmd-attrs") {
			attrk, attrv, err := xAmzMetaToAttr(k, head[k][0])
			if err != nil {
				errs <- err
				return
			}
			attrs = append(attrs, protoutils.Attribute(attrk, attrv))
		}
	}
	v, err := strconv.ParseInt(head["Content-Length"][0], 10, 64)
	if err != nil {
		errs <- err
		return
	}
	attrs = append(attrs, protoutils.Attribute("contentLength", protoutils.ValueInt(v)))
	state := protoutils.ObjectState(attrs)
	payload = protoutils.PayloadState(state)

	ts, err := time.Parse(time.RFC1123, head["Last-Modified"][0])
	if err != nil {
		errs <- err
		return
	}
	obj := protoutils.LogOperation(
		key,
		bucket,
		qpu.LogOperation_S3OBJECT,
		protoutils.Vectorclock(map[string]uint64{"s3server": uint64(ts.UnixNano() / int64(time.Millisecond))}),
		payload,
	)
	msg <- obj
}

func (ds S3DataStore) formatOperation(update *s3.NotificationStream) (*qpu.LogOperation, error) {
	var payload *qpu.Payload
	var stateNew, stateOld *qpu.ObjectState
	var err error
	if update.GetPayload().GetNewState() != nil {
		stateNew, err = s3UpdateToObjectState(
			update.GetPayload().GetNewState().GetAttributes(),
			update.GetPayload().GetNewState().GetContentLength(),
			update.GetPayload().GetNewState().GetLastModified())
		if err != nil {
			return nil, err
		}
	}
	if update.GetPayload().GetOldState() != nil {
		stateOld, err = s3UpdateToObjectState(
			update.GetPayload().GetOldState().GetAttributes(),
			update.GetPayload().GetOldState().GetContentLength(),
			update.GetPayload().GetOldState().GetLastModified())
		if err != nil {
			return nil, err
		}
	}
	payload = protoutils.PayloadDelta(stateOld, stateNew)
	return protoutils.LogOperation(
		update.GetPayload().GetObjectId(),
		update.GetPayload().GetBucket(),
		qpu.LogOperation_S3OBJECT,
		protoutils.Vectorclock(map[string]uint64{"s3server": uint64(update.GetTimestamp())}),
		payload), nil
}

func s3UpdateToObjectState(updateAttrs map[string]string, contentLen, lastModDate int64) (*qpu.ObjectState, error) {
	attrs := make([]*qpu.Attribute, len(updateAttrs)+2)
	i := 0
	for attrK, attrV := range updateAttrs {
		k, v, err := xAmzMetaToAttr(attrK, attrV)
		if err != nil {
			return nil, err
		}
		attrs[i] = protoutils.Attribute(k, v)
		i++
	}
	attrs[i] = protoutils.Attribute("contentLength", protoutils.ValueInt(contentLen))
	i++
	attrs[i] = protoutils.Attribute("lastModified", protoutils.ValueInt(lastModDate))

	return protoutils.ObjectState(attrs), nil
}

// xAmzMetaToAttr a key-value pair (strings) of s3 metadata attributes
// and based on the format of the key, it infers the datatype of the attribute's value
// and returns the key without the "x-amz-meta.." prefix,
// and the value (*qpu.Value) of the attribute
func xAmzMetaToAttr(k string, v string) (string, *qpu.Value, error) {
	if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-f-") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return "", nil, err
		}
		return strings.Split(strings.ToLower(k), "x-amz-meta-f-")[1], protoutils.ValueFlt(f), err
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-i-") {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", nil, err
		}
		return strings.Split(strings.ToLower(k), "x-amz-meta-i-")[1], protoutils.ValueInt(i), nil
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
		return strings.Split(strings.ToLower(k), "x-amz-meta-")[1], protoutils.ValueStr(v), nil
	}
	return k, protoutils.ValueStr(v), nil
}
