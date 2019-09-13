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
	"github.com/dvasilas/proteus/src/protos"
	pbS3 "github.com/dvasilas/proteus/src/protos/s3"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//S3DataStore represents a s3datastore object
type S3DataStore struct {
	awsAccessKeyID     string
	awsSecretAccessKey string
	endpoint           string
	bucketName         string
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
func New(aKeyID string, aSecretKey string, endP string, bName string, logSEndP string) S3DataStore {
	s := S3DataStore{
		awsAccessKeyID:     aKeyID,
		awsSecretAccessKey: aSecretKey,
		endpoint:           endP,
		bucketName:         bName,
		logStreamEndpoint:  logSEndP,
	}
	return s
}

//SubscribeOps subscribes to updates from scality/cloudserver (supports sync and async mode)
// each time an update is received, it is formated as a pbUtils.LogOperation
// and sent to datastoredriver via a channel
func (ds S3DataStore) SubscribeOps(msg chan *pbUtils.LogOperation, ack chan bool, sync bool) (*grpc.ClientConn, <-chan error) {
	errs := make(chan error, 1)
	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		errs <- err
		return nil, errs
	}
	client := pbS3.NewNotificationServiceClient(conn)
	ctx := context.Background()
	stream, err := client.SubscribeNotifications(ctx)
	if err != nil {
		errs <- err
		return conn, errs
	}
	if err := stream.Send(&pbS3.RequestStream{Val: &pbS3.RequestStream_Request{Request: &pbS3.SubRequest{Sync: sync, Timestamp: time.Now().UnixNano()}}}); err != nil {
		errs <- err
		return conn, errs
	}
	ds.opConsumer(stream, msg, ack, errs, sync)
	return conn, errs
}

//GetSnapshot reads a snapshot of all objects in the specified bucket, and their metadata attributes
// by performing a http GET request to list all objects in the bucket
// and the a http HEAD to read the metadata attributes of each object
func (ds S3DataStore) GetSnapshot(msg chan *pbUtils.LogOperation) <-chan error {
	errs := make(chan error, 1)
	ds.readSnapshot(msg, errs, ds.processAndForwardObject)
	return errs
}

// Op ...
func (ds S3DataStore) Op(op *pbUtils.LogOperation) {}

//----------- Stream Consumer Functions ------------

//opConsumer creates a goroutine that receives a stream of updates from cloudserver,
// each time an update is received, it is parsed to a pbUtils.LogOperation object
// which is then sent to the datastoredriver via a channel
func (ds S3DataStore) opConsumer(stream pbS3.NotificationService_SubscribeNotificationsClient, msg chan *pbUtils.LogOperation, ack chan bool, errs chan error, sync bool) {
	go func() {
		for {
			op, err := stream.Recv()
			if err == io.EOF {
				errs <- errors.New("s3Datastore:opConsumer received EOF")
				break
			}
			if err != nil {
				errs <- err
				break
			}

			log.WithFields(log.Fields{
				"op": op,
			}).Debug("S3DataStore:opConsumer received op")

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

				if err := stream.Send(&pbS3.RequestStream{Val: &pbS3.RequestStream_Ack{Ack: &pbS3.AckMsg{Timestamp: op.GetTimestamp()}}}); err != nil {
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
func (ds S3DataStore) readSnapshot(msg chan *pbUtils.LogOperation, errs chan error, processObj func(string, http.Header, chan *pbUtils.LogOperation, chan error)) {
	buff := bytes.NewBuffer([]byte{})
	requestURL := fmt.Sprintf("%s/%s", ds.endpoint, ds.bucketName)

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

	log.WithFields(log.Fields{"contents": res.Contents}).Debug("s3DataStore: response")

	go func() {
		for _, r := range res.Contents {
			buff := bytes.NewBuffer([]byte{})
			requestURL = fmt.Sprintf("%s/%s/%s", ds.endpoint, ds.bucketName, r.Key)
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
			processObj(r.Key, resp.Header, msg, errs)
		}
		close(msg)
	}()
}

//processAndForwardObject receives the metadata attributes of an object
// in the form of an http header, it creates a *pbUtils.LogOperation
// and sends it to the datastoredriver to datastoredriver via a channel
func (ds S3DataStore) processAndForwardObject(key string, head http.Header, msg chan *pbUtils.LogOperation, errs chan error) {
	var payload *pbUtils.Payload
	attrs := make([]*pbUtils.Attribute, 0)

	for k := range head {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			attrk, attrt, attrv, err := xAmzMetaToAttr(k, head[k][0])
			if err != nil {
				errs <- err
				return
			}
			attrs = append(attrs, protoutils.Attribute(attrk, attrt, attrv))
		}
	}
	v, err := strconv.ParseInt(head["Content-Length"][0], 10, 64)
	if err != nil {
		errs <- err
		return
	}
	attrs = append(attrs, protoutils.Attribute("contentLength", pbUtils.Attribute_S3TAGINT, protoutils.ValueInt(v)))
	state := protoutils.ObjectState(attrs)
	payload = protoutils.PayloadState(state)

	ts, err := time.Parse(time.RFC1123, head["Last-Modified"][0])
	if err != nil {
		errs <- err
		return
	}
	obj := protoutils.LogOperation(
		key,
		ds.bucketName,
		pbUtils.LogOperation_S3OBJECT,
		protoutils.Vectorclock(map[string]uint64{"s3server": uint64(ts.UnixNano())}),
		payload,
	)

	log.WithFields(log.Fields{
		"object": obj,
	}).Debug("s3DataStore: snapshot")
	msg <- obj
}

func (ds S3DataStore) formatOperation(update *pbS3.NotificationStream) (*pbUtils.LogOperation, error) {
	var payload *pbUtils.Payload
	var stateNew, stateOld *pbUtils.ObjectState
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
		pbUtils.LogOperation_S3OBJECT,
		protoutils.Vectorclock(map[string]uint64{"s3server": uint64(update.GetTimestamp())}),
		payload), nil
}

func s3UpdateToObjectState(updateAttrs map[string]string, contentLen, lastModDate int64) (*pbUtils.ObjectState, error) {
	attrs := make([]*pbUtils.Attribute, len(updateAttrs)+2)
	i := 0
	for attrK, attrV := range updateAttrs {
		k, t, v, err := xAmzMetaToAttr(attrK, attrV)
		if err != nil {
			return nil, err
		}
		attrs[i] = protoutils.Attribute(k, t, v)
		i++
	}
	attrs[i] = protoutils.Attribute("contentLength", pbUtils.Attribute_S3TAGINT, protoutils.ValueInt(contentLen))
	i++
	attrs[i] = protoutils.Attribute("lastModified", pbUtils.Attribute_S3TAGINT, protoutils.ValueInt(lastModDate))

	return protoutils.ObjectState(attrs), nil
}

//xAmzMetaToAttr a key-value pair (strings) of s3 metadata attributes
// and based on the format of the key, it infers the datatype of the attribute's value
// and returns the key without the "x-amz-meta.." prefix,
// the data type (pbUtils.Attribute_AttributeType)
// and the value (*pbUtils.Value) of the attribute
func xAmzMetaToAttr(k string, v string) (string, pbUtils.Attribute_AttributeType, *pbUtils.Value, error) {
	if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-f-") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return "", pbUtils.Attribute_S3TAGFLT, nil, err
		}
		return strings.Split(strings.ToLower(k), "x-amz-meta-f-")[1], pbUtils.Attribute_S3TAGFLT, protoutils.ValueFlt(f), err
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-i-") {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", pbUtils.Attribute_S3TAGINT, nil, err
		}
		return strings.Split(strings.ToLower(k), "x-amz-meta-i-")[1], pbUtils.Attribute_S3TAGINT, protoutils.ValueInt(i), nil
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
		return strings.Split(strings.ToLower(k), "x-amz-meta-")[1], pbUtils.Attribute_S3TAGSTR, protoutils.ValueStr(v), nil
	}
	return k, pbUtils.Attribute_S3TAGSTR, protoutils.ValueStr(v), nil
}
