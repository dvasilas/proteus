package store

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	utils "github.com/dimitriosvasilas/modqp"
	pb "github.com/dimitriosvasilas/modqp/protos/s3"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

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

//S3DataStore ...
type S3DataStore struct {
	awsAccessKeyID     string
	awsSecretAccessKey string
	endpoint           string
	bucketName         string
	logStreamEndpoint  string
}

func (ds S3DataStore) watchSync(stream pb.S3DataStore_WatchSyncClient, msg chan *pbQPU.Operation, done chan bool, ack chan bool, errs chan error) {
	for {
		op, err := stream.Recv()
		log.WithFields(log.Fields{
			"operation": op.GetOperation(),
		}).Debug("S3DataStore:watchSync received operation")
		if err == io.EOF {
			done <- true
			errs <- nil
			return
		}
		if err != nil {
			done <- true
			errs <- err
			return
		}
		done <- false
		outOp := &pbQPU.Operation{
			OpId: op.GetOperation().GetOpId(),
			Object: &pbQPU.Object{
				Key: op.GetOperation().GetObject().GetKey(),
				Attributes: map[string]*pbQPU.Value{
					"size": utils.ValInt(op.Operation.Object.Attributes["size"].GetIntValue()),
				},
			},
		}
		for attrK := range op.Operation.Object.Attributes {
			if strings.HasPrefix(attrK, "x-amz-meta-") && strings.Compare(attrK, "x-amz-meta-s3cmd-attrs") != 0 {
				attrK, attrV, err := formatAttributes(attrK, op.Operation.Object.Attributes[attrK].GetStringValue())
				if err != nil {
					done <- true
					errs <- err
				}
				outOp.Object.Attributes[attrK] = attrV
			}
		}
		log.Debug("S3DataStore:watchSync processed and forwarding operation")
		msg <- outOp

		ackMsg := <-ack
		log.WithFields(log.Fields{
			"message": ackMsg,
		}).Debug("S3DataStore:watchSync received ACK, sending ACK to data store")
		if err := stream.Send(&pb.OpAck{Msg: "ack", OpId: op.GetOperation().GetOpId()}); err != nil {
			done <- true
			errs <- err
			return
		}
	}
}

func (ds S3DataStore) watchAsync(stream pb.S3DataStore_WatchAsyncClient, msg chan *pbQPU.Operation, done chan bool, errs chan error) {
	for {
		op, err := stream.Recv()
		if err == io.EOF {
			done <- true
			errs <- nil
		}
		if err != nil {
			done <- true
			errs <- err
		}
		done <- false
		outOp := &pbQPU.Operation{
			Object: &pbQPU.Object{
				Key: op.Operation.Object.Key,
				Attributes: map[string]*pbQPU.Value{
					"size": utils.ValInt(op.Operation.Object.Attributes["size"].GetIntValue()),
				},
			},
		}
		for attrK := range op.Operation.Object.Attributes {
			if strings.HasPrefix(attrK, "x-amz-meta-") && strings.Compare(attrK, "x-amz-meta-s3cmd-attrs") != 0 {
				attrK, attrV, err := formatAttributes(attrK, op.Operation.Object.Attributes[attrK].GetStringValue())
				if err != nil {
					done <- true
					errs <- err
				}
				outOp.Object.Attributes[attrK] = attrV
			}
		}
		msg <- outOp
	}
}

//New ...
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

//GetSnapshot ...
func (ds S3DataStore) GetSnapshot(msg chan *pbQPU.Object, done chan bool, errs chan error) {
	buff := bytes.NewBuffer([]byte{})
	requestURL := fmt.Sprintf("%s/%s", ds.endpoint, ds.bucketName)
	request, err := http.NewRequest("GET", requestURL, buff)
	if err != nil {
		done <- true
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
		done <- true
		errs <- err
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		done <- true
		errs <- err
		return
	}
	var res listBucketResult
	xml.Unmarshal(body, &res)

	for _, r := range res.Contents {
		buff := bytes.NewBuffer([]byte{})
		requestURL = fmt.Sprintf("%s/%s/%s", ds.endpoint, ds.bucketName, r.Key)
		request, err = http.NewRequest("HEAD", requestURL, buff)
		if err != nil {
			done <- true
			errs <- err
			return
		}
		reader := bytes.NewReader(buff.Bytes())
		signer.Sign(request, reader, "s3", "us-east-1", time.Now())

		resp, err = client.Do(request)
		if err != nil {
			done <- true
			errs <- err
			return
		}

		outObject := &pbQPU.Object{
			Key: r.Key,
			Attributes: map[string]*pbQPU.Value{
				"size": utils.ValInt(r.Size),
			},
		}
		for k := range resp.Header {
			if strings.HasPrefix(k, "X-Amz-Meta") && k != "X-Amz-Meta-S3cmd-Attrs" {
				attrK, attrV, err := formatAttributes(k, resp.Header[k][0])
				if err != nil {
					done <- true
					errs <- err
					return
				}
				outObject.Attributes[attrK] = attrV
			}
		}
		done <- false
		msg <- outObject
	}
	done <- true
	errs <- nil
}

func formatAttributes(k string, v string) (string, *pbQPU.Value, error) {
	if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-f-") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {

		}
		return strings.ToLower(k), utils.ValFlt(f), nil
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-i-") {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", &pbQPU.Value{}, err
		}
		return strings.ToLower(k), utils.ValInt(i), nil
	} else {
		return strings.ToLower(k), utils.ValStr(v), nil
	}
}

//SubscribeOpsSync ...
func (ds S3DataStore) SubscribeOpsSync(msg chan *pbQPU.Operation, done chan bool, ack chan bool, errs chan error) {
	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		done <- true
		errs <- err
	}
	defer conn.Close()

	client := pb.NewS3DataStoreClient(conn)
	stream, err := client.WatchSync(context.Background())
	ctx := stream.Context()
	if err != nil {
		done <- true
		errs <- err
	}
	go ds.watchSync(stream, msg, done, ack, errs)

	go func() {
		<-ctx.Done()
		if err := ctx.Err(); err != nil {
			done <- true
			errs <- err
		}
	}()

	<-errs
}

//SubscribeOpsAsync ...
func (ds S3DataStore) SubscribeOpsAsync(msg chan *pbQPU.Operation, done chan bool, errs chan error) {
	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		done <- true
		errs <- err
	}
	defer conn.Close()
	ctx := context.Background()
	client := pb.NewS3DataStoreClient(conn)
	stream, err := client.WatchAsync(ctx, &pb.SubRequest{Timestamp: time.Now().UnixNano()})
	if err != nil {
		done <- true
		errs <- err
	}
	go ds.watchAsync(stream, msg, done, errs)
	<-errs
}
