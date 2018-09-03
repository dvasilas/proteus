package store

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	utils "github.com/dimitriosvasilas/modqp"
	pb "github.com/dimitriosvasilas/modqp/protos/s3"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
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

func (ds S3DataStore) watch(stream pb.S3DataStore_WatchClient, msg chan *pbQPU.Operation, done chan bool, errs chan error) {
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
		msg <- &pbQPU.Operation{
			Key: op.Operation.Object.Key,
			Op:  op.Operation.Op,
			Object: &pbQPU.Object{
				Key: op.Operation.Object.Key,
				Attributes: map[string]*pbQPU.Value{
					"size": utils.ValInt(op.Operation.Object.Attributes["size"].GetIntValue()),
				},
			},
		}
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
		done <- false
		msg <- &pbQPU.Object{
			Key: r.Key,
			Attributes: map[string]*pbQPU.Value{
				"size": utils.ValInt(r.Size),
			},
		}
	}
	done <- true
	errs <- nil
}

//SubscribeOps ...
func (ds S3DataStore) SubscribeOps(msg chan *pbQPU.Operation, done chan bool, errs chan error) {
	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		done <- true
		errs <- err
	}
	defer conn.Close()
	ctx := context.Background()
	client := pb.NewS3DataStoreClient(conn)
	stream, err := client.Watch(ctx, &pb.SubRequest{Timestamp: time.Now().UnixNano()})
	if err != nil {
		done <- true
		errs <- err
	}
	go ds.watch(stream, msg, done, errs)
	<-errs
}
