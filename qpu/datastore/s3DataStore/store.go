package store

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	utils "github.com/dvasilas/proteus"
	"github.com/dvasilas/proteus/protos"
	pb "github.com/dvasilas/proteus/protos/s3"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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

func (ds S3DataStore) watchSync(stream pb.S3DataStore_WatchSyncClient, msg chan *pbQPU.Operation, ack chan bool, errs chan error) {
	go func() {
		for {
			inMsg, err := stream.Recv()

			if err == io.EOF {
				errs <- nil
				return
			}
			if err != nil {
				errs <- err
				return
			}

			op := inMsg.GetOperation()

			log.WithFields(log.Fields{
				"opId":   op.GetOpId(),
				"key":    op.GetKey(),
				"bucket": op.GetBucket(),
				"object": op.GetOpPayload().GetState(),
			}).Debug("S3DataStore:watchSync received op")

			op, err = formatAttributes(op)
			if err != nil {
				errs <- err
				return
			}

			log.Debug("S3DataStore:watchSync processed and forwarding operation")
			msg <- op

			ackMsg := <-ack
			log.WithFields(log.Fields{
				"message": ackMsg,
			}).Debug("S3DataStore:watchSync received ACK, sending ACK to data store")
			if err := stream.Send(protoutils.OpAckS3("ack", op.GetOpId())); err != nil {
				errs <- err
				return
			}
		}
	}()
}

func (ds S3DataStore) watchAsync(stream pb.S3DataStore_WatchAsyncClient, msg chan *pbQPU.Operation, errs chan error) {
	go func() {
		for {
			inMsg, err := stream.Recv()

			if err == io.EOF {
				errs <- nil
				return
			}
			if err != nil {
				errs <- err
				return
			}

			op := inMsg.GetOperation()

			log.WithFields(log.Fields{
				"opId":   op.GetOpId(),
				"key":    op.GetKey(),
				"bucket": op.GetBucket(),
				"object": op.GetOpPayload().GetState(),
			}).Debug("S3DataStore:watchAsync received op")

			op, err = formatAttributes(op)
			if err != nil {
				errs <- err
				return
			}

			msg <- op
		}
	}()
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

	err := viper.BindEnv("DEBUG")
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("BindEnv DEBUG failed")
	}
	debug := viper.GetBool("DEBUG")
	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	return s
}

//GetSnapshot ...
func (ds S3DataStore) GetSnapshot(msg chan *pbQPU.Object) chan error {
	log.Debug("s3DataStore: GetSnapshot()")
	errCh := make(chan error)

	go func() {
		buff := bytes.NewBuffer([]byte{})
		requestURL := fmt.Sprintf("%s/%s", ds.endpoint, ds.bucketName)

		request, err := http.NewRequest("GET", requestURL, buff)
		if err != nil {
			close(msg)
			errCh <- err
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
			close(msg)
			errCh <- err
			return
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			close(msg)
			errCh <- err
			return
		}
		var res listBucketResult
		xml.Unmarshal(body, &res)

		log.WithFields(log.Fields{
			"contents": res.Contents,
		}).Debug("s3DataStore: response")

		for _, r := range res.Contents {
			buff := bytes.NewBuffer([]byte{})
			requestURL = fmt.Sprintf("%s/%s/%s", ds.endpoint, ds.bucketName, r.Key)
			request, err = http.NewRequest("HEAD", requestURL, buff)
			if err != nil {
				close(msg)
				errCh <- err
				return
			}
			reader := bytes.NewReader(buff.Bytes())
			signer.Sign(request, reader, "s3", "us-east-1", time.Now())

			resp, err = client.Do(request)
			if err != nil {
				close(msg)
				errCh <- err
				return
			}

			outObject := protoutils.Object(
				r.Key,
				map[string]*pbQPU.Value{
					"size": protoutils.ValueInt(r.Size),
				},
			)
			for k := range resp.Header {
				if strings.HasPrefix(k, "X-Amz-Meta") && k != "X-Amz-Meta-S3cmd-Attrs" {
					attrK, attrV, err := utils.AttrToVal(k, resp.Header[k][0])
					if err != nil {
						close(msg)
						errCh <- err
						return
					}
					outObject.Attributes[attrK] = attrV
				}
			}
			outObject.Attributes["x-amz-meta-s3cmd-attrs"] = protoutils.ValueStr(resp.Header["X-Amz-Meta-S3cmd-Attrs"][0])

			log.WithFields(log.Fields{
				"object": outObject,
			}).Debug("s3DataStore: snapshot")

			msg <- outObject
		}
		close(msg)
		errCh <- nil
	}()
	return errCh
}

//SubscribeOps ...
func (ds S3DataStore) SubscribeOps(msg chan *pbQPU.Operation, ack chan bool, sync bool) (*grpc.ClientConn, chan error) {
	errCh := make(chan error)

	conn, err := grpc.Dial(ds.logStreamEndpoint, grpc.WithInsecure())
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	client := pb.NewS3DataStoreClient(conn)
	if !sync {
		ctx := context.Background()
		stream, err := client.WatchAsync(ctx, protoutils.SubRequestS3(time.Now().UnixNano()))
		if err != nil {
			errCh <- err
			return conn, errCh
		}
		ds.watchAsync(stream, msg, errCh)
	} else {
		stream, err := client.WatchSync(context.Background())
		ctx := stream.Context()
		if err != nil {
			errCh <- err
			return nil, errCh
		}
		ds.watchSync(stream, msg, ack, errCh)
		go func() {
			<-ctx.Done()
			if err := ctx.Err(); err != nil {
				errCh <- err
				return
			}
		}()
	}
	return conn, errCh
}

func formatAttributes(op *pbQPU.Operation) (*pbQPU.Operation, error) {
	for attrK := range op.GetOpPayload().GetState().GetAttributes() {
		if strings.HasPrefix(attrK, "x-amz-meta-") && strings.Compare(attrK, "x-amz-meta-s3cmd-attrs") != 0 {
			attrK, attrV, err := utils.AttrToVal(attrK, op.GetOpPayload().GetState().GetAttributes()[attrK].GetStr())
			if err != nil {
				return nil, err
			}
			op.GetOpPayload().GetState().GetAttributes()[attrK] = attrV
		}
	}
	return op, nil
}
