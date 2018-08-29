package store

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
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
}

//New ...
func New(aKeyID string, aSecretKey string, endP string, bName string) S3DataStore {
	return S3DataStore{
		awsAccessKeyID:     aKeyID,
		awsSecretAccessKey: aSecretKey,
		endpoint:           endP,
		bucketName:         bName,
	}
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
	done <- true
	errs <- errors.New("not implemented")
}
