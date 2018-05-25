package main

import (
	"context"
	"os"
	"testing"

	pb "github.com/dimitriosvasilas/modqp/dataStore/datastore"
	"github.com/stretchr/testify/assert"
)

var putTests = []struct {
	key         string
	attributes  map[string]string
	msgExpected string
}{
	{
		key:         "key1",
		attributes:  map[string]string{"size": "1024"},
		msgExpected: "OK",
	},
	{
		key:         "key2",
		attributes:  map[string]string{},
		msgExpected: "OK",
	},
	{
		key:         "key3",
		msgExpected: "OK",
	},
	{
		key:         "",
		msgExpected: "error_key_empty",
	},
	{
		msgExpected: "error_key_empty",
	},
}
var getTests = []struct {
	key        string
	attributes map[string]string
}{
	{
		key:        "key1",
		attributes: map[string]string{"size": "1024", "type": "json"},
	},
}

var readPastPuts = []struct {
	key         string
	attributes  map[string]string
	msgExpected string
}{
	{
		key:        "key",
		attributes: map[string]string{},
	},
	{
		key:        "key",
		attributes: map[string]string{"size": "256"},
	},
	{
		key:        "key",
		attributes: map[string]string{"size": "512"},
	},
	{
		key:        "key",
		attributes: map[string]string{"size": "1024"},
	},
}

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestPutObjectMDArgs(t *testing.T) {
	s := newDSServer(":50051")
	for _, tt := range putTests {
		req := &pb.PutObjMDRequest{Key: tt.key, Attributes: tt.attributes}
		resp, err := s.PutObjectMD(context.Background(), req)
		if assert.Nil(t, err) {
			assert.Equal(t, resp.Message, tt.msgExpected, "")
		}
	}
}
func TestGetObjectMDArgs(t *testing.T) {
	s := newDSServer(":50051")
	for _, tt := range getTests {
		putReq := &pb.PutObjMDRequest{Key: tt.key, Attributes: tt.attributes}
		putResp, _ := s.PutObjectMD(context.Background(), putReq)
		getReq := &pb.GetObjMDRequest{Key: tt.key, Timestamp: putResp.Timestamp}
		resp, err := s.GetObjectMD(context.Background(), getReq)
		if assert.Nil(t, err) {
			assert.Equal(t, resp.Message, "OK", "")
			assert.Equal(t, resp.Object.Key, tt.key, "")
			assert.Equal(t, resp.Object.State, tt.attributes, "")
		}
	}
}

func TestGetObjectMDReadPast(t *testing.T) {
	s := newDSServer(":50051")
	timestamps := make([]int64, 1000)
	for i, tt := range readPastPuts {
		putReq := &pb.PutObjMDRequest{Key: tt.key, Attributes: tt.attributes}
		putResp, _ := s.PutObjectMD(context.Background(), putReq)
		timestamps[i] = putResp.Timestamp
	}
	for i, tt := range readPastPuts {
		getReq := &pb.GetObjMDRequest{Key: "key", Timestamp: timestamps[i]}
		resp, err := s.GetObjectMD(context.Background(), getReq)
		if assert.Nil(t, err) {
			assert.Equal(t, resp.Message, "OK", "")
			assert.Equal(t, resp.Object.Key, tt.key, "")
			assert.Equal(t, resp.Object.State, tt.attributes, "")
		}
	}
}
