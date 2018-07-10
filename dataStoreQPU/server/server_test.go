package main

import (
	"os"
	"testing"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	fS "github.com/dimitriosvasilas/modqp/dataStoreQPU/fsDataStore"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var s Server

type mockDataStoreQPU_GetSnapshotServer struct {
	grpc.ServerStream
	results []*pb.StateStream
}

func (m *mockDataStoreQPU_GetSnapshotServer) Send(obj *pb.StateStream) error {
	m.results = append(m.results, obj)
	return nil
}

func TestMain(m *testing.M) {
	getConfig()
	s = Server{ds: fS.FSDataStore{}}
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestGetSnapshot(t *testing.T) {
	req := &pb.SubRequest{}

	mock := &mockDataStoreQPU_GetSnapshotServer{}
	err := s.GetSnapshot(req, mock)
	if assert.Nil(t, err) {
		assert.NotEmpty(t, mock.results, "GetSnapshot return empty result")
		assert.NotNil(t, mock.results[0].Object, "")
	}
}
