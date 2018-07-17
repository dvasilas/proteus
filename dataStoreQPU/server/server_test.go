package main

import (
	"os"
	"testing"
	"time"

	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb"
	fS "github.com/dimitriosvasilas/modqp/dataStoreQPU/fsDataStore"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var s Server

type mockDataStoreQPUGetSnapshotServer struct {
	grpc.ServerStream
	results []*pb.StateStream
}

type DataStoreQPUSubscribeOpsServer struct {
	grpc.ServerStream
	ops []*pb.OpStream
}

func (m *mockDataStoreQPUGetSnapshotServer) Send(obj *pb.StateStream) error {
	m.results = append(m.results, obj)
	return nil
}

func (m *DataStoreQPUSubscribeOpsServer) Send(op *pb.OpStream) error {
	m.ops = append(m.ops, op)
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
	mock := &mockDataStoreQPUGetSnapshotServer{}
	err := s.GetSnapshot(req, mock)
	if assert.Nil(t, err) {
		assert.NotEmpty(t, mock.results, "GetSnapshot return empty result")
		assert.NotNil(t, mock.results[0].Object, "")
	}
}

func TestSubscribeOps(t *testing.T) {
	req := &pb.SubRequest{}
	mock := &DataStoreQPUSubscribeOpsServer{}
	go s.SubscribeOps(req, mock)

	time.Sleep(100 * time.Millisecond)

	path := viper.Get("HOME").(string) + viper.GetString("datastore.fs.dataDir")

	f, err := os.OpenFile(path+"temp.txt", os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	_, err = f.WriteString("testing...\n")
	f.Sync()
	f.Close()

	err = os.Remove(path + "temp.txt")
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, mock.ops[0].Operation.Key, path+"temp.txt")
	assert.Equal(t, mock.ops[0].Operation.Op, "CREATE")
}

func testEndToEndSubscribeOps(t *testing.T) {
}
