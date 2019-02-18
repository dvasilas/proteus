package main

import (
	"os"
	"testing"
	"time"

	config "github.com/dimitriosvasilas/proteus/config"
	fS "github.com/dimitriosvasilas/proteus/dataStoreQPU/fsDataStore"
	pb "github.com/dimitriosvasilas/proteus/protos/datastore"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var s Server
var conf config.DSQPUConfig

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
	var conf config.DSQPUConfig
	if err := config.GetConfig("filter.1", &conf); err != nil {
		return
	}
	s = Server{ds: fS.New(viper.Get("HOME").(string) + conf.DataStore.DataDir)}
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

	f, err := os.OpenFile(viper.Get("HOME").(string)+conf.DataStore.DataDir+"temp.txt", os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	time.Sleep(100 * time.Millisecond)

	go s.SubscribeOpsAsync(req, mock)

	time.Sleep(100 * time.Millisecond)

	_, _ = f.WriteString("testing...\n")
	f.Sync()
	f.Close()

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, mock.ops[0].Operation.Key, viper.Get("HOME").(string)+conf.DataStore.DataDir+"temp.txt")
}

func testEndToEndSubscribeOps(t *testing.T) {
}
