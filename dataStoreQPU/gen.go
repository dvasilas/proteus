//go:generate protoc --proto_path=./dsqpupb --proto_path=../qpuUtilspb --go_out=plugins=grpc:$GOPATH/src ./dsqpupb/dsqpu.proto
//go:generate protoc --proto_path=./s3pb --go_out=plugins=grpc:$GOPATH/src ./s3pb/s3oplog.proto
//go:generate mockgen -destination=client/mocks/dsqpu_mock.go github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb DataStoreQPUClient,DataStoreQPU_GetSnapshotClient

package gen
