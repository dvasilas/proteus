//go:generate protoc --proto_path=./dsqpupb --proto_path=../qpupb --go_out=plugins=grpc:$GOPATH/src ./dsqpupb/dsqpu.proto
//go:generate mockgen -destination=client/mocks/dsqpu_mock.go github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpupb DataStoreQPUClient,DataStoreQPU_GetSnapshotClient

package gen
