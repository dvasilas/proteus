//go:generate protoc --proto_path=./protos --proto_path=../protos --go_out=plugins=grpc:$GOPATH/src ./protos/dsqpu.proto
//go:generate mockgen -destination=client/mocks/dsqpu_mock.go github.com/dimitriosvasilas/modqp/dataStoreQPU/protos DataStoreQPUClient,DataStoreQPU_GetSnapshotClient

package gen
