//go:generate protoc --proto_path=./qpupb --proto_path=../qpuUtilspb --go_out=plugins=grpc:$GOPATH/src/ ./qpupb/qpu.proto

package gen
