//go:generate protoc --proto_path=./sqpupb --proto_path=../qpupb --go_out=plugins=grpc:$GOPATH/src/ ./sqpupb/sqpu.proto

package gen
