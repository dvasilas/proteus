//go:generate protoc --proto_path=./protos --proto_path=../protos --go_out=plugins=grpc:$GOPATH/src ./protos/store.proto

package gen
