PROTOC := $(shell which protoc)
UNAME := $(shell uname)

$(PROTOC):
ifeq ($(UNAME), Darwin)
	https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-osx-x86_64.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif
ifeq ($(UNAME), Linux)
	curl -L https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip -o /tmp/protoc.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif

all: compile_ds compile_qpu

dep:

./protos/utils/utils.pb.go: ./protos/utils/utils.proto
	protoc --go_out=plugins=grpc:$(GOPATH)/src/ ./protos/utils/utils.proto

./protos/datastore/datastore.pb.go: ./protos/datastore/datastore.proto
	protoc --proto_path=./protos/utils/ --proto_path=./protos/datastore/ --go_out=plugins=grpc:$(GOPATH)/src ./protos/datastore/datastore.proto

./protos/s3/s3.pb.go: ./protos/s3/s3.proto
	protoc --proto_path=./protos/utils --proto_path=./protos/s3 --go_out=plugins=grpc:$(GOPATH)/src ./protos/s3/s3.proto

./protos/qpu/qpu.pb.go: ./protos/qpu/qpu.proto
	protoc --proto_path=./protos/qpu --proto_path=./protos/utils --go_out=plugins=grpc:$(GOPATH)/src/ ./protos/qpu/qpu.proto

.PHONY: compile_ds
compile_ds: dep ./protos/utils/utils.pb.go ./protos/datastore/datastore.pb.go ./protos/s3/s3.pb.go

.PHONY: compile_qpu
compile_qpu: dep ./protos/utils/utils.pb.go ./protos/qpu/qpu.pb.go

clean:
	rm ./protos/utils/utils.pb.go ./protos/datastore/datastore.pb.go ./protos/s3/s3.pb.go ./protos/qpu/qpu.pb.go