
all: build_qpu_server build_ds_server build_shell

PROTOC := $(shell which protoc)
UNAME := $(shell uname)

$(PROTOC_CMD):
ifeq ($(UNAME), Darwin)
	https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-osx-x86_64.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif
ifeq ($(UNAME), Linux)
	curl -L https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip -o /tmp/protoc.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif

dep:
	go get github.com/golang/protobuf/protoc-gen-go
	go get -d github.com/golang/mock/gomock
	go install github.com/golang/mock/mockgen
	go get -d google.golang.org/grpc
	go get -d github.com/hpcloud/tail
	go get -d github.com/stretchr/testify
	go get -d github.com/abiosoft/ishell
	go get -d github.com/spf13/viper
	go get -d github.com/fsnotify/fsnotify
	go get -d github.com/google/btree
	go get -d github.com/sirupsen/logrus
	go get -d github.com/aws/aws-sdk-go

proto: $(PROTOC_CMD)
	protoc --go_out=plugins=grpc:$(GOPATH)/src/ ./protos/utils/utils.proto
	protoc --proto_path=./protos/utils/ --proto_path=./protos/datastore/ --go_out=plugins=grpc:$(GOPATH)/src ./protos/datastore/datastore.proto
	protoc --proto_path=./protos/utils --proto_path=./protos/s3 --go_out=plugins=grpc:$(GOPATH)/src ./protos/s3/s3.proto
	protoc --proto_path=./protos/utils --proto_path=./protos/antidote --go_out=plugins=grpc:$(GOPATH)/src ./protos/antidote/antidote.proto
	protoc --proto_path=./protos/qpu --proto_path=./protos/utils --go_out=plugins=grpc:$(GOPATH)/src/ ./protos/qpu/qpu.proto
	go generate ./...

build_ds_server: dep proto
	go build -o bin/ds_server -v ./dataStoreQPU/server/server.go

build_qpu_server: dep proto
	go build -o bin/qpu_server -v ./qpu/server/server.go

build_shell: dep proto
	go build -o bin/shell -v ./shell/shell.go

# Cross compilation
build_ds_server_linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/ds_server_linux -v ./dataStoreQPU/server/server.go
build_qpu_server_linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/qpu_server_linux -v ./qpu/server/server.go

serve_ds:
	./bin/ds_server

serve_scan_qpu:
	$(PWD)/bin/qpu_server -qpu=scanQPU

serve_index_qpu:
	$(PWD)/bin/qpu_server -qpu=indexQPU

test:
	go test -v ./...

clean:
	rm ./protos/utils/utils.pb.go ./protos/datastore/datastore.pb.go ./protos/s3/s3.pb.go ./protos/qpu/qpu.pb.go
	rm -rf ./bin
	rm -rf ./dataStoreQPU/client/mocks

.PHONY: build_qpu_server build_qpu_server test clean
