APP      := proteus
BIN_DIR  := ${CURDIR}/bin
PKGS     := $(or $(PKG),$(shell env GO111MODULE=on go list ./...))
TESTPKGS := $(shell env GO111MODULE=on go list -f \
			'{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' \
			$(PKGS))

DOCKER_NET := proteus-local-dev-net1

REPO_PROTEUS := dvasilas/proteus
TAG := $(shell git log -1 --pretty=%H | cut -c1-8)
IMG_QPU := ${REPO_PROTEUS}:${TAG}
IMG_REG := 127.0.0.1:5000/${APP}:${TAG}
IMG_REG_SL := 127.0.0.1:5000/${APP}-stateless:${TAG}

$(PROTOC_CMD):
ifeq ($(UNAME), Darwin)
	https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-osx-x86_64.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif
ifeq ($(UNAME), Linux)
	curl -L https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip -o /tmp/protoc.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif

export GO111MODULE=on

.PHONY: qpu
## qpu: build the qpu application
qpu:
	@echo "Building..."
	@go build -o ${BIN_DIR}/qpu cmd/qpu/main.go

.PHONY: query
## query: build the qpu application
query:
	@echo "Building..."
	@go build -o ${BIN_DIR}/query cmd/query/main.go

.PHONY: fmt
## fmt: runs gofmt on all source files
fmt: ; $(info $(M) running gofmt…)
	@go fmt $(PKGS)

.PHONY: test
## test: run tests
test: $(info $(M) running $(NAME:%=% )tests…)
	@go test $(TESTPKGS)

.PHONY: proto
## proto: Compiles the protobuf files
proto: $(PROTOC_CMD)
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go install google.golang.org/protobuf/cmd/protoc-gen-go
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	protoc --go_out=paths=source_relative:./internal/proto/qpu -I api/protobuf-spec qpu.proto
	protoc --go-grpc_out=paths=source_relative:./internal/proto/qpu -I api/protobuf-spec qpu.proto
	protoc --go_out=paths=source_relative:./internal/proto/qpuextapi -I api/protobuf-spec qpuextapi.proto
	protoc --go-grpc_out=paths=source_relative:./internal/proto/qpuextapi -I api/protobuf-spec qpuextapi.proto
	protoc --go_out=paths=source_relative:./internal/proto/qpuapi -I api/protobuf-spec qpuapi.proto
	protoc --go-grpc_out=paths=source_relative:./internal/proto/qpuapi -I api/protobuf-spec qpuapi.proto
	protoc --go_out=paths=source_relative:./internal/proto/mysql -I api/protobuf-spec mysql.proto
	protoc --go-grpc_out=paths=source_relative:./internal/proto/mysql -I api/protobuf-spec mysql.proto
	protoc --go_out=paths=source_relative:./internal/proto/s3 -I api/protobuf-spec s3.proto
	protoc --go-grpc_out=paths=source_relative:./internal/proto/s3 -I api/protobuf-spec s3.proto

#	protoc api/protobuf-spec/qpu.proto --go_out=plugins=grpc:${GOPATH}/src/
#	protoc api/protobuf-spec/qpuextapi.proto --go_out=plugins=grpc:$(GOPATH)/src/
#	protoc api/protobuf-spec/qpuapi.proto --go_out=plugins=grpc:${GOPATH}/src/
#	protoc api/protobuf-spec/mysql.proto --go_out=plugins=grpc:$(GOPATH)/src/
#	protoc api/protobuf-spec/s3.proto --go_out=plugins=grpc:$(GOPATH)/src/

.PHONY: image-build
## image-build:
image-build:
	# docker build -f build/proteus/localdev/Dockerfile-qpu -t qpu/dev .
	docker build -f build/proteus/localdev/Dockerfile-qpu-stateless -t qpu/stateless .

.PHONY: image-push
## image-push: Pushes image to docker hub
image-push:
	docker tag qpu/dev ${IMG_QPU}
	docker push ${IMG_QPU}

.PHONY: image-push-registry
## image-push-registry: Pushes image to registry service
image-push-registry:
#	docker tag qpu/dev ${IMG_REG}
#	docker push ${IMG_REG}
	docker tag qpu/stateless ${IMG_REG_SL}
	docker push ${IMG_REG_SL}

.PHONY: clean
## clean: cleans the binary
clean:
	@echo "Cleaning"
	@rm -f ${BIN_DIR}/*

.PHONY: help
## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'
