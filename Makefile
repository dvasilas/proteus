APP      := proteus
BIN_DIR  := ${CURDIR}/bin
PKGS     := $(or $(PKG),$(shell env GO111MODULE=on go list ./...))
TESTPKGS := $(shell env GO111MODULE=on go list -f \
			'{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' \
			$(PKGS))

DOCKER_NET := proteus-local-dev-net1

REPO_DATASTORE := dvasilas/proteus-lobsters
REPO_PROTEUS := dvasilas/proteus
TAG := $(shell git log -1 --pretty=%H | cut -c1-8)
IMG_DATASTORE_PLAIN := ${REPO_DATASTORE}:plain_${TAG}
IMG_DATASTORE_MV := ${REPO_DATASTORE}:mv_${TAG}
IMG_DATASTORE_TRIGGER := ${REPO_DATASTORE}:trigger_${TAG}
IMG_QPU := ${REPO_PROTEUS}:${TAG}

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

.PHONY: bench
## query: build the qpu application
bench:
	@echo "Building..."
	@go build -o ${BIN_DIR}/benchmark cmd/benchmark/main.go


.PHONY: fmt
## fmt: runs gofmt on all source files
fmt: ; $(info $(M) running gofmt…)
	@go fmt $(PKGS)

.PHONY: test
## test: run tests
test: fmt ; $(info $(M) running $(NAME:%=% )tests…)
	@go test $(TESTPKGS)

.PHONY: docker-run-s3
## docker-run-s3: Runs a container with an s3 server (scality/cloudserver)
docker-run-s3: docker-prepare
	docker run -ti --rm --name cloudserver-0 --network=${DOCKER_NET} -p 127.0.0.1:8000:8000 -p 127.0.0.1:50000:50000 -e S3BACKEND=mem -e REMOTE_MANAGEMENT_DISABLE=1 dvasilas/cloudserver:latest

.PHONY: docker-run-qpu
## docker-run-qpu: Runs a container with the a QPU server
docker-run-qpu: docker-build-qpu-localdev
	docker run --rm -ti --name ${CONT_NAME} --network=${DOCKER_NET} -p ${PORT}:${PORT} ${APP}/${TAG} -c ${CONFIG} -d

.PHONY: docker-run-query
## docker-run-query: Runs a container with the query executable
docker-run-query: docker-build-query-localdev
	docker run --rm -ti --name ${CONT_NAME} --network=${DOCKER_NET} ${APP}/${TAG} -e ${ENDPOINT} -q ${QUERY}

.PHONY: docker-build-qpu-localdev
docker-build-qpu-localdev: docker-prepare
## docker-build-qpu-localdev: Builds a proteus qpu docker image based on the local source code
	@docker build -f build/localdev/Dockerfile-qpu -t ${APP}/localdev .

.PHONY: docker-build-query-localdev
docker-build-query-localdev: docker-prepare
## docker-build-query-localdev: Builds a proteus query docker image based on the local source code
	docker build -f build/localdev/Dockerfile-query -t ${APP}/localdev .

docker-prepare:
	@docker network inspect ${DOCKER_NET} >/dev/null 2>&1 || docker network create ${DOCKER_NET}

.PHONY: proto
## proto: Compiles the protobuf files
proto: $(PROTOC_CMD)
	# go get ./vendor/github.com/golang/protobuf/protoc-gen-go
	protoc api/protobuf-spec/qpu.proto --go_out=plugins=grpc:${GOPATH}/src/
	protoc api/protobuf-spec/qpu_api.proto --go_out=plugins=grpc:${GOPATH}/src/
	protoc api/protobuf-spec/mysql.proto --go_out=plugins=grpc:$(GOPATH)/src/


.PHONY: image-push
## image-push: Pushes iamges to docker hub
image-push:
	docker tag lobsters/plain ${IMG_DATASTORE_PLAIN}
	docker tag lobsters/mv ${IMG_DATASTORE_MV}
	docker tag lobsters/proteus ${IMG_DATASTORE_TRIGGER}
	docker tag qpu/dev ${IMG_QPU}
	#docker push ${IMG_DATASTORE_PLAIN}
	#docker push ${IMG_DATASTORE_MV}
	#docker push ${IMG_DATASTORE_TRIGGER}
	docker push ${IMG_QPU}

docker push dvasilas/proteus-lobsters:tagname


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