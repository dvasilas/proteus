APP      := proteus
TARGET   := qpu
BIN_DIR  := ${CURDIR}/bin
PKGS     := $(or $(PKG),$(shell env GO111MODULE=on go list ./...))
TESTPKGS := $(shell env GO111MODULE=on go list -f \
			'{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' \
			$(PKGS))

DOCKER_NET := proteus-local-dev-net1

export GO111MODULE=on

.PHONY: build
## build: build the application
build: clean
	@echo "Building..."
	@go build -o ${BIN_DIR}/${TARGET} cmd/${TARGET}/main.go

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
docker-run-qpu: docker-build-localdev
	docker run --rm -ti --name ${CONT_NAME} --network=${DOCKER_NET} ${APP}/${TAG} -c ${CONFIG} -d

.PHONY: docker-prepare docker-build-localdev
docker-build-localdev:
## docker-build-localdev: Builds a proteus docker image based on the local source code
	@docker build -f build/localdev/Dockerfile -t ${APP}/localdev .

docker-prepare:
	@docker network inspect ${DOCKER_NET} >/dev/null 2>&1 || docker network create ${DOCKER_NET}

.PHONY: clean
## clean: cleans the binary
clean:
	@echo "Cleaning"
	@rm -f ${BIN_DIR}/${TARGET}

.PHONY: help
## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'