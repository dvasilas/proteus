APP      := proteus
BIN_DIR  := ${CURDIR}/bin
PKGS     := $(or $(PKG),$(shell env GO111MODULE=on go list ./...))
TESTPKGS := $(shell env GO111MODULE=on go list -f \
			'{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' \
			$(PKGS))

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

.PHONY: bench
## bench: build the benchmark
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

.PHONY: proto
## proto: Compiles the protobuf files
proto: $(PROTOC_CMD)
	# go get ./vendor/github.com/golang/protobuf/protoc-gen-go
	protoc pkg/proteus-go-client/api/qpu_api.proto --go_out=plugins=grpc:${GOPATH}/src/

.PHONY: help
## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'