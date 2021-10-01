BINARY=main

# params for local test
TEST_DIR=test/sample/samples
TEST_FN_DIR=${TEST_DIR}/functions

build: fmt vet
	@echo "  >  Building binary..."
	@go build -o bin/${BINARY} main.go

fmt:
	@go fmt ./...

vet:
	@go vet ./...

test:
	@go test -p 1 -count=1 ./...

docker:
	docker build -t tass-io/cli:latest .

clean:
	@echo "  >  Cleaning build cache"
	@rm bin/${BINARY}

submodule:
	@git submodule foreach git pull origin master

local:
	@go run main.go -l -w ${TEST_DIR}/pipeline/direct.yaml -f ${TEST_FN_DIR}/function1.yaml,${TEST_FN_DIR}/function2.yaml,${TEST_FN_DIR}/function3.yaml

lint:
	golangci-lint run --deadline=10m ./...

.PHONY: build fmt vet test docker clean help lint


