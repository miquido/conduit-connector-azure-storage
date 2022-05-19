.PHONY: build test lint

build:
	go build -o conduit-connector-azure-storage cmd/blob/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run
