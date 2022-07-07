.PHONY: build test lint

build:
	go build -o conduit-connector-azure-storage cmd/blob/main.go

test:
	docker compose -f test/docker-compose.yml -p tests up --quiet-pull -d --wait
	go test --tags=unit,integration $(GOTEST_FLAGS) -race ./...; ret=$$?; \
	  	docker compose -f test/docker-compose.yml -p tests down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

lint:
	golangci-lint run
