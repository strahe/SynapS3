BINARY   := synaps3
MODULE   := github.com/strahe/synaps3
PKG      := ./cmd/synaps3
GOFLAGS  := -trimpath

.PHONY: all build test lint fmt clean run

all: build

build:
	go build $(GOFLAGS) -o bin/$(BINARY) $(PKG)

test:
	go test -race -count=1 ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint run ./...

fmt:
	gofmt -s -w .
	goimports -w .

clean:
	rm -rf bin/

run: build
	./bin/$(BINARY) --config config.example.yaml

.PHONY: migrate
migrate:
	go run $(PKG) --config config.example.yaml
