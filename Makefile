BINARY   := synaps3
MODULE   := github.com/strahe/synaps3
PKG      := ./cmd/synaps3
GOFLAGS  := -trimpath

VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE     := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS  := -X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
            -X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
            -X $(MODULE)/internal/buildinfo.Date=$(DATE)

.PHONY: all build test lint fmt clean run

all: build

build:
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o bin/$(BINARY) $(PKG)

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
	./bin/$(BINARY) serve --config config.example.yaml

.PHONY: migrate
migrate: build
	./bin/$(BINARY) migrate --config config.example.yaml
