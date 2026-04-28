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

.PHONY: all build test lint fmt check clean run ui-install ui-build ui-dev

all: build

ui-install:
	cd ui && pnpm install --frozen-lockfile --config.confirmModulesPurge=false

ui-build: ui-install
	cd ui && pnpm run build

build: ui-build
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o bin/$(BINARY) $(PKG)

test:
	go test -race -count=1 ./cmd/... ./internal/...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint run
	cd ui && pnpm run check

fmt:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint fmt
	cd ui && pnpm run format

check:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint config verify
	golangci-lint fmt --diff
	golangci-lint run
	cd ui && pnpm run check
	cd ui && pnpm run test
	$(MAKE) test

clean:
	rm -rf bin/
	rm -rf ui/dist/
	rm -rf ui/node_modules/

run: build
	./bin/$(BINARY) serve --config config.example.yaml

ui-dev:
	cd ui && pnpm run dev

.PHONY: migrate
migrate: build
	./bin/$(BINARY) migrate --config config.example.yaml
