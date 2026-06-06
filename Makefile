BINARY   := synaps3
MODULE   := github.com/strahe/synaps3
PKG      := ./cmd/synaps3
GOFLAGS  := -trimpath
CGO_ENABLED := 1
CGO      := CGO_ENABLED=$(CGO_ENABLED)

VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE     := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS  := -X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
            -X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
            -X $(MODULE)/internal/buildinfo.Date=$(DATE)

.PHONY: all build build-go docs-build test test-fast test-race test-docker-entrypoint lint fmt check verify-fast verify-race clean run ui-install ui-build ui-dev

all: build

ui-install:
	cd ui && pnpm install --frozen-lockfile --config.confirmModulesPurge=false

ui-build: ui-install
	cd ui && pnpm run build

docs-build:
	cd docs && pnpm install --frozen-lockfile
	cd docs && pnpm run build

build: ui-build build-go

build-go:
	@test -f ui/dist/index.html || { echo "ui/dist/index.html not found; run make ui-build first"; exit 1; }
	$(CGO) go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o bin/$(BINARY) $(PKG)

test: test-race

test-fast:
	$(CGO) go test -count=1 ./cmd/... ./internal/...

test-race:
	$(CGO) go test -race -count=1 ./cmd/... ./internal/...

test-docker-entrypoint:
	sh docker/entrypoint.test.sh

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	$(CGO) golangci-lint run
	cd ui && pnpm run check

fmt:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint fmt
	cd ui && pnpm run format

check: ui-build
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint config verify
	golangci-lint fmt --diff
	$(CGO) golangci-lint run
	cd ui && pnpm run check
	cd ui && pnpm run test
	$(MAKE) test-race

verify-fast: ui-build
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint config verify
	golangci-lint fmt --diff
	$(CGO) golangci-lint run
	cd ui && pnpm run check
	cd ui && pnpm run test
	$(MAKE) test-docker-entrypoint
	$(MAKE) test-fast
	$(MAKE) build-go

verify-race:
	$(CGO) go test -race -tags dev -count=1 ./cmd/... ./internal/...

clean:
	rm -rf bin/
	rm -rf ui/dist/
	rm -rf ui/node_modules/

run: build
	./bin/$(BINARY) serve

ui-dev:
	cd ui && pnpm run dev

.PHONY: migrate
migrate: build
	./bin/$(BINARY) migrate
