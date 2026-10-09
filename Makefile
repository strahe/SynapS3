BINARY   := synaps3
SYSTEMTEST_BINARY := synaps3-systemtest
INTEGRATION_BINARY := synaps3-integration-server
MODULE   := github.com/strahe/synaps3
PKG      := ./cmd/synaps3
GOFLAGS  := -trimpath
CGO_ENABLED := 1
CGO      := CGO_ENABLED=$(CGO_ENABLED)

DOCKER_DEPLOYMENT ?= sh docker/deployment.sh

# Packages whose production code shares memory across goroutines. Database
# concurrency is covered by the PostgreSQL tests, which the race detector
# cannot observe.
RACE_PACKAGES := ./internal/admin ./internal/app ./internal/backend ./internal/cache ./internal/cacheaccess \
                 ./internal/objectreader ./internal/observability ./internal/provider \
                 ./internal/storagecommit ./internal/synapse ./internal/task ./internal/task/transfer ./internal/worker

VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE     := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS  := -X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
            -X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
            -X $(MODULE)/internal/buildinfo.Date=$(DATE)

.PHONY: all build build-go build-systemtest-server build-integration-server docs-build test test-fast test-race test-race-ci test-postgres test-system test-system-support test-system-postgres test-s3-compatibility test-s3-clients test-integration test-ui-e2e test-docker-entrypoint test-docker-deployment lint fmt check verify-e2e verify-fast verify-ci verify-norace clean run ui-install ui-build ui-dev ui-e2e-install
.PHONY: docker-init docker-up docker-verify docker-down docker-status docker-logs docker-password

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

build-systemtest-server:
	@test -f ui/dist/index.html || { echo "ui/dist/index.html not found; run make ui-build first"; exit 1; }
	$(CGO) go build $(GOFLAGS) -tags=systemtest -o bin/$(SYSTEMTEST_BINARY) ./cmd/synaps3-systemtest

build-integration-server:
	$(CGO) go build $(GOFLAGS) -tags=dev -ldflags '$(LDFLAGS)' -o bin/$(INTEGRATION_BINARY) $(PKG)

test: test-fast

test-fast:
	$(CGO) go test -count=1 ./cmd/... ./internal/...

test-race:
	$(CGO) go test -race -tags dev -count=1 $(RACE_PACKAGES)

# Keep PR race checks focused on shared-memory contracts; run test-race for
# the complete package suites when investigating concurrency changes.
test-race-ci:
	$(CGO) go test -race -tags dev -count=1 ./internal/cache ./internal/cacheaccess ./internal/objectreader ./internal/task/transfer
	$(CGO) go test -race -tags dev -count=1 -run '^Test(AdminEventHub.*|CachedWalletQuerier_(CoalescesConcurrentMisses|WaiterContextCanCancel))$$' ./internal/admin
	$(CGO) go test -race -tags dev -count=1 -run '^Test(PutObject|CopyObject|CompleteMultipart)HoldsContentGateThroughVersionTransaction$$' ./internal/backend
	$(CGO) go test -race -tags dev -count=1 -run '^Test(CacheCapacity(CompletionPreservesDemandUntilArchiveCommits|MergesRefusalBeforeCompletingAfterUpload|TaskEvictsLRUItemsOnlyToCleanupTarget)|LRUDeletionWaitsForOpenReaderAndCancelsAfterNewAccess)$$' ./internal/task
	$(CGO) go test -race -tags dev -count=1 -run '^TestEngine(ShutdownDiscardsHandlerResultAndForcesRecovery|RenewalFailureCancelsBeforeSafetyBoundary|RecoveryQueueDoesNotDropLeaseShorteningWork|TypeConcurrencyPreservesQueueAndFairness|GlobalConcurrencyIncludesUnrestrictedTypes|ActiveClaimCannotBeReclaimedBeforeInvocationReturns|ConcurrencyHeldThroughSettlement|ShutdownWaitsForInvocations)$$' ./internal/worker

test-postgres:
	$(CGO) go test -tags=postgres -count=1 ./internal/testpg ./internal/db/migrations ./internal/db/repository ./internal/worker

test-system:
	$(CGO) go test $(GOFLAGS) -tags='dev systemtest' -count=1 ./tests/testutil/... ./internal/systemtest ./tests/system

test-system-support:
	$(CGO) go test $(GOFLAGS) -tags='dev systemtest' -count=1 ./tests/testutil/... ./internal/systemtest

test-system-postgres:
	$(CGO) go test $(GOFLAGS) -tags='dev systemtest postgres' -count=1 ./tests/system

test-s3-compatibility:
	$(CGO) go test $(GOFLAGS) -tags='dev systemtest s3compat' -count=1 -run '^(TestS3CompatibilityMatrix|TestS3Clients)$$' ./tests/system

test-s3-clients:
	$(CGO) go test $(GOFLAGS) -tags='dev systemtest s3compat' -count=1 -run '^TestS3Clients$$' ./tests/system

test-integration: build-integration-server
	$(CGO) go test -v $(GOFLAGS) -tags=integration -count=1 -timeout=45m ./tests/integration/...

ui-e2e-install:
	cd ui && pnpm exec playwright install $(PLAYWRIGHT_INSTALL_FLAGS) chromium

test-ui-e2e:
	@test -f ui/dist/index.html || { echo "ui/dist/index.html not found; run make ui-build first"; exit 1; }
	@test -x bin/$(SYSTEMTEST_BINARY) || { echo "bin/$(SYSTEMTEST_BINARY) not found; run make build-systemtest-server first"; exit 1; }
	cd ui && pnpm exec playwright test

test-docker-entrypoint:
	sh docker/entrypoint.test.sh

test-docker-deployment:
	sh docker/deployment.test.sh

docker-init:
	@$(DOCKER_DEPLOYMENT) init

docker-up:
	@$(DOCKER_DEPLOYMENT) up

docker-verify:
	@$(DOCKER_DEPLOYMENT) verify

docker-down:
	@$(DOCKER_DEPLOYMENT) down

docker-status:
	@$(DOCKER_DEPLOYMENT) status

docker-logs:
	@$(DOCKER_DEPLOYMENT) logs

docker-password:
	@$(DOCKER_DEPLOYMENT) password

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
	$(MAKE) test

verify-norace: ui-build
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found"; exit 1; }
	golangci-lint config verify
	golangci-lint fmt --diff
	$(CGO) golangci-lint run
	cd ui && pnpm run check
	cd ui && pnpm run test
	$(MAKE) test-docker-entrypoint
	$(MAKE) build-go

verify-fast: verify-norace
	$(MAKE) test-fast

verify-ci: verify-norace
	$(MAKE) test
	$(MAKE) test-system-support

verify-e2e: ui-build build-systemtest-server test-system test-ui-e2e

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
