BINARY   := synaps3
SYSTEMTEST_BINARY := synaps3-systemtest
INTEGRATION_BINARY := synaps3-integration-server
MODULE   := github.com/strahe/synaps3
PKG      := ./cmd/synaps3
GOFLAGS  := -trimpath
CGO_ENABLED := 1
CGO      := CGO_ENABLED=$(CGO_ENABLED)

DOCKER_COMPOSE ?= docker compose
CURL ?= curl
DOCKER_ENV_FILE ?= .env
DOCKER_WAIT_TIMEOUT ?= 120
DOCKER_VERIFY_ATTEMPTS ?= 10
DOCKER_VERIFY_DELAY ?= 3
DOCKER_LOG_TAIL ?= 100
DOCKER_LOG_FOLLOW ?= 0
DOCKER_SERVICE ?=
ADMIN_DOMAIN ?=
IMAGE_SOURCE ?= published
WALLET_ADDRESS ?=
ADMIN_ARGS ?=
BACKUP_CONFIRMED ?= 0

export DOCKER_WAIT_TIMEOUT DOCKER_VERIFY_ATTEMPTS DOCKER_VERIFY_DELAY
export DOCKER_LOG_TAIL DOCKER_LOG_FOLLOW DOCKER_SERVICE
unexport ADMIN_DOMAIN IMAGE_SOURCE WALLET_ADDRESS ADMIN_ARGS BACKUP_CONFIRMED

VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE     := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS  := -X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
            -X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
            -X $(MODULE)/internal/buildinfo.Date=$(DATE)

.PHONY: all build build-go build-systemtest-server build-integration-server docs-build test test-fast test-race test-system test-integration test-ui-e2e test-docker-entrypoint test-docker-deployment lint fmt check verify-e2e verify-fast verify-norace verify-race clean run ui-install ui-build ui-dev ui-e2e-install
.PHONY: docker-help docker-init docker-check docker-build docker-wallet docker-fund docker-up docker-verify docker-stop docker-down docker-status docker-logs docker-password docker-admin docker-upgrade _docker-upgrade-apply

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

test: test-race

test-fast:
	$(CGO) go test -count=1 ./cmd/... ./internal/...

test-race:
	$(CGO) go test -race -count=1 ./cmd/... ./internal/...

test-system:
	$(CGO) go test $(GOFLAGS) -tags='dev systemtest' -count=1 ./tests/testutil/... ./internal/systemtest ./tests/system

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

docker-help:
	@printf '%s\n' \
		'Docker deployment commands:' \
		'' \
		'  Setup' \
		'    make docker-init ADMIN_DOMAIN=admin.example.com' \
		'        Create a protected .env for the published image and Admin HTTPS.' \
		'    make docker-init ADMIN_DOMAIN=admin.example.com IMAGE_SOURCE=local' \
		'        Create .env for a locally built image.' \
		'    make docker-wallet' \
		'        Generate a Calibration wallet. Save the private key securely.' \
		'    make docker-fund WALLET_ADDRESS=0x...' \
		'        Request Calibration assets for a wallet.' \
		'' \
		'  Lifecycle' \
		'    make docker-up       Start or reconcile the deployment.' \
		'    make docker-stop     Stop services and preserve containers and data.' \
		'    make docker-down     Remove containers and preserve data volumes.' \
		'' \
		'  Diagnostics' \
		'    make docker-check    Validate prerequisites and deployment config.' \
		'    make docker-verify   Check local health, public HTTPS, and redirect.' \
		'    make docker-status   Show container and health status.' \
		'    make docker-logs [DOCKER_SERVICE=synaps3|caddy] [DOCKER_LOG_FOLLOW=1]' \
		'    make docker-password' \
		'        Print the initial Admin password in the current terminal.' \
		"    make docker-admin ADMIN_ARGS='task stats'" \
		'        Run a SynapS3 Admin command inside the container.' \
		'' \
		'  Maintenance' \
		'    make docker-build' \
		'        Build the local image selected by IMAGE_SOURCE=local.' \
		'    make docker-upgrade BACKUP_CONFIRMED=1' \
		'        Update the checkout and deployment to the latest edge build.'

docker-init: export ADMIN_DOMAIN := $(ADMIN_DOMAIN)
docker-init: export IMAGE_SOURCE := $(IMAGE_SOURCE)
docker-init:
	@if [ -e "$(DOCKER_ENV_FILE)" ]; then echo "$(DOCKER_ENV_FILE) already exists; refusing to overwrite it." >&2; exit 1; fi
	@set -eu; \
		domain="$${ADMIN_DOMAIN:-}"; \
		if [ -z "$$domain" ]; then \
			echo "ADMIN_DOMAIN is required. Run: make docker-init ADMIN_DOMAIN=admin.example.com" >&2; \
			exit 1; \
		fi; \
		if [ "$${#domain}" -gt 253 ] || ! printf '%s\n' "$$domain" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$$' || ! printf '%s\n' "$${domain##*.}" | grep -Eq '[A-Za-z]'; then \
			echo "ADMIN_DOMAIN must be a public hostname such as admin.example.com, without a scheme, port, path, or wildcard." >&2; \
			exit 1; \
		fi; \
		case "$${IMAGE_SOURCE:-published}" in \
			published) compose_files='compose.yaml:compose.admin-https.yaml' ;; \
			local) compose_files='compose.yaml:compose.local.yaml:compose.admin-https.yaml' ;; \
			*) echo "IMAGE_SOURCE must be published or local." >&2; exit 1 ;; \
		esac; \
		umask 077; \
		set -C; \
		{ \
			printf '# Docker deployment selection. Managed by make docker-init.\n'; \
			printf 'COMPOSE_FILE=%s\n' "$$compose_files"; \
			printf 'ADMIN_DOMAIN=%s\n' "$$domain"; \
			printf 'IMAGE_SOURCE=%s\n\n' "$${IMAGE_SOURCE:-published}"; \
			cat .env.example; \
		} >"$(DOCKER_ENV_FILE)"; \
		chmod 600 "$(DOCKER_ENV_FILE)"; \
		echo "Created $(DOCKER_ENV_FILE) for https://$$domain. Review it before starting SynapS3."

docker-check:
	@command -v $(word 1,$(DOCKER_COMPOSE)) >/dev/null 2>&1 || { echo "Docker CLI not found. Install Docker Engine and Docker Compose v2.24 or later." >&2; exit 1; }
	@set -eu; \
		version=$$($(DOCKER_COMPOSE) version --short 2>/dev/null || true); \
		version=$${version#v}; \
		major=$${version%%.*}; \
		rest=$${version#*.}; \
		minor=$${rest%%.*}; \
		case "$$major:$$minor" in *[!0-9:]*) echo "Could not determine the Docker Compose version." >&2; exit 1 ;; esac; \
		if [ -z "$$major" ] || [ -z "$$minor" ] || [ "$$major" -lt 2 ] || { [ "$$major" -eq 2 ] && [ "$$minor" -lt 24 ]; }; then \
			echo "Docker Compose v2.24 or later is required; found $${version:-unknown}." >&2; \
			exit 1; \
		fi
	@if [ ! -f "$(DOCKER_ENV_FILE)" ]; then echo "$(DOCKER_ENV_FILE) not found. Run: make docker-init ADMIN_DOMAIN=admin.example.com" >&2; exit 1; fi
	@set -eu; \
		mode=$$(stat -c '%a' "$(DOCKER_ENV_FILE)" 2>/dev/null || stat -f '%Lp' "$(DOCKER_ENV_FILE)"); \
		if [ "$$mode" != 600 ]; then \
			echo "$(DOCKER_ENV_FILE) permissions are $$mode; run: chmod 600 $(DOCKER_ENV_FILE)" >&2; \
			exit 1; \
		fi
	@set -eu; \
		domain=$$(sed -n 's/^ADMIN_DOMAIN=//p' "$(DOCKER_ENV_FILE)" | tr -d '\r'); \
		if [ -z "$$domain" ] || [ "$${#domain}" -gt 253 ] || ! printf '%s\n' "$$domain" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$$' || ! printf '%s\n' "$${domain##*.}" | grep -Eq '[A-Za-z]'; then \
			echo "ADMIN_DOMAIN in $(DOCKER_ENV_FILE) must be a public hostname such as admin.example.com." >&2; \
			exit 1; \
		fi
	@$(DOCKER_COMPOSE) config --quiet
	@echo "Docker deployment configuration is valid."

docker-build: docker-check
	@grep -Fq 'compose.local.yaml' "$(DOCKER_ENV_FILE)" || { echo "docker-build requires IMAGE_SOURCE=local. Recreate $(DOCKER_ENV_FILE) with docker-init if needed." >&2; exit 1; }
	$(DOCKER_COMPOSE) build synaps3

docker-wallet:
	@echo "The next command prints a private key. Run it only in a private terminal and save the key in a protected location." >&2
	$(DOCKER_COMPOSE) run --rm synaps3 synaps3 wallet generate

docker-fund: export WALLET_ADDRESS := $(WALLET_ADDRESS)
docker-fund:
	@printf '%s\n' "$${WALLET_ADDRESS:-}" | grep -Eq '^0x[0-9A-Fa-f]{40}$$' || { echo "WALLET_ADDRESS must be a 0x-prefixed 20-byte address." >&2; exit 1; }
	$(DOCKER_COMPOSE) run --rm synaps3 synaps3 wallet fund-testnet "$${WALLET_ADDRESS}"

docker-up: docker-check
	@printf '%s\n' "$${DOCKER_WAIT_TIMEOUT}" | grep -Eq '^[1-9][0-9]*$$' || { echo "DOCKER_WAIT_TIMEOUT must be a positive number of seconds." >&2; exit 1; }
	$(DOCKER_COMPOSE) up -d --remove-orphans --wait --wait-timeout "$${DOCKER_WAIT_TIMEOUT}"
	@$(DOCKER_COMPOSE) ps
	@echo "Containers are running. Certificate provisioning is checked separately with: make docker-verify"

docker-verify: docker-check
	@command -v $(word 1,$(CURL)) >/dev/null 2>&1 || { echo "curl is required for Docker deployment verification." >&2; exit 1; }
	@printf '%s\n' "$${DOCKER_VERIFY_ATTEMPTS}" | grep -Eq '^[1-9][0-9]*$$' || { echo "DOCKER_VERIFY_ATTEMPTS must be a positive integer." >&2; exit 1; }
	@printf '%s\n' "$${DOCKER_VERIFY_DELAY}" | grep -Eq '^[0-9]+$$' || { echo "DOCKER_VERIFY_DELAY must be a non-negative number of seconds." >&2; exit 1; }
	@set -eu; \
		domain=$$(sed -n 's/^ADMIN_DOMAIN=//p' "$(DOCKER_ENV_FILE)" | tr -d '\r'); \
		body_file=$$(mktemp "$${TMPDIR:-/tmp}/synaps3-health.XXXXXX"); \
		trap 'rm -f "$$body_file"' EXIT HUP INT TERM; \
		local_code=$$($(CURL) --silent --show-error --output "$$body_file" --write-out '%{http_code}' --max-time 10 http://127.0.0.1:9090/healthz || true); \
		local_body=$$(cat "$$body_file"); \
		if [ "$$local_code" != 200 ] && [ "$$local_code" != 503 ]; then \
			echo "Local Admin health check failed (HTTP $${local_code:-000}). Run: make docker-logs DOCKER_SERVICE=synaps3" >&2; \
			exit 1; \
		fi; \
		echo "Local Admin health: $$local_body"; \
		attempt=1; \
		https_code=000; \
		while [ "$$attempt" -le "$${DOCKER_VERIFY_ATTEMPTS}" ]; do \
			: >"$$body_file"; \
			https_code=$$($(CURL) --silent --output "$$body_file" --write-out '%{http_code}' --connect-timeout 5 --max-time 15 "https://$$domain/healthz" || true); \
			if [ "$$https_code" = 200 ] || [ "$$https_code" = 503 ]; then break; fi; \
			if [ "$$attempt" -lt "$${DOCKER_VERIFY_ATTEMPTS}" ]; then sleep "$${DOCKER_VERIFY_DELAY}"; fi; \
			attempt=$$((attempt + 1)); \
		done; \
		if [ "$$https_code" != 200 ] && [ "$$https_code" != 503 ]; then \
			echo "Public Admin HTTPS is not ready (HTTP $$https_code). Check DNS, ports 80/443, and: make docker-logs DOCKER_SERVICE=caddy" >&2; \
			exit 1; \
		fi; \
		https_body=$$(cat "$$body_file"); \
		redirect=$$($(CURL) --silent --output /dev/null --write-out '%{http_code} %{redirect_url}' --connect-timeout 5 --max-time 10 "http://$$domain/" || true); \
		set -- $$redirect; \
		case "$${1:-}" in 301|302|307|308) ;; *) echo "HTTP does not redirect to HTTPS for $$domain." >&2; exit 1 ;; esac; \
		case "$${2:-}" in https://$$domain/*) ;; *) echo "HTTP redirect target is not https://$$domain/." >&2; exit 1 ;; esac; \
		echo "Public Admin HTTPS: $$https_body"; \
		case "$$https_body" in \
			*'"status":"ok"'*) echo "SynapS3 Admin HTTPS is ready at https://$$domain/." ;; \
			*'"status":"setup"'*) echo "Admin HTTPS is ready at https://$$domain/, but SynapS3 still requires setup." ;; \
			*'"status":"unhealthy"'*) echo "Admin HTTPS is ready, but SynapS3 is unhealthy. Run: make docker-logs DOCKER_SERVICE=synaps3" >&2; exit 1 ;; \
			*) echo "Admin HTTPS returned an unexpected health response." >&2; exit 1 ;; \
		esac

docker-stop:
	$(DOCKER_COMPOSE) stop
	@echo "Services stopped. Containers, $(DOCKER_ENV_FILE), runtime data, and certificates were preserved."

docker-down:
	$(DOCKER_COMPOSE) down --remove-orphans
	@echo "Containers removed. $(DOCKER_ENV_FILE), runtime data, and certificate volumes were preserved."

docker-status:
	$(DOCKER_COMPOSE) ps

docker-logs:
	@printf '%s\n' "$${DOCKER_LOG_TAIL}" | grep -Eq '^[1-9][0-9]*$$' || { echo "DOCKER_LOG_TAIL must be a positive integer." >&2; exit 1; }
	@case "$${DOCKER_LOG_FOLLOW}" in 0|1) ;; *) echo "DOCKER_LOG_FOLLOW must be 0 or 1." >&2; exit 1 ;; esac
	@case "$${DOCKER_SERVICE}" in ''|synaps3|caddy) ;; *) echo "DOCKER_SERVICE must be synaps3, caddy, or empty." >&2; exit 1 ;; esac
	$(DOCKER_COMPOSE) logs --tail="$${DOCKER_LOG_TAIL}" $(if $(filter 1,$(DOCKER_LOG_FOLLOW)),-f) $${DOCKER_SERVICE}

docker-password:
	@$(DOCKER_COMPOSE) exec -T synaps3 cat /var/lib/synaps3/admin-initial-password

docker-admin: export ADMIN_ARGS := $(ADMIN_ARGS)
docker-admin:
	@if [ -z "$${ADMIN_ARGS:-}" ]; then echo "ADMIN_ARGS is required. Example: make docker-admin ADMIN_ARGS='task stats'" >&2; exit 1; fi
	@set -f; $(DOCKER_COMPOSE) exec -T synaps3 synaps3 admin $${ADMIN_ARGS}

docker-upgrade: export BACKUP_CONFIRMED := $(BACKUP_CONFIRMED)
docker-upgrade:
	@if [ "$${BACKUP_CONFIRMED}" != 1 ]; then echo "Create and verify a consistent backup first, then rerun with BACKUP_CONFIRMED=1." >&2; exit 1; fi
	@git diff --quiet && git diff --cached --quiet || { echo "Tracked files have local changes; commit or revert them before upgrading." >&2; exit 1; }
	@echo "Updating the checkout and deployment to the latest edge build. Automatic rollback is not available."
	git pull --ff-only
	@$(MAKE) --no-print-directory _docker-upgrade-apply

_docker-upgrade-apply:
	@$(MAKE) --no-print-directory docker-check
	@set -eu; \
		if grep -Fq 'compose.local.yaml' "$(DOCKER_ENV_FILE)"; then \
			$(DOCKER_COMPOSE) pull caddy; \
			$(DOCKER_COMPOSE) build --pull synaps3; \
		else \
			$(DOCKER_COMPOSE) pull; \
		fi
	@$(MAKE) --no-print-directory docker-up
	@$(MAKE) --no-print-directory docker-verify

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

verify-e2e: ui-build build-systemtest-server test-system test-ui-e2e

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
