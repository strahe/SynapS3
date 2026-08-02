#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/synaps3-deployment-test.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT HUP INT TERM
unset ADMIN_DOMAIN COMPOSE_FILE IMAGE_SOURCE SYNAPS3_CONFIG

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  file=$1
  expected=$2
  if ! grep -Fq "$expected" "$file"; then
    echo "Expected text not found: $expected" >&2
    echo "Actual file:" >&2
    cat "$file" >&2
    exit 1
  fi
}

assert_not_contains() {
  file=$1
  unexpected=$2
  if grep -Fq -- "$unexpected" "$file"; then
    echo "Unexpected text found: $unexpected" >&2
    echo "Actual file:" >&2
    cat "$file" >&2
    exit 1
  fi
}

file_mode() {
  stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"
}

new_case_dir() {
  mktemp -d "$TEST_ROOT/case.XXXXXX"
}

copy_deployment_files() {
  target=$1
  mkdir -p "$target/docker"
  cp \
    "$ROOT_DIR/Makefile" \
    "$ROOT_DIR/.env.example" \
    "$ROOT_DIR/compose.yaml" \
    "$ROOT_DIR/compose.local.yaml" \
    "$ROOT_DIR/compose.admin-https.yaml" \
    "$target/"
  cp "$ROOT_DIR/docker/Caddyfile" "$target/docker/"
}

install_fake_tools() {
  target=$1
  bin_dir="$target/test-bin"
  mkdir -p "$bin_dir"

  cat >"$bin_dir/docker-compose" <<'EOF'
#!/usr/bin/env sh
set -eu

if [ "${1:-}" = version ] && [ "${2:-}" = --short ]; then
  echo "2.24.0"
  exit 0
fi

printf '%s\n' "$*" >>"$SYNAPS3_TEST_COMPOSE_LOG"
printf 'env ADMIN_DOMAIN=%s\n' "${ADMIN_DOMAIN-unset}" >>"$SYNAPS3_TEST_COMPOSE_LOG"
exit 0
EOF

  cat >"$bin_dir/curl" <<'EOF'
#!/usr/bin/env sh
set -eu

output_file=
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      shift
      output_file=$1
      ;;
    --write-out|--connect-timeout|--max-time)
      shift
      ;;
    --silent|--show-error)
      ;;
    http://*|https://*)
      url=$1
      ;;
  esac
  shift
done

status=${SYNAPS3_TEST_HEALTH_STATUS:-ok}
domain=${SYNAPS3_TEST_DOMAIN:-admin.example.test}

case "$url" in
  http://127.0.0.1:9090/healthz)
    printf '{"status":"%s"}' "$status" >"$output_file"
    if [ "$status" = unhealthy ]; then printf '503'; else printf '200'; fi
    ;;
  "https://$domain/healthz")
    if [ "${SYNAPS3_TEST_HTTPS_READY:-1}" != 1 ]; then
      : >"$output_file"
      printf '000'
      exit 7
    fi
    printf '{"status":"%s"}' "$status" >"$output_file"
    if [ "$status" = unhealthy ]; then printf '503'; else printf '200'; fi
    ;;
  "http://$domain/")
    printf '308 https://%s/' "$domain"
    ;;
  *)
    echo "unexpected curl URL: $url" >&2
    exit 22
    ;;
esac
EOF

  chmod +x "$bin_dir/docker-compose" "$bin_dir/curl"
  printf '%s\n' "$bin_dir"
}

test_init_contract() {
  case_dir=$(new_case_dir)
  copy_deployment_files "$case_dir"
  out_file="$case_dir/output.log"
  err_file="$case_dir/error.log"

  if make --no-print-directory -C "$case_dir" docker-init >"$out_file" 2>"$err_file"; then
    fail "docker-init succeeded without ADMIN_DOMAIN"
  fi
  assert_contains "$err_file" "ADMIN_DOMAIN is required"

  if make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN='https://admin.example.test' >"$out_file" 2>"$err_file"; then
    fail "docker-init accepted a URL instead of a hostname"
  fi
  assert_contains "$err_file" "ADMIN_DOMAIN must be a public hostname"

  make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN=admin.example.test >"$out_file"
  [ "$(file_mode "$case_dir/.env")" = 600 ] || fail ".env was not created with mode 600"
  assert_contains "$case_dir/.env" "COMPOSE_FILE=compose.yaml:compose.admin-https.yaml"
  assert_contains "$case_dir/.env" "ADMIN_DOMAIN=admin.example.test"
  assert_contains "$case_dir/.env" "IMAGE_SOURCE=published"

  printf '%s\n' 'SYNAPS3_FILECOIN_PRIVATE_KEY=preserve-this-value' >>"$case_dir/.env"
  if make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN=other.example.test >"$out_file" 2>"$err_file"; then
    fail "docker-init overwrote an existing .env"
  fi
  assert_contains "$err_file" "refusing to overwrite"
  assert_contains "$case_dir/.env" "SYNAPS3_FILECOIN_PRIVATE_KEY=preserve-this-value"

  local_dir=$(new_case_dir)
  copy_deployment_files "$local_dir"
  make --no-print-directory -C "$local_dir" docker-init ADMIN_DOMAIN=admin.example.test IMAGE_SOURCE=local >"$out_file"
  assert_contains "$local_dir/.env" "COMPOSE_FILE=compose.yaml:compose.local.yaml:compose.admin-https.yaml"
  assert_contains "$local_dir/.env" "IMAGE_SOURCE=local"
}

test_make_lifecycle_contract() {
  case_dir=$(new_case_dir)
  copy_deployment_files "$case_dir"
  make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN=admin.example.test >/dev/null
  printf '%s\n' 'SYNAPS3_FILECOIN_PRIVATE_KEY=must-not-appear-in-output' >>"$case_dir/.env"

  bin_dir=$(install_fake_tools "$case_dir")
  compose_log="$case_dir/compose.log"
  output_log="$case_dir/output.log"
  error_log="$case_dir/error.log"
  : >"$compose_log"

  ADMIN_DOMAIN=override.example.test SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-up \
      DOCKER_COMPOSE="$bin_dir/docker-compose" DOCKER_WAIT_TIMEOUT=7 >"$output_log" 2>"$error_log"
  assert_contains "$compose_log" "config --quiet"
  assert_contains "$compose_log" "up -d --remove-orphans --wait --wait-timeout 7"
  assert_contains "$compose_log" "env ADMIN_DOMAIN=unset"
  assert_not_contains "$compose_log" "pull"
  assert_not_contains "$output_log" "must-not-appear-in-output"
  assert_not_contains "$error_log" "must-not-appear-in-output"

  : >"$compose_log"
  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-down DOCKER_COMPOSE="$bin_dir/docker-compose" >"$output_log"
  assert_contains "$compose_log" "down --remove-orphans"
  assert_not_contains "$compose_log" "--volumes"
  assert_not_contains "$compose_log" " -v"

  : >"$compose_log"
  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-logs \
      DOCKER_COMPOSE="$bin_dir/docker-compose" DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1 >"$output_log"
  assert_contains "$compose_log" "logs --tail=100 -f caddy"

  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HEALTH_STATUS=setup SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-verify \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log"
  assert_contains "$output_log" "still requires setup"

  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HEALTH_STATUS=unhealthy SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-verify \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log" 2>"$error_log"; then
    fail "docker-verify accepted an unhealthy deployment"
  fi
  assert_contains "$error_log" "SynapS3 is unhealthy"

  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HTTPS_READY=0 SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-verify \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_ATTEMPTS=1 DOCKER_VERIFY_DELAY=0 >"$output_log" 2>"$error_log"; then
    fail "docker-verify accepted unavailable HTTPS"
  fi
  assert_contains "$error_log" "make docker-logs DOCKER_SERVICE=caddy"
}

test_upgrade_contract() {
  seed_dir=$(new_case_dir)
  case_dir=$(new_case_dir)
  remote_dir=$(new_case_dir)
  copy_deployment_files "$seed_dir"

  git init --bare -q "$remote_dir/repository.git"
  git -C "$seed_dir" init -q
  git -C "$seed_dir" config user.email test@example.invalid
  git -C "$seed_dir" config user.name "Deployment Test"
  git -C "$seed_dir" add Makefile .env.example compose.yaml compose.local.yaml compose.admin-https.yaml docker/Caddyfile
  git -C "$seed_dir" commit -qm "test: seed deployment"
  git -C "$seed_dir" branch -M main
  git -C "$seed_dir" remote add origin "$remote_dir/repository.git"
  git -C "$seed_dir" push -qu -u origin main

  git clone -q --depth 1 --branch main "file://$remote_dir/repository.git" "$case_dir"
  make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN=admin.example.test >/dev/null
  bin_dir=$(install_fake_tools "$case_dir")
  compose_log="$case_dir/compose.log"
  output_log="$case_dir/output.log"
  error_log="$case_dir/error.log"
  : >"$compose_log"

  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-upgrade \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" >"$output_log" 2>"$error_log"; then
    fail "docker-upgrade succeeded without backup confirmation"
  fi
  assert_contains "$error_log" "BACKUP_CONFIRMED=1"

  printf '%s\n' '# dirty' >>"$case_dir/.env.example"
  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-upgrade BACKUP_CONFIRMED=1 \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" >"$output_log" 2>"$error_log"; then
    fail "docker-upgrade accepted tracked local changes"
  fi
  assert_contains "$error_log" "Tracked files have local changes"
  git -C "$case_dir" checkout -- .env.example

  publisher_dir=$(new_case_dir)
  git clone -q --branch main "file://$remote_dir/repository.git" "$publisher_dir"
  git -C "$publisher_dir" config user.email test@example.invalid
  git -C "$publisher_dir" config user.name "Deployment Test"
  awk '
    { print }
    $0 == "_docker-upgrade-apply:" { print "\t@echo \"Loaded updated Makefile after pull.\"" }
  ' "$publisher_dir/Makefile" >"$publisher_dir/Makefile.next"
  mv "$publisher_dir/Makefile.next" "$publisher_dir/Makefile"
  git -C "$publisher_dir" add Makefile
  git -C "$publisher_dir" commit -qm "test: update deployment recipe"
  git -C "$publisher_dir" push -q

  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HEALTH_STATUS=ok SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-upgrade BACKUP_CONFIRMED=1 \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log" 2>"$error_log"
  assert_contains "$output_log" "latest edge build"
  assert_contains "$output_log" "Loaded updated Makefile after pull"
  assert_contains "$compose_log" "pull"
  assert_contains "$compose_log" "up -d --remove-orphans --wait"
  assert_contains "$output_log" "SynapS3 Admin HTTPS is ready"
}

test_compose_and_caddy_config() {
  case_dir=$(new_case_dir)
  copy_deployment_files "$case_dir"
  make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN=admin.example.test >/dev/null

  make --no-print-directory -C "$case_dir" docker-check >/dev/null

  docker compose --project-directory "$case_dir" config >"$case_dir/rendered.yaml"
  assert_contains "$case_dir/rendered.yaml" "image: caddy:2.11.4-alpine"
  assert_contains "$case_dir/rendered.yaml" "SYNAPS3_ADMIN_AUTH_ENABLED: \"true\""
  assert_contains "$case_dir/rendered.yaml" "SYNAPS3_ADMIN_TRUSTED_PROXIES: 127.0.0.1/32"
  assert_contains "$case_dir/rendered.yaml" "name: synaps3-caddy-data"
  assert_contains "$case_dir/rendered.yaml" "name: synaps3-caddy-config"

  sed 's/^ADMIN_DOMAIN=.*/ADMIN_DOMAIN=/' "$case_dir/.env" >"$case_dir/.env.invalid"
  chmod 600 "$case_dir/.env.invalid"
  if docker compose --project-directory "$case_dir" --env-file "$case_dir/.env.invalid" config --quiet 2>"$case_dir/error.log"; then
    fail "Compose accepted an empty ADMIN_DOMAIN"
  fi
  assert_contains "$case_dir/error.log" "Set ADMIN_DOMAIN in .env"

  if docker info >/dev/null 2>&1; then
    docker run --rm \
      --env ADMIN_DOMAIN=admin.example.test \
      --volume "$ROOT_DIR/docker/Caddyfile:/etc/caddy/Caddyfile:ro" \
      caddy:2.11.4-alpine \
      caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
  elif [ "${SYNAPS3_REQUIRE_DOCKER_DAEMON:-0}" = 1 ]; then
    fail "Docker daemon is required for Caddyfile validation"
  else
    echo "SKIP: Docker daemon unavailable; Caddyfile container validation did not run." >&2
  fi
}

test_init_contract
test_make_lifecycle_contract
test_upgrade_contract
test_compose_and_caddy_config

echo "deployment tests passed"
