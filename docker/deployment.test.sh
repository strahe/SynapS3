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
  cp "$ROOT_DIR/docker/Caddyfile" "$ROOT_DIR/docker/deployment.sh" "$target/docker/"
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
printf 'env COMPOSE_FILE=%s\n' "${COMPOSE_FILE-unset}" >>"$SYNAPS3_TEST_COMPOSE_LOG"
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

  make --no-print-directory -C "$case_dir" docker-init >"$out_file"
  [ "$(file_mode "$case_dir/.env")" = 600 ] || fail ".env was not created with mode 600"
  assert_contains "$case_dir/.env" "COMPOSE_FILE=compose.yaml"
  assert_not_contains "$case_dir/.env" "ADMIN_DOMAIN="
  assert_contains "$out_file" "Admin remains local at http://127.0.0.1:9090/"

  printf '%s\n' 'SYNAPS3_FILECOIN_PRIVATE_KEY=preserve-this-value' >>"$case_dir/.env"
  if make --no-print-directory -C "$case_dir" docker-init ADMIN_DOMAIN=other.example.test >"$out_file" 2>"$err_file"; then
    fail "docker-init overwrote an existing .env"
  fi
  assert_contains "$err_file" "refusing to overwrite"
  assert_contains "$case_dir/.env" "SYNAPS3_FILECOIN_PRIVATE_KEY=preserve-this-value"

  https_dir=$(new_case_dir)
  copy_deployment_files "$https_dir"
  if make --no-print-directory -C "$https_dir" docker-init ADMIN_DOMAIN='https://admin.example.test' >"$out_file" 2>"$err_file"; then
    fail "docker-init accepted a URL instead of a hostname"
  fi
  assert_contains "$err_file" "ADMIN_DOMAIN must be a public hostname"

  injection_dir=$(new_case_dir)
  copy_deployment_files "$injection_dir"
  injected_domain=$(printf 'admin.example.test\nCOMPOSE_FILE=override.yaml')
  if ADMIN_DOMAIN="$injected_domain" make --no-print-directory -C "$injection_dir" docker-init >"$out_file" 2>"$err_file"; then
    fail "docker-init accepted a multiline ADMIN_DOMAIN"
  fi
  assert_contains "$err_file" "ADMIN_DOMAIN must be a public hostname"
  [ ! -e "$injection_dir/.env" ] || fail "docker-init created .env from a multiline ADMIN_DOMAIN"

  make --no-print-directory -C "$https_dir" docker-init ADMIN_DOMAIN=admin.example.test >"$out_file"
  assert_contains "$https_dir/.env" "COMPOSE_FILE=compose.yaml:compose.admin-https.yaml"
  assert_contains "$https_dir/.env" "ADMIN_DOMAIN=admin.example.test"
  assert_contains "$out_file" "Admin HTTPS will use https://admin.example.test/"

  local_dir=$(new_case_dir)
  copy_deployment_files "$local_dir"
  make --no-print-directory -C "$local_dir" docker-init ADMIN_DOMAIN=admin.example.test IMAGE_SOURCE=local >"$out_file"
  assert_contains "$local_dir/.env" "COMPOSE_FILE=compose.yaml:compose.local.yaml:compose.admin-https.yaml"
}

test_make_lifecycle_contract() {
  case_dir=$(new_case_dir)
  copy_deployment_files "$case_dir"
  make --no-print-directory -C "$case_dir" docker-init >/dev/null
  printf '%s\n' 'SYNAPS3_FILECOIN_PRIVATE_KEY=must-not-appear-in-output' >>"$case_dir/.env"

  bin_dir=$(install_fake_tools "$case_dir")
  compose_log="$case_dir/compose.log"
  output_log="$case_dir/output.log"
  error_log="$case_dir/error.log"
  : >"$compose_log"

  chmod 644 "$case_dir/.env"
  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-up \
      DOCKER_COMPOSE="$bin_dir/docker-compose" >"$output_log" 2>"$error_log"; then
    fail "docker-up accepted an unprotected .env"
  fi
  assert_contains "$error_log" ".env permissions are 644"
  chmod 600 "$case_dir/.env"

  ADMIN_DOMAIN=override.example.test COMPOSE_FILE=override.yaml SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-up \
      DOCKER_COMPOSE="$bin_dir/docker-compose" DOCKER_WAIT_TIMEOUT=7 >"$output_log" 2>"$error_log"
  assert_contains "$compose_log" "config --quiet"
  assert_contains "$compose_log" "up -d --remove-orphans --wait --wait-timeout 7"
  assert_contains "$compose_log" "env ADMIN_DOMAIN=unset"
  assert_contains "$compose_log" "env COMPOSE_FILE=unset"
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

  local_dir=$(new_case_dir)
  copy_deployment_files "$local_dir"
  make --no-print-directory -C "$local_dir" docker-init IMAGE_SOURCE=local >/dev/null
  local_bin_dir=$(install_fake_tools "$local_dir")
  local_compose_log="$local_dir/compose.log"
  : >"$local_compose_log"
  SYNAPS3_TEST_COMPOSE_LOG="$local_compose_log" \
    make --no-print-directory -C "$local_dir" docker-up \
      DOCKER_COMPOSE="$local_bin_dir/docker-compose" >"$output_log"
  assert_contains "$local_compose_log" "up -d --build --remove-orphans --wait"
}

test_verify_contract() {
  case_dir=$(new_case_dir)
  copy_deployment_files "$case_dir"
  make --no-print-directory -C "$case_dir" docker-init >/dev/null
  bin_dir=$(install_fake_tools "$case_dir")
  compose_log="$case_dir/compose.log"
  output_log="$case_dir/output.log"
  error_log="$case_dir/error.log"
  : >"$compose_log"

  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HEALTH_STATUS=setup SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-verify \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log"
  assert_contains "$output_log" "Local Admin is reachable, but SynapS3 still requires setup"

  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HEALTH_STATUS=ok SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-verify \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log"
  assert_contains "$output_log" "Local Admin is ready at http://127.0.0.1:9090/"

  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" SYNAPS3_TEST_HEALTH_STATUS=unhealthy SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$case_dir" docker-verify \
      DOCKER_COMPOSE="$bin_dir/docker-compose" CURL="$bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log" 2>"$error_log"; then
    fail "docker-verify accepted an unhealthy deployment"
  fi
  assert_contains "$error_log" "SynapS3 is unhealthy"

  https_dir=$(new_case_dir)
  copy_deployment_files "$https_dir"
  make --no-print-directory -C "$https_dir" docker-init ADMIN_DOMAIN=admin.example.test >/dev/null
  https_bin_dir=$(install_fake_tools "$https_dir")
  https_compose_log="$https_dir/compose.log"
  : >"$https_compose_log"

  SYNAPS3_TEST_COMPOSE_LOG="$https_compose_log" SYNAPS3_TEST_HEALTH_STATUS=setup SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$https_dir" docker-verify \
      DOCKER_COMPOSE="$https_bin_dir/docker-compose" CURL="$https_bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log"
  assert_contains "$output_log" "Admin HTTPS is ready at https://admin.example.test/, but SynapS3 still requires setup"

  SYNAPS3_TEST_COMPOSE_LOG="$https_compose_log" SYNAPS3_TEST_HEALTH_STATUS=ok SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$https_dir" docker-verify \
      DOCKER_COMPOSE="$https_bin_dir/docker-compose" CURL="$https_bin_dir/curl" DOCKER_VERIFY_DELAY=0 >"$output_log"
  assert_contains "$output_log" "SynapS3 Admin HTTPS is ready at https://admin.example.test/"

  if SYNAPS3_TEST_COMPOSE_LOG="$https_compose_log" SYNAPS3_TEST_HTTPS_READY=0 SYNAPS3_TEST_DOMAIN=admin.example.test \
    make --no-print-directory -C "$https_dir" docker-verify \
      DOCKER_COMPOSE="$https_bin_dir/docker-compose" CURL="$https_bin_dir/curl" DOCKER_VERIFY_ATTEMPTS=1 DOCKER_VERIFY_DELAY=0 >"$output_log" 2>"$error_log"; then
    fail "docker-verify accepted unavailable HTTPS"
  fi
  assert_contains "$error_log" "make docker-logs DOCKER_SERVICE=caddy"
}

test_compose_and_caddy_config() {
  case_dir=$(new_case_dir)
  copy_deployment_files "$case_dir"
  make --no-print-directory -C "$case_dir" docker-init >/dev/null

  (cd "$case_dir" && sh docker/deployment.sh check >/dev/null)

  docker compose --project-directory "$case_dir" config >"$case_dir/rendered.yaml"
  assert_not_contains "$case_dir/rendered.yaml" "image: caddy:2.11.4-alpine"

  https_dir=$(new_case_dir)
  copy_deployment_files "$https_dir"
  make --no-print-directory -C "$https_dir" docker-init ADMIN_DOMAIN=admin.example.test >/dev/null
  (cd "$https_dir" && sh docker/deployment.sh check >/dev/null)

  docker compose --project-directory "$https_dir" config >"$https_dir/rendered.yaml"
  assert_contains "$https_dir/rendered.yaml" "image: caddy:2.11.4-alpine"
  assert_contains "$https_dir/rendered.yaml" "SYNAPS3_ADMIN_AUTH_ENABLED: \"true\""
  assert_contains "$https_dir/rendered.yaml" "SYNAPS3_ADMIN_TRUSTED_PROXIES: 127.0.0.1/32"
  assert_contains "$https_dir/rendered.yaml" "name: synaps3-caddy-data"
  assert_contains "$https_dir/rendered.yaml" "name: synaps3-caddy-config"

  sed 's/^ADMIN_DOMAIN=.*/ADMIN_DOMAIN=/' "$https_dir/.env" >"$https_dir/.env.invalid"
  chmod 600 "$https_dir/.env.invalid"
  if docker compose --project-directory "$https_dir" --env-file "$https_dir/.env.invalid" config --quiet 2>"$https_dir/error.log"; then
    fail "Compose accepted an empty ADMIN_DOMAIN"
  fi
  assert_contains "$https_dir/error.log" "Set ADMIN_DOMAIN in .env"

  printf '%s\n' 'ADMIN_DOMAIN=duplicate.example.test' >>"$https_dir/.env"
  if (cd "$https_dir" && sh docker/deployment.sh check >"$https_dir/output.log" 2>"$https_dir/error.log"); then
    fail "deployment check accepted duplicate ADMIN_DOMAIN entries"
  fi
  assert_contains "$https_dir/error.log" "exactly one ADMIN_DOMAIN entry"

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
test_verify_contract
test_compose_and_caddy_config

echo "deployment tests passed"
