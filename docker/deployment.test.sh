#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/synaps3-deployment-test.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT HUP INT TERM
unset ADMIN_DOMAIN COMPOSE_FILE IMAGE_SOURCE SYNAPS3_CONFIG DATABASE_SOURCE POSTGRES_APP_PASSWORD POSTGRES_PORT

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
    "$ROOT_DIR/compose.postgres.yaml" \
    "$target/"
  cp "$ROOT_DIR/docker/Caddyfile" "$ROOT_DIR/docker/deployment.sh" "$ROOT_DIR/docker/postgres-init.sql" "$target/docker/"
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
  [ "$(file_mode "$case_dir/.postgres-admin-password")" = 600 ] || fail ".postgres-admin-password was not created with mode 600"
  grep -Eq '^[0-9a-f]{48}$' "$case_dir/.postgres-admin-password" || fail "PostgreSQL administrator password is not 48 hex characters"
  assert_contains "$case_dir/.env" "COMPOSE_FILE=compose.yaml:compose.postgres.yaml"
  grep -Eq '^POSTGRES_APP_PASSWORD=[0-9a-f]{48}$' "$case_dir/.env" || fail "application database password is missing from .env"
  assert_contains "$case_dir/.env" "POSTGRES_PORT=15432"
  if grep -Fq "$(cat "$case_dir/.postgres-admin-password")" "$case_dir/.env"; then
    fail "the PostgreSQL administrator password leaked into .env"
  fi
  assert_not_contains "$case_dir/.env" "ADMIN_DOMAIN="
  assert_contains "$out_file" "Admin remains local at http://127.0.0.1:9090/"
  assert_contains "$out_file" "Managed PostgreSQL will listen on 127.0.0.1:15432"
  assert_contains "$out_file" "set SYNAPS3_FILECOIN_PRIVATE_KEY in .env"

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
  assert_contains "$err_file" "ADMIN_DOMAIN must be a hostname"

  injection_dir=$(new_case_dir)
  copy_deployment_files "$injection_dir"
  injected_domain=$(printf 'admin.example.test\nCOMPOSE_FILE=override.yaml')
  if ADMIN_DOMAIN="$injected_domain" make --no-print-directory -C "$injection_dir" docker-init >"$out_file" 2>"$err_file"; then
    fail "docker-init accepted a multiline ADMIN_DOMAIN"
  fi
  assert_contains "$err_file" "ADMIN_DOMAIN must be a hostname"
  [ ! -e "$injection_dir/.env" ] || fail "docker-init created .env from a multiline ADMIN_DOMAIN"

  make --no-print-directory -C "$https_dir" docker-init ADMIN_DOMAIN=admin.example.test >"$out_file"
  assert_contains "$https_dir/.env" "COMPOSE_FILE=compose.yaml:compose.postgres.yaml:compose.admin-https.yaml"
  assert_contains "$https_dir/.env" "ADMIN_DOMAIN=admin.example.test"
  assert_contains "$out_file" "Admin HTTPS will use https://admin.example.test/"

  local_dir=$(new_case_dir)
  copy_deployment_files "$local_dir"
  make --no-print-directory -C "$local_dir" docker-init IMAGE_SOURCE=local ADMIN_DOMAIN=admin.example.test >"$out_file"
  assert_contains "$local_dir/.env" "COMPOSE_FILE=compose.yaml:compose.postgres.yaml:compose.local.yaml:compose.admin-https.yaml"

  external_dir=$(new_case_dir)
  copy_deployment_files "$external_dir"
  make --no-print-directory -C "$external_dir" docker-init DATABASE_SOURCE=external >"$out_file"
  assert_contains "$external_dir/.env" "COMPOSE_FILE=compose.yaml"
  assert_not_contains "$external_dir/.env" "compose.postgres.yaml"
  assert_not_contains "$external_dir/.env" "POSTGRES_"
  [ ! -e "$external_dir/.postgres-admin-password" ] || fail "external database initialization created a PostgreSQL password file"
  assert_contains "$out_file" "set SYNAPS3_DATABASE_DSN in .env"

  port_dir=$(new_case_dir)
  copy_deployment_files "$port_dir"
  make --no-print-directory -C "$port_dir" docker-init POSTGRES_PORT=25432 >"$out_file"
  assert_contains "$port_dir/.env" "POSTGRES_PORT=25432"
  for invalid in 'DATABASE_SOURCE=sqlite' 'POSTGRES_PORT=0' 'POSTGRES_PORT=65536' 'POSTGRES_PORT=5432x'; do
    invalid_dir=$(new_case_dir)
    copy_deployment_files "$invalid_dir"
    if make --no-print-directory -C "$invalid_dir" docker-init "$invalid" >"$out_file" 2>"$err_file"; then
      fail "docker-init accepted $invalid"
    fi
    [ ! -e "$invalid_dir/.env" ] && [ ! -e "$invalid_dir/.postgres-admin-password" ] || fail "docker-init with $invalid left deployment files"
  done
  assert_contains "$err_file" "POSTGRES_PORT must be a TCP port"

  password_exists_dir=$(new_case_dir)
  copy_deployment_files "$password_exists_dir"
  printf '%s' preserve-this-password >"$password_exists_dir/.postgres-admin-password"
  if make --no-print-directory -C "$password_exists_dir" docker-init >"$out_file" 2>"$err_file"; then
    fail "docker-init overwrote an existing PostgreSQL password file"
  fi
  assert_contains "$err_file" ".postgres-admin-password already exists"
  assert_contains "$password_exists_dir/.postgres-admin-password" "preserve-this-password"
  [ ! -e "$password_exists_dir/.env" ] || fail "docker-init created .env next to an existing PostgreSQL password file"

  failure_dir=$(new_case_dir)
  copy_deployment_files "$failure_dir"
  mv "$failure_dir/.env.example" "$failure_dir/.env.example.missing"
  if make --no-print-directory -C "$failure_dir" docker-init >"$out_file" 2>"$err_file"; then
    fail "docker-init succeeded without its environment template"
  fi
  [ ! -e "$failure_dir/.env" ] || fail "failed docker-init left a partial .env"
  [ ! -e "$failure_dir/.postgres-admin-password" ] || fail "failed docker-init left a PostgreSQL password file"
  for leftover in "$failure_dir"/.env.tmp.* "$failure_dir"/.postgres-admin-password.tmp.*; do
    [ ! -e "$leftover" ] || fail "failed docker-init left a temporary file"
  done

  link_failure_dir=$(new_case_dir)
  copy_deployment_files "$link_failure_dir"
  mkdir -p "$link_failure_dir/test-bin"
  # Only publishing .env fails, after the password file is already in place.
  cat >"$link_failure_dir/test-bin/ln" <<'EOF'
#!/usr/bin/env sh
case "$2" in
  .env) exit 1 ;;
esac
exec /bin/ln "$@"
EOF
  chmod +x "$link_failure_dir/test-bin/ln"
  if PATH="$link_failure_dir/test-bin:$PATH" make --no-print-directory -C "$link_failure_dir" docker-init >"$out_file" 2>"$err_file"; then
    fail "docker-init succeeded when .env could not be published"
  fi
  assert_contains "$err_file" "Could not create .env atomically"
  [ ! -e "$link_failure_dir/.env" ] || fail "failed .env publication left a partial .env"
  [ ! -e "$link_failure_dir/.postgres-admin-password" ] || fail "failed .env publication left the PostgreSQL password file"
  for leftover in "$link_failure_dir"/.env.tmp.* "$link_failure_dir"/.postgres-admin-password.tmp.*; do
    [ ! -e "$leftover" ] || fail "failed .env publication left a temporary file"
  done
}

test_make_lifecycle_contract() {
  uninitialized_dir=$(new_case_dir)
  copy_deployment_files "$uninitialized_dir"
  uninitialized_bin_dir=$(install_fake_tools "$uninitialized_dir")
  uninitialized_compose_log="$uninitialized_dir/compose.log"
  uninitialized_output_log="$uninitialized_dir/output.log"
  uninitialized_error_log="$uninitialized_dir/error.log"
  : >"$uninitialized_compose_log"
  if SYNAPS3_TEST_COMPOSE_LOG="$uninitialized_compose_log" \
    make --no-print-directory -C "$uninitialized_dir" docker-up \
      DOCKER_COMPOSE="$uninitialized_bin_dir/docker-compose" >"$uninitialized_output_log" 2>"$uninitialized_error_log"; then
    fail "docker-up started without Docker deployment configuration"
  fi
  assert_contains "$uninitialized_error_log" ".env not found. Run: make docker-init"

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

  for deployment_override in ADMIN_DOMAIN COMPOSE_FILE POSTGRES_APP_PASSWORD POSTGRES_PORT; do
    : >"$compose_log"
    if env "$deployment_override=override.example.test" SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
      make --no-print-directory -C "$case_dir" docker-up \
        DOCKER_COMPOSE="$bin_dir/docker-compose" >"$output_log" 2>"$error_log"; then
      fail "docker-up accepted the $deployment_override override"
    fi
    assert_contains "$error_log" "reads ADMIN_DOMAIN, COMPOSE_FILE, POSTGRES_APP_PASSWORD, and POSTGRES_PORT from .env"
    [ ! -s "$compose_log" ] || fail "docker-up invoked Compose after rejecting the $deployment_override override"
  done

  chmod 644 "$case_dir/.postgres-admin-password"
  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-up \
      DOCKER_COMPOSE="$bin_dir/docker-compose" >"$output_log" 2>"$error_log"; then
    fail "docker-up accepted an unprotected PostgreSQL password file"
  fi
  assert_contains "$error_log" ".postgres-admin-password permissions are 644"
  chmod 600 "$case_dir/.postgres-admin-password"
  mv "$case_dir/.postgres-admin-password" "$case_dir/password.moved"
  if SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-up \
      DOCKER_COMPOSE="$bin_dir/docker-compose" >"$output_log" 2>"$error_log"; then
    fail "docker-up started without the PostgreSQL password file"
  fi
  assert_contains "$error_log" ".postgres-admin-password not found"
  mv "$case_dir/password.moved" "$case_dir/.postgres-admin-password"

  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
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
  SYNAPS3_TEST_COMPOSE_LOG="$compose_log" \
    make --no-print-directory -C "$case_dir" docker-logs \
      DOCKER_COMPOSE="$bin_dir/docker-compose" DOCKER_SERVICE=postgres >"$output_log"
  assert_contains "$compose_log" "logs --tail=100 postgres"

  external_dir=$(new_case_dir)
  copy_deployment_files "$external_dir"
  make --no-print-directory -C "$external_dir" docker-init DATABASE_SOURCE=external >/dev/null
  external_bin_dir=$(install_fake_tools "$external_dir")
  external_compose_log="$external_dir/compose.log"
  : >"$external_compose_log"
  if SYNAPS3_TEST_COMPOSE_LOG="$external_compose_log" \
    make --no-print-directory -C "$external_dir" docker-up \
      DOCKER_COMPOSE="$external_bin_dir/docker-compose" >"$output_log" 2>"$error_log"; then
    fail "docker-up started an external deployment without a database URL"
  fi
  assert_contains "$error_log" "exactly one SYNAPS3_DATABASE_DSN entry"
  printf '%s\n' 'SYNAPS3_DATABASE_DSN=postgres://synaps3:secret@db.example.test:5432/synaps3' >>"$external_dir/.env"
  SYNAPS3_TEST_COMPOSE_LOG="$external_compose_log" \
    make --no-print-directory -C "$external_dir" docker-up \
      DOCKER_COMPOSE="$external_bin_dir/docker-compose" >"$output_log"
  assert_contains "$external_compose_log" "up -d --remove-orphans --wait"

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

  assert_contains "$ROOT_DIR/docker/Caddyfile" 'header Strict-Transport-Security "max-age=31536000"'
  assert_not_contains "$ROOT_DIR/docker/Caddyfile" "includeSubDomains"
  assert_not_contains "$ROOT_DIR/docker/Caddyfile" "preload"

  (cd "$case_dir" && sh docker/deployment.sh check >/dev/null)

  docker compose --project-directory "$case_dir" config >"$case_dir/rendered.yaml"
  assert_not_contains "$case_dir/rendered.yaml" "image: caddy:2.11.4-alpine"
  assert_contains "$case_dir/rendered.yaml" "image: postgres:18"
  assert_contains "$case_dir/rendered.yaml" "host_ip: 127.0.0.1"
  assert_contains "$case_dir/rendered.yaml" 'published: "15432"'
  assert_contains "$case_dir/rendered.yaml" "@127.0.0.1:15432/synaps3?sslmode=disable"
  assert_contains "$case_dir/rendered.yaml" "target: /docker-entrypoint-initdb.d/10-synaps3.sql"
  assert_contains "$case_dir/rendered.yaml" "source: postgres-admin-password"
  assert_contains "$case_dir/rendered.yaml" "name: synaps3-postgres-data"
  assert_contains "$case_dir/rendered.yaml" "target: /var/lib/postgresql"
  assert_not_contains "$case_dir/rendered.yaml" "target: /var/lib/postgresql/data"
  assert_contains "$case_dir/rendered.yaml" "condition: service_healthy"

  sed '/^POSTGRES_APP_PASSWORD=/d' "$case_dir/.env" >"$case_dir/.env.invalid"
  chmod 600 "$case_dir/.env.invalid"
  if docker compose --project-directory "$case_dir" --env-file "$case_dir/.env.invalid" config --quiet 2>"$case_dir/error.log"; then
    fail "Compose accepted a managed database without POSTGRES_APP_PASSWORD"
  fi
  assert_contains "$case_dir/error.log" "Set POSTGRES_APP_PASSWORD in .env"

  external_dir=$(new_case_dir)
  copy_deployment_files "$external_dir"
  make --no-print-directory -C "$external_dir" docker-init DATABASE_SOURCE=external >/dev/null
  docker compose --project-directory "$external_dir" config >"$external_dir/rendered.yaml"
  assert_not_contains "$external_dir/rendered.yaml" "image: postgres"

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
