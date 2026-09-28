#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/synaps3-test-postgres.XXXXXX")"
trap 'rm -rf "$test_root"' EXIT
mkdir -p "$test_root/bin" "$test_root/go-only"
export TESTPG_DOCKER_LOG="$test_root/docker.log"
export TESTPG_GO_LOG="$test_root/go.log"

cat >"$test_root/bin/docker" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >>"$TESTPG_DOCKER_LOG"
if [[ "${TESTPG_CONTEXT_FAIL:-0}" == 1 ]]; then
  exit 42
fi
case "$1 $2" in
  'context show') printf '%s\n' "${DOCKER_CONTEXT:-colima}" ;;
  'context inspect')
    [[ "$3" == "${DOCKER_CONTEXT:-colima}" ]]
    printf '%s\n' "${TESTPG_ENDPOINT-unix:///tmp/.colima/default/docker.sock}"
    ;;
  *) echo "Unexpected Docker operation: $*" >&2; exit 1 ;;
esac
EOF

cat >"$test_root/bin/go" <<'EOF'
#!/bin/bash
set -euo pipefail
printf 'host=%s\nsocket=%s\n' "${DOCKER_HOST:-}" "${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE:-}" >"$TESTPG_GO_LOG"
printf '%s\n' "$@" >>"$TESTPG_GO_LOG"
exit "${TESTPG_GO_EXIT:-0}"
EOF
chmod +x "$test_root/bin/docker" "$test_root/bin/go"
cp "$test_root/bin/go" "$test_root/go-only/go"
export PATH="$test_root/bin:$PATH"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

reset_case() {
  unset SYNAPS3_POSTGRES_TEST_DSN DOCKER_HOST DOCKER_CONTEXT TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
  unset TESTPG_ENDPOINT TESTPG_CONTEXT_FAIL TESTPG_GO_EXIT
  : >"$TESTPG_DOCKER_LOG"
  rm -f "$TESTPG_GO_LOG"
}

run_entrypoint() {
  bash "$script_dir/test-postgres.sh" ./internal/testpg -run '^TestSchema$' -v
}

assert_go() {
  local expected
  expected="host=$1"$'\n'"socket=$2"$'\n'"test"$'\n'"-tags=postgres"$'\n'"-count=1"$'\n'"./internal/testpg"$'\n'"-run"$'\n'"^TestSchema$"$'\n'"-v"
  [[ "$(<"$TESTPG_GO_LOG")" == "$expected" ]] || fail "Go environment or arguments changed"
}

reset_case
run_entrypoint
assert_go unix:///tmp/.colima/default/docker.sock /var/run/docker.sock
[[ "$(<"$TESTPG_DOCKER_LOG")" == $'context show\ncontext inspect colima --format {{.Endpoints.docker.Host}}' ]] || fail "Current context was not read"
[[ -z "${DOCKER_HOST:-}" && -z "${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE:-}" ]] || fail "Caller environment changed"

reset_case
export DOCKER_CONTEXT=colima-work
export TESTPG_ENDPOINT=unix:///tmp/.colima/work/docker.sock
run_entrypoint
assert_go "$TESTPG_ENDPOINT" /var/run/docker.sock
[[ "$(<"$TESTPG_DOCKER_LOG")" == $'context show\ncontext inspect colima-work --format {{.Endpoints.docker.Host}}' ]] || fail "DOCKER_CONTEXT was ignored"

reset_case
export DOCKER_HOST=unix:///explicit/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/explicit/daemon.sock
run_entrypoint
assert_go "$DOCKER_HOST" "$TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"
[[ ! -s "$TESTPG_DOCKER_LOG" ]] || fail "Explicit Docker host was replaced"

reset_case
export SYNAPS3_POSTGRES_TEST_DSN=postgres://external/test
run_entrypoint
assert_go '' ''
[[ ! -s "$TESTPG_DOCKER_LOG" ]] || fail "External DSN invoked Docker"

reset_case
export TESTPG_ENDPOINT=unix:///var/run/docker.sock
export DOCKER_CONTEXT=default
run_entrypoint
assert_go "$TESTPG_ENDPOINT" ''

reset_case
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/custom/daemon.sock
run_entrypoint
assert_go unix:///tmp/.colima/default/docker.sock "$TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"

for failure in context empty_endpoint; do
  reset_case
  if [[ "$failure" == context ]]; then
    export TESTPG_CONTEXT_FAIL=1
  else
    export TESTPG_ENDPOINT=''
  fi
  if run_entrypoint >"$test_root/failure.log" 2>&1; then
    fail "Invalid Docker context succeeded"
  fi
  [[ ! -f "$TESTPG_GO_LOG" ]] || fail "Invalid Docker context fell back"
done

reset_case
export TESTPG_GO_EXIT=23
status=0
run_entrypoint || status=$?
[[ "$status" == 23 ]] || fail "Go exit status was not preserved"

reset_case
PATH="$test_root/go-only" /bin/bash "$script_dir/test-postgres.sh" ./internal/testpg -run '^TestSchema$' -v
assert_go '' ''
[[ ! -s "$TESTPG_DOCKER_LOG" ]] || fail "Docker CLI was required"

echo "PostgreSQL test entrypoint tests passed."
