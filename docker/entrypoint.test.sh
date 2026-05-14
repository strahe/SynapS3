#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ENTRYPOINT="$ROOT_DIR/docker/entrypoint.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_file_contains() {
  file=$1
  expected=$2
  if ! grep -Fqx "$expected" "$file"; then
    echo "Expected line not found: $expected" >&2
    echo "Actual file:" >&2
    cat "$file" >&2
    exit 1
  fi
}

assert_dir_mode() {
  dir=$1
  expected=$2
  mode=$(stat -c %a "$dir" 2>/dev/null || stat -f %Lp "$dir")
  if [ "$mode" != "$expected" ]; then
    fail "$dir mode = $mode, want $expected"
  fi
}

new_case_dir() {
  mktemp -d "${TMPDIR:-/tmp}/synaps3-entrypoint-test.XXXXXX"
}

install_fake_synaps3() {
  bin_dir=$1
  log_file=$2
  mkdir -p "$bin_dir"
  cat >"$bin_dir/synaps3" <<'EOF'
#!/usr/bin/env sh
set -eu

echo "synaps3 $*" >>"$SYNAPS3_TEST_LOG"

if [ "$1" = "init" ]; then
  shift
  [ "$1" = "--dir" ] || exit 21
  mkdir -p "$2"
  printf 'generated\n' >"$2/config.toml"
  exit 0
fi

exit 0
EOF
  chmod +x "$bin_dir/synaps3"
  export PATH="$bin_dir:$PATH"
  export SYNAPS3_TEST_LOG="$log_file"
}

test_initializes_missing_config_and_serves() {
  case_dir=$(new_case_dir)
  data_dir="$case_dir/data"
  bin_dir="$case_dir/bin"
  log_file="$case_dir/calls.log"
  touch "$log_file"
  install_fake_synaps3 "$bin_dir" "$log_file"

  SYNAPS3_DATA_DIR="$data_dir" "$ENTRYPOINT"

  [ -f "$data_dir/config.toml" ] || fail "config.toml was not created"
  assert_file_contains "$log_file" "synaps3 init --dir $data_dir"
  assert_file_contains "$log_file" "synaps3 serve --config $data_dir/config.toml"
}

test_existing_config_skips_init_and_serves() {
  case_dir=$(new_case_dir)
  data_dir="$case_dir/data"
  bin_dir="$case_dir/bin"
  log_file="$case_dir/calls.log"
  mkdir -p "$data_dir"
  printf 'existing\n' >"$data_dir/config.toml"
  touch "$log_file"
  install_fake_synaps3 "$bin_dir" "$log_file"

  SYNAPS3_DATA_DIR="$data_dir" "$ENTRYPOINT"

  if grep -Fq "synaps3 init" "$log_file"; then
    fail "init was called for an existing config"
  fi
  assert_file_contains "$log_file" "synaps3 serve --config $data_dir/config.toml"
}

test_existing_data_dir_permissions_are_restricted() {
  case_dir=$(new_case_dir)
  data_dir="$case_dir/data"
  bin_dir="$case_dir/bin"
  log_file="$case_dir/calls.log"
  mkdir -p "$data_dir"
  chmod 0755 "$data_dir"
  printf 'existing\n' >"$data_dir/config.toml"
  touch "$log_file"
  install_fake_synaps3 "$bin_dir" "$log_file"

  SYNAPS3_DATA_DIR="$data_dir" "$ENTRYPOINT"

  assert_dir_mode "$data_dir" "700"
}

test_custom_command_passthrough() {
  case_dir=$(new_case_dir)
  bin_dir="$case_dir/bin"
  log_file="$case_dir/calls.log"
  mkdir -p "$bin_dir"
  cat >"$bin_dir/custom-command" <<'EOF'
#!/usr/bin/env sh
set -eu
echo "custom-command $*" >>"$SYNAPS3_TEST_LOG"
EOF
  chmod +x "$bin_dir/custom-command"
  touch "$log_file"

  PATH="$bin_dir:$PATH" SYNAPS3_TEST_LOG="$log_file" "$ENTRYPOINT" custom-command alpha beta

  assert_file_contains "$log_file" "custom-command alpha beta"
}

test_custom_config_path_is_used() {
  case_dir=$(new_case_dir)
  data_dir="$case_dir/data"
  config_path="$case_dir/custom/config.toml"
  bin_dir="$case_dir/bin"
  log_file="$case_dir/calls.log"
  mkdir -p "$(dirname "$config_path")"
  printf 'custom\n' >"$config_path"
  touch "$log_file"
  install_fake_synaps3 "$bin_dir" "$log_file"

  SYNAPS3_DATA_DIR="$data_dir" SYNAPS3_CONFIG="$config_path" "$ENTRYPOINT"

  if grep -Fq "synaps3 init" "$log_file"; then
    fail "init was called for an existing custom config"
  fi
  assert_file_contains "$log_file" "synaps3 serve --config $config_path"
}

test_missing_custom_config_fails_without_init() {
  case_dir=$(new_case_dir)
  data_dir="$case_dir/data"
  config_path="$case_dir/custom/config.toml"
  bin_dir="$case_dir/bin"
  log_file="$case_dir/calls.log"
  err_file="$case_dir/stderr.log"
  touch "$log_file"
  install_fake_synaps3 "$bin_dir" "$log_file"

  if SYNAPS3_DATA_DIR="$data_dir" SYNAPS3_CONFIG="$config_path" "$ENTRYPOINT" 2>"$err_file"; then
    fail "entrypoint succeeded with a missing custom config"
  fi

  if grep -Fq "synaps3 init" "$log_file"; then
    fail "init was called for a missing custom config"
  fi
  assert_file_contains "$err_file" "SYNAPS3_CONFIG points to a missing file: $config_path"
}

[ -x "$ENTRYPOINT" ] || fail "entrypoint is not executable: $ENTRYPOINT"

test_initializes_missing_config_and_serves
test_existing_config_skips_init_and_serves
test_existing_data_dir_permissions_are_restricted
test_custom_command_passthrough
test_custom_config_path_is_used
test_missing_custom_config_fails_without_init

echo "entrypoint tests passed"
