#!/usr/bin/env sh
set -eu

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

: "${SYNAPS3_DATA_DIR:=/var/lib/synaps3}"
default_config="$SYNAPS3_DATA_DIR/config.toml"
if [ "${SYNAPS3_CONFIG+x}" = "x" ]; then
  config_was_set=1
else
  config_was_set=0
fi
: "${SYNAPS3_CONFIG:=$default_config}"

mkdir -p "$SYNAPS3_DATA_DIR"
chmod 0700 "$SYNAPS3_DATA_DIR"

if [ ! -f "$SYNAPS3_CONFIG" ]; then
  if [ "$config_was_set" = "1" ] && [ "$SYNAPS3_CONFIG" != "$default_config" ]; then
    echo "SYNAPS3_CONFIG points to a missing file: $SYNAPS3_CONFIG" >&2
    exit 1
  fi
  synaps3 init --dir "$SYNAPS3_DATA_DIR"
fi

exec synaps3 serve --config "$SYNAPS3_CONFIG"
