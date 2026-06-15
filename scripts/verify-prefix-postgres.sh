#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SYNAPS3_POSTGRES_TEST_DSN:-}" ]]; then
  echo "SYNAPS3_POSTGRES_TEST_DSN is required" >&2
  exit 2
fi

go test ./internal/db/repository -run '^TestPostgresPrefixPlan$' -count=1 -v
