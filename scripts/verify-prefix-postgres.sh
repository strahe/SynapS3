#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$script_dir/test-postgres.sh" ./internal/db/repository -run '^TestPostgresPrefixPlan$' -v
