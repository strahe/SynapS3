#!/usr/bin/env bash
set -euo pipefail

go test -tags=postgres ./internal/db/repository -run '^TestPostgresPrefixPlan$' -count=1 -v
