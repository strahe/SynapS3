#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SYNAPS3_POSTGRES_TEST_DSN:-}" ]]; then
  if [[ -z "${DOCKER_HOST:-}" ]] && command -v docker >/dev/null 2>&1; then
    docker_context="$(docker context show)"
    docker_host="$(docker context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}')"
    if [[ -z "$docker_host" ]]; then
      echo "Cannot run PostgreSQL tests: the current Docker context has no Docker endpoint. Set DOCKER_HOST or select a usable Docker context." >&2
      exit 1
    fi
    export DOCKER_HOST="$docker_host"
  fi

  if [[ -z "${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE:-}" ]]; then
    case "${DOCKER_HOST:-}" in
      unix://*/.colima/*)
        # Ryuk mounts the socket inside Colima's VM.
        export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
        ;;
    esac
  fi
fi

exec go test -tags=postgres -count=1 "$@"
