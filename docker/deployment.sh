#!/usr/bin/env sh
set -eu

DOCKER_COMPOSE=${DOCKER_COMPOSE:-docker compose}
CURL=${CURL:-curl}
ENV_FILE=.env
DOCKER_WAIT_TIMEOUT=${DOCKER_WAIT_TIMEOUT:-120}
DOCKER_VERIFY_ATTEMPTS=${DOCKER_VERIFY_ATTEMPTS:-10}
DOCKER_VERIFY_DELAY=${DOCKER_VERIFY_DELAY:-3}
DOCKER_LOG_TAIL=${DOCKER_LOG_TAIL:-100}
DOCKER_LOG_FOLLOW=${DOCKER_LOG_FOLLOW:-0}
DOCKER_SERVICE=${DOCKER_SERVICE:-}

compose() {
  # DOCKER_COMPOSE intentionally supports commands such as "docker compose".
  # shellcheck disable=SC2086
  $DOCKER_COMPOSE "$@"
}

valid_domain() {
  domain=$1
  [ -n "$domain" ] && [ "${#domain}" -le 253 ] || return 1
  case "$domain" in
    *[!A-Za-z0-9.-]*) return 1 ;;
  esac
  printf '%s\n' "$domain" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$' || return 1
  printf '%s\n' "${domain##*.}" | grep -Eq '[A-Za-z]'
}

init_deployment() {
  if [ -e "$ENV_FILE" ]; then
    echo "$ENV_FILE already exists; refusing to overwrite it." >&2
    exit 1
  fi

  domain=${ADMIN_DOMAIN:-}
  if [ -n "$domain" ] && ! valid_domain "$domain"; then
    echo "ADMIN_DOMAIN must be a public hostname such as admin.example.com, without a scheme, port, path, or wildcard." >&2
    exit 1
  fi

  case "${IMAGE_SOURCE:-published}" in
    published) compose_files=compose.yaml ;;
    local) compose_files=compose.yaml:compose.local.yaml ;;
    *)
      echo "IMAGE_SOURCE must be published or local." >&2
      exit 1
      ;;
  esac
  if [ -n "$domain" ]; then
    compose_files=$compose_files:compose.admin-https.yaml
  fi

  umask 077
  set -C
  {
    printf '# Docker deployment selection. Managed by make docker-init.\n'
    printf 'COMPOSE_FILE=%s\n' "$compose_files"
    if [ -n "$domain" ]; then
      printf 'ADMIN_DOMAIN=%s\n' "$domain"
    fi
    printf '\n'
    cat .env.example
  } >"$ENV_FILE"
  chmod 600 "$ENV_FILE"

  if [ -n "$domain" ]; then
    echo "Created $ENV_FILE. Admin HTTPS will use https://$domain/."
  else
    echo "Created $ENV_FILE. Admin remains local at http://127.0.0.1:9090/."
  fi
}

check_deployment() {
  docker_command=${DOCKER_COMPOSE%% *}
  command -v "$docker_command" >/dev/null 2>&1 || {
    echo "Docker CLI not found. Install Docker Engine and Docker Compose v2.24 or later." >&2
    exit 1
  }

  version=$(compose version --short 2>/dev/null || true)
  version=${version#v}
  major=${version%%.*}
  rest=${version#*.}
  minor=${rest%%.*}
  case "$major:$minor" in
    *[!0-9:]*)
      echo "Could not determine the Docker Compose version." >&2
      exit 1
      ;;
  esac
  if [ -z "$major" ] || [ -z "$minor" ] || [ "$major" -lt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -lt 24 ]; }; then
    echo "Docker Compose v2.24 or later is required; found ${version:-unknown}." >&2
    exit 1
  fi

  if [ ! -f "$ENV_FILE" ]; then
    echo "$ENV_FILE not found. Run: make docker-init" >&2
    exit 1
  fi
  mode=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || stat -f '%Lp' "$ENV_FILE")
  if [ "$mode" != 600 ]; then
    echo "$ENV_FILE permissions are $mode; run: chmod 600 $ENV_FILE" >&2
    exit 1
  fi

  compose_file_count=$(grep -c '^COMPOSE_FILE=' "$ENV_FILE" || true)
  if [ "$compose_file_count" -ne 1 ]; then
    echo "$ENV_FILE must contain exactly one COMPOSE_FILE entry." >&2
    exit 1
  fi
  compose_files=$(sed -n 's/^COMPOSE_FILE=//p' "$ENV_FILE" | tr -d '\r')

  https_enabled=0
  case ":$compose_files:" in
    *:compose.admin-https.yaml:*)
      https_enabled=1
      domain_count=$(grep -c '^ADMIN_DOMAIN=' "$ENV_FILE" || true)
      if [ "$domain_count" -ne 1 ]; then
        echo "$ENV_FILE must contain exactly one ADMIN_DOMAIN entry when Admin HTTPS is enabled." >&2
        exit 1
      fi
      domain=$(sed -n 's/^ADMIN_DOMAIN=//p' "$ENV_FILE" | tr -d '\r')
      ;;
    *) domain= ;;
  esac
  if [ "$https_enabled" = 1 ]; then
    if ! valid_domain "$domain"; then
      echo "ADMIN_DOMAIN in $ENV_FILE must be a public hostname such as admin.example.com." >&2
      exit 1
    fi
  fi

  compose config --quiet
}

up_deployment() {
  check_deployment
  printf '%s\n' "$DOCKER_WAIT_TIMEOUT" | grep -Eq '^[1-9][0-9]*$' || {
    echo "DOCKER_WAIT_TIMEOUT must be a positive number of seconds." >&2
    exit 1
  }

  set -- up -d
  case ":$compose_files:" in
    *:compose.local.yaml:*) set -- "$@" --build ;;
  esac
  set -- "$@" --remove-orphans --wait --wait-timeout "$DOCKER_WAIT_TIMEOUT"
  compose "$@"
  compose ps
  echo "Containers are running. Run make docker-verify before using the deployment."
}

verify_deployment() {
  check_deployment
  curl_command=${CURL%% *}
  command -v "$curl_command" >/dev/null 2>&1 || {
    echo "curl is required for Docker deployment verification." >&2
    exit 1
  }
  printf '%s\n' "$DOCKER_VERIFY_ATTEMPTS" | grep -Eq '^[1-9][0-9]*$' || {
    echo "DOCKER_VERIFY_ATTEMPTS must be a positive integer." >&2
    exit 1
  }
  printf '%s\n' "$DOCKER_VERIFY_DELAY" | grep -Eq '^[0-9]+$' || {
    echo "DOCKER_VERIFY_DELAY must be a non-negative number of seconds." >&2
    exit 1
  }

  body_file=$(mktemp "${TMPDIR:-/tmp}/synaps3-health.XXXXXX")
  trap 'rm -f "$body_file"' EXIT HUP INT TERM
  # CURL intentionally supports a test command path or the standard curl binary.
  # shellcheck disable=SC2086
  local_code=$($CURL --silent --show-error --output "$body_file" --write-out '%{http_code}' --max-time 10 http://127.0.0.1:9090/healthz || true)
  local_body=$(cat "$body_file")
  if [ "$local_code" != 200 ] && [ "$local_code" != 503 ]; then
    echo "Local Admin health check failed (HTTP ${local_code:-000}). Run: make docker-logs DOCKER_SERVICE=synaps3" >&2
    exit 1
  fi
  echo "Local Admin health: $local_body"

  case "$local_body" in
    *'"status":"ok"'*) local_status=ok ;;
    *'"status":"setup"'*) local_status=setup ;;
    *'"status":"unhealthy"'*)
      echo "SynapS3 is unhealthy. Run: make docker-logs DOCKER_SERVICE=synaps3" >&2
      exit 1
      ;;
    *)
      echo "Local Admin returned an unexpected health response." >&2
      exit 1
      ;;
  esac

  case ":$compose_files:" in
    *:compose.admin-https.yaml:*) ;;
    *)
      if [ "$local_status" = ok ]; then
        echo "Local Admin is ready at http://127.0.0.1:9090/."
      else
        echo "Local Admin is reachable, but SynapS3 still requires setup."
      fi
      return
      ;;
  esac
  attempt=1
  https_code=000
  while [ "$attempt" -le "$DOCKER_VERIFY_ATTEMPTS" ]; do
    : >"$body_file"
    # shellcheck disable=SC2086
    https_code=$($CURL --silent --output "$body_file" --write-out '%{http_code}' --connect-timeout 5 --max-time 15 "https://$domain/healthz" || true)
    if [ "$https_code" = 200 ] || [ "$https_code" = 503 ]; then
      break
    fi
    if [ "$attempt" -lt "$DOCKER_VERIFY_ATTEMPTS" ]; then
      sleep "$DOCKER_VERIFY_DELAY"
    fi
    attempt=$((attempt + 1))
  done
  if [ "$https_code" != 200 ] && [ "$https_code" != 503 ]; then
    echo "Public Admin HTTPS is not ready (HTTP $https_code). Check DNS, ports 80/443, and: make docker-logs DOCKER_SERVICE=caddy" >&2
    exit 1
  fi

  https_body=$(cat "$body_file")
  # shellcheck disable=SC2086
  redirect=$($CURL --silent --output /dev/null --write-out '%{http_code} %{redirect_url}' --connect-timeout 5 --max-time 10 "http://$domain/" || true)
  set -- $redirect
  case "${1:-}" in
    301 | 302 | 307 | 308) ;;
    *)
      echo "HTTP does not redirect to HTTPS for $domain." >&2
      exit 1
      ;;
  esac
  case "${2:-}" in
    "https://$domain/"*) ;;
    *)
      echo "HTTP redirect target is not https://$domain/." >&2
      exit 1
      ;;
  esac

  echo "Public Admin HTTPS: $https_body"
  case "$https_body" in
    *'"status":"ok"'*) echo "SynapS3 Admin HTTPS is ready at https://$domain/." ;;
    *'"status":"setup"'*) echo "Admin HTTPS is ready at https://$domain/, but SynapS3 still requires setup." ;;
    *'"status":"unhealthy"'*)
      echo "Admin HTTPS is ready, but SynapS3 is unhealthy. Run: make docker-logs DOCKER_SERVICE=synaps3" >&2
      exit 1
      ;;
    *)
      echo "Admin HTTPS returned an unexpected health response." >&2
      exit 1
      ;;
  esac
}

logs_deployment() {
  printf '%s\n' "$DOCKER_LOG_TAIL" | grep -Eq '^[1-9][0-9]*$' || {
    echo "DOCKER_LOG_TAIL must be a positive integer." >&2
    exit 1
  }
  case "$DOCKER_LOG_FOLLOW" in
    0 | 1) ;;
    *)
      echo "DOCKER_LOG_FOLLOW must be 0 or 1." >&2
      exit 1
      ;;
  esac
  case "$DOCKER_SERVICE" in
    '' | synaps3 | caddy) ;;
    *)
      echo "DOCKER_SERVICE must be synaps3, caddy, or empty." >&2
      exit 1
      ;;
  esac

  set -- logs "--tail=$DOCKER_LOG_TAIL"
  if [ "$DOCKER_LOG_FOLLOW" = 1 ]; then
    set -- "$@" -f
  fi
  if [ -n "$DOCKER_SERVICE" ]; then
    set -- "$@" "$DOCKER_SERVICE"
  fi
  compose "$@"
}

command=${1:-}
if [ "$command" != init ]; then
  unset ADMIN_DOMAIN COMPOSE_FILE
fi

case "$command" in
  init) init_deployment ;;
  check) check_deployment ;;
  up) up_deployment ;;
  verify) verify_deployment ;;
  down)
    compose down --remove-orphans
    echo "Containers removed. $ENV_FILE, runtime data, and any certificate volumes were preserved."
    ;;
  status) compose ps ;;
  logs) logs_deployment ;;
  password) compose exec -T synaps3 cat /var/lib/synaps3/admin-initial-password ;;
  *)
    echo "Usage: $0 {init|check|up|verify|down|status|logs|password}" >&2
    exit 2
    ;;
esac
