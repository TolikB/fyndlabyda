#!/usr/bin/env bash
set -euo pipefail

readonly expected_project="funding_arbitrage_v1"
project="${COMPOSE_PROJECT_NAME:-$expected_project}"
env_file="${COMPOSE_ENV_FILE:-.env.live}"
compose_file="${COMPOSE_FILE:-docker-compose.yml}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "host preflight requires Linux" >&2
  exit 2
fi
if [[ "$project" != "$expected_project" ]]; then
  echo "unexpected Compose project: $project" >&2
  exit 2
fi
if [[ ! -f "$compose_file" || ! -f "$env_file" ]]; then
  echo "Compose file and live env file must exist" >&2
  exit 2
fi

compose_env_args=(--env-file "$env_file")
for overlay in secrets/exchange/runtime.env secrets/exchange/telegram.env; do
  if [[ -f "$overlay" ]]; then
    compose_env_args+=(--env-file "$overlay")
  fi
done
for command_name in chronyc docker findmnt ss timedatectl; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required command unavailable: $command_name" >&2
    exit 2
  }
done

test "$(timedatectl show --property=Timezone --value)" = "UTC"
test "$(timedatectl show --property=NTPSynchronized --value)" = "yes"
chronyc waitsync 10 0.1 >/dev/null

docker info >/dev/null
docker compose version >/dev/null
docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" config --quiet

memory_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
root_free_kib="$(df --output=avail / | tail -n 1 | tr -d ' ')"
if (( memory_kib < 3000000 )); then
  echo "host memory is below the 3 GiB safety floor" >&2
  exit 1
fi
if (( root_free_kib < 10485760 )); then
  echo "root filesystem has less than 10 GiB free" >&2
  exit 1
fi

for port in 5432 9108 9109; do
  while read -r endpoint; do
    [[ -z "$endpoint" ]] && continue
    case "$endpoint" in
      127.0.0.1:* | "[::1]:"*) ;;
      *)
        echo "forbidden public listener detected on port $port: $endpoint" >&2
        exit 1
        ;;
    esac
  done < <(ss -H -ltn "sport = :$port" | awk '{print $4}')
done

for secret_file in \
  secrets/exchange/runtime.env \
  secrets/exchange/telegram.env \
  secrets/exchange/credential-policy.json \
  secrets/internal/ca.crt \
  secrets/internal/app-client.crt \
  secrets/internal/app-client.key \
  secrets/internal/postgres-server.crt \
  secrets/internal/postgres-server.key \
  secrets/internal/redis-server.crt \
  secrets/internal/redis-server.key \
  secrets/internal/clickhouse-server.crt \
  secrets/internal/clickhouse-server.key \
  secrets/internal/clickhouse-client.crt \
  secrets/internal/clickhouse-client.key \
  secrets/internal/redis-password; do
  test -s "$secret_file" || {
    echo "required secret artifact is missing: $secret_file" >&2
    exit 1
  }
done
for private_file in \
  secrets/exchange/runtime.env \
  secrets/exchange/telegram.env \
  secrets/exchange/credential-policy.json \
  secrets/internal/app-client.key \
  secrets/internal/postgres-server.key \
  secrets/internal/redis-server.key \
  secrets/internal/clickhouse-server.key \
  secrets/internal/clickhouse-client.key \
  secrets/internal/redis-password; do
  mode="$(stat -c '%a' "$private_file")"
  if [[ ! "$mode" =~ ^[0-7]{3,4}$ ]]; then
    echo "private artifact permissions are invalid: $private_file ($mode)" >&2
    exit 1
  fi
  mode_value=$((8#$mode))
  if (( (mode_value & 077) != 0 )); then
    echo "private artifact permissions expose group/world bits: $private_file ($mode)" >&2
    exit 1
  fi
done
internal_dir_mode="$(stat -c '%a' secrets/internal)"
internal_dir_uid="$(stat -c '%u' secrets/internal)"
if [[ "$internal_dir_mode" != "711" || "$internal_dir_uid" != "0" ]]; then
  echo "internal secret directory must be root-owned mode 0711" >&2
  exit 1
fi

check_private_owner() {
  local file="$1"
  local expected_uid="$2"
  local expected_gid="$3"
  local actual_uid
  local actual_gid
  actual_uid="$(stat -c '%u' "$file")"
  actual_gid="$(stat -c '%g' "$file")"
  if [[ "$actual_uid" != "$expected_uid" || "$actual_gid" != "$expected_gid" ]]; then
    echo "private artifact owner is invalid: $file ($actual_uid:$actual_gid)" >&2
    exit 1
  fi
}

check_private_owner secrets/internal/app-client.key 10001 10001
check_private_owner secrets/internal/postgres-server.key 70 70
check_private_owner secrets/internal/redis-server.key 999 1000
check_private_owner secrets/internal/redis-password 999 1000
check_private_owner secrets/internal/clickhouse-server.key 101 101
check_private_owner secrets/internal/clickhouse-client.key 101 101

bash scripts/verify_internal_tls.sh secrets/internal 86400

echo "Linux host preflight passed for $project"
