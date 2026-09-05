#!/usr/bin/env bash
set -euo pipefail

image="${1:-}"
if [[ -z "$image" || ! "$image" =~ ^[A-Za-z0-9._/@:-]+$ ]]; then
  echo "usage: ci_shadow_smoke.sh <validated-local-image>" >&2
  exit 2
fi

run_id="${GITHUB_RUN_ID:-local}"
attempt="${GITHUB_RUN_ATTEMPT:-1}"
if [[ ! "$run_id" =~ ^[A-Za-z0-9_.-]+$ || ! "$attempt" =~ ^[0-9]+$ ]]; then
  echo "invalid CI run identity" >&2
  exit 2
fi

suffix="${run_id}-${attempt}"
app_container="funding-shadow-app-${suffix}"
db_container="funding-shadow-db-${suffix}"
network="funding-shadow-net-${suffix}"
db_image="postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
db_password="ci-shadow-postgres-password-only"

for resource in "$app_container" "$db_container"; do
  if docker container inspect "$resource" >/dev/null 2>&1; then
    echo "refusing to overwrite existing CI container: $resource" >&2
    exit 2
  fi
done
if docker network inspect "$network" >/dev/null 2>&1; then
  echo "refusing to overwrite existing CI network: $network" >&2
  exit 2
fi

app_started=false
db_started=false
network_created=false
cleanup() {
  if [[ "$app_started" == true ]]; then
    docker rm --force "$app_container" >/dev/null 2>&1 || true
  fi
  if [[ "$db_started" == true ]]; then
    docker rm --force "$db_container" >/dev/null 2>&1 || true
  fi
  if [[ "$network_created" == true ]]; then
    docker network rm "$network" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker image inspect "$image" >/dev/null
docker network create --internal "$network" >/dev/null
network_created=true

docker run --detach \
  --name "$db_container" \
  --user 70:70 \
  --init \
  --pids-limit 128 \
  --cpus 0.50 \
  --memory 384m \
  --network "$network" \
  --network-alias postgres \
  --env POSTGRES_USER=funding \
  --env "POSTGRES_PASSWORD=${db_password}" \
  --env POSTGRES_DB=funding \
  --read-only \
  --tmpfs /tmp:size=32m,mode=1777 \
  --tmpfs /run/postgresql:size=8m,uid=70,gid=70,mode=0770 \
  --tmpfs /var/lib/postgresql/data:size=256m,uid=70,gid=70,mode=0700 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  "$db_image" >/dev/null
db_started=true

for _ in $(seq 1 60); do
  if docker exec "$db_container" pg_isready --username funding --dbname funding \
    >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! docker exec "$db_container" pg_isready --username funding --dbname funding \
  >/dev/null 2>&1; then
  docker logs --tail 200 "$db_container" >&2 || true
  echo "shadow PostgreSQL did not become ready" >&2
  exit 1
fi

docker run --detach \
  --name "$app_container" \
  --user 10001:10001 \
  --init \
  --pids-limit 256 \
  --cpus 1.00 \
  --memory 1024m \
  --network "$network" \
  --read-only \
  --tmpfs /tmp:size=64m,mode=1777 \
  --tmpfs /app/.runtime:size=64m,uid=10001,gid=10001,mode=0700 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --env APP_ENV=ci_shadow \
  --env RUN_MODE=paper_test \
  --env TRADING_MODE=SHADOW \
  --env MARKET_DATA_MODE=mock \
  --env EXECUTION_MODE=paper \
  --env PAPER_AUTOTRADE=false \
  --env LIVE_AUTOTRADE=false \
  --env PAPER_AUTO_INIT_DATABASE=false \
  --env PAPER_LOOP_INTERVAL_SECONDS=1 \
  --env "DATABASE_URL=postgresql+asyncpg://funding:${db_password}@postgres:5432/funding" \
  "$image" \
  sh -c 'alembic upgrade head && exec uvicorn funding_arbitrage.main:app --host 0.0.0.0 --port 8000' \
  >/dev/null
app_started=true

ready=false
for _ in $(seq 1 120); do
  if docker exec "$app_container" python -c \
    'import json,urllib.request; p=json.load(urllib.request.urlopen("http://127.0.0.1:8000/health/ready",timeout=2)); assert p["status"]=="ready"; assert p["run_mode"]=="paper_test"; assert p["market_data_mode"]=="mock"' \
    >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs --tail 200 "$app_container" >&2 || true
  echo "isolated shadow application did not become ready" >&2
  exit 1
fi

docker exec "$app_container" python -c \
  'import json,urllib.request; p=json.load(urllib.request.urlopen("http://127.0.0.1:8000/health",timeout=2)); assert p["execution_mode"]=="paper"; assert p["paper_autotrade_enabled"] is False'

restart_count="$(docker inspect --format '{{.RestartCount}}' "$app_container")"
if [[ "$restart_count" != "0" ]]; then
  echo "shadow application restarted unexpectedly: $restart_count" >&2
  exit 1
fi

echo "isolated shadow deployment passed with zero restarts"
