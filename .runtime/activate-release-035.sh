#!/usr/bin/env bash
set -euo pipefail

root=/home/tolik1992s/funding_arbitrage_paper
rollback_archive=/home/tolik1992s/funding-pnl-v2-20260811-034.tar.gz
env_backup=/tmp/funding-release-035.env.backup
rollback_log=/tmp/funding-release-035-rollback.log

cd "$root"

rollback() {
    status=$?
    if [[ $status -ne 0 ]]; then
        echo "ACTIVATION_FAILED status=$status; restoring release-034"
        tar -xzf "$rollback_archive" -C "$root"
        cp -p "$env_backup" "$root/.env"
        docker compose build app >"$rollback_log" 2>&1
        docker compose up -d --no-deps app
    fi
    exit "$status"
}
trap rollback EXIT

docker compose up -d --no-build --no-deps app
container=$(docker compose ps -q app)
test -n "$container"

healthy=0
for _ in $(seq 1 40); do
    health=$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || true)
    if [[ "$health" == "healthy" ]]; then
        healthy=1
        break
    fi
    sleep 3
done
test "$healthy" = 1
curl -fsS --max-time 10 http://127.0.0.1:8000/health/ready >/tmp/funding-release-035-ready.json

printf '%s\n' 'funding-pnl-v2-20260811-035' >.release
tar -czf /home/tolik1992s/funding-pnl-v2-20260811-035.tar.gz \
    --exclude='./.env' \
    --exclude='./.git' \
    --exclude='./.runtime' \
    --exclude='./.venv' \
    --exclude='./data' \
    --exclude='./postgres_data' \
    .

trap - EXIT
echo "ACTIVATION_OK"
cat /tmp/funding-release-035-ready.json
