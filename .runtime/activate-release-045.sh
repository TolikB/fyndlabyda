#!/usr/bin/env bash
set -euo pipefail

root=/home/tolik1992s/funding_arbitrage_paper
rollback_archive=/home/tolik1992s/funding-pnl-v2-20260811-044.tar.gz
env_backup=/tmp/funding-release-045.env.backup
rollback_log=/tmp/funding-release-045-rollback.log

cd "$root"

rollback() {
    status=$?
    if [[ $status -ne 0 ]]; then
        echo "ACTIVATION_FAILED status=$status; restoring release-044"
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
for _ in $(seq 1 80); do
    health=$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || true)
    if [[ "$health" == "healthy" ]]; then
        healthy=1
        break
    fi
    sleep 3
done
test "$healthy" = 1
curl -fsS --max-time 45 http://127.0.0.1:8000/health >/tmp/funding-release-045-health.json
curl -fsS --max-time 45 http://127.0.0.1:8000/health/ready \
    >/tmp/funding-release-045-ready.json
curl -fsS --max-time 45 \
    'http://127.0.0.1:8000/analytics/paper?simulation_version=v16-oos-candidate&limit=1' \
    >/tmp/funding-release-045-candidate.json
curl -fsS --max-time 45 \
    'http://127.0.0.1:8000/analytics/paper?simulation_version=v16-oos-baseline&limit=1' \
    >/tmp/funding-release-045-baseline.json
python3 -c "import json; p=json.load(open('/tmp/funding-release-045-health.json')); assert p['run_mode']=='paper_test' and p['market_data_mode']=='live_public' and p['execution_mode']=='paper'"
python3 -c "import json; p=json.load(open('/tmp/funding-release-045-ready.json')); assert p['status']=='ready' and p['comparison_enabled'] is True"
python3 -c "import json; p=json.load(open('/tmp/funding-release-045-candidate.json')); assert p['simulation_version']=='v16-oos-candidate'"
python3 -c "import json; p=json.load(open('/tmp/funding-release-045-baseline.json')); assert p['simulation_version']=='v16-oos-baseline'"

printf '%s\n' 'funding-pnl-v2-20260811-045' >.release
tar -czf /home/tolik1992s/funding-pnl-v2-20260811-045.tar.gz \
    --exclude='./.env' \
    --exclude='./.git' \
    --exclude='./.runtime' \
    --exclude='./.venv' \
    --exclude='./data' \
    --exclude='./postgres_data' \
    .

trap - EXIT
echo "ACTIVATION_OK"
cat /tmp/funding-release-045-ready.json
