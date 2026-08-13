#!/usr/bin/env bash
set -euo pipefail

root=/home/tolik1992s/funding_arbitrage_paper
release_archive=/tmp/funding-release-037.tar.gz
rollback_archive=/home/tolik1992s/funding-pnl-v2-20260811-036.tar.gz
env_backup=/tmp/funding-release-037.env.backup
build_log=/tmp/funding-release-037-build.log

cd "$root"
test "$(cat .release)" = "funding-pnl-v2-20260811-036"
test -s "$release_archive"
test -s "$rollback_archive"
cp -p .env "$env_backup"

rollback() {
    status=$?
    if [[ $status -ne 0 ]]; then
        echo "DEPLOY_FAILED status=$status; restoring release-036"
        tar -xzf "$rollback_archive" -C "$root"
        cp -p "$env_backup" "$root/.env"
    fi
    exit "$status"
}
trap rollback EXIT

set_env() {
    local key=$1
    local value=$2
    if grep -q "^${key}=" .env; then
        sed -i "s|^${key}=.*|${key}=${value}|" .env
    else
        printf '%s=%s\n' "$key" "$value" >>.env
    fi
}

tar -xzf "$release_archive" -C "$root"
set_env EXECUTION_MODE paper
set_env PAPER_SIMULATION_VERSION v10-oos-candidate
set_env PAPER_BASELINE_SIMULATION_VERSION v10-oos-baseline
chmod 600 .env
docker compose config --quiet
docker compose build app >"$build_log" 2>&1

trap - EXIT
echo "BUILD_OK"
tail -n 12 "$build_log"
