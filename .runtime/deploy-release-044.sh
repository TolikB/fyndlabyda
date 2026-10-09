#!/usr/bin/env bash
set -euo pipefail

root=/home/tolik1992s/funding_arbitrage_paper
release_archive=/tmp/funding-release-044.tar.gz
rollback_archive=/home/tolik1992s/funding-pnl-v2-20260811-043.tar.gz
env_backup=/tmp/funding-release-044.env.backup
build_log=/tmp/funding-release-044-build.log

cd "$root"
test "$(cat .release)" = "funding-pnl-v2-20260811-043"
test -s "$release_archive"
test -s "$rollback_archive"
cp -p .env "$env_backup"

rollback() {
    status=$?
    if [[ $status -ne 0 ]]; then
        echo "DEPLOY_FAILED status=$status; restoring release-043"
        tar -xzf "$rollback_archive" -C "$root"
        cp -p "$env_backup" "$root/.env"
    fi
    exit "$status"
}
trap rollback EXIT

tar -xzf "$release_archive" -C "$root"
chmod 600 .env
grep -Fxq 'RUN_MODE=paper_test' .env
grep -Fxq 'MARKET_DATA_MODE=live_public' .env
grep -Fxq 'EXECUTION_MODE=paper' .env
grep -Fxq 'PAPER_SIMULATION_VERSION=v15-oos-candidate' .env
grep -Fxq 'PAPER_BASELINE_SIMULATION_VERSION=v15-oos-baseline' .env
docker compose config --quiet
docker compose build app >"$build_log" 2>&1

trap - EXIT
echo "BUILD_OK"
tail -n 12 "$build_log"
