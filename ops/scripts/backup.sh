#!/usr/bin/env bash
# Back up a funding-bot PostgreSQL database and the deployment configuration.
#
# * The dump uses pg_dump custom format from inside the database container.
# * .env is copied with mode 600 into a root-only directory (secrets stay on the VM);
#   a redacted copy with secret values masked is written next to it for review.
#
# Usage:
#   ops/scripts/backup.sh                         # postgres service of this compose project
#   ops/scripts/backup.sh --container NAME        # any postgres container (e.g. the legacy DB)
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/funding_arbitrage_paper}"
PROJECT_NAME="${PROJECT_NAME:-funding_arbitrage_paper}"
DB_USER="${DB_USER:-funding}"
DB_NAME="${DB_NAME:-funding}"
container=""
if [ "${1:-}" = "--container" ]; then
  container="${2:?container name required}"
fi
if [ -z "${container}" ]; then
  container="$(docker ps --filter "label=com.docker.compose.project=${PROJECT_NAME}" \
    --filter "label=com.docker.compose.service=postgres" --format '{{.Names}}' | head -n 1)"
fi
if [ -z "${container}" ]; then
  echo "no running postgres container found; pass --container NAME" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${PROJECT_DIR}/backups/${STAMP}-${container}"
umask 077
mkdir -p "${DEST}"

echo "dumping ${DB_NAME} from ${container}"
docker exec "${container}" pg_dump -U "${DB_USER}" -d "${DB_NAME}" -Fc > "${DEST}/${DB_NAME}.dump"
docker exec -i "${container}" pg_restore --list < "${DEST}/${DB_NAME}.dump" > /dev/null \
  || { echo "dump verification failed" >&2; exit 1; }

for file in .env docker-compose.yml config/paper_series.yaml; do
  if [ -f "${PROJECT_DIR}/${file}" ]; then
    mkdir -p "${DEST}/config/$(dirname "${file}")"
    cp -p "${PROJECT_DIR}/${file}" "${DEST}/config/${file}"
  fi
done
if [ -f "${PROJECT_DIR}/.env" ]; then
  sed -E 's/^([A-Za-z0-9_]*(TOKEN|SECRET|PASSWORD|PASSPHRASE|KEY)[A-Za-z0-9_]*)=.+$/\1=***redacted***/' \
    "${PROJECT_DIR}/.env" > "${DEST}/config/env.redacted"
fi

(cd "${DEST}" && find . -type f ! -name SHA256SUMS -print0 | xargs -0 sha256sum > SHA256SUMS)
echo "backup written to ${DEST} (mode 700). Keep a copy off the VM for the legacy database."
