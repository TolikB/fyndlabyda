#!/usr/bin/env bash
# Back up a funding-bot PostgreSQL database and the deployment configuration.
#
# * The dump uses pg_dump custom format from inside the database container.
# * .env stays in a separate root-only directory on the VM.
# * Only the explicitly selected off-vm.tar.gz is safe to copy off the VM.
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
actual_project="$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "${container}")"
if [ "${actual_project}" != "${PROJECT_NAME}" ]; then
  echo "refusing to back up a container outside ${PROJECT_NAME}" >&2
  exit 1
fi
if [ -z "${container}" ]; then
  echo "no running postgres container found; pass --container NAME" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${PROJECT_DIR}/backups/${STAMP}-${container}"
umask 077
mkdir -p "${DEST}/config"

echo "dumping ${DB_NAME} from ${container}"
docker exec "${container}" sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" exec pg_dump -U "$1" -d "$2" -Fc' \
  sh "${DB_USER}" "${DB_NAME}" > "${DEST}/${DB_NAME}.dump"
docker exec -i "${container}" pg_restore --list < "${DEST}/${DB_NAME}.dump" > /dev/null \
  || { echo "dump verification failed" >&2; exit 1; }

for file in docker-compose.yml config/paper_series.yaml; do
  if [ -f "${PROJECT_DIR}/${file}" ]; then
    mkdir -p "${DEST}/config/$(dirname "${file}")"
    cp -p "${PROJECT_DIR}/${file}" "${DEST}/config/${file}"
  fi
done
if [ -f "${PROJECT_DIR}/.env" ]; then
  mkdir -p "${DEST}/private" "${DEST}/config"
  cp "${PROJECT_DIR}/.env" "${DEST}/private/.env"
  chmod 600 "${DEST}/private/.env"
  # Redact every value, including credentials embedded in connection URLs.
  sed -nE 's/^([A-Za-z_][A-Za-z0-9_]*)=.*/\1=***redacted***/p' \
    "${PROJECT_DIR}/.env" > "${DEST}/config/env.redacted"
fi

(cd "${DEST}" && find "${DB_NAME}.dump" config -type f -print0 | xargs -0 sha256sum > SHA256SUMS)
tar -czf "${DEST}/off-vm.tar.gz" -C "${DEST}" "${DB_NAME}.dump" config SHA256SUMS
chmod 600 "${DEST}/off-vm.tar.gz"
sha256sum "${DEST}/off-vm.tar.gz" > "${DEST}/off-vm.tar.gz.sha256"
echo "backup written to ${DEST} (mode 700). Copy only off-vm.tar.gz off the VM."
