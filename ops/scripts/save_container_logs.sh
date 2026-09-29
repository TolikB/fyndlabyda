#!/usr/bin/env bash
# Preserve logs and state of containers before they are stopped or removed
# (e.g. the failed Shadow window). Environment variables are never exported.
#
# Usage: ops/scripts/save_container_logs.sh <container> [<container> ...]
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <container> [<container> ...]" >&2
  exit 2
fi

PROJECT_DIR="${PROJECT_DIR:-/opt/funding_arbitrage_paper}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${PROJECT_DIR}/backups/container-logs-${STAMP}"
mkdir -p "${DEST}"
chmod 700 "${DEST}"

for name in "$@"; do
  echo "saving ${name}"
  docker logs --timestamps "${name}" > "${DEST}/${name}.log" 2>&1 || echo "logs unavailable" > "${DEST}/${name}.log"
  docker inspect --format \
    '{{.Name}} image={{.Config.Image}} created={{.Created}} status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}} restarts={{.RestartCount}} started={{.State.StartedAt}} finished={{.State.FinishedAt}} exit={{.State.ExitCode}} project={{index .Config.Labels "com.docker.compose.project"}} service={{index .Config.Labels "com.docker.compose.service"}} cmd={{json .Config.Cmd}}' \
    "${name}" > "${DEST}/${name}.state.txt"
  if docker inspect --format '{{if .State.Health}}yes{{end}}' "${name}" | grep -q yes; then
    docker inspect --format '{{json .State.Health}}' "${name}" > "${DEST}/${name}.health.json"
  fi
done

(cd "${DEST}" && sha256sum ./* > SHA256SUMS)
tar -czf "${DEST}.tar.gz" -C "$(dirname "${DEST}")" "$(basename "${DEST}")"
chmod 600 "${DEST}.tar.gz"
echo "saved to ${DEST}.tar.gz"
