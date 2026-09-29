#!/usr/bin/env bash
# Read-only inventory of the VM and this project's containers.
# Prints no environment variables and no secrets; safe to share the output.
#
# Usage: ops/scripts/vm_inventory.sh [output-file]
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/funding_arbitrage_paper}"
PROJECT_NAME="${PROJECT_NAME:-funding_arbitrage_paper}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${1:-${PROJECT_DIR}/backups/inventory-${STAMP}.txt}"
mkdir -p "$(dirname "$OUT")"

section() { printf '\n==== %s ====\n' "$1"; }

{
  section "host"
  date -u
  uname -a
  uptime
  df -h / /var/lib/docker 2>/dev/null || df -h /
  free -m
  nproc

  section "docker"
  docker --version
  docker compose version
  systemctl is-enabled docker 2>/dev/null || echo "systemctl unavailable"

  section "all containers (names, images, status, compose project)"
  docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Label "com.docker.compose.project"}}'

  section "compose projects"
  docker compose ls -a

  section "this project (${PROJECT_NAME})"
  for name in $(docker ps -a --filter "label=com.docker.compose.project=${PROJECT_NAME}" --format '{{.Names}}'); do
    docker inspect --format \
      '{{.Name}} image={{.Config.Image}} status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}} restarts={{.RestartCount}} started={{.State.StartedAt}} finished={{.State.FinishedAt}} exit={{.State.ExitCode}}' \
      "$name"
    if docker inspect --format '{{if .State.Health}}yes{{end}}' "$name" | grep -q yes; then
      echo "  last health probes:"
      docker inspect --format '{{range .State.Health.Log}}  {{.End}} exit={{.ExitCode}} {{printf "%.160s" .Output}}{{"\n"}}{{end}}' "$name" | tail -n 5
    fi
  done

  section "resource usage"
  docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}'

  section "volumes and disk use"
  docker volume ls
  docker system df

  section "database table sizes (if the project postgres runs)"
  pg="$(docker ps --filter "label=com.docker.compose.project=${PROJECT_NAME}" --filter "label=com.docker.compose.service=postgres" --format '{{.Names}}' | head -n 1)"
  if [ -n "${pg}" ]; then
    docker exec "${pg}" psql -U funding -d funding -c \
      "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size, n_live_tup AS rows
         FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 20;" || true
  else
    echo "no running postgres container for ${PROJECT_NAME}"
  fi

  section "source tree"
  if [ -d "${PROJECT_DIR}/.git" ]; then
    git -C "${PROJECT_DIR}" log --oneline -5 || true
    echo "uncommitted changes (file names only):"
    git -C "${PROJECT_DIR}" status --short || true
  else
    echo "${PROJECT_DIR} is not a git checkout"
  fi
} > "${OUT}" 2>&1

chmod 600 "${OUT}"
echo "inventory written to ${OUT}"
