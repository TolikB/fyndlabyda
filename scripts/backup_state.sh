#!/usr/bin/env bash
set -euo pipefail

readonly expected_project="funding_arbitrage_v1"
readonly marker_value="funding-arbitrage-v1"
backup_root="${BACKUP_ROOT:-/var/backups/funding-arbitrage-v1}"
compose_file="${COMPOSE_FILE:-docker-compose.yml}"
env_file="${COMPOSE_ENV_FILE:-.env.live}"
project="${COMPOSE_PROJECT_NAME:-$expected_project}"
recipient="${AGE_RECIPIENT:-}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is unavailable: $1" >&2
    exit 2
  }
}
for command_name in age docker git realpath sha256sum stat; do
  require_command "$command_name"
done

if [[ "$project" != "$expected_project" ]]; then
  echo "refusing unexpected Compose project: $project" >&2
  exit 2
fi
if [[ -z "$recipient" || "$recipient" == *$'\n'* ]]; then
  echo "AGE_RECIPIENT must contain one explicit age or SSH recipient" >&2
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

postgres_exec() {
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" exec -T postgres sh -c \
    'export PGPASSWORD="$POSTGRES_PASSWORD"; exec "$@"' sh "$@"
}

backup_root="$(realpath -e -- "$backup_root")"
if [[ "$backup_root" == "/" || ! -f "$backup_root/.funding-backup-root" ]]; then
  echo "backup root identity marker is missing" >&2
  exit 2
fi
if [[ "$(tr -d '\r\n' < "$backup_root/.funding-backup-root")" != "$marker_value" ]]; then
  echo "backup root identity marker is invalid" >&2
  exit 2
fi
root_mode="$(stat -c '%a' "$backup_root")"
if [[ ! "$root_mode" =~ ^[0-7]{3,4}$ ]]; then
  echo "backup root permissions are invalid: $root_mode" >&2
  exit 2
fi
root_mode_value=$((8#$root_mode))
if (( (root_mode_value & 077) != 0 )); then
  echo "backup root permissions expose group/world bits: $root_mode" >&2
  exit 2
fi

docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" config --quiet
running_services="$(docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" ps --status running --services)"
if ! grep -Fxq postgres <<<"$running_services"; then
  echo "PostgreSQL is not running in the expected Compose project" >&2
  exit 2
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
base="funding-v1-postgres-${timestamp}"
archive="$backup_root/${base}.dump.age"
checksum="$archive.sha256"
manifest="$archive.json"
for target in "$archive" "$checksum" "$manifest"; do
  if [[ -e "$target" ]]; then
    echo "refusing to overwrite backup artifact: $target" >&2
    exit 2
  fi
done

tmp_archive="$(mktemp --tmpdir="$backup_root" ".${base}.dump.age.tmp.XXXXXX")"
tmp_checksum="$(mktemp --tmpdir="$backup_root" ".${base}.sha256.tmp.XXXXXX")"
tmp_manifest="$(mktemp --tmpdir="$backup_root" ".${base}.json.tmp.XXXXXX")"
cleanup() {
  rm -f -- "$tmp_archive" "$tmp_checksum" "$tmp_manifest"
}
trap cleanup EXIT
chmod 0600 "$tmp_archive" "$tmp_checksum" "$tmp_manifest"

postgres_user="${POSTGRES_USER:-funding}"
postgres_db="${POSTGRES_DB:-funding}"
postgres_exec pg_dump --username "$postgres_user" --dbname "$postgres_db" \
  --format=custom --compress=9 --no-owner --no-acl \
  | age --recipient "$recipient" --output "$tmp_archive"

test -s "$tmp_archive"
archive_hash="$(sha256sum "$tmp_archive" | awk '{print $1}')"
archive_size="$(stat -c '%s' "$tmp_archive")"
commit_sha="$(git rev-parse HEAD 2>/dev/null || true)"
if [[ ! "$commit_sha" =~ ^[0-9a-f]{40}$ ]]; then
  commit_sha="unknown"
fi
migration_head="$(postgres_exec psql --username "$postgres_user" \
  --dbname "$postgres_db" --tuples-only --no-align \
  --command 'SELECT version_num FROM alembic_version LIMIT 1' | tr -d '\r\n')"
if [[ ! "$migration_head" =~ ^[A-Za-z0-9_-]{1,64}$ ]]; then
  echo "could not establish a valid Alembic migration head" >&2
  exit 1
fi

printf '%s  %s\n' "$archive_hash" "$(basename "$archive")" > "$tmp_checksum"
printf '{\n  "archive": "%s",\n  "created_at_utc": "%s",\n  "sha256": "%s",\n  "size_bytes": %s,\n  "git_commit": "%s",\n  "alembic_head": "%s",\n  "compose_project": "%s",\n  "encrypted": true\n}\n' \
  "$(basename "$archive")" "$timestamp" "$archive_hash" "$archive_size" \
  "$commit_sha" "$migration_head" "$project" > "$tmp_manifest"

mv -- "$tmp_archive" "$archive"
mv -- "$tmp_checksum" "$checksum"
mv -- "$tmp_manifest" "$manifest"
trap - EXIT
chmod 0600 "$archive" "$checksum" "$manifest"
echo "encrypted PostgreSQL backup created: $archive"