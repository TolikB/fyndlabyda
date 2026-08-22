#!/usr/bin/env bash
set -euo pipefail

readonly expected_project="funding_arbitrage_v1"
readonly marker_value="funding-arbitrage-v1"
readonly required_confirmation="RESTORE_FUNDING_V1_POSTGRES_AND_KEEP_APP_STOPPED"

archive="${1:-}"
backup_root="${BACKUP_ROOT:-/var/backups/funding-arbitrage-v1}"
compose_file="${COMPOSE_FILE:-docker-compose.yml}"
env_file="${COMPOSE_ENV_FILE:-.env.live}"
project="${COMPOSE_PROJECT_NAME:-$expected_project}"
pre_restore_backup="${PRE_RESTORE_BACKUP:-}"
identity_file="${AGE_IDENTITY_FILE:-}"
confirmation="${CONFIRM_RESTORE:-}"
change_ticket="${RESTORE_CHANGE_TICKET:-}"
max_pre_restore_age_seconds="${MAX_PRE_RESTORE_BACKUP_AGE_SECONDS:-900}"
maintenance_marker="${RESTORE_MAINTENANCE_MARKER:-.restore-maintenance}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is unavailable: $1" >&2
    exit 2
  }
}
for command_name in age awk cat date dirname docker jq realpath sha256sum stat; do
  require_command "$command_name"
done

require_exact_line() {
  local path="$1"
  local expected="$2"
  local label="$3"
  if [[ "$(awk 'END { print NR }' "$path")" != "1" ]] ||
     [[ "$(cat -- "$path")" != "$expected" ]]; then
    echo "$label is invalid" >&2
    exit 2
  fi
}

if [[ "$project" != "$expected_project" ]]; then
  echo "refusing unexpected Compose project: $project" >&2
  exit 2
fi
if [[ "$confirmation" != "$required_confirmation" ]]; then
  echo "exact CONFIRM_RESTORE phrase is required" >&2
  exit 2
fi
if [[ ! "$change_ticket" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$ ]]; then
  echo "RESTORE_CHANGE_TICKET must be a traceable identifier" >&2
  exit 2
fi
if [[ ! "$max_pre_restore_age_seconds" =~ ^[0-9]+$ ]] ||
   (( max_pre_restore_age_seconds < 60 || max_pre_restore_age_seconds > 3600 )); then
  echo "MAX_PRE_RESTORE_BACKUP_AGE_SECONDS must be between 60 and 3600" >&2
  exit 2
fi
if [[ -z "$archive" || -z "$pre_restore_backup" ]]; then
  echo "usage: restore_state.sh <archive.dump.age>; PRE_RESTORE_BACKUP is required" >&2
  exit 2
fi
if [[ -z "$identity_file" ]]; then
  echo "AGE_IDENTITY_FILE must name one explicit private age identity file" >&2
  exit 2
fi
if [[ -z "$maintenance_marker" || ! -e "$maintenance_marker" ]]; then
  echo "RESTORE_MAINTENANCE_MARKER must name the active restore fence" >&2
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
    'if [ -z "${POSTGRES_PASSWORD:-}" ] || [ -z "${POSTGRES_USER:-}" ] || [ -z "${POSTGRES_DB:-}" ]; then
       echo "PostgreSQL runtime credentials are missing" >&2
       exit 2
     fi
     export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER" PGDATABASE="$POSTGRES_DB"
     exec "$@"' sh "$@"
}

backup_root="$(realpath -e -- "$backup_root")"
archive="$(realpath -e -- "$archive")"
pre_restore_backup="$(realpath -e -- "$pre_restore_backup")"
identity_file="$(realpath -e -- "$identity_file")"
maintenance_marker="$(realpath -e -- "$maintenance_marker")"
compose_root="$(dirname "$(realpath -e -- "$compose_file")")"
if [[ "$maintenance_marker" != "$compose_root/.restore-maintenance" ]]; then
  echo "restore maintenance marker must be the fence beside the Compose file" >&2
  exit 2
fi
if [[ ! -f "$identity_file" || ! -r "$identity_file" ]]; then
  echo "AGE_IDENTITY_FILE must be a readable regular file" >&2
  exit 2
fi
identity_mode="$(stat -c '%a' "$identity_file")"
identity_uid="$(stat -c '%u' "$identity_file")"
if [[ ! "$identity_mode" =~ ^[0-7]{3,4}$ ]]; then
  echo "AGE_IDENTITY_FILE permissions are invalid: $identity_mode" >&2
  exit 2
fi
identity_mode_value=$((8#$identity_mode))
if (( (identity_mode_value & 077) != 0 )) || [[ "$identity_uid" != "$EUID" ]]; then
  echo "AGE_IDENTITY_FILE must be owned by the current user without group/world access" >&2
  exit 2
fi
identity_parent="$(dirname "$identity_file")"
identity_parent_mode="$(stat -c '%a' "$identity_parent")"
identity_parent_uid="$(stat -c '%u' "$identity_parent")"
if [[ ! "$identity_parent_mode" =~ ^[0-7]{3,4}$ ]]; then
  echo "AGE_IDENTITY_FILE parent permissions are invalid: $identity_parent_mode" >&2
  exit 2
fi
identity_parent_mode_value=$((8#$identity_parent_mode))
if (( (identity_parent_mode_value & 022) != 0 )) || [[ "$identity_parent_uid" != "$EUID" ]]; then
  echo "AGE_IDENTITY_FILE parent must be owned by the current user without group/world write access" >&2
  exit 2
fi
if [[ ! -f "$maintenance_marker" || ! -r "$maintenance_marker" ]]; then
  echo "restore maintenance marker must be a readable regular file" >&2
  exit 2
fi
marker_mode="$(stat -c '%a' "$maintenance_marker")"
marker_uid="$(stat -c '%u' "$maintenance_marker")"
if [[ ! "$marker_mode" =~ ^[0-7]{3,4}$ ]]; then
  echo "restore maintenance marker permissions are invalid: $marker_mode" >&2
  exit 2
fi
marker_mode_value=$((8#$marker_mode))
if (( (marker_mode_value & 077) != 0 )) || [[ "$marker_uid" != "$EUID" ]]; then
  echo "restore maintenance marker must be owned by the current user without group/world access" >&2
  exit 2
fi
expected_marker="funding-arbitrage-v1-restore:${change_ticket}"
require_exact_line "$maintenance_marker" "$expected_marker" "restore maintenance marker"
if [[ "$backup_root" == "/" || ! -f "$backup_root/.funding-backup-root" ]]; then
  echo "backup root identity marker is missing" >&2
  exit 2
fi
require_exact_line "$backup_root/.funding-backup-root" "$marker_value" "backup root identity marker"
archive_created_at=""
pre_restore_created_at=""
for candidate in "$archive" "$pre_restore_backup"; do
  if [[ "$candidate" != "$backup_root/"* || "$candidate" != *.dump.age ]]; then
    echo "backup artifact is outside the verified root or has an invalid extension" >&2
    exit 2
  fi
  if [[ ! -f "$candidate.sha256" || ! -f "$candidate.json" || ! -f "$candidate.complete" ]]; then
    echo "backup checksum, manifest, or completion marker is missing: $candidate" >&2
    exit 2
  fi
  (
    cd "$backup_root"
    sha256sum --check --status "$(basename "$candidate.sha256")"
  ) || {
    echo "backup checksum validation failed: $candidate" >&2
    exit 1
  }
  actual_hash="$(sha256sum "$candidate" | awk '{print $1}')"
  require_exact_line "$candidate.complete" "$actual_hash" "backup completion marker for $candidate"
  if ! jq --exit-status \
    --arg archive "$(basename "$candidate")" \
    --arg hash "$actual_hash" \
    --arg project "$project" \
    '.archive == $archive and
     .sha256 == $hash and
     .compose_project == $project and
     .encrypted == true and
     (.size_bytes | type == "number" and . > 0) and
     (.created_at_utc | type == "string" and test("^[0-9]{8}T[0-9]{6}Z$")) and
     (.alembic_head | type == "string" and test("^[A-Za-z0-9_-]{1,64}$"))' \
    "$candidate.json" >/dev/null; then
    echo "backup manifest validation failed: $candidate" >&2
    exit 1
  fi
  created_at="$(jq --raw-output '.created_at_utc' "$candidate.json")"
  if [[ "$candidate" == "$archive" ]]; then
    archive_created_at="$created_at"
  else
    pre_restore_created_at="$created_at"
  fi
done
if [[ "$archive" == "$pre_restore_backup" ]]; then
  echo "PRE_RESTORE_BACKUP must be a distinct newer safety backup" >&2
  exit 2
fi
if [[ ! "$pre_restore_created_at" > "$archive_created_at" ]]; then
  echo "PRE_RESTORE_BACKUP manifest must be newer than the restore target" >&2
  exit 2
fi
pre_restore_iso="${pre_restore_created_at:0:4}-${pre_restore_created_at:4:2}-${pre_restore_created_at:6:2}T${pre_restore_created_at:9:2}:${pre_restore_created_at:11:2}:${pre_restore_created_at:13:2}Z"
if ! pre_restore_epoch="$(date -u -d "$pre_restore_iso" +%s 2>/dev/null)"; then
  echo "PRE_RESTORE_BACKUP timestamp cannot be parsed" >&2
  exit 2
fi
now_epoch="$(date -u +%s)"
pre_restore_age_seconds=$((now_epoch - pre_restore_epoch))
if (( pre_restore_age_seconds < 0 || pre_restore_age_seconds > max_pre_restore_age_seconds )); then
  echo "PRE_RESTORE_BACKUP is not a fresh current-state safety backup" >&2
  exit 2
fi

docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" config --quiet
running_services="$(docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" ps --status running --services)"
if grep -Fxq app <<<"$running_services"; then
  echo "refusing restore while the application service is running" >&2
  exit 2
fi
if ! grep -Fxq postgres <<<"$running_services"; then
  echo "PostgreSQL must be running in the expected Compose project" >&2
  exit 2
fi

app_container_id="$(docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" ps --all --quiet app)"
if [[ ! "$app_container_id" =~ ^[0-9a-f]{64}$ ]]; then
  echo "expected exactly one stopped application container" >&2
  exit 2
fi
docker update --restart=no "$app_container_id" >/dev/null
if [[ "$(docker inspect "$app_container_id" --format '{{.HostConfig.RestartPolicy.Name}}')" != "no" ]]; then
  echo "application restart policy could not be fenced" >&2
  exit 1
fi
running_app_ids="$(docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" ps --status running --quiet app)"
if [[ -n "$running_app_ids" ]] ||
   [[ "$(docker inspect "$app_container_id" --format '{{.State.Running}}')" != "false" ]]; then
  echo "application started while the restore fence was being established" >&2
  exit 1
fi
require_exact_line \
  "$maintenance_marker" "$expected_marker" \
  "restore maintenance marker before destructive restore"

current_migration_head="$(postgres_exec psql --tuples-only --no-align \
  --command 'SELECT version_num FROM alembic_version LIMIT 1' | tr -d '\r\n')"
pre_restore_migration_head="$(jq --raw-output '.alembic_head' "$pre_restore_backup.json")"
if [[ ! "$current_migration_head" =~ ^[A-Za-z0-9_-]{1,64}$ ]] ||
   [[ "$pre_restore_migration_head" != "$current_migration_head" ]]; then
  echo "PRE_RESTORE_BACKUP does not match the current database migration head" >&2
  exit 2
fi

age --decrypt --identity "$identity_file" "$archive" \
  | postgres_exec pg_restore --clean --if-exists --single-transaction --exit-on-error \
    --no-owner --no-acl

docker compose --project-name "$project" "${compose_env_args[@]}" \
  --file "$compose_file" run --rm --no-deps app alembic upgrade head
postgres_exec psql --set ON_ERROR_STOP=1 \
  --command 'SELECT version_num FROM alembic_version; SELECT COUNT(*) FROM canonical_events;'

echo "restore verified for ticket $change_ticket; application remains stopped and fenced"
