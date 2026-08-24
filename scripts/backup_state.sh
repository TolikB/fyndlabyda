#!/usr/bin/env bash
set -euo pipefail

readonly expected_project="funding_arbitrage_v1"
readonly marker_value="funding-arbitrage-v1"
backup_root="${BACKUP_ROOT:-/var/backups/funding-arbitrage-v1}"
compose_file="${COMPOSE_FILE:-docker-compose.yml}"
env_file="${COMPOSE_ENV_FILE:-.env.live}"
project="${COMPOSE_PROJECT_NAME:-$expected_project}"
recipient="${AGE_RECIPIENT:-}"
allow_stopped_app="${BACKUP_ALLOW_STOPPED_APP:-false}"
stopped_app_confirmation="${BACKUP_STOPPED_APP_CONFIRM:-}"
change_ticket="${RESTORE_CHANGE_TICKET:-}"
maintenance_marker="${RESTORE_MAINTENANCE_MARKER:-.restore-maintenance}"
verified_maintenance_marker=""

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is unavailable: $1" >&2
    exit 2
  }
}
for command_name in age awk cat dirname docker flock git realpath sha256sum stat; do
  require_command "$command_name"
done

require_exact_line() {
  local path="$1"
  local expected="$2"
  local label="${3:-backup root identity marker}"
  if [[ "$(awk 'END { print NR }' "$path")" != "1" ]] ||
     [[ "$(cat -- "$path")" != "$expected" ]]; then
    echo "$label is invalid" >&2
    exit 2
  fi
}

verify_stopped_app_backup_fence() {
  local compose_root marker_resolved marker_mode marker_uid marker_mode_value expected_marker
  compose_root="$(dirname "$(realpath -e -- "$compose_file")")"
  marker_resolved="$(realpath -e -- "$maintenance_marker")"
  if [[ "$marker_resolved" != "$compose_root/.restore-maintenance" ||
        ! -f "$marker_resolved" || ! -r "$marker_resolved" ]]; then
    echo "stopped-app backup requires the exact active restore fence" >&2
    exit 2
  fi
  marker_mode="$(stat -c '%a' "$marker_resolved")"
  marker_uid="$(stat -c '%u' "$marker_resolved")"
  if [[ ! "$marker_mode" =~ ^[0-7]{3,4}$ ]]; then
    echo "stopped-app backup restore-fence permissions are invalid" >&2
    exit 2
  fi
  marker_mode_value=$((8#$marker_mode))
  if (( (marker_mode_value & 077) != 0 )) || [[ "$marker_uid" != "$EUID" ]]; then
    echo "stopped-app backup restore fence must be operator-owned without group/world access" >&2
    exit 2
  fi
  expected_marker="funding-arbitrage-v1-restore:$change_ticket"
  require_exact_line "$marker_resolved" "$expected_marker" "stopped-app backup restore fence"
  verified_maintenance_marker="$marker_resolved"
}
if [[ "$project" != "$expected_project" ]]; then
  echo "refusing unexpected Compose project: $project" >&2
  exit 2
fi
if [[ -z "$recipient" || "$recipient" == *$'\n'* ]]; then
  echo "AGE_RECIPIENT must contain one explicit age or SSH recipient" >&2
  exit 2
fi
if [[ "$allow_stopped_app" != "true" && "$allow_stopped_app" != "false" ]]; then
  echo "BACKUP_ALLOW_STOPPED_APP must be true or false" >&2
  exit 2
fi
if [[ "$allow_stopped_app" == "true" ]]; then
  if [[ "$stopped_app_confirmation" != "BACKUP_FUNDING_V1_POSTGRES_WHILE_APP_STOPPED_AND_FENCED" ]]; then
    echo "exact BACKUP_STOPPED_APP_CONFIRM phrase is required" >&2
    exit 2
  fi
  if [[ ! "$change_ticket" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$ ]]; then
    echo "RESTORE_CHANGE_TICKET must be a traceable identifier for stopped-app backup" >&2
    exit 2
  fi
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

resolve_release_commit() {
  local script_path script_root explicit_sha metadata_sha git_sha resolved_sha
  local app_container_id runtime_sha tracked_changes
  script_path="$(realpath -e -- "${BASH_SOURCE[0]}")"
  script_root="$(dirname "$(dirname "$script_path")")"
  explicit_sha="${RELEASE_COMMIT_SHA:-}"
  metadata_sha=""
  git_sha=""
  if [[ -n "$explicit_sha" && ! "$explicit_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "RELEASE_COMMIT_SHA is not a commit SHA" >&2
    exit 2
  fi
  if [[ -f "$script_root/.release-sha" ]]; then
    if [[ "$(awk 'END { print NR }' "$script_root/.release-sha")" != "1" ]]; then
      echo "release provenance file must contain exactly one line" >&2
      exit 2
    fi
    metadata_sha="$(cat -- "$script_root/.release-sha")"
    if [[ ! "$metadata_sha" =~ ^[0-9a-f]{40}$ ]]; then
      echo "release provenance file does not contain a commit SHA" >&2
      exit 2
    fi
  fi
  if git -C "$script_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git_sha="$(git -C "$script_root" rev-parse HEAD 2>/dev/null || true)"
    tracked_changes="$(git -C "$script_root" status --porcelain --untracked-files=no)"
    if [[ ! "$git_sha" =~ ^[0-9a-f]{40}$ ]]; then
      echo "Git checkout does not resolve to a commit SHA" >&2
      exit 2
    fi
    if [[ -n "$tracked_changes" ]]; then
      echo "Git checkout has tracked changes and cannot identify an immutable release" >&2
      exit 2
    fi
  fi
  resolved_sha="$explicit_sha"
  for provenance_sha in "$metadata_sha" "$git_sha"; do
    if [[ -n "$provenance_sha" && -n "$resolved_sha" && "$provenance_sha" != "$resolved_sha" ]]; then
      echo "release commit provenance sources disagree" >&2
      exit 2
    fi
    if [[ -n "$provenance_sha" ]]; then resolved_sha="$provenance_sha"; fi
  done
  if [[ ! "$resolved_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "a verified release commit SHA is required for backup" >&2
    exit 2
  fi
  app_container_id="$(docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" ps --all --quiet app)"
  if [[ ! "$app_container_id" =~ ^[0-9a-f]{64}$ ]]; then
    echo "exactly one application container is required for backup provenance" >&2
    exit 2
  fi
  if [[ "$allow_stopped_app" == "true" ]]; then
    if [[ "$(docker inspect "$app_container_id" --format '{{.State.Running}}')" != "false" ||
          "$(docker inspect "$app_container_id" --format '{{.HostConfig.RestartPolicy.Name}}')" != "no" ]]; then
      echo "stopped-app backup requires a stopped restart-fenced application container" >&2
      exit 2
    fi
    verify_stopped_app_backup_fence
  elif [[ "$(docker inspect "$app_container_id" --format '{{.State.Running}}')" != "true" ]]; then
    echo "exactly one running application container is required for backup provenance" >&2
    exit 2
  fi
  runtime_sha="$(docker inspect "$app_container_id" \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')"
  if [[ ! "$runtime_sha" =~ ^[0-9a-f]{40}$ || "$runtime_sha" != "$resolved_sha" ]]; then
    echo "application image revision does not match release provenance" >&2
    exit 2
  fi
  printf '%s\n' "$resolved_sha"
}

backup_root="$(realpath -e -- "$backup_root")"
if [[ "$backup_root" == "/" || ! -f "$backup_root/.funding-backup-root" ]]; then
  echo "backup root identity marker is missing" >&2
  exit 2
fi
require_exact_line "$backup_root/.funding-backup-root" "$marker_value"
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

lock_file="$backup_root/.backup.lock"
exec 9>"$lock_file"
chmod 0600 "$lock_file"
if ! flock --nonblock 9; then
  echo "another funding backup is already running" >&2
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

if [[ "$allow_stopped_app" == "true" ]]; then
  verify_stopped_app_backup_fence
  exec 8<"$verified_maintenance_marker"
  if ! flock --nonblock 8; then
    echo "another funding restore or stopped-app backup is already running" >&2
    exit 2
  fi
  verify_stopped_app_backup_fence
fi

commit_sha_before="$(resolve_release_commit)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
base="funding-v1-postgres-${timestamp}"
archive="$backup_root/${base}.dump.age"
checksum="$archive.sha256"
manifest="$archive.json"
complete="$archive.complete"
for target in "$archive" "$checksum" "$manifest" "$complete"; do
  if [[ -e "$target" ]]; then
    echo "refusing to overwrite backup artifact: $target" >&2
    exit 2
  fi
done

tmp_archive="$(mktemp --tmpdir="$backup_root" ".${base}.dump.age.tmp.XXXXXX")"
tmp_checksum="$(mktemp --tmpdir="$backup_root" ".${base}.sha256.tmp.XXXXXX")"
tmp_manifest="$(mktemp --tmpdir="$backup_root" ".${base}.json.tmp.XXXXXX")"
tmp_complete="$(mktemp --tmpdir="$backup_root" ".${base}.complete.tmp.XXXXXX")"
cleanup() {
  rm -f -- "$tmp_archive" "$tmp_checksum" "$tmp_manifest" "$tmp_complete"
}
trap cleanup EXIT
chmod 0600 "$tmp_archive" "$tmp_checksum" "$tmp_manifest" "$tmp_complete"

migration_head_before="$(postgres_exec psql --tuples-only --no-align \
  --command 'SELECT version_num FROM alembic_version LIMIT 1' | tr -d '\r\n')"
if [[ ! "$migration_head_before" =~ ^[A-Za-z0-9_-]{1,64}$ ]]; then
  echo "could not establish a valid Alembic migration head before backup" >&2
  exit 1
fi
postgres_exec pg_dump \
  --format=custom --compress=9 --no-owner --no-acl \
  | age --recipient "$recipient" --output "$tmp_archive"

test -s "$tmp_archive"
archive_hash="$(sha256sum "$tmp_archive" | awk '{print $1}')"
archive_size="$(stat -c '%s' "$tmp_archive")"
commit_sha_after="$(resolve_release_commit)"
if [[ "$commit_sha_after" != "$commit_sha_before" ]]; then
  echo "release provenance changed while backup was running" >&2
  exit 1
fi
commit_sha="$commit_sha_before"
migration_head_after="$(postgres_exec psql --tuples-only --no-align \
  --command 'SELECT version_num FROM alembic_version LIMIT 1' | tr -d '\r\n')"
if [[ ! "$migration_head_after" =~ ^[A-Za-z0-9_-]{1,64}$ ]] ||
   [[ "$migration_head_after" != "$migration_head_before" ]]; then
  echo "Alembic migration head changed while backup was running" >&2
  exit 1
fi
migration_head="$migration_head_before"

printf '%s  %s\n' "$archive_hash" "$(basename "$archive")" > "$tmp_checksum"
printf '{\n  "archive": "%s",\n  "created_at_utc": "%s",\n  "sha256": "%s",\n  "size_bytes": %s,\n  "git_commit": "%s",\n  "alembic_head": "%s",\n  "compose_project": "%s",\n  "encrypted": true\n}\n' \
  "$(basename "$archive")" "$timestamp" "$archive_hash" "$archive_size" \
  "$commit_sha" "$migration_head" "$project" > "$tmp_manifest"
manifest_hash="$(sha256sum "$tmp_manifest" | awk '{print $1}')"
printf '%s  %s\n%s  %s\n' \
  "$archive_hash" "$(basename "$archive")" \
  "$manifest_hash" "$(basename "$manifest")" > "$tmp_complete"

mv -- "$tmp_archive" "$archive"
mv -- "$tmp_checksum" "$checksum"
mv -- "$tmp_manifest" "$manifest"
mv -- "$tmp_complete" "$complete"
trap - EXIT
chmod 0600 "$archive" "$checksum" "$manifest" "$complete"
echo "encrypted PostgreSQL backup created: $archive"
