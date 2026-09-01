#!/usr/bin/env bash
set -euo pipefail

readonly project="funding_arbitrage_v1"
readonly ticket="CI-RESTORE-DRILL"
readonly stopped_backup_confirmation="BACKUP_FUNDING_V1_POSTGRES_WHILE_APP_STOPPED_AND_FENCED"
readonly restore_confirmation="RESTORE_FUNDING_V1_POSTGRES_AND_KEEP_APP_STOPPED"

artifact_dir="${1:-}"
image_ref="${2:-}"
expected_revision="${3:-}"
if [[ -z "$artifact_dir" || -z "$image_ref" || ! "$expected_revision" =~ ^[0-9a-f]{40}$ ]]; then
  echo "usage: ci_restore_drill.sh <artifact-dir> <candidate-image> <revision>" >&2
  exit 2
fi
for command_name in age age-keygen docker find jq realpath sha256sum; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required CI restore command is unavailable: $command_name" >&2
    exit 2
  }
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
artifact_dir="$(realpath -e -- "$artifact_dir")"
if [[ "${GITHUB_ACTIONS:-}" != "true" || "${CI:-}" != "true" || -z "${RUNNER_TEMP:-}" ]]; then
  echo "restore drill is restricted to an isolated GitHub Actions runner" >&2
  exit 2
fi
runner_temp="$(realpath -e -- "$RUNNER_TEMP")"
if [[ "$runner_temp" == "/" || "$runner_temp" == "/tmp" ||
      "$runner_temp" == "/var/tmp" || "$runner_temp" == "/home" ||
      "$runner_temp" != */_temp ]]; then
  echo "RUNNER_TEMP is not an isolated GitHub Actions temporary root" >&2
  exit 2
fi

work_root=""
env_file="$repo_root/.env.restore-ci"
release_sha="$repo_root/.release-sha"
fence="$repo_root/.restore-maintenance"
swap_state="$repo_root/.restore-swap-state"
host_tmpfs="/dev/shm/funding-arbitrage-v1-restore-ci"
compose=(docker compose --project-name "$project" --env-file "$env_file" --file "$repo_root/docker-compose.yml")
app_image="${project}-app"
cleanup_authorized=false

for owned_path in "$env_file" "$release_sha" "$fence" "$swap_state"; do
  if [[ -e "$owned_path" || -L "$owned_path" ]]; then
    echo "restore drill refuses to overwrite an existing repository artifact: $owned_path" >&2
    exit 2
  fi
done
if [[ -e "$host_tmpfs" || -L "$host_tmpfs" ]]; then
  echo "restore drill refuses an existing host tmpfs workspace" >&2
  exit 2
fi
existing_containers="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$project")"
existing_volumes="$(docker volume ls --quiet --filter "label=com.docker.compose.project=$project")"
existing_networks="$(docker network ls --quiet --filter "label=com.docker.compose.project=$project")"
if [[ -n "$existing_containers" || -n "$existing_volumes" || -n "$existing_networks" ]]; then
  echo "restore drill refuses an existing Compose project" >&2
  exit 2
fi
if docker image inspect "$app_image" >/dev/null 2>&1; then
  echo "restore drill refuses an existing candidate image tag" >&2
  exit 2
fi

work_root="$(mktemp -d "$runner_temp/funding-restore-ci.XXXXXX")"
backup_root="$work_root/backups"
identity_file="$work_root/identity.txt"
secrets_dir="$work_root/exchange"
pki_dir="$work_root/pki"

cleanup() {
  set +e
  if [[ "$cleanup_authorized" == "true" ]]; then
    "${compose[@]}" down --volumes --timeout 30 >/dev/null 2>&1
  fi
  rm -f -- "$env_file" "$release_sha" "$fence" "$swap_state"
  rm -f -- "$host_tmpfs/target.dump" "$host_tmpfs/safety.dump"
  rmdir -- "$host_tmpfs" >/dev/null 2>&1 || true
  if [[ -n "$work_root" && "$work_root" == "$runner_temp"/funding-restore-ci.* &&
        -d "$work_root" && ! -L "$work_root" ]]; then
    sudo find "$work_root" -depth -delete
  fi
  docker image rm "$app_image" >/dev/null 2>&1 || true
}
trap cleanup EXIT

install -d -m 0700 "$backup_root" "$secrets_dir"
printf 'funding-arbitrage-v1\n' > "$backup_root/.funding-backup-root"
printf '\n' > "$secrets_dir/runtime.env"
printf '\n' > "$secrets_dir/telegram.env"
chmod 0600 "$secrets_dir/runtime.env" "$secrets_dir/telegram.env"
cp "$repo_root/.env.paper-test.example" "$env_file"
{
  printf 'APP_ENV_FILE=%s\n' "$env_file"
  printf 'LIVE_SECRETS_DIR=%s\n' "$secrets_dir"
  printf 'INTERNAL_TLS_SECRETS_DIR=%s\n' "$pki_dir"
  printf 'PAPER_AUTOTRADE=false\n'
  printf 'TELEGRAM_ENABLED=false\n'
  printf 'LIVE_AUTOTRADE=false\n'
  printf 'LIVE_TRADING_CONFIRM=NO\n'
} >> "$env_file"
chmod 0600 "$env_file"
cp "$artifact_dir/.release-sha" "$release_sha"
test "$(tr -d '\r\n' < "$release_sha")" = "$expected_revision"

sudo ALLOW_EPHEMERAL_TEST_PKI=YES bash "$repo_root/scripts/generate_ephemeral_test_pki.sh" "$pki_dir"
docker tag "$image_ref" "$app_image"
cleanup_authorized=true
"${compose[@]}" up --detach --no-build postgres redis app
for _ in $(seq 1 90); do
  app_status="$("${compose[@]}" ps --all --format json app | jq -r 'if type == "array" then .[0].Health else .Health end')"
  if [[ "$app_status" == "healthy" ]]; then break; fi
  sleep 2
done
test "$app_status" = "healthy"
app_container_id="$("${compose[@]}" ps --all --quiet app)"
test "$(docker inspect "$app_container_id" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')" = "$expected_revision"

pg_exec() {
  local database_name="$1"
  local statement="$2"
  "${compose[@]}" exec -T postgres sh -c '
    set -eu
    export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER"
    exec psql --dbname="$1" --set ON_ERROR_STOP=1 --command "$2"
  ' sh "$database_name" "$statement"
}
pg_scalar() {
  local database_name="$1"
  local statement="$2"
  "${compose[@]}" exec -T postgres sh -c '
    set -eu
    export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER"
    exec psql --dbname="$1" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "$2"
  ' sh "$database_name" "$statement" | tr -d '\r\n'
}
pg_tool() {
  "${compose[@]}" exec -T postgres sh -c '
    set -eu
    export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER"
    exec "$@"
  ' sh "$@"
}

# Seed non-empty target state before the target backup. The drill later adds
# post-target rows and proves that restore returns the exact original payload.
pg_exec funding '
  CREATE TABLE restore_exactness_sentinel(
    id integer PRIMARY KEY,
    marker text NOT NULL
  );
  INSERT INTO restore_exactness_sentinel VALUES (1, $$target-row$$);
  INSERT INTO canonical_events(
    event_id, kind, source, sequence_id, native_sequence, correlation_id,
    payload_version, quality, exchange_timestamp, receive_timestamp,
    monotonic_ns, payload_hash, payload
  ) VALUES (
    $$ci-restore-target$$, $$ticker$$, $$ci_restore$$, $$target-1$$, 1,
    $$ci-restore$$, 1, $$valid$$,
    $$2026-01-01T00:00:00Z$$::timestamptz,
    $$2026-01-01T00:00:00.001Z$$::timestamptz,
    1, repeat($$a$$, 64),
    $json${"marker":"target","notional":"17.25"}$json$::json
  );'
test "$(pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE source = $$ci_restore$$')" = "1"
test "$(pg_scalar funding 'SELECT id || $$|$$ || marker FROM restore_exactness_sentinel')" = "1|target-row"

age-keygen -o "$identity_file" >/dev/null
chmod 0600 "$identity_file"
recipient="$(age-keygen -y "$identity_file")"
backup_output="$(
  cd "$repo_root"
  AGE_RECIPIENT="$recipient" BACKUP_ROOT="$backup_root" COMPOSE_PROJECT_NAME="$project" \
  COMPOSE_FILE="$repo_root/docker-compose.yml" COMPOSE_ENV_FILE="$env_file" \
  bash scripts/backup_state.sh
)"
target="${backup_output##*: }"
test -s "$target"

pg_exec funding '
  INSERT INTO restore_exactness_sentinel VALUES (2, $$post-target-row$$);
  INSERT INTO canonical_events(
    event_id, kind, source, sequence_id, native_sequence, correlation_id,
    payload_version, quality, exchange_timestamp, receive_timestamp,
    monotonic_ns, payload_hash, payload
  ) VALUES (
    $$ci-restore-post-target$$, $$ticker$$, $$ci_restore$$, $$post-target-2$$, 2,
    $$ci-restore$$, 1, $$valid$$,
    $$2026-01-01T00:00:01Z$$::timestamptz,
    $$2026-01-01T00:00:01.001Z$$::timestamptz,
    2, repeat($$b$$, 64),
    $json${"marker":"post-target","notional":"99.99"}$json$::json
  );'
test "$(pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE source = $$ci_restore$$')" = "2"
test "$(pg_scalar funding 'SELECT COUNT(*) FROM restore_exactness_sentinel')" = "2"
printf 'funding-arbitrage-v1-restore:%s\n' "$ticket" > "$fence"
chmod 0600 "$fence"
"${compose[@]}" stop --timeout 30 app
docker update --restart=no "$app_container_id" >/dev/null
test "$(docker inspect "$app_container_id" --format '{{.State.Running}}|{{.HostConfig.RestartPolicy.Name}}')" = "false|no"
sleep 1

backup_output="$(
  cd "$repo_root"
  AGE_RECIPIENT="$recipient" BACKUP_ROOT="$backup_root" COMPOSE_PROJECT_NAME="$project" \
  COMPOSE_FILE="$repo_root/docker-compose.yml" COMPOSE_ENV_FILE="$env_file" \
  BACKUP_ALLOW_STOPPED_APP=true BACKUP_STOPPED_APP_CONFIRM="$stopped_backup_confirmation" \
  RESTORE_CHANGE_TICKET="$ticket" RESTORE_MAINTENANCE_MARKER="$fence" \
  bash scripts/backup_state.sh
)"
safety="${backup_output##*: }"
test -s "$safety"
test "$target" != "$safety"

run_restore() {
  local active_ticket="$1"
  (
    cd "$repo_root"
    BACKUP_ROOT="$backup_root" COMPOSE_PROJECT_NAME="$project" \
    COMPOSE_FILE="$repo_root/docker-compose.yml" COMPOSE_ENV_FILE="$env_file" \
    PRE_RESTORE_BACKUP="$safety" AGE_IDENTITY_FILE="$identity_file" \
    CONFIRM_RESTORE="$restore_confirmation" RESTORE_CHANGE_TICKET="$active_ticket" \
    RESTORE_MAINTENANCE_MARKER="$fence" RESTORE_TMPFS_DIR="$host_tmpfs" \
    MAX_PRE_RESTORE_BACKUP_AGE_SECONDS=3600 \
    bash scripts/restore_state.sh "$target"
  )
}

run_restore "$ticket"
test "$(pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE source = $$ci_restore$$')" = "1"
test "$(pg_scalar funding 'SELECT payload->>$$marker$$ FROM canonical_events WHERE event_id = $$ci-restore-target$$')" = "target"
test "$(pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE event_id = $$ci-restore-post-target$$')" = "0"
test "$(pg_scalar funding 'SELECT id || $$|$$ || marker FROM restore_exactness_sentinel')" = "1|target-row"
test "$(pg_scalar funding 'SELECT version_num FROM alembic_version')" = "0017_multi_regime_runtime"
test "$(pg_scalar postgres "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'restore_%' OR datname LIKE 'rollback_%'")" = "0"
test ! -e "$swap_state"

ticket_hash="$(printf '%s' "$ticket" | sha256sum | awk '{print $1}')"
suffix="${ticket_hash:0:12}"
restore_database="restore_$suffix"
rollback_database="rollback_$suffix"
archive_sha256="$(sha256sum "$target" | awk '{print $1}')"
safety_sha256="$(sha256sum "$safety" | awk '{print $1}')"

write_state() {
  local stage="$1"
  local tmp
  tmp="$(mktemp "$repo_root/.restore-swap-state.XXXXXX")"
  chmod 0600 "$tmp"
  jq --compact-output --null-input \
    --arg ticket_hash "$ticket_hash" --arg database_name funding \
    --arg restore_database "$restore_database" --arg rollback_database "$rollback_database" \
    --arg archive_sha256 "$archive_sha256" --arg safety_sha256 "$safety_sha256" \
    --arg stage "$stage" \
    '{version: 1, ticket_hash: $ticket_hash, database_name: $database_name,
      restore_database: $restore_database, rollback_database: $rollback_database,
      archive_sha256: $archive_sha256, safety_sha256: $safety_sha256, stage: $stage}' > "$tmp"
  sync -f "$tmp"
  mv -- "$tmp" "$swap_state"
  sync -f "$repo_root"
}

prepare_case() {
  pg_exec funding \
    'DROP SCHEMA IF EXISTS ci_restore_stop CASCADE; DROP TABLE IF EXISTS restore_exactness_sentinel; CREATE TABLE restore_exactness_sentinel(id integer PRIMARY KEY, marker text NOT NULL); INSERT INTO restore_exactness_sentinel VALUES (1, $$recovery-row$$); CREATE SCHEMA ci_restore_stop;'
  pg_tool createdb --maintenance-db=postgres --template=funding "$restore_database"
  pg_exec "$restore_database" 'DROP TABLE restore_exactness_sentinel'
  pg_exec postgres "ALTER DATABASE $restore_database WITH ALLOW_CONNECTIONS false"
}

invoke_recovery_gate() {
  local expected_sentinel="$1"
  set +e
  output="$(run_restore "$ticket" 2>&1)"
  status=$?
  set -e
  test "$status" -eq 2
  grep -Fq "unsupported current application schemas" <<<"$output"
  test "$(pg_scalar funding "SELECT to_regclass('public.restore_exactness_sentinel') IS NOT NULL")" = "$expected_sentinel"
  test "$(pg_scalar postgres "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'restore_%' OR datname LIKE 'rollback_%'")" = "0"
  test ! -e "$swap_state"
  pg_exec funding 'DROP SCHEMA ci_restore_stop CASCADE; DROP TABLE IF EXISTS restore_exactness_sentinel'
}

for stage in prepared canonical_locked; do
  prepare_case
  if [[ "$stage" == "canonical_locked" ]]; then
    pg_exec postgres 'ALTER DATABASE funding WITH ALLOW_CONNECTIONS false'
  fi
  write_state "$stage"
  invoke_recovery_gate t
done

prepare_case
pg_exec postgres 'ALTER DATABASE funding WITH ALLOW_CONNECTIONS false'
pg_exec postgres "ALTER DATABASE funding RENAME TO $rollback_database"
write_state original_renamed
invoke_recovery_gate t

for stage in replacement_renamed validated; do
  prepare_case
  pg_exec postgres 'ALTER DATABASE funding WITH ALLOW_CONNECTIONS false'
  pg_exec postgres "ALTER DATABASE funding RENAME TO $rollback_database"
  pg_exec postgres "ALTER DATABASE $restore_database RENAME TO funding"
  write_state "$stage"
  if [[ "$stage" == "replacement_renamed" ]]; then
    invoke_recovery_gate t
  else
    invoke_recovery_gate f
  fi
done

prepare_case
pg_exec postgres 'ALTER DATABASE funding WITH ALLOW_CONNECTIONS false'
pg_exec postgres "ALTER DATABASE funding RENAME TO $rollback_database"
pg_exec postgres "ALTER DATABASE $restore_database RENAME TO funding"
write_state validated
pg_tool dropdb --maintenance-db=postgres "$rollback_database"
invoke_recovery_gate f

prepare_case
pg_exec postgres 'ALTER DATABASE funding WITH ALLOW_CONNECTIONS false'
pg_exec postgres "ALTER DATABASE funding RENAME TO $rollback_database"
pg_exec postgres "ALTER DATABASE $restore_database RENAME TO funding"
write_state replacement_renamed
wrong_ticket="CI-RESTORE-WRONG"
printf 'funding-arbitrage-v1-restore:%s\n' "$wrong_ticket" > "$fence"
set +e
wrong_output="$(run_restore "$wrong_ticket" 2>&1)"
wrong_status=$?
set -e
test "$wrong_status" -eq 1
grep -Fq "does not match this exact restore operation" <<<"$wrong_output"
test -s "$swap_state"
printf 'funding-arbitrage-v1-restore:%s\n' "$ticket" > "$fence"
invoke_recovery_gate t

test "$(docker inspect "$app_container_id" --format '{{.State.Running}}|{{.HostConfig.RestartPolicy.Name}}|{{.RestartCount}}')" = "false|no|0"
test -z "$(find "$host_tmpfs" -maxdepth 1 -type f -print -quit 2>/dev/null || true)"
test -z "$("${compose[@]}" exec -T postgres find /dev/shm/funding-arbitrage-v1-restore -maxdepth 1 -type f -print -quit 2>/dev/null || true)"
echo "CI restore drill passed: exact restore, stopped backup, stage recovery, wrong-ticket rejection"
