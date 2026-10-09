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
restore_tmpfs_dir="${RESTORE_TMPFS_DIR:-/dev/shm/funding-arbitrage-v1-restore}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is unavailable: $1" >&2
    exit 2
  }
}
for command_name in age awk cat date df dirname docker findmnt flock install jq mktemp realpath sha256sum stat sync; do
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
     if [ "$1" = pg_restore ]; then
       shift
       exec pg_restore --dbname="$POSTGRES_DB" "$@"
     fi
     exec "$@"' sh "$@"
}

validate_database_name() {
  local value="$1"
  if [[ ! "$value" =~ ^[a-z_][a-z0-9_]{0,62}$ ]]; then
    echo "unsafe PostgreSQL database name" >&2
    exit 2
  fi
}

postgres_database_exec() {
  local database_name="$1"
  shift
  validate_database_name "$database_name"
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" exec -T postgres sh -c \
    'if [ -z "${POSTGRES_PASSWORD:-}" ] || [ -z "${POSTGRES_USER:-}" ]; then
       echo "PostgreSQL runtime credentials are missing" >&2
       exit 2
     fi
     database_name="$1"
     shift
     export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER" PGDATABASE="$database_name"
     exec "$@"' sh "$database_name" "$@"
}

postgres_archive_workspace_cleanup() {
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" exec -T postgres sh -c '
      set -eu
      workspace=/dev/shm/funding-arbitrage-v1-restore
      if [ -L "$workspace" ] || { [ -e "$workspace" ] && [ ! -d "$workspace" ]; }; then
        echo "unsafe PostgreSQL restore workspace" >&2
        exit 2
      fi
      mkdir -p "$workspace"
      chmod 0700 "$workspace"
      if [ "$(stat -c %u "$workspace")" != "$(id -u)" ] ||
         [ "$(stat -c %a "$workspace")" != "700" ]; then
        echo "PostgreSQL restore workspace ownership or mode is invalid" >&2
        exit 2
      fi
      for archive_path in "$workspace/list.dump" "$workspace/apply.dump"; do
        if [ -e "$archive_path" ] || [ -L "$archive_path" ]; then
          if [ -L "$archive_path" ] || [ ! -f "$archive_path" ] ||
             [ "$(stat -c %u "$archive_path")" != "$(id -u)" ] ||
             [ "$(stat -c %a "$archive_path")" != "600" ]; then
            echo "unsafe stale PostgreSQL restore artifact" >&2
            exit 2
          fi
          rm -f -- "$archive_path"
        fi
      done
    '
}

postgres_archive_validate() {
  local expected_size="$1"
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" exec -T postgres sh -c '
      set -eu
      expected_size="$1"
      workspace=/dev/shm/funding-arbitrage-v1-restore
      archive_path="$workspace/list.dump"
      available_kib="$(df -Pk "$workspace" | awk "END { print \$4 }")"
      if [ -z "$available_kib" ] ||
         [ "$expected_size" -gt "$((available_kib * 1024 - 1048576))" ]; then
        echo "insufficient PostgreSQL shared-memory capacity for archive validation" >&2
        exit 2
      fi
      cleanup() { rm -f -- "$archive_path"; }
      trap cleanup EXIT HUP INT TERM
      (
        umask 077
        set -C
        : > "$archive_path"
      )
      cat > "$archive_path"
      [ "$(stat -c %s "$archive_path")" = "$expected_size" ]
      pg_restore --list "$archive_path" >/dev/null
    ' sh "$expected_size"
}

postgres_restore_archive() {
  local database_name="$1"
  local expected_size="$2"
  shift 2
  validate_database_name "$database_name"
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" exec -T postgres sh -c '
      set -eu
      if [ -z "${POSTGRES_PASSWORD:-}" ] || [ -z "${POSTGRES_USER:-}" ]; then
        echo "PostgreSQL runtime credentials are missing" >&2
        exit 2
      fi
      database_name="$1"
      expected_size="$2"
      shift 2
      workspace=/dev/shm/funding-arbitrage-v1-restore
      archive_path="$workspace/apply.dump"
      available_kib="$(df -Pk "$workspace" | awk "END { print \$4 }")"
      if [ -z "$available_kib" ] ||
         [ "$expected_size" -gt "$((available_kib * 1024 - 1048576))" ]; then
        echo "insufficient PostgreSQL shared-memory capacity for restore archive" >&2
        exit 2
      fi
      cleanup() { rm -f -- "$archive_path"; }
      trap cleanup EXIT HUP INT TERM
      (
        umask 077
        set -C
        : > "$archive_path"
      )
      cat > "$archive_path"
      [ "$(stat -c %s "$archive_path")" = "$expected_size" ]
      export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER"
      pg_restore --dbname="$database_name" "$@" "$archive_path"
    ' sh "$database_name" "$expected_size" "$@"
}

postgres_admin() {
  local action="$1"
  local database_name="$2"
  local extra_name="${3:-}"
  validate_database_name "$database_name"
  case "$action" in
    allow)
      if [[ "$extra_name" != "true" && "$extra_name" != "false" ]]; then
        echo "invalid PostgreSQL connection state" >&2
        exit 2
      fi
      ;;
    rename)
      validate_database_name "$extra_name"
      ;;
    create | drop | exists | probe | terminate) ;;
    *)
      echo "invalid PostgreSQL administration action" >&2
      exit 2
      ;;
  esac
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" exec -T postgres sh -c \
    'if [ -z "${POSTGRES_PASSWORD:-}" ] || [ -z "${POSTGRES_USER:-}" ]; then
       echo "PostgreSQL runtime credentials are missing" >&2
       exit 2
     fi
     action="$1"
     database_name="$2"
     extra_name="${3:-}"
     export PGPASSWORD="$POSTGRES_PASSWORD" PGUSER="$POSTGRES_USER"
     case "$action" in
       create)
         exec createdb --maintenance-db=postgres --template=template0 \
           --owner="$POSTGRES_USER" "$database_name"
         ;;
       drop)
         exec dropdb --maintenance-db=postgres --if-exists "$database_name"
         ;;
       allow)
         exec psql --dbname=postgres --set ON_ERROR_STOP=1 \
           --command "ALTER DATABASE $database_name WITH ALLOW_CONNECTIONS $extra_name"
         ;;
       terminate)
         termination_result="$(
           PGOPTIONS="-c funding.restore_database=$database_name" \
             psql --dbname=postgres --tuples-only --no-align --set ON_ERROR_STOP=1 \
               --command "SELECT COALESCE(bool_and(pg_terminate_backend(pid, 5000)), true) FROM pg_stat_activity WHERE datname = current_setting(\$\$funding.restore_database\$\$) AND pid <> pg_backend_pid()"
         )"
         if [ "$termination_result" != "t" ]; then
           echo "PostgreSQL backend termination timed out" >&2
           exit 1
         fi
         remaining_sessions="$(
           PGOPTIONS="-c funding.restore_database=$database_name" \
             psql --dbname=postgres --tuples-only --no-align --set ON_ERROR_STOP=1 \
               --command "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_setting(\$\$funding.restore_database\$\$) AND pid <> pg_backend_pid()"
         )"
         if [ "$remaining_sessions" != "0" ]; then
           echo "PostgreSQL database is not quiescent" >&2
           exit 1
         fi
         ;;
       exists)
         PGOPTIONS="-c funding.restore_database=$database_name" \
           exec psql --dbname=postgres --tuples-only --no-align --set ON_ERROR_STOP=1 \
             --command "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = current_setting(\$\$funding.restore_database\$\$))"
         ;;
       probe)
         exec psql --dbname="$database_name" --tuples-only --no-align \
           --set ON_ERROR_STOP=1 --command "SELECT 1"
         ;;
       rename)
         exec psql --dbname=postgres --set ON_ERROR_STOP=1 \
           --command "ALTER DATABASE $database_name RENAME TO $extra_name"
         ;;
       *) exit 2 ;;
     esac' sh "$action" "$database_name" "$extra_name"
}

database_schema_count() {
  local database_name="$1"
  postgres_database_exec "$database_name" psql --tuples-only --no-align \
    --command "SELECT COUNT(*)
               FROM pg_namespace
               WHERE nspname NOT IN ('public', 'pg_catalog', 'information_schema')
                 AND nspname !~ '^pg_toast'
                 AND nspname !~ '^pg_temp_'" | tr -d '\r\n'
}
validate_restored_database() {
  local database_name="$1"
  local expected_migration_head="$2"
  local context="$3"
  local schema_count migration_head missing_critical_table_count profile_trigger_count

  if ! schema_count="$(database_schema_count "$database_name")"; then
    echo "$context schema validation failed" >&2
    return 1
  fi
  if [[ ! "$schema_count" =~ ^[0-9]+$ ]] || (( schema_count != 0 )); then
    echo "$context contains unsupported application schemas" >&2
    return 1
  fi
  if ! migration_head="$(
    postgres_database_exec "$database_name" psql --tuples-only --no-align \
      --set ON_ERROR_STOP=1 \
      --command 'SELECT version_num FROM alembic_version LIMIT 1' | tr -d '\r\n'
  )"; then
    echo "$context Alembic validation failed" >&2
    return 1
  fi
  if [[ "$migration_head" != "$expected_migration_head" ]]; then
    echo "$context did not reach the expected Alembic migration head" >&2
    return 1
  fi
  if ! missing_critical_table_count="$(
    postgres_database_exec "$database_name" psql --tuples-only --no-align \
      --set ON_ERROR_STOP=1 \
      --command "WITH required_table(name) AS (
                   VALUES
                     ('instruments'),
                     ('ticker_snapshots'),
                     ('funding_snapshots'),
                     ('funding_history'),
                     ('market_candles'),
                     ('exchanges'),
                     ('orderbook_snapshots'),
                     ('opportunities'),
                     ('paper_positions'),
                     ('paper_fills'),
                     ('paper_funding_payments'),
                     ('portfolio_snapshots'),
                     ('paper_runtime_incidents'),
                     ('backtest_runs'),
                     ('backtest_results'),
                     ('market_replay_jobs'),
                     ('telegram_daily_reports'),
                     ('live_intents'),
                     ('live_orders'),
                     ('live_positions'),
                     ('live_account_snapshots'),
                     ('live_reconciliations'),
                     ('live_daily_reports'),
                     ('live_funding_payments'),
                     ('canonical_events'),
                     ('canonical_journal_profiles'),
                     ('multi_regime_decision_batches'),
                     ('multi_regime_paper_checkpoints'),
                     ('analytics_replication_checkpoints'),
                     ('risk_decisions'),
                     ('oms_order_states'),
                     ('execution_fills'),
                     ('position_states'),
                     ('balance_states'),
                     ('ledger_transactions'),
                     ('ledger_postings'),
                     ('reconciliation_audits'),
                     ('withdrawal_states'),
                     ('api_idempotency_records'),
                     ('immutable_audit_log')
                 )
                 SELECT COUNT(*)
                 FROM required_table
                 WHERE NOT EXISTS (
                   SELECT 1
                   FROM pg_catalog.pg_class AS relation
                   JOIN pg_catalog.pg_namespace AS namespace
                     ON namespace.oid = relation.relnamespace
                   WHERE namespace.nspname = 'public'
                     AND relation.relname = required_table.name
                     AND relation.relkind IN ('r', 'p')
                 )" \
      | tr -d '\r\n'
  )"; then
    echo "$context critical-table validation failed" >&2
    return 1
  fi
  if [[ ! "$missing_critical_table_count" =~ ^[0-9]+$ ]]; then
    echo "$context critical-table count is invalid" >&2
    return 1
  fi
  if (( missing_critical_table_count != 0 )); then
    echo "$context is missing required application tables" >&2
    return 1
  fi
  if ! profile_trigger_count="$(
    postgres_database_exec "$database_name" psql --tuples-only --no-align \
      --set ON_ERROR_STOP=1 \
      --command "SELECT COUNT(*)
                 FROM pg_catalog.pg_trigger AS trigger
                 JOIN pg_catalog.pg_class AS relation
                   ON relation.oid = trigger.tgrelid
                 JOIN pg_catalog.pg_namespace AS namespace
                   ON namespace.oid = relation.relnamespace
                 JOIN pg_catalog.pg_proc AS procedure
                   ON procedure.oid = trigger.tgfoid
                 WHERE namespace.nspname = 'public'
                   AND relation.relname = 'canonical_journal_profiles'
                   AND NOT trigger.tgisinternal
                   AND procedure.proname = 'funding_reject_immutable_mutation'
                   AND trigger.tgname IN (
                     'trg_canonical_journal_profiles_reject_update_delete',
                     'trg_canonical_journal_profiles_reject_truncate'
                   )" | tr -d '\r\n'
  )"; then
    echo "$context canonical journal profile trigger validation failed" >&2
    return 1
  fi
  if [[ "$profile_trigger_count" != "2" ]]; then
    echo "$context is missing canonical journal profile immutability triggers" >&2
    return 1
  fi
}

run_migrations_for_database() {
  local database_name="$1"
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" run --rm --no-deps \
    --env "RESTORE_DATABASE_NAME=$database_name" app sh -ec '
      export DATABASE_URL="$(
        python -c "import os; from urllib.parse import urlsplit, urlunsplit; u = urlsplit(os.environ['"'"'DATABASE_URL'"'"']); print(urlunsplit((u.scheme, u.netloc, '"'"'/'"'"' + os.environ['"'"'RESTORE_DATABASE_NAME'"'"'], u.query, u.fragment)))"
      )"
      exec alembic upgrade head
    '
}

write_swap_stage() {
  local stage="$1"
  local state_tmp
  case "$stage" in
    prepared | canonical_locked | original_renamed | replacement_renamed | validated) ;;
    *)
      echo "invalid restore swap stage" >&2
      exit 2
      ;;
  esac
  state_tmp="$(mktemp "$compose_root/.restore-swap-state.XXXXXX")"
  chmod 0600 "$state_tmp"
  jq --compact-output --null-input \
    --arg ticket_hash "$change_ticket_hash" \
    --arg database_name "$database_name" \
    --arg restore_database "$restore_database" \
    --arg rollback_database "$rollback_database" \
    --arg archive_sha256 "$archive_sha256" \
    --arg safety_sha256 "$pre_restore_sha256" \
    --arg stage "$stage" \
    '{version: 1,
      ticket_hash: $ticket_hash,
      database_name: $database_name,
      restore_database: $restore_database,
      rollback_database: $rollback_database,
      archive_sha256: $archive_sha256,
      safety_sha256: $safety_sha256,
      stage: $stage}' > "$state_tmp"
  sync -f "$state_tmp"
  mv -- "$state_tmp" "$swap_state"
  sync -f "$compose_root"
}

remove_swap_stage() {
  if [[ -e "$swap_state" || -L "$swap_state" ]]; then
    rm -f -- "$swap_state" || return 1
    sync -f "$compose_root" || return 1
  fi
}

database_presence() {
  local database_name="$1"
  local result
  if ! result="$(postgres_admin exists "$database_name" | tr -d '\r\n')"; then
    echo "could not query PostgreSQL database identity" >&2
    return 1
  fi
  if [[ "$result" != "t" && "$result" != "f" ]]; then
    echo "could not establish PostgreSQL database identity" >&2
    return 1
  fi
  printf '%s\n' "$result"
}

drop_database_if_exists() {
  local database_name="$1"
  local presence
  if ! presence="$(database_presence "$database_name")"; then
    return 1
  fi
  if [[ "$presence" == "t" ]]; then
    postgres_admin allow "$database_name" false >/dev/null || return 1
    postgres_admin terminate "$database_name" >/dev/null || return 1
    postgres_admin drop "$database_name" >/dev/null || return 1
  fi
}

reconcile_interrupted_swap() {
  local stage="" canonical_presence restore_presence rollback_presence

  if [[ -e "$swap_state" || -L "$swap_state" ]]; then
    if [[ -L "$swap_state" || ! -f "$swap_state" ||
          "$(stat -c '%u' "$swap_state")" != "$EUID" ||
          "$(stat -c '%a' "$swap_state")" != "600" ||
          "$(awk 'END { print NR }' "$swap_state")" != "1" ]]; then
      echo "restore swap-state marker is unsafe" >&2
      return 1
    fi
    if ! stage="$(
      jq --exit-status --raw-output \
        --arg ticket_hash "$change_ticket_hash" \
        --arg database_name "$database_name" \
        --arg restore_database "$restore_database" \
        --arg rollback_database "$rollback_database" \
        --arg archive_sha256 "$archive_sha256" \
        --arg safety_sha256 "$pre_restore_sha256" \
        'if (
           type == "object" and length == 8 and
           .version == 1 and
           .ticket_hash == $ticket_hash and
           .database_name == $database_name and
           .restore_database == $restore_database and
           .rollback_database == $rollback_database and
           .archive_sha256 == $archive_sha256 and
           .safety_sha256 == $safety_sha256 and
           (.stage == "prepared" or
            .stage == "canonical_locked" or
            .stage == "original_renamed" or
            .stage == "replacement_renamed" or
            .stage == "validated")
         ) then .stage else error("restore swap-state identity mismatch") end' \
        "$swap_state"
    )"; then
      echo "restore swap-state marker does not match this exact restore operation" >&2
      return 1
    fi
  fi

  canonical_presence="$(database_presence "$database_name")" || return 1
  restore_presence="$(database_presence "$restore_database")" || return 1
  rollback_presence="$(database_presence "$rollback_database")" || return 1

  if [[ -z "$stage" ]]; then
    if [[ "$rollback_presence" == "t" ]]; then
      echo "rollback database exists without a verified swap-state marker" >&2
      return 1
    fi
    if [[ "$canonical_presence" != "t" ]]; then
      echo "canonical PostgreSQL database is missing" >&2
      return 1
    fi
    if [[ "$restore_presence" == "t" ]]; then
      drop_database_if_exists "$restore_database" || return 1
    fi
    return 0
  fi

  if [[ "$stage" == "validated" && "$canonical_presence" == "t" ]]; then
    postgres_admin allow "$database_name" true >/dev/null || return 1
    postgres_admin probe "$database_name" >/dev/null || return 1
    if [[ "$rollback_presence" == "t" ]]; then
      drop_database_if_exists "$rollback_database" || return 1
    fi
    if [[ "$restore_presence" == "t" ]]; then
      drop_database_if_exists "$restore_database" || return 1
    fi
    remove_swap_stage || return 1
    return 0
  fi

  if [[ "$canonical_presence" != "t" ]]; then
    if [[ "$rollback_presence" != "t" ]]; then
      echo "cannot recover interrupted restore without the canonical or rollback database" >&2
      return 1
    fi
    postgres_admin rename "$rollback_database" "$database_name" >/dev/null || return 1
    postgres_admin allow "$database_name" true >/dev/null || return 1
    postgres_admin probe "$database_name" >/dev/null || return 1
    if [[ "$restore_presence" == "t" ]]; then
      drop_database_if_exists "$restore_database" || return 1
    fi
    remove_swap_stage || return 1
    return 0
  fi

  if [[ "$rollback_presence" == "t" ]]; then
    if [[ "$restore_presence" == "t" ]]; then
      echo "ambiguous interrupted restore database set" >&2
      return 1
    fi
    postgres_admin allow "$database_name" false >/dev/null || return 1
    postgres_admin terminate "$database_name" >/dev/null || return 1
    postgres_admin rename "$database_name" "$restore_database" >/dev/null || return 1
    postgres_admin rename "$rollback_database" "$database_name" >/dev/null || return 1
    postgres_admin allow "$database_name" true >/dev/null || return 1
    postgres_admin probe "$database_name" >/dev/null || return 1
    drop_database_if_exists "$restore_database" || return 1
    remove_swap_stage || return 1
    return 0
  fi

  postgres_admin allow "$database_name" true >/dev/null || return 1
  postgres_admin probe "$database_name" >/dev/null || return 1
  if [[ "$restore_presence" == "t" ]]; then
    drop_database_if_exists "$restore_database" || return 1
  fi
  remove_swap_stage || return 1
}

maintenance_marker="$(realpath -e -- "$maintenance_marker")"
compose_root="$(dirname "$(realpath -e -- "$compose_file")")"
if [[ "$maintenance_marker" != "$compose_root/.restore-maintenance" ]]; then
  echo "restore maintenance marker must be the fence beside the Compose file" >&2
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
expected_marker="funding-arbitrage-v1-restore:$change_ticket"
require_exact_line "$maintenance_marker" "$expected_marker" "restore maintenance marker"
exec 8<"$maintenance_marker"
if ! flock --nonblock 8; then
  echo "another funding restore or stopped-app backup is already running" >&2
  exit 2
fi

restore_tmpfs_dir="$(realpath -m -- "$restore_tmpfs_dir")"
if [[ "$restore_tmpfs_dir" != /dev/shm/* || -L "$restore_tmpfs_dir" ]]; then
  echo "RESTORE_TMPFS_DIR must be a real directory below /dev/shm" >&2
  exit 2
fi
install -d -m 0700 "$restore_tmpfs_dir"
restore_tmpfs_dir="$(realpath -e -- "$restore_tmpfs_dir")"
tmpfs_mode="$(stat -c '%a' "$restore_tmpfs_dir")"
tmpfs_uid="$(stat -c '%u' "$restore_tmpfs_dir")"
if [[ "$(findmnt --noheadings --output FSTYPE --target "$restore_tmpfs_dir" | tr -d ' ')" != "tmpfs" ||
      "$tmpfs_mode" != "700" || "$tmpfs_uid" != "$EUID" ]]; then
  echo "RESTORE_TMPFS_DIR must be an operator-owned mode-0700 tmpfs directory" >&2
  exit 2
fi

target_plain="$restore_tmpfs_dir/target.dump"
safety_plain="$restore_tmpfs_dir/safety.dump"
for plaintext_path in "$target_plain" "$safety_plain"; do
  if [[ -e "$plaintext_path" || -L "$plaintext_path" ]]; then
    if [[ -L "$plaintext_path" || ! -f "$plaintext_path" ||
          "$(stat -c '%u' "$plaintext_path")" != "$EUID" ||
          "$(stat -c '%a' "$plaintext_path")" != "600" ]]; then
      echo "unsafe stale restore plaintext artifact" >&2
      exit 2
    fi
    rm -f -- "$plaintext_path"
  fi
done

backup_root="$(realpath -e -- "$backup_root")"
archive="$(realpath -e -- "$archive")"
pre_restore_backup="$(realpath -e -- "$pre_restore_backup")"
identity_file="$(realpath -e -- "$identity_file")"


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

if [[ "$backup_root" == "/" || ! -f "$backup_root/.funding-backup-root" ]]; then
  echo "backup root identity marker is missing" >&2
  exit 2
fi
require_exact_line "$backup_root/.funding-backup-root" "$marker_value" "backup root identity marker"
archive_created_at=""
pre_restore_created_at=""
archive_sha256=""
pre_restore_sha256=""
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
  actual_manifest_hash="$(sha256sum "$candidate.json" | awk '{print $1}')"
  expected_completion="$(printf '%s  %s\n%s  %s' \
    "$actual_hash" "$(basename "$candidate")" \
    "$actual_manifest_hash" "$(basename "$candidate.json")")"
  if [[ "$(awk 'END { print NR }' "$candidate.complete")" != "2" ]] ||
     [[ "$(cat -- "$candidate.complete")" != "$expected_completion" ]]; then
    echo "backup completion marker validation failed: $candidate" >&2
    exit 1
  fi
  (
    cd "$backup_root"
    sha256sum --check --status "$(basename "$candidate.complete")"
  ) || {
    echo "backup set integrity validation failed: $candidate" >&2
    exit 1
  }
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
     (.alembic_head | type == "string" and test("^[A-Za-z0-9_-]{1,64}$")) and
     (.git_commit | type == "string" and test("^[0-9a-f]{40}$"))' \
    "$candidate.json" >/dev/null; then
    echo "backup manifest validation failed: $candidate" >&2
    exit 1
  fi
  created_at="$(jq --raw-output '.created_at_utc' "$candidate.json")"
  if [[ "$candidate" == "$archive" ]]; then
    archive_created_at="$created_at"
    archive_sha256="$actual_hash"
  else
    pre_restore_created_at="$created_at"
    pre_restore_sha256="$actual_hash"
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

postgres_archive_workspace_cleanup

database_name="$(
  docker compose --project-name "$project" "${compose_env_args[@]}" \
    --file "$compose_file" config --format json |
    jq -er '.services.postgres.environment.POSTGRES_DB |
      select(type == "string" and length > 0)'
)"
validate_database_name "$database_name"
if [[ "$database_name" == "postgres" || "$database_name" == "template0" ||
      "$database_name" == "template1" ]]; then
  echo "POSTGRES_DB cannot be a PostgreSQL maintenance or template database" >&2
  exit 2
fi
change_ticket_hash="$(printf '%s' "$change_ticket" | sha256sum | awk '{print $1}')"
database_suffix="${change_ticket_hash:0:12}"
restore_database="restore_${database_suffix}"
rollback_database="rollback_${database_suffix}"
validate_database_name "$restore_database"
validate_database_name "$rollback_database"
swap_state="$compose_root/.restore-swap-state"
reconcile_interrupted_swap

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

current_migration_head="$(postgres_exec psql --tuples-only --no-align \
  --command 'SELECT version_num FROM alembic_version LIMIT 1' | tr -d '\r\n')"
pre_restore_migration_head="$(jq --raw-output '.alembic_head' "$pre_restore_backup.json")"
if [[ ! "$current_migration_head" =~ ^[A-Za-z0-9_-]{1,64}$ ]] ||
   [[ "$pre_restore_migration_head" != "$current_migration_head" ]]; then
  echo "PRE_RESTORE_BACKUP does not match the current database migration head" >&2
  exit 2
fi
unexpected_current_schemas="$(database_schema_count "$database_name")"
if [[ ! "$unexpected_current_schemas" =~ ^[0-9]+$ ]] ||
   (( unexpected_current_schemas != 0 )); then
  echo "refusing restore while unsupported current application schemas exist" >&2
  exit 2
fi
target_encrypted_size_bytes="$(stat -c '%s' "$archive")"
safety_encrypted_size_bytes="$(stat -c '%s' "$pre_restore_backup")"
available_tmpfs_kib="$(df -Pk "$restore_tmpfs_dir" | awk 'END { print $4 }')"
required_tmpfs_bytes=$((target_encrypted_size_bytes + safety_encrypted_size_bytes + 10485760))
if [[ ! "$available_tmpfs_kib" =~ ^[0-9]+$ ]] ||
   (( required_tmpfs_bytes > available_tmpfs_kib * 1024 )); then
  echo "insufficient host tmpfs capacity for decrypted restore archives" >&2
  exit 2
fi
for plaintext_path in "$target_plain" "$safety_plain"; do
  (
    set -o noclobber
    umask 077
    : > "$plaintext_path"
  )
done
replacement_database_created=false

cleanup_restore() {
  local status=$?
  trap - EXIT
  set +e
  if (( status != 0 )); then
    if [[ -e "$swap_state" || -L "$swap_state" ]]; then
      if ! reconcile_interrupted_swap; then
        echo "automatic database-swap recovery is incomplete; keep the restore fence active" >&2
      fi
    elif [[ "$replacement_database_created" == "true" ]]; then
      drop_database_if_exists "$restore_database"
    fi
  fi
  rm -f -- "$target_plain" "$safety_plain"
  exit "$status"
}
trap cleanup_restore EXIT

age --decrypt --identity "$identity_file" "$archive" > "$target_plain"
age --decrypt --identity "$identity_file" "$pre_restore_backup" > "$safety_plain"
test -s "$target_plain"
test -s "$safety_plain"
target_size_bytes="$(stat -c '%s' "$target_plain")"
safety_size_bytes="$(stat -c '%s' "$safety_plain")"
postgres_archive_validate "$target_size_bytes" < "$target_plain"
postgres_archive_validate "$safety_size_bytes" < "$safety_plain"

postgres_admin create "$restore_database"
replacement_database_created=true
postgres_restore_archive "$restore_database" "$target_size_bytes" \
  --single-transaction --exit-on-error --no-owner --no-acl < "$target_plain"
run_migrations_for_database "$restore_database"

validate_restored_database "$restore_database" "$current_migration_head" "target backup"

write_swap_stage prepared
postgres_admin allow "$restore_database" false >/dev/null
postgres_admin terminate "$restore_database" >/dev/null
postgres_admin allow "$database_name" false >/dev/null
write_swap_stage canonical_locked
postgres_admin terminate "$database_name" >/dev/null
postgres_admin rename "$database_name" "$rollback_database" >/dev/null
write_swap_stage original_renamed
postgres_admin rename "$restore_database" "$database_name" >/dev/null
replacement_database_created=false
write_swap_stage replacement_renamed
postgres_admin allow "$database_name" true >/dev/null
postgres_admin probe "$database_name" >/dev/null

validate_restored_database "$database_name" "$current_migration_head" "restored database"
write_swap_stage validated

drop_database_if_exists "$rollback_database"
remove_swap_stage

echo "restore verified for ticket $change_ticket; application remains stopped and fenced"
