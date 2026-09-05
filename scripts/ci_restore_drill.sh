#!/usr/bin/env bash
set -euo pipefail

readonly project="funding_arbitrage_v1"
readonly ticket="CI-RESTORE-DRILL"
readonly stopped_backup_confirmation="BACKUP_FUNDING_V1_POSTGRES_WHILE_APP_STOPPED_AND_FENCED"
readonly restore_confirmation="RESTORE_FUNDING_V1_POSTGRES_AND_KEEP_APP_STOPPED"

artifact_dir="${1:-}"
image_ref="${2:-}"
expected_revision="${3:-}"
evidence_dir="${4:-}"
if [[ -z "$artifact_dir" || -z "$image_ref" || -z "$evidence_dir" ||
      ! "$expected_revision" =~ ^[0-9a-f]{40}$ ]]; then
  echo "usage: ci_restore_drill.sh <artifact-dir> <candidate-image> <revision> <evidence-dir>" >&2
  exit 2
fi
for command_name in age age-keygen date docker find jq python realpath sha256sum stat; do
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
evidence_dir="$(realpath -e -- "$evidence_dir")"
if [[ "$evidence_dir" == "$runner_temp" || "$evidence_dir" != "$runner_temp/"* ||
      -L "$evidence_dir" || "$(stat -c '%u:%a' "$evidence_dir")" != "$EUID:700" ]]; then
  echo "restore evidence directory must be an operator-owned mode-0700 runner directory" >&2
  exit 2
fi
evidence="$evidence_dir/funding-disaster-recovery.json"
evidence_checksum="$evidence.sha256"
if [[ -e "$evidence" || -L "$evidence" ||
      -e "$evidence_checksum" || -L "$evidence_checksum" ]]; then
  echo "restore drill refuses existing evidence output" >&2
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
  printf 'APP_IMAGE=%s\n' "$app_image"
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
drill_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

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
assert_profile_boundary_immutable() {
  local output status
  set +e
  output="$(pg_exec funding '
    UPDATE canonical_journal_profiles
    SET profile = $$disabled$$
    WHERE boundary_id = $$ci-restore-profile$$
  ' 2>&1)"
  status=$?
  set -e
  test "$status" -ne 0
  grep -Fq "is append-only" <<<"$output"
}
critical_state_sha256() {
  local payload
  payload="$(pg_scalar funding '
    SELECT jsonb_agg(
      jsonb_build_object(
        $$entity$$, entity,
        $$key$$, record_key,
        $$value$$, record_value
      ) ORDER BY entity, record_key
    )::text
    FROM (
      SELECT $$paper_position$$ AS entity, position_id AS record_key,
             to_jsonb(paper_positions)::text AS record_value
      FROM paper_positions WHERE position_id = $$ci-restore-paper-position$$
      UNION ALL
      SELECT $$paper_fill$$, fill_id, to_jsonb(paper_fills)::text
      FROM paper_fills WHERE fill_id = $$ci-restore-paper-fill$$
      UNION ALL
      SELECT $$portfolio$$, simulation_version, to_jsonb(portfolio_snapshots)::text
      FROM portfolio_snapshots WHERE simulation_version = $$ci-restore$$
      UNION ALL
      SELECT $$risk_decision$$, decision_id, to_jsonb(risk_decisions)::text
      FROM risk_decisions WHERE decision_id = $$ci-restore-risk$$
      UNION ALL
      SELECT $$oms_order$$, client_order_id, to_jsonb(oms_order_states)::text
      FROM oms_order_states WHERE client_order_id = $$ci-restore-order$$
      UNION ALL
      SELECT $$execution_fill$$, fill_id, to_jsonb(execution_fills)::text
      FROM execution_fills WHERE fill_id = $$ci-restore-execution-fill$$
      UNION ALL
      SELECT $$position$$, position_id, to_jsonb(position_states)::text
      FROM position_states WHERE position_id = $$ci-restore-position$$
      UNION ALL
      SELECT $$balance$$, venue || $$:$$ || asset, to_jsonb(balance_states)::text
      FROM balance_states WHERE venue = $$CI$$ AND asset = $$USDT$$
      UNION ALL
      SELECT $$ledger_transaction$$, transaction_id,
             to_jsonb(ledger_transactions)::text
      FROM ledger_transactions WHERE transaction_id = $$ci-restore-ledger$$
      UNION ALL
      SELECT $$ledger_postings$$, transaction_id,
             jsonb_agg(to_jsonb(ledger_postings) ORDER BY posting_index)::text
      FROM ledger_postings WHERE transaction_id = $$ci-restore-ledger$$
      GROUP BY transaction_id
      UNION ALL
      SELECT $$reconciliation$$, run_id, to_jsonb(reconciliation_audits)::text
      FROM reconciliation_audits WHERE run_id = $$ci-restore-reconciliation$$
      UNION ALL
      SELECT $$daily_report$$, report_date::text,
             to_jsonb(telegram_daily_reports)::text
      FROM telegram_daily_reports WHERE report_date = $$2026-01-01$$::date
      UNION ALL
      SELECT $$audit$$, audit_event_id, to_jsonb(immutable_audit_log)::text
      FROM immutable_audit_log WHERE audit_event_id = $$ci-restore-audit$$
      UNION ALL
      SELECT $$idempotency$$, principal_id || $$:$$ || idempotency_key,
             to_jsonb(api_idempotency_records)::text
      FROM api_idempotency_records
      WHERE principal_id = $$ci-restore$$ AND idempotency_key = $$ci-restore-key$$
      UNION ALL
      SELECT $$journal_profile$$, boundary_id,
             to_jsonb(canonical_journal_profiles)::text
      FROM canonical_journal_profiles
      WHERE boundary_id = $$ci-restore-profile$$
    ) AS critical_state
  ')" || return 1
  if [[ -z "$payload" || "$payload" == "null" ]]; then
    echo "critical restore-state payload is empty" >&2
    return 1
  fi
  if [[ "$(jq --raw-output 'length' <<<"$payload")" != "15" ]]; then
    echo "critical restore-state entity coverage is incomplete" >&2
    return 1
  fi
  printf '%s' "$payload" | sha256sum | awk '{print $1}'
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
  );
  INSERT INTO canonical_journal_profiles(
    boundary_id, started_at, after_event_row_id, profile,
    high_frequency_events_enabled, minimum_interval_seconds,
    simulation_versions, config_sha256
  ) VALUES (
    $$ci-restore-profile$$,
    $$2026-01-01T00:00:00.002Z$$::timestamptz,
    (SELECT id FROM canonical_events WHERE event_id = $$ci-restore-target$$),
    $$full$$, true, $$0$$, json_build_array($$ci-restore$$),
    repeat($$c$$, 64)
  );
  INSERT INTO paper_positions(
    position_id, opportunity_id, state, asset, capital, simulation_version,
    opened_at, closed_at, payload
  ) VALUES (
    $$ci-restore-paper-position$$, NULL, $$OPEN$$, $$BTC$$, 17.25,
    $$ci-restore$$, $$2026-01-01T00:00:00Z$$::timestamptz, NULL,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO paper_fills(
    fill_id, position_id, exchange, symbol, instrument_type, side,
    filled_quantity, price, fee, slippage, status, timestamp, payload
  ) VALUES (
    $$ci-restore-paper-fill$$, $$ci-restore-paper-position$$, $$bybit$$,
    $$BTC/USDT$$, $$perpetual$$, $$BUY$$, 0.01, 1725, 0.10, 0.05,
    $$FILLED$$, $$2026-01-01T00:00:00Z$$::timestamptz,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO portfolio_snapshots(
    timestamp, simulation_version, snapshot_scope, equity, cash,
    locked_capital, total_pnl, funding_pnl, fees, balances
  ) VALUES (
    $$2026-01-01T00:00:00Z$$::timestamptz, $$ci-restore$$, $$combined$$,
    1001.25, 984.00, 17.25, 1.25, 1.40, 0.15,
    json_build_object($$CI$$, json_build_object($$USDT$$, $$1001.25$$))
  );
  INSERT INTO risk_decisions(
    decision_id, signal_id, approved, rejection_reason, approved_risk_usdt,
    approved_quantity, approved_notional, decided_at, payload
  ) VALUES (
    $$ci-restore-risk$$, $$ci-restore-signal$$, true, NULL, 17.25,
    0.01, 17.25, $$2026-01-01T00:00:00Z$$::timestamptz,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO oms_order_states(
    client_order_id, simulation_version, exchange_order_id, risk_decision_id,
    signal_id, venue, instrument_id, side, order_type, status,
    requested_quantity, filled_quantity, limit_price, reduce_only, version,
    created_at, updated_at, payload
  ) VALUES (
    $$ci-restore-order$$, $$ci-restore$$, $$ci-exchange-order$$,
    $$ci-restore-risk$$, $$ci-restore-signal$$, $$CI$$,
    $$CI:PERP:BTC/USDT$$, $$BUY$$, $$LIMIT$$, $$FILLED$$,
    0.01, 0.01, 1725, false, 1,
    $$2026-01-01T00:00:00Z$$::timestamptz,
    $$2026-01-01T00:00:01Z$$::timestamptz,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO execution_fills(
    fill_id, simulation_version, client_order_id, exchange_order_id, venue,
    instrument_id, side, price, quantity, fee_amount, fee_asset,
    liquidity_role, exchange_timestamp, receive_timestamp, payload
  ) VALUES (
    $$ci-restore-execution-fill$$, $$ci-restore$$, $$ci-restore-order$$,
    $$ci-exchange-order$$, $$CI$$, $$CI:PERP:BTC/USDT$$, $$BUY$$,
    1725, 0.01, 0.10, $$USDT$$, $$TAKER$$,
    $$2026-01-01T00:00:01Z$$::timestamptz,
    $$2026-01-01T00:00:01.001Z$$::timestamptz,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO position_states(
    position_id, simulation_version, strategy_id, venue, instrument_id,
    status, signed_quantity, entry_price, mark_price, realized_pnl,
    unrealized_pnl, collateral, opened_at, closed_at, updated_at, payload
  ) VALUES (
    $$ci-restore-position$$, $$ci-restore$$, $$ci-strategy$$, $$CI$$,
    $$CI:PERP:BTC/USDT$$, $$OPEN$$, 0.01, 1725, 1730, 0, 0.05,
    17.25, $$2026-01-01T00:00:01Z$$::timestamptz, NULL,
    $$2026-01-01T00:00:02Z$$::timestamptz,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO balance_states(
    venue, asset, total, available, locked, borrowed, observed_at, payload
  ) VALUES (
    $$CI$$, $$USDT$$, 1001.25, 984.00, 17.25, 0,
    $$2026-01-01T00:00:02Z$$::timestamptz,
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO ledger_transactions(
    sequence, transaction_id, timestamp, reference_type, reference_id,
    description, previous_hash, transaction_hash, payload
  ) VALUES (
    1, $$ci-restore-ledger$$, $$2026-01-01T00:00:02Z$$::timestamptz,
    $$ci_restore$$, $$ci-restore-order$$, $$CI restore target$$,
    repeat($$0$$, 64), repeat($$5$$, 64),
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO ledger_postings(
    transaction_id, posting_index, account, account_kind, asset, amount,
    venue, strategy_id, position_id
  ) VALUES
    ($$ci-restore-ledger$$, 0, $$cash:ci:usdt$$, $$asset$$, $$USDT$$,
     -17.25, $$CI$$, $$ci-strategy$$, $$ci-restore-position$$),
    ($$ci-restore-ledger$$, 1, $$collateral:ci:usdt$$, $$asset$$, $$USDT$$,
     17.25, $$CI$$, $$ci-strategy$$, $$ci-restore-position$$);
  INSERT INTO reconciliation_audits(
    sequence, run_id, timestamp, passed, critical_count, warning_count,
    input_hash, previous_hash, audit_hash, issues
  ) VALUES (
    1, $$ci-restore-reconciliation$$,
    $$2026-01-01T00:00:03Z$$::timestamptz, true, 0, 0,
    repeat($$3$$, 64), repeat($$0$$, 64), repeat($$4$$, 64),
    json_build_array()
  );
  INSERT INTO telegram_daily_reports(
    report_date, status, sent_at, message, error
  ) VALUES (
    $$2026-01-01$$::date, $$sent$$,
    $$2026-01-02T00:00:00Z$$::timestamptz,
    $$CI target report$$, NULL
  );
  INSERT INTO immutable_audit_log(
    sequence, audit_event_id, timestamp, actor_id, actor_role, action,
    resource_type, resource_id, idempotency_key, outcome, payload_hash,
    previous_hash, audit_hash, payload
  ) VALUES (
    1, $$ci-restore-audit$$, $$2026-01-01T00:00:03Z$$::timestamptz,
    $$ci$$, $$administrator$$, $$restore_test$$, $$database$$,
    $$funding$$, NULL, $$success$$, repeat($$1$$, 64),
    repeat($$0$$, 64), repeat($$2$$, 64),
    json_build_object($$marker$$, $$target$$)
  );
  INSERT INTO api_idempotency_records(
    principal_id, idempotency_key, request_hash, state, status_code,
    response_body, response_headers, created_at, updated_at, expires_at
  ) VALUES (
    $$ci-restore$$, $$ci-restore-key$$, repeat($$7$$, 64), $$completed$$,
    200, NULL, json_build_object($$content-type$$, $$application/json$$),
    $$2026-01-01T00:00:03Z$$::timestamptz,
    $$2026-01-01T00:00:03Z$$::timestamptz,
    $$2026-01-02T00:00:03Z$$::timestamptz
  );'
target_event_count_in_backup="$(
  pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE source = $$ci_restore$$'
)"
test "$target_event_count_in_backup" = "1"
test "$(pg_scalar funding 'SELECT id || $$|$$ || marker FROM restore_exactness_sentinel')" = "1|target-row"
assert_profile_boundary_immutable
target_critical_state_sha256="$(critical_state_sha256)"
[[ "$target_critical_state_sha256" =~ ^[0-9a-f]{64}$ ]]

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
  );
  UPDATE position_states
  SET mark_price = 9999, updated_at = $$2026-01-01T00:00:04Z$$::timestamptz
  WHERE position_id = $$ci-restore-position$$;
  UPDATE balance_states
  SET total = 9999, available = 9981.75,
      observed_at = $$2026-01-01T00:00:04Z$$::timestamptz
  WHERE venue = $$CI$$ AND asset = $$USDT$$;
  UPDATE telegram_daily_reports
  SET message = $$post-target-report$$
  WHERE report_date = $$2026-01-01$$::date;'
source_event_count_before_restore="$(
  pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE source = $$ci_restore$$'
)"
test "$source_event_count_before_restore" = "2"
test "$(pg_scalar funding 'SELECT COUNT(*) FROM restore_exactness_sentinel')" = "2"
post_target_critical_state_sha256="$(critical_state_sha256)"
[[ "$post_target_critical_state_sha256" =~ ^[0-9a-f]{64}$ ]]
test "$post_target_critical_state_sha256" != "$target_critical_state_sha256"
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

database_restore_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
run_restore "$ticket"
database_restore_completed_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
restored_target_event_count="$(
  pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE source = $$ci_restore$$'
)"
restored_target_marker="$(
  pg_scalar funding 'SELECT payload->>$$marker$$ FROM canonical_events WHERE event_id = $$ci-restore-target$$'
)"
restored_post_target_event_count="$(
  pg_scalar funding 'SELECT COUNT(*) FROM canonical_events WHERE event_id = $$ci-restore-post-target$$'
)"
restored_sentinel="$(
  pg_scalar funding 'SELECT id || $$|$$ || marker FROM restore_exactness_sentinel'
)"
restored_alembic_head="$(pg_scalar funding 'SELECT version_num FROM alembic_version')"
restored_critical_state_sha256="$(critical_state_sha256)"
orphan_restore_database_count="$(
  pg_scalar postgres "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'restore_%' OR datname LIKE 'rollback_%'"
)"
test "$restored_target_event_count" = "1"
test "$restored_target_marker" = "target"
test "$restored_post_target_event_count" = "0"
test "$restored_sentinel" = "1|target-row"
test "$restored_alembic_head" = "0018_journal_profiles"
test "$restored_critical_state_sha256" = "$target_critical_state_sha256"
assert_profile_boundary_immutable
test "$orphan_restore_database_count" = "0"
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

app_restore_state="$(
  docker inspect "$app_container_id" \
    --format '{{.State.Running}}|{{.HostConfig.RestartPolicy.Name}}|{{.RestartCount}}'
)"
test "$app_restore_state" = "false|no|0"
host_plaintext_artifact="$(
  find "$host_tmpfs" -maxdepth 1 -type f -print -quit 2>/dev/null || true
)"
database_plaintext_artifact="$(
  "${compose[@]}" exec -T postgres \
    find /dev/shm/funding-arbitrage-v1-restore -maxdepth 1 -type f -print -quit \
    2>/dev/null || true
)"
test -z "$host_plaintext_artifact"
test -z "$database_plaintext_artifact"

candidate_image_id="$(tr -d '\r\n' < "$artifact_dir/funding-candidate-image.id")"
test "$candidate_image_id" = "$(docker image inspect --format '{{.Id}}' "$image_ref")"
target_created_compact="$(jq --raw-output '.created_at_utc' "$target.json")"
safety_created_compact="$(jq --raw-output '.created_at_utc' "$safety.json")"
for backup_timestamp in "$target_created_compact" "$safety_created_compact"; do
  [[ "$backup_timestamp" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
done
target_created_at="${target_created_compact:0:4}-${target_created_compact:4:2}-${target_created_compact:6:2}T${target_created_compact:9:2}:${target_created_compact:11:2}:${target_created_compact:13:2}Z"
safety_created_at="${safety_created_compact:0:4}-${safety_created_compact:4:2}-${safety_created_compact:6:2}T${safety_created_compact:9:2}:${safety_created_compact:11:2}:${safety_created_compact:13:2}Z"
target_manifest_sha256="$(sha256sum "$target.json" | awk '{print $1}')"
target_completion_sha256="$(sha256sum "$target.complete" | awk '{print $1}')"
safety_manifest_sha256="$(sha256sum "$safety.json" | awk '{print $1}')"
safety_completion_sha256="$(sha256sum "$safety.complete" | awk '{print $1}')"
target_size_bytes="$(stat -c '%s' "$target")"
safety_size_bytes="$(stat -c '%s' "$safety")"
target_revision="$(jq --raw-output '.git_commit' "$target.json")"
safety_revision="$(jq --raw-output '.git_commit' "$safety.json")"
target_alembic_head="$(jq --raw-output '.alembic_head' "$target.json")"
safety_alembic_head="$(jq --raw-output '.alembic_head' "$safety.json")"
test "$target_revision" = "$expected_revision"
test "$safety_revision" = "$expected_revision"
test "$target_alembic_head" = "$restored_alembic_head"
test "$safety_alembic_head" = "$restored_alembic_head"
drill_completed_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"

facts="$work_root/disaster-recovery-facts.json"
umask 077
jq --null-input --compact-output \
  --arg drill_started_at "$drill_started_at" \
  --arg database_restore_started_at "$database_restore_started_at" \
  --arg database_restore_completed_at "$database_restore_completed_at" \
  --arg drill_completed_at "$drill_completed_at" \
  --arg target_archive_sha256 "$archive_sha256" \
  --arg target_manifest_sha256 "$target_manifest_sha256" \
  --arg target_completion_sha256 "$target_completion_sha256" \
  --argjson target_size_bytes "$target_size_bytes" \
  --arg target_created_at "$target_created_at" \
  --arg target_revision "$target_revision" \
  --arg target_alembic_head "$target_alembic_head" \
  --arg safety_archive_sha256 "$safety_sha256" \
  --arg safety_manifest_sha256 "$safety_manifest_sha256" \
  --arg safety_completion_sha256 "$safety_completion_sha256" \
  --argjson safety_size_bytes "$safety_size_bytes" \
  --arg safety_created_at "$safety_created_at" \
  --arg safety_revision "$safety_revision" \
  --arg safety_alembic_head "$safety_alembic_head" \
  --argjson source_event_count_before_restore "$source_event_count_before_restore" \
  --argjson target_event_count_in_backup "$target_event_count_in_backup" \
  --argjson restored_target_event_count "$restored_target_event_count" \
  --argjson restored_post_target_event_count "$restored_post_target_event_count" \
  --arg restored_target_marker "$restored_target_marker" \
  --arg restored_sentinel "$restored_sentinel" \
  --arg restored_alembic_head "$restored_alembic_head" \
  --arg target_critical_state_sha256 "$target_critical_state_sha256" \
  --arg post_target_critical_state_sha256 "$post_target_critical_state_sha256" \
  --arg restored_critical_state_sha256 "$restored_critical_state_sha256" \
  --argjson orphan_restore_database_count "$orphan_restore_database_count" \
  '{
    document_kind: "disaster-recovery-drill-facts",
    schema_version: 1,
    drill_started_at: $drill_started_at,
    database_restore_started_at: $database_restore_started_at,
    database_restore_completed_at: $database_restore_completed_at,
    drill_completed_at: $drill_completed_at,
    target_backup: {
      role: "target",
      archive_sha256: $target_archive_sha256,
      manifest_sha256: $target_manifest_sha256,
      completion_sha256: $target_completion_sha256,
      encrypted_size_bytes: $target_size_bytes,
      created_at: $target_created_at,
      code_revision: $target_revision,
      alembic_head: $target_alembic_head,
      compose_project: "funding_arbitrage_v1",
      encrypted: true
    },
    pre_restore_backup: {
      role: "pre_restore",
      archive_sha256: $safety_archive_sha256,
      manifest_sha256: $safety_manifest_sha256,
      completion_sha256: $safety_completion_sha256,
      encrypted_size_bytes: $safety_size_bytes,
      created_at: $safety_created_at,
      code_revision: $safety_revision,
      alembic_head: $safety_alembic_head,
      compose_project: "funding_arbitrage_v1",
      encrypted: true
    },
    source_event_count_before_restore: $source_event_count_before_restore,
    target_event_count_in_backup: $target_event_count_in_backup,
    restored_target_event_count: $restored_target_event_count,
    restored_post_target_event_count: $restored_post_target_event_count,
    restored_target_marker: $restored_target_marker,
    restored_sentinel: $restored_sentinel,
    restored_alembic_head: $restored_alembic_head,
    critical_state_entity_count: 14,
    target_critical_state_sha256: $target_critical_state_sha256,
    post_target_critical_state_sha256: $post_target_critical_state_sha256,
    restored_critical_state_sha256: $restored_critical_state_sha256,
    orphan_restore_database_count: $orphan_restore_database_count,
    recovered_crash_stages: [
      "prepared",
      "canonical_locked",
      "original_renamed",
      "replacement_renamed",
      "validated"
    ],
    wrong_ticket_rejected: true,
    target_catalog_verified: true,
    safety_catalog_verified: true,
    restored_schema_verified: true,
    critical_tables_verified: true,
    app_running_during_restore: false,
    app_restart_policy: "no",
    app_restart_count: 0,
    host_plaintext_artifact_count: 0,
    database_plaintext_artifact_count: 0
  }' > "$facts"
chmod 0600 "$facts"

PYTHONPATH="$repo_root/src" python "$repo_root/scripts/disaster_recovery_evidence.py" \
  seal \
  --facts "$facts" \
  --output "$evidence" \
  --revision "$expected_revision" \
  --image-id "$candidate_image_id" \
  --github-run-id "$GITHUB_RUN_ID" \
  --github-run-attempt "$GITHUB_RUN_ATTEMPT"
test -s "$evidence"
test -s "$evidence_checksum"
echo "CI restore drill passed with release-bound evidence: $evidence"
