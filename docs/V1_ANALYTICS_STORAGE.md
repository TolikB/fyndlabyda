# V1 analytical storage

PostgreSQL remains the authoritative append-only source for canonical events and
multi-regime decision batches. Two independent durable replicators project those
sources into ClickHouse:

- ClickHouseEventReplicator reads canonical_events by primary-key cursor;
- ClickHouseDecisionReplicator reads multi_regime_decision_batches by an
  independent primary-key cursor.

Each cursor advances only after every required ClickHouse projection acknowledges
the complete source batch. Every HTTP block carries a stable content-derived
insert deduplication token, and every non-replicated MergeTree table keeps a
100,000-block deduplication window. This gives bounded retry idempotency while a
block remains in that window. Stable row IDs plus ReplacingMergeTree provide a
second eventual cleanup layer; duplicate removal is not immediate. The checkpoint
row is locked while it is advanced, so concurrent consumers fail instead of silently skipping data. A
stored decision payload is checksum-verified before it can reach analytics.

## Tables and retention

| Table | Contents | Online TTL |
| --- | --- | --- |
| raw_market_events | complete canonical market envelopes | 180 days |
| normalized_trades | typed trade projections | 730 days |
| orderbook_deltas | native L2 updates and sequence ranges | 180 days |
| orderbook_snapshots | authoritative bounded L2 snapshots | 365 days |
| feature_snapshots | technical, orderflow, structure, derivatives | 730 days |
| regime_snapshots | deterministic regime state | 730 days |
| strategy_decisions | signals, orchestration, risk and plan attribution | 730 days |
| execution_telemetry | execution-plan and runtime telemetry | 90 days |

Large source batches are split by both row count and encoded byte size. The source
cursor is still acknowledged only after every chunk and every domain table has
succeeded. ClickHouse credentials are redacted, HTTPS with internal mTLS is
mandatory when analytics is enabled, and an unhealthy or lagging replicator makes
the runtime entry-health gate fail closed.

ClickHouse is not an accounting authority. Orders, fills, positions, risk
decisions, immutable audit records and replication checkpoints remain in
PostgreSQL.

## Existing-volume upgrade

Before enabling analytics against a ClickHouse volume created by an older release,
start only ClickHouse, execute `001_v1_analytics.sql` and then
`002_v1_typed_analytics_upgrade.sql` through the TLS ClickHouse client, and verify
all eight tables with `DESCRIBE TABLE`. Both SQL files are idempotent; the upgrade
adds `native_sequence` without dropping the legacy tables or data.

```sh
docker compose --profile analytics up -d clickhouse
docker compose --profile analytics exec -T clickhouse sh -ec '
  export SSL_CERT_FILE=/run/secrets/internal/ca.crt
  for migration in \
    /docker-entrypoint-initdb.d/001_v1_analytics.sql \
    /docker-entrypoint-initdb.d/002_v1_typed_analytics_upgrade.sql
  do
    clickhouse-client --secure --host clickhouse --port 9440 \
      --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
      --multiquery --queries-file "$migration"
  done
'
```

The market-event replicator uses the new durable consumer identity
clickhouse_market_projections_v2. Its cursor begins at zero and reprojects the
PostgreSQL canonical journal in configured bounded batches, including historical
trade and L2 rows. Do not copy the retired clickhouse_raw_market_events_v1 cursor
to the new consumer. PostgreSQL remains authoritative during this backfill, and
entry health remains closed until both replication consumers catch up.

TTL deletion and ReplacingMergeTree duplicate cleanup occur during ClickHouse
merges, so neither should be treated as an immediate accounting guarantee.
