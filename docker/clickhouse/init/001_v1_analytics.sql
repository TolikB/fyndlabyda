CREATE DATABASE IF NOT EXISTS funding_analytics;

CREATE TABLE IF NOT EXISTS funding_analytics.raw_market_events
(
    row_id String,
    event_kind LowCardinality(String),
    source LowCardinality(String),
    instrument_id String,
    sequence_id String,
    native_sequence Nullable(Int64),
    correlation_id String,
    quality LowCardinality(String),
    event_time DateTime64(6, 'UTC'),
    receive_time DateTime64(6, 'UTC'),
    monotonic_ns UInt64,
    payload_version UInt16,
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source, instrument_id, event_time, sequence_id, row_id)
TTL event_time + INTERVAL 180 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.normalized_trades
(
    row_id String,
    source LowCardinality(String),
    instrument_id String,
    quality LowCardinality(String),
    event_time DateTime64(6, 'UTC'),
    receive_time DateTime64(6, 'UTC'),
    trade_id String,
    price Decimal(38, 18),
    quantity Decimal(38, 18),
    aggressor_side LowCardinality(String),
    sequence_id String,
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source, instrument_id, event_time, trade_id, row_id)
TTL event_time + INTERVAL 730 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.orderbook_deltas
(
    row_id String,
    source LowCardinality(String),
    instrument_id String,
    quality LowCardinality(String),
    event_time DateTime64(6, 'UTC'),
    receive_time DateTime64(6, 'UTC'),
    first_sequence UInt64,
    last_sequence UInt64,
    previous_sequence Nullable(UInt64),
    checksum Nullable(String),
    update_count UInt32,
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source, instrument_id, event_time, last_sequence, row_id)
TTL event_time + INTERVAL 180 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.orderbook_snapshots
(
    row_id String,
    source LowCardinality(String),
    instrument_id String,
    quality LowCardinality(String),
    event_time DateTime64(6, 'UTC'),
    receive_time DateTime64(6, 'UTC'),
    sequence UInt64,
    checksum Nullable(String),
    bid_count UInt32,
    ask_count UInt32,
    best_bid Nullable(Decimal(38, 18)),
    best_ask Nullable(Decimal(38, 18)),
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source, instrument_id, event_time, sequence, row_id)
TTL event_time + INTERVAL 365 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.feature_snapshots
(
    row_id String,
    batch_id String,
    feature_set_version String,
    feature_name LowCardinality(String),
    instrument_id String,
    event_time DateTime64(6, 'UTC'),
    source_event_id String,
    value Float64,
    quality LowCardinality(String),
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (feature_set_version, feature_name, instrument_id, event_time, row_id)
TTL event_time + INTERVAL 730 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.regime_snapshots
(
    row_id String,
    batch_id String,
    source_event_id String,
    instrument_id String,
    event_time DateTime64(6, 'UTC'),
    regime LowCardinality(String),
    candidate LowCardinality(String),
    confidence Float64,
    quality LowCardinality(String),
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (instrument_id, event_time, batch_id, row_id)
TTL event_time + INTERVAL 730 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.strategy_decisions
(
    row_id String,
    batch_id String,
    source_event_id String,
    signal_id String,
    strategy_id LowCardinality(String),
    mode LowCardinality(String),
    regime LowCardinality(String),
    instrument_id String,
    event_time DateTime64(6, 'UTC'),
    expires_at Nullable(DateTime64(6, 'UTC')),
    decision_count UInt16,
    active_signal_count UInt16,
    approved_risk_count UInt16,
    execution_plan_count UInt16,
    quality_score Float64,
    confidence Float64,
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (strategy_id, instrument_id, event_time, row_id)
TTL event_time + INTERVAL 730 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS funding_analytics.execution_telemetry
(
    row_id String,
    source_event_id String,
    instrument_id String,
    service LowCardinality(String),
    metric LowCardinality(String),
    venue LowCardinality(String),
    strategy_id LowCardinality(String),
    event_time DateTime64(6, 'UTC'),
    value Float64,
    unit LowCardinality(String),
    labels String,
    payload_hash FixedString(64),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (service, metric, venue, strategy_id, event_time, row_id)
TTL event_time + INTERVAL 90 DAY DELETE
SETTINGS non_replicated_deduplication_window = 100000;
