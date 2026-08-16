CREATE DATABASE IF NOT EXISTS funding_analytics;

CREATE TABLE IF NOT EXISTS funding_analytics.raw_market_events
(
    row_id String,
    event_kind LowCardinality(String),
    source LowCardinality(String),
    instrument_id String,
    sequence_id String,
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
TTL event_time + INTERVAL 180 DAY DELETE;

CREATE TABLE IF NOT EXISTS funding_analytics.feature_events
(
    row_id String,
    feature_set_version String,
    feature_name LowCardinality(String),
    instrument_id String,
    event_time DateTime64(6, 'UTC'),
    source_event_id String,
    value Float64,
    quality LowCardinality(String),
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (feature_set_version, feature_name, instrument_id, event_time, row_id)
TTL event_time + INTERVAL 365 DAY DELETE;

CREATE TABLE IF NOT EXISTS funding_analytics.signal_events
(
    row_id String,
    signal_id String,
    strategy_id LowCardinality(String),
    signal_type LowCardinality(String),
    mode LowCardinality(String),
    regime LowCardinality(String),
    instrument_id String,
    event_time DateTime64(6, 'UTC'),
    expires_at DateTime64(6, 'UTC'),
    quality_score Float64,
    confidence Float64,
    payload String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (strategy_id, signal_type, instrument_id, event_time, signal_id)
TTL event_time + INTERVAL 730 DAY DELETE;

CREATE TABLE IF NOT EXISTS funding_analytics.telemetry_events
(
    row_id String,
    service LowCardinality(String),
    metric LowCardinality(String),
    venue LowCardinality(String),
    strategy_id LowCardinality(String),
    event_time DateTime64(6, 'UTC'),
    value Float64,
    unit LowCardinality(String),
    labels String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (service, metric, venue, strategy_id, event_time, row_id)
TTL event_time + INTERVAL 90 DAY DELETE;
