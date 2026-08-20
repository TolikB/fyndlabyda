-- Idempotent upgrade for ClickHouse volumes created before typed V1 analytics.
-- Run 001 first so every target table exists, then apply these in-place changes.
ALTER TABLE funding_analytics.raw_market_events
    ADD COLUMN IF NOT EXISTS native_sequence Nullable(Int64) AFTER sequence_id;

ALTER TABLE funding_analytics.raw_market_events
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.normalized_trades
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.orderbook_deltas
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.orderbook_snapshots
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.feature_snapshots
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.regime_snapshots
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.strategy_decisions
    MODIFY SETTING non_replicated_deduplication_window = 100000;
ALTER TABLE funding_analytics.execution_telemetry
    MODIFY SETTING non_replicated_deduplication_window = 100000;
