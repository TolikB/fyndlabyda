"""Low-cardinality operational and business metrics."""

from prometheus_client import Counter, Gauge, Histogram

api_errors_total = Counter("funding_api_errors_total", "API errors", ["path"])
api_request_latency_seconds = Histogram(
    "funding_api_request_latency_seconds", "API request latency", ["method", "path"]
)
websocket_connections = Gauge(
    "funding_websocket_connections", "Active application WebSocket clients"
)
websocket_reconnects_total = Counter(
    "funding_websocket_reconnects_total", "Exchange WebSocket reconnect attempts", ["exchange"]
)

# Market data -----------------------------------------------------------------
market_data_age_seconds = Gauge(
    "funding_market_data_age_seconds",
    "Seconds since the venue last delivered a complete snapshot (0 = current cycle)",
    ["exchange"],
)
market_venue_up = Gauge(
    "funding_market_venue_up", "1 when the venue was collected in the last cycle", ["exchange"]
)
market_data_latency_seconds = Histogram(
    "funding_market_data_latency_seconds",
    "Venue snapshot collection latency",
    ["exchange"],
    buckets=(0.25, 0.5, 1, 2, 4, 8, 16, 32),
)
market_data_errors_total = Counter(
    "funding_market_data_errors_total", "Venue snapshot failures", ["exchange", "error"]
)
orderbook_fetch_errors_total = Counter(
    "funding_orderbook_fetch_errors_total", "Order book fetch failures", ["exchange", "error"]
)

# Scanner ---------------------------------------------------------------------
opportunities_total = Gauge("funding_opportunities_total", "Current ranked opportunities")
confirmed_opportunities_total = Gauge(
    "funding_confirmed_opportunities_total", "Current confirmed opportunities"
)

# Paper runner ------------------------------------------------------------------
paper_runner_cycles_total = Counter(
    "funding_paper_runner_cycles_total", "Completed paper-test runner cycles"
)
paper_runner_errors_total = Counter(
    "funding_paper_runner_errors_total", "Paper-test runner cycle errors", ["stage"]
)
paper_runner_last_cycle_timestamp = Gauge(
    "funding_paper_runner_last_cycle_timestamp", "Unix timestamp of the last successful cycle"
)
paper_runner_cycle_seconds = Histogram(
    "funding_paper_runner_cycle_seconds",
    "Paper runner cycle duration",
    buckets=(0.5, 1, 2, 4, 8, 15, 30, 60, 120),
)
paper_persistence_failures_total = Counter(
    "funding_paper_persistence_failures_total", "Failed paper ledger commits"
)

# Paper series (label cardinality = number of configured series) ------------------
paper_equity = Gauge("funding_paper_equity", "Virtual paper equity", ["series"])
paper_cash = Gauge("funding_paper_cash", "Virtual free cash", ["series"])
paper_locked_capital = Gauge("funding_paper_locked_capital", "Collateral in positions", ["series"])
paper_pnl_total = Gauge("funding_paper_pnl_total", "Virtual paper total PnL", ["series"])
funding_pnl_total = Gauge("funding_paper_funding_pnl_total", "Settled funding PnL", ["series"])
paper_positions_open = Gauge("funding_paper_positions_open", "Open paper positions", ["series"])
paper_invariant_diff = Gauge(
    "funding_paper_invariant_diff",
    "Absolute accounting invariant difference (must stay below 0.01)",
    ["series"],
)
paper_entry_rejections_total = Counter(
    "funding_paper_entry_rejections_total", "Entries not simulated", ["series", "reason"]
)
paper_close_deferrals_total = Counter(
    "funding_paper_close_deferrals_total", "Exits postponed for lack of a fresh book", ["series"]
)
paper_funding_settlements_total = Counter(
    "funding_paper_funding_settlements_total", "Funding events settled", ["series", "source"]
)

# Notifications -------------------------------------------------------------------
telegram_messages_total = Counter(
    "funding_telegram_messages_total", "Telegram messages", ["kind", "status"]
)
