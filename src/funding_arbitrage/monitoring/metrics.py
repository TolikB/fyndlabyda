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
market_data_age_seconds = Gauge(
    "funding_market_data_age_seconds", "Age of the latest normalized market snapshot", ["exchange"]
)
market_tickers_usable = Gauge(
    "funding_market_tickers_usable", "Usable normalized tickers", ["exchange"]
)
orderbook_coverage_ratio = Gauge(
    "funding_orderbook_coverage_ratio", "Requested typed books successfully loaded", ["exchange"]
)
stale_or_missing_orderbooks = Gauge(
    "funding_stale_or_missing_orderbooks",
    "Requested order books unavailable or outside the freshness threshold",
    ["exchange"],
)
funding_history_coverage_ratio = Gauge(
    "funding_history_coverage_ratio",
    "Requested funding histories successfully loaded",
    ["exchange"],
)
market_data_dropped_total = Counter(
    "funding_market_data_dropped_total", "Rejected market-data records", ["exchange", "reason"]
)
opportunities_total = Gauge("funding_opportunities_total", "Current ranked opportunities")
confirmed_opportunities_total = Gauge(
    "funding_confirmed_opportunities_total", "Current confirmed opportunities"
)
opportunity_candidates = Gauge(
    "funding_opportunity_candidates", "Current raw scanner candidates", ["strategy"]
)
opportunity_filter_rejections = Gauge(
    "funding_opportunity_filter_rejections",
    "Current scanner candidates rejected by each filter",
    ["reason"],
)
opportunity_coverage_ratio = Gauge(
    "funding_opportunity_coverage_ratio",
    "Eligible opportunities divided by raw scanner candidates",
)
paper_positions_open = Gauge("funding_paper_positions_open", "Open paper positions")
paper_equity = Gauge("funding_paper_equity", "Virtual paper portfolio equity")
paper_pnl_total = Gauge("funding_paper_pnl_total", "Virtual paper total PnL")
funding_pnl_total = Gauge("funding_paper_funding_pnl_total", "Virtual paper funding PnL")
paper_runner_cycles_total = Counter(
    "funding_paper_runner_cycles_total", "Completed paper-test runner cycles"
)
paper_runner_errors_total = Counter(
    "funding_paper_runner_errors_total", "Paper-test runner cycle errors"
)
paper_runner_cycle_duration_seconds = Histogram(
    "funding_paper_runner_cycle_duration_seconds", "Paper runner cycle duration"
)
paper_runner_stage_duration_seconds = Histogram(
    "funding_paper_runner_stage_duration_seconds",
    "Paper runner stage duration",
    ["stage"],
)
paper_trade_rejections_total = Counter(
    "funding_paper_trade_rejections_total", "Paper trade rejections", ["reason"]
)
paper_runner_last_cycle_timestamp = Gauge(
    "funding_paper_runner_last_cycle_timestamp", "Unix timestamp of the last paper cycle"
)
