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
opportunities_total = Gauge("funding_opportunities_total", "Current ranked opportunities")
confirmed_opportunities_total = Gauge(
    "funding_confirmed_opportunities_total", "Current confirmed opportunities"
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
paper_runner_last_cycle_timestamp = Gauge(
    "funding_paper_runner_last_cycle_timestamp", "Unix timestamp of the last paper cycle"
)
