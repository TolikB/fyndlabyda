# Funding Arbitrage Research Bot

Read-only, market-neutral funding/basis research system. It normalizes public
market data from Bybit, Gate.io, OKX, Binance, and Hyperliquid; ranks funding,
spot/perpetual, perp/perp, and dated-futures opportunities; and keeps all
execution strictly in `PaperTradingExecutor`. No API keys or live orders are
used in v1.

## Local setup

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,analytics]"
copy .env.example .env
alembic upgrade head
```

Safe commands:

```powershell
funding-arbitrage api       # FastAPI on :8000
funding-arbitrage collect   # one public REST collection and DB write
funding-arbitrage scan      # collection, ranking, and opportunity history write
funding-arbitrage paper     # virtual balances only; no trading
funding-arbitrage backtest --monthly-pnl monthly.json
```

The API exposes `/health`, `/exchanges`, `/scan`, `/opportunities`,
`/portfolio`, `/positions`, `/analytics/*` (including durable `/analytics/paper`), `/backtests`, `/metrics`, and the
read-only dashboard at `/dashboard/`. WebSocket feeds are available at
`/ws/opportunities`, `/ws/portfolio`, and `/ws/market`.

`monthly.json` is a simple object such as
`{"2026-01": "125.50", "2026-02": "-30"}`. Backtests are event-driven and
report net profit after fees, funding income, drawdown, volatility, Sharpe-like
score, win rate, profit factor, utilization, and monthly percentiles.

## Safety and data assumptions

The adapters use public REST for initial snapshots, history, and recovery, and
provide reconnecting public ticker streams. Orderbook depth is used for paper
slippage estimates when supplied. Missing historical depth is treated
conservatively as unavailable rather than inferred. Funding is normalized to
daily and annualized comparison metrics; realized paper PnL is settled from
timestamped funding events.

## Docker

For the real public-market-data paper deployment:

```powershell
copy .env.paper-live-data.example .env
docker compose up -d --build
```

This starts the production-shaped paper-test deployment: real public market
data from Bybit, Gate, OKX, Binance, and Hyperliquid, automatic paper
execution, funding settlement, PostgreSQL persistence, Prometheus, and
Grafana. It never sends exchange orders. Use `.env.paper-test.example` only
for the fully offline deterministic mock profile.
