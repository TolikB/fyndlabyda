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
reconnecting public ticker and typed spot/perpetual order-book streams as the
primary incremental source. Orderbook depth is used for paper
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
Grafana support. The default lightweight profile starts only app, PostgreSQL,
and Redis; add `--profile observability` when the host has enough resources for
Prometheus and Grafana. It never sends exchange orders. Use
`.env.paper-test.example` only for the fully offline deterministic mock profile.
The Docker base image is digest-pinned and `requirements.lock` fixes the exact
tested runtime dependency graph so a rebuild cannot silently change the
candidate or baseline environment.

The live-data paper profile persists full market snapshots every five minutes
to keep PostgreSQL growth bounded. Diagnose current public-market candidates
without writing data or placing orders with:

```powershell
docker compose exec app python scripts/paper_scan_probe.py
```

Port 8000 is bound to VM localhost by default. Reach the dashboard safely with
an SSH tunnel such as `ssh -L 8000:127.0.0.1:8000 user@vm`, then open
`http://127.0.0.1:8000/dashboard/` locally.

## PnL-correct comparison mode

The clean shared-feed OOS candidate is versioned as `v22-oos-candidate`. It requires typed,
fresh order books for both legs, closes the exact opened quantity, applies the
taker fee of each venue, and separates legacy results from the current equity
curve. A deterministic adverse second-leg move is charged in signal sizing,
paper fills, PnL, and replay attribution through `PAPER_LEGGING_MOVE_PERCENT`.
Candidate positions are fill-or-kill at entry and close after their target settlement unless
the next exact venue settlement is projected to cover exit, re-entry, legging, and incremental
borrow costs. Missing or shallow close books persist a restart-safe exit request until both
legs can be neutralized; the executor never invents a fill while liquidity is unavailable.
Ticker and typed spot/perpetual order-book WebSockets are the primary
incremental source; REST is used for initial snapshots, periodic validation,
recovery, and funding history.

Run the corrected fixed-size baseline beside the risk-adjusted candidate using
one shared market-data feed:

The candidate enforces separate opportunity, asset, exchange, strategy, cash
reserve, and configured correlated-asset exposure limits. Correlation groups
are supplied through `PAPER_CORRELATION_GROUPS`; the baseline deliberately
retains fixed sizing as the unchanged control portfolio.

```bash
PAPER_COMPARISON_ENABLED=true docker compose up -d --build
```

The baseline has no separate container or published port and Telegram is
disabled for it. Both
profiles share PostgreSQL but restore and report only their own
`simulation_version`. Compare them with:

```text
GET  /analytics/compare
GET  /analytics/attribution?simulation_version=v22-oos-candidate
POST /backtests/replay-paper
POST /backtests/compare-market
POST /backtests/compare-market/jobs
GET  /backtests/compare-market/jobs/{job_id}
```

`/analytics/compare` keeps `accepted=false` until at least 30 days of evidence
exist and the candidate beats baseline net PnL by 10%, improves median monthly
PnL, does not increase drawdown, and is profitable in two of three windows.
Paper-cycle failures are stored as redacted, version-scoped PostgreSQL
incidents. Runner process epochs are stored in the same ledger, so a container
restart cannot reset or hide a failed canary window.

Build an idempotent 30–90 day research dataset from public hourly candles and
actual settled funding events, then compare both profiles without look-ahead:

```bash
docker compose exec app python scripts/historical_backfill.py --days 90
curl -X POST http://127.0.0.1:8000/backtests/compare-market/jobs \
  -H 'content-type: application/json' \
  -d '{"start":"2026-05-12T00:00:00Z","end":"2026-08-10T00:00:00Z","initial_capital":"15000"}'
# Poll the returned job ID; completed jobs include the full comparison result.
curl http://127.0.0.1:8000/backtests/compare-market/jobs/<job-id>
```

The response includes a canonical dataset SHA-256, per-series candle coverage
and largest gaps, position counts, costs, drawdown, rolling-window checks, and
persisted baseline/candidate run IDs. If historical order books are unavailable,
replay explicitly uses a conservative synthetic spread/depth model; it never
silently assumes zero slippage.

Run long replays in a dedicated `RUN_MODE=api`, `PAPER_AUTOTRADE=false` worker.
This keeps paper market polling responsive and prevents an HTTP client timeout
from cancelling result delivery. Candidate entries use the same portfolio risk
limits as the live-data paper runner and require the nearest settlement to cover
round-trip costs with the configured safety margin.

By default the backfill discovers a bounded 12-asset universe using current
funding potential and liquidity while retaining BTC, ETH, and SOL. Override it
with `--assets BTC,ETH,SOL` or adjust breadth with `--asset-limit`. Historical
funding is collected for every perpetual in the selected runner universe.
