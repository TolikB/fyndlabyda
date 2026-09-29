# Funding Arbitrage Research Bot

Read-only, market-neutral funding research and **paper trading** system. It
normalizes public market data from Bybit, Gate, OKX, Binance, and Hyperliquid,
ranks spot/perpetual and perpetual/perpetual funding opportunities, and trades
them only in a simulator. There is no live execution path: no API keys, no
private endpoints, no orders. The service refuses to start if exchange
credentials are present in its environment.

* Operations (Ukrainian): [`ops/PAPER_TEST_RUNBOOK.md`](ops/PAPER_TEST_RUNBOOK.md)
* Analysis and launch plan (Ukrainian): [`docs/ANALYSIS_AND_PLAN.md`](docs/ANALYSIS_AND_PLAN.md)

## Paper simulator v2

* **Series.** `config/paper_series.yaml` defines independently accounted series
  (`candidate`, `baseline`) that trade the same market snapshots. Each has its own
  ledger and a 1000 USDT total starting balance. A series' identity covers its
  settings, the simulator version, fees, venues, and fill rules; changing any of
  them requires a new `label`, so statistics are never mixed.
* **Accounting.** Every cash movement is an idempotent ledger entry. Equity is
  `cash + locked collateral + unrealized PnL`; the invariant and a reconciliation
  against fills, funding payments, positions, and snapshots must hold within $0.01.
* **Fills.** Only against fresh, non-crossed public order books with enough depth;
  VWAP price, per-venue spot/perp taker fees, lot and minimum-size rules. No book,
  no fill: entries are rejected and exits postponed.
* **Funding.** Booked only for settlements published in the venue's funding
  history (timestamp and rate), on quantity x mark at settlement; missed events are
  caught up after a restart.
* **Telegram.** Start/stop notices and one report for the previous Kyiv day.

## Local development

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check . && ruff format --check . && mypy
# PostgreSQL integration tests (accounting, funding, restart, Telegram) need a database:
TEST_DATABASE_URL=postgresql+asyncpg://funding:funding@localhost:5432/funding_test pytest -q
```

Release gate (lint, types, tests without skips, SHA-256 release manifest):
`ops/scripts/release_check.sh`.

## CLI

```bash
funding-arbitrage api                 # API; runs the paper runner when RUN_MODE=paper_test
funding-arbitrage preflight --json r.json   # verify every public feed
funding-arbitrage paper               # series summaries from the database
funding-arbitrage reconcile           # ledger reconciliation per series
funding-arbitrage readiness --hours 72
funding-arbitrage release-manifest check
funding-arbitrage collect | scan | backtest --monthly-pnl monthly.json
```

## API

`/health`, `/health/ready`, `/exchanges`, `/opportunities`, `/portfolio`,
`/positions`, `/analytics/series`, `/analytics/series/{id}` (`/equity`,
`/attribution`, `/reconciliation`), `/analytics/compare?a=candidate&b=baseline`,
`/analytics/readiness`, `/metrics/`, dashboard at `/dashboard/`. Mutating
research endpoints (`POST /scan`, `POST /backtests`) are disabled in paper mode.

## Docker

```bash
cp .env.paper-live-data.example .env    # set POSTGRES_PASSWORD, Telegram values
docker compose up -d --build            # ports bind to 127.0.0.1 only
```

`.env.paper-test.example` is a fully offline profile with mock venues and fast
funding settlements (`config/paper_series.mock.yaml`).
