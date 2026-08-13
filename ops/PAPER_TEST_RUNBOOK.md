# Paper-test deployment runbook

This deployment behaves like a continuously running production service while
using public market-data adapters and `PaperTradingExecutor` only. It never
uses exchange trading credentials and never sends a live order. A fully
offline deterministic mock profile is also available.

## Required VM prerequisites

- Linux VM with Docker Engine and Docker Compose v2.
- At least 2 CPU, 4 GB RAM, and persistent disk for PostgreSQL.
- TCP access to port `8000` for the API/dashboard, and optionally `9090` and
  `3000` for Prometheus/Grafana. Keep database and Redis ports private.

## Start

```bash
cd /path/to/funding-bot
cp .env.paper-live-data.example .env
docker compose up -d --build
docker compose ps
```

The app runs Alembic migrations before Uvicorn. The paper runner starts in the
background and performs a cycle every 15 seconds with real public data from
Bybit, Gate, OKX, Binance, and Hyperliquid. All five venues receive $1,000 of
tradable virtual balance plus a separate reserve.

The default deployment is resource-limited and starts app, PostgreSQL, and
Redis only. On a larger host, start monitoring with:

```bash
docker compose --profile observability up -d
```

For the baseline/candidate PnL comparison on the current small VM, enable the
shared-feed comparison in `.env`:

```dotenv
PAPER_COMPARISON_ENABLED=true
PAPER_AUTOTRADE_START_UTC=2026-08-13T20:00:00Z
PAPER_SIMULATION_VERSION=v23-oos-candidate
PAPER_BASELINE_SIMULATION_VERSION=v23-oos-baseline
```

Then run `docker compose up -d --build`. Candidate and baseline retain separate
portfolios and simulation-version ledgers, but process the exact same immutable
`MarketSnapshot` from one collector. This avoids doubling public API/WebSocket
load and removes feed timing as a source of comparison bias. Do not combine the
comparison and observability profiles on a constrained 2-vCPU VM unless
capacity has been checked.

## Verify

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/health/ready
curl http://127.0.0.1:8000/portfolio
curl http://127.0.0.1:8000/analytics/paper
curl http://127.0.0.1:8000/analytics/compare
curl 'http://127.0.0.1:8000/analytics/attribution?simulation_version=v23-oos-candidate'
curl http://127.0.0.1:8000/metrics | grep funding_paper_runner
docker compose logs -f app
```

After the first cycle, `/health/ready` becomes `ready`. After confirmation and
the configured hold/settlement intervals, `/analytics/paper` shows fills,
funding payments, closed positions, fees, and the equity curve.

## Current VM acceptance gates

The restart-safe, pre-market-filtered v23 canary starts with release
`funding-pnl-v2-20260813-060`. Its clean evidence boundary and enforced
autotrade boundary are both `2026-08-13T20:00:00Z`; use that exact timestamp as
`<V23_DURABLE_START_UTC>` below. Run the read-only audit from the project
directory after the relevant deadline:

```bash
# Earliest useful run: 2026-08-16T20:00:00Z.
python3 scripts/paper_acceptance_audit.py \
  --start <V23_DURABLE_START_UTC> \
  --gate canary \
  --timeout 45

# Earliest useful run: 2026-09-12T20:00:00Z.
python3 scripts/paper_acceptance_audit.py \
  --start <V23_DURABLE_START_UTC> \
  --gate acceptance \
  --timeout 45
```

The script prints a JSON evidence bundle. Exit code `0` means the requested
gate passed. Exit code `2` means the service responded correctly but the gate
is not ready yet (for example, fewer than 72 hours or 30 days have elapsed, or
an acceptance condition is still false). Any connection or malformed-response
failure exits with another non-zero code and should be investigated.

Both gates require a paper-only runtime, distinct candidate and baseline
simulation versions, exact shared snapshot timestamps, no accounting invariant
errors, acceptable snapshot gaps, zero current runner errors, a fresh latest
cycle, and complete funding-history/orderbook coverage with zero stale books on
Binance, Bybit, Gate, Hyperliquid, and OKX. Recent normalized ticker and
orderbook messages must also be observed from each venue's WebSocket stream;
REST fallback alone cannot satisfy the gate. Cycle failures are persisted per
simulation version in PostgreSQL and any incident inside the requested window
invalidates both gates after a container restart. Every process start is also
persisted, so an unplanned restart inside the requested window invalidates the
window even when no exception could be recorded first. Short public-data gaps
are excluded from both ledgers and remain visible as snapshot gaps and skip
metrics. The 30-day gate additionally
requires candidate net PnL to exceed baseline by at least 10%, higher median
monthly PnL, no worse max drawdown, and profitable candidate PnL in at least two
of three rolling windows.

The release also requires the digest-pinned Python base image and exact
`requirements.lock` dependency graph. Any dependency update is a new release
and starts a new evidence window.

## Telegram daily report

Set these values in `.env` when the bot credentials are ready:

```dotenv
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=<bot-token>
TELEGRAM_CHAT_ID=<chat-id>
TELEGRAM_TIMEZONE=Europe/Kyiv
TELEGRAM_REPORT_HOUR=0
TELEGRAM_REPORT_MINUTE=0
```

The report is generated for the previous local calendar day and sent once per
day. The database ledger prevents duplicate reports after restarts. Until the
token and chat ID are set, no Telegram request is made.

## Stop without deleting data

```bash
docker compose stop
docker compose start
```

Do not use `docker compose down -v` unless the PostgreSQL paper history should
be intentionally deleted.

## Configuration knobs

- `PAPER_INITIAL_BALANCE_USD`: virtual starting equity.
- `PAPER_POSITION_SIZE_USD`: virtual capital per opportunity.
- `PAPER_MAX_HOLD_SECONDS`: automatic paper close time.
- `PAPER_SETTLEMENT_INTERVAL_SECONDS`: accelerated mock funding event interval.
- `PAPER_LOOP_INTERVAL_SECONDS`: runner cadence.
- `PAPER_MAX_OPEN_POSITIONS`: portfolio cap.
- `PAPER_SIMULATION_VERSION`: durable ledger namespace; never reuse it for a
  materially different accounting model.
- `PAPER_AUTOTRADE_START_UTC`: timezone-aware shared OOS boundary. Market data
  warms before this time, but neither portfolio may open a position before it.
- `PAPER_STRATEGY_PROFILE`: `candidate` for robust schedules/dynamic allocation
  or `baseline` for corrected fixed-size comparison.
- `PAPER_COMPARISON_ENABLED`: run an isolated baseline ledger beside the
  candidate inside the same process and on the same market snapshots.
- `PAPER_BASELINE_SIMULATION_VERSION`: durable namespace for the shared-feed
  baseline; it must differ from `PAPER_SIMULATION_VERSION`.
- `PAPER_EXIT_EDGE_MISS_CYCLES`: candidate exit debounce after edge disappears.
- `PAPER_FUNDING_HORIZON_HOURS`: exact settlement-count forecast horizon.
- `PAPER_ENTRY_WINDOW_HOURS`: maximum time capital may sit idle before the
  nearest venue-specific settlement.
- `PAPER_MIN_SETTLEMENT_COST_COVERAGE`: minimum nearest-settlement funding PnL
  divided by full round-trip costs; defaults to `2`.
- `PAPER_MAX_ADVERSE_BASIS_PERCENT`: candidate exits when combined two-leg
  mark-to-market loss exceeds this fraction of per-leg capital.
- Candidate positions also exit after the targeted funding event unless the next
  venue-specific settlement covers exit plus re-entry costs. A missing or shallow
  close book latches a restart-safe exit request and is retried without a fabricated
  partial close once both legs have executable depth.
- `PAPER_MARKET_ASSET_LIMIT`: liquid base-assets retained per venue; their
  available spot/perp pairs remain together.
- `PAPER_HISTORY_SYMBOL_LIMIT`: funding-history queries per venue per refresh.

The accelerated settlement interval changes wall-clock test speed only; it does
not enable real funding or real trading. It is used only by the deterministic
`MARKET_DATA_MODE=mock` profile. The `live_public` profile accrues exact,
symbol-scoped historical funding events reported by each venue, preserving the
venue event timestamp and variable schedule across Bybit, Gate, OKX, Binance,
and Hyperliquid.
