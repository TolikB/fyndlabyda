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

## Start on an isolated local host

```bash
cd /path/to/funding-bot
cp .env.paper-live-data.example .env
docker compose up -d --build
docker compose ps
```

The app runs Alembic migrations before Uvicorn. The paper runner starts in the
background and performs a cycle every 15 seconds with real public data from
Bybit, Gate, OKX, Binance, Hyperliquid, MEXC, KuCoin, and HTX. The portfolio
starts with $1,000 total virtual equity: the configured reserve is set aside,
and the remainder is split equally among the eight venues. In comparison mode,
candidate and baseline each have their own independent $1,000 portfolio; their
balances must not be added together as one trading budget.

The default deployment is resource-limited and starts app, PostgreSQL, and
Redis only. On a larger host, start monitoring with:

```bash
docker compose --profile observability up -d
```

## Contabo paper-only release

On the shared Contabo host, use the existing `/opt/funding_arbitrage_paper`
database and Compose project `funding_arbitrage_paper`. Keep each source release
under that project's `releases/` directory. Do not deploy a release over the
existing source tree or recreate PostgreSQL/Redis while updating the app.
Before any app startup (which runs Alembic migrations), back up the existing
paper PostgreSQL database with `pg_dump -Fc`, the project `.env`, Compose
overrides, release markers, and internal TLS directory. Verify the dump with
`pg_restore -l`, record its checksum, and keep the previous immutable release
and its runtime env as the rollback source. If a migration changes the schema,
restoring the old database requires a separate controlled outage and approval;
do not assume merely restarting the old image will undo a migration.

Use `docker-compose.paper-contabo.yml` with a release-specific copy of
`ops/paper-contabo-runtime.env.example`. The Compose overlay pins `paper_test`,
`PAPER`, `live_public`, and paper execution; it replaces the exchange-secret
mount with an empty directory, and Docker restarts this paper-only app after a
host reboot. The base Compose file alone does **not** provide these guarantees.
The existing project `.env` supplies database and Telegram secrets; the second
runtime env file contains only non-secret mode, budget, and version settings.
Both files must be mode `0600`, and the empty exchange-secret directory must
contain no files. Replace `RELEASE`, `SHA7`, and
`REPLACE_WITH_EXACT_40_HEX_SHA` in the runtime template with the cloned commit
directory, its seven-character prefix, and its verified full 40-character SHA.
The existing project `.env` must supply the Compose interpolation values
`POSTGRES_PASSWORD`, `CLICKHOUSE_PASSWORD`, and `GRAFANA_ADMIN_PASSWORD` even
when optional services are inactive. Never print or commit those values.

Run Compose from the immutable release directory, passing the existing project
env and the release-specific runtime env in that order:

```bash
docker compose -p funding_arbitrage_paper \
  --env-file /opt/funding_arbitrage_paper/.env \
  --env-file ./paper-runtime.env \
  -f docker-compose.yml -f docker-compose.paper-contabo.yml config --quiet
docker compose -p funding_arbitrage_paper \
  --env-file /opt/funding_arbitrage_paper/.env \
  --env-file ./paper-runtime.env \
  -f docker-compose.yml -f docker-compose.paper-contabo.yml \
  up -d --no-deps --build app
```

For preflight, keep `PAPER_AUTOTRADE=false`, `TELEGRAM_ENABLED=false`, and a
fresh preflight-only simulation version. Confirm all eight public venues,
funding history, and fresh executable books before enabling entries. For the
active run, create a **different** runtime env file with unique candidate and
baseline versions, an explicit UTC `PAPER_AUTOTRADE_START_UTC`,
`PAPER_AUTOTRADE=true`, `PAPER_COMPARISON_ENABLED=true`, and
`TELEGRAM_ENABLED=true`; point its `PAPER_RUNTIME_ENV_FILE` at itself. Check
the effective configuration without printing secrets, then recreate **only**
the app with `up -d --no-deps --force-recreate app`. Do not combine historical
preflight or older simulation PnL with the active run. Candidate and baseline
each start with an independent $1,000 virtual balance; the $100 funding limit
is aggregate across each portfolio's two-leg funding positions.

For the baseline/candidate PnL comparison, set unique versions and a new UTC
start boundary in the release-specific runtime env. The following is an
illustration only; never reuse these historical namespaces or boundary:

```dotenv
PAPER_COMPARISON_ENABLED=true
PAPER_AUTOTRADE_START_UTC=2026-08-14T08:40:00Z
PAPER_SIMULATION_VERSION=v33-multi-regime-candidate
PAPER_BASELINE_SIMULATION_VERSION=v33-multi-regime-baseline
```

On Contabo, use the complete Compose command above with the exact project and
both files, and recreate `app` only. Candidate and baseline retain separate
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
curl 'http://127.0.0.1:8000/analytics/attribution?simulation_version=v33-multi-regime-candidate'
curl http://127.0.0.1:8000/metrics | grep funding_paper_runner
docker compose -p funding_arbitrage_paper \
  --env-file /opt/funding_arbitrage_paper/.env \
  --env-file ./paper-runtime.env \
  -f docker-compose.yml -f docker-compose.paper-contabo.yml logs -f app
```

After the first cycle, `/health/ready` becomes `ready`. After confirmation and
the configured hold/settlement intervals, `/analytics/paper` shows fills,
funding payments, closed positions, fees, and the equity curve.

## Current VM acceptance gates

The mark-to-market, pinned-open-market, evidence-aware, restart-safe,
reverse-route-deduplicated v31 canary starts with release
`funding-pnl-v2-20260814-073`. Its clean evidence boundary and enforced
autotrade boundary are both `2026-08-14T08:40:00Z`; use
that exact timestamp as the `--start` value below. Run the read-only audit
inside the deployed application container after the relevant deadline:

```bash
# Earliest useful run: 2026-08-17T08:41:00Z.
cd /opt/funding_arbitrage_paper
docker compose -p funding_arbitrage_paper exec -T app \
  python scripts/paper_acceptance_audit.py \
  --candidate-version v31-oos-candidate \
  --baseline-version v31-oos-baseline \
  --start 2026-08-14T08:40:00Z \
  --gate canary \
  --timeout 45

# Earliest useful run: 2026-09-13T08:41:00Z.
cd /opt/funding_arbitrage_paper
docker compose -p funding_arbitrage_paper exec -T app \
  python scripts/paper_acceptance_audit.py \
  --candidate-version v31-oos-candidate \
  --baseline-version v31-oos-baseline \
  --start 2026-08-14T08:40:00Z \
  --gate acceptance \
  --timeout 45

# Run at both gates; require at least one real live-public payment as evidence.
docker compose -p funding_arbitrage_paper exec -T app \
  python scripts/funding_payment_audit.py \
  --candidate-version v31-oos-candidate \
  --baseline-version v31-oos-baseline \
  --start 2026-08-14T08:40:00Z \
  --require-payments
```

The script prints a JSON evidence bundle. Exit code `0` means the requested
gate passed. Exit code `2` means the service responded correctly but the gate
is not ready yet (for example, fewer than 72 hours or 30 days have elapsed, or
an acceptance condition is still false). Any connection or malformed-response
failure exits with another non-zero code and should be investigated.

Historical replay snapshots are emitted only at canonical source-candle
timestamps. They include unrealized two-leg PnL and accrued borrow, and their
final point reconciles to the forced-close event ledger. Do not interpolate an
hourly dataset into synthetic five-minute observations: its real `3600s` gap
must remain visible, so the historical comparison can support economic and risk
analysis but cannot substitute for the live `300s` telemetry gate.

For an evidence window already running on an older immutable release, stream
the tracked current script to `python -` inside that same app container instead
of rebuilding it. This applies correctness fixes to the read-only operator gate
without changing the service process, restart count, database, or clean boundary.
By default the operator gate samples WebSocket receipt metrics twice, two seconds
apart. It keeps error counters, coverage, stale-book counts, and cycle age from
the latest sample, while accepting a fresh ticker/orderbook heartbeat observed
in either sample. This prevents a task-resubscription heartbeat reset from
creating a false negative without allowing a genuinely stale stream to pass.

Both gates require a paper-only runtime, distinct candidate and baseline
simulation versions, exact shared snapshot timestamps, no accounting invariant
errors, non-empty comparable snapshot series, a maximum snapshot gap of five
minutes, zero current runner errors, a fresh latest
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
of three rolling windows. Median monthly PnL, drawdown, and validation-window
PnL must come from the continuous durable portfolio snapshot curves, including
unrealized mark-to-market PnL for open positions. It also independently
reconstructs canonical exposure keys for every persisted position and rejects
any overlapping holding intervals, including duplicates that opened and closed
between audit runs.

For every paper funding payment, the timed audit must also reconcile the exact
`(exchange, symbol, funding_timestamp, funding_rate)` against durable raw
`funding_history`, match the payment to the perpetual leg and its side, and
recalculate signed PnL from notional and settled rate. A venue's actual event
timestamp is authoritative and may differ by seconds from the predicted target;
do not round it to a nominal wall-clock boundary. The funding-payment audit also
rejects any raw history event missing from the payment ledger while its position
was held, a payment outside its holding interval, a wrong notional, stale
`funding_events`/`settled_funding_at` state, duplicate payments, or a mismatch
between payment totals and position funding PnL. The 300-second target grace is
applied to the first actual payment at or after each persisted entry target;
later settlements are checked against their own exact raw venue timestamps,
not incorrectly compared with the original target.

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

Telegram sends only three human-facing message types: bot started after the
first healthy paper cycle, bot stopped after graceful shutdown, and one concise
trading report for the previous local calendar day. The report shows daily and
all-time result, balance, funding, costs, trades, open positions, and a
plain-language no-trade reason. Runtime versions, snapshots, restarts, coverage,
and other diagnostic fields remain in logs and metrics. Before a daily report
is submitted, the database ledger durably claims that local date. An ambiguous
Telegram delivery is not retried automatically, which prevents duplicates after
timeouts or restarts; operators can inspect the delivery_unknown ledger state
if a report is missing. Until the token and chat ID are set, no Telegram request
is made.

## Stop only the Contabo paper app without deleting data

```bash
docker compose -p funding_arbitrage_paper \
  --env-file /opt/funding_arbitrage_paper/.env \
  --env-file ./paper-runtime.env \
  -f docker-compose.yml -f docker-compose.paper-contabo.yml stop app
docker compose -p funding_arbitrage_paper \
  --env-file /opt/funding_arbitrage_paper/.env \
  --env-file ./paper-runtime.env \
  -f docker-compose.yml -f docker-compose.paper-contabo.yml start app
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
- Candidate allocation may use any profitable executable quote in the configured
  `$100`-to-`$5,000` depth grid; `PAPER_POSITION_SIZE_USD` is the baseline's
  fixed minimum and must not constrain candidate sizing.
- `PAPER_COMPARISON_ENABLED`: run an isolated baseline ledger beside the
  candidate inside the same process and on the same market snapshots.
- `PAPER_BASELINE_SIMULATION_VERSION`: durable namespace for the shared-feed
  baseline; it must differ from `PAPER_SIMULATION_VERSION`.
- `PAPER_EXIT_EDGE_MISS_CYCLES`: candidate exit debounce after edge disappears.
- `PAPER_FUNDING_HORIZON_HOURS`: exact settlement-count forecast horizon.
- `PAPER_FUNDING_RECONCILIATION_WINDOW_SECONDS`: post-close deadline for the final
  funding-history check; the default is two hours, it is persisted, and restarts
  never extend it. Polling continues through the full window. Completion requires
  one successful fresh history query covering the deadline. Exact per-event
  markers admit late out-of-order events once.
- `PAPER_FUNDING_RECONCILIATION_POLL_SECONDS`: minimum retry cadence for delayed
  raw history; defaults to 60 seconds and cannot exceed the reconciliation window.
- `PAPER_FUNDING_RECONCILIATION_MAX_POST_DEADLINE_ATTEMPTS`: bounded final-query
  retries after the deadline; defaults to five. Exhaustion is persisted on the
  position and in the runtime incident ledger, then forced polling stops so one
  unavailable or delisted contract cannot block every future paper cycle.
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

The v33 namespaces are intentionally new because post-close funding reconciliation
changes the accounting model. Never reuse a pre-v33 namespace for a new run or
mix its PnL with v33 acceptance evidence.

The accelerated settlement interval changes wall-clock test speed only; it does
not enable real funding or real trading. It is used only by the deterministic
`MARKET_DATA_MODE=mock` profile. The `live_public` profile accrues exact,
symbol-scoped historical funding events reported by each venue, preserving the
venue event timestamp and variable schedule across Bybit, Gate, OKX, Binance,
and Hyperliquid.
