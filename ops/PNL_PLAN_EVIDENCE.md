# Sustainable net PnL plan — evidence ledger

This file is the durable completion checklist for the paper-only PnL plan. A
passing unit test or historical replay does not replace a timed VM gate. The
goal remains open until every row is proven, including the last two rows.

## Accounting and execution correctness

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| Close exact filled quantities for expensive and cheap assets | `tests/test_pnl_v2.py` exact-quantity BTC/TUT open-close tests | Proven |
| Distinct spot/perpetual market keys | Typed `(exchange, symbol, instrument_type)` books and `tests/test_pnl_v2.py` | Proven |
| Cross-venue locked capital and equity invariant | `PaperPortfolio.locked_capital`, `tests/test_pnl_v2.py`, DB replay reconciliation | Proven |
| Venue/leg-specific fees | `CostEngine` fee schedules and cross-venue fee test | Proven |
| Reject missing, stale, shallow, or partial paper books | Executor tests in `tests/test_pnl_v2.py`; no fabricated partial close | Proven |
| Mark open positions to market without close-time double count | Typed fresh tickers, `PaperTradingExecutor.mark_to_market`, open/close equity regressions, and DB replay reconciliation | Proven locally and deployed |
| Ratio units for spread/slippage | Default `0.0015`/`0.0020` plus config tests | Proven |
| Enforce `PAPER_AUTOTRADE` and a UTC start boundary | Runtime health fields and runner tests | Proven |
| Exclude pre-fix data | Invalidated v25 through v30 remain queryable but excluded. Release 073 uses `v31-oos-*` with an enforced `2026-08-14T08:40:00Z` audit boundary; deployment proved zero carry-in v31 positions/fills/payments and records every process start before that boundary. The first post-boundary audit found zero carry-in and zero runtime incidents | Proven running |

## Signal, funding, borrow, and market data

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| No duplicate or reverse-route two-instrument exposure | Runtime/replay canonical key; API independently derives keys for every persisted position and gate rejects missing/mismatched keys plus any overlapping historical holding intervals | Proven locally |
| Venue-specific future funding events | Adapter schedule tests cover 1/4/8-hour intervals and settlement accounting. A fail-closed live-public probe at `2026-08-14T03:17:33Z` selected exact BTC/USDT-family perpetuals on Binance, Bybit, Gate, OKX, and Hyperliquid; every current next timestamp was future/present, history was ordered and non-future, and recent event deltas matched venue metadata within `5s`. Observed universes were Binance `1/4/8h`, Bybit `1/4/8h`, Gate `1/4/8h`, OKX `4/8h`, and Hyperliquid `1h` | Proven locally and live-public |
| Robust median/EWMA, persistence, sign changes, two-sided outliers | Funding statistics/forecast code and scanner tests | Proven |
| Time-synchronized cross-venue differential | Historical-window projection and no-look-ahead replay tests | Proven |
| No unsupported short spot; hourly borrow accrual where enabled | Scanner gate, borrow config, and runner accrual test | Proven |
| Liquidity/staleness/history filters before confirmation | Opportunity filters, candidate history routing, and funnel evidence | Proven |
| WebSocket tickers/order books primary; REST recovery | Collector stream tests and VM two-sample stream ages prove fresh ticker/orderbook data on all five venues; REST remains snapshot/recovery/history only | Proven locally and running |
| Concurrent venue collection, limits, circuit breakers, and cycle latency | Collector/adapters and failure-path tests. With a configured `30s` loop, a post-boundary DB sample of 45 exact gaps per profile was identical for candidate/baseline: average `31.495s`, p50 `31.158s`, p95 `48.280s`, max `63.862s`, and zero gaps over `300s`. Release 072 additionally skips isolated OKX tickers whose `last` is blank instead of dropping the venue; its live pre-boundary smoke reached six completed cycles, zero runner errors, complete funding/book coverage on all five venues, zero stale books, and no collection failures after deployment | Proven locally and running |

## Capital, exits, and observability

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| Candidate chooses best executable `$100`–`$5,000` quote | Candidate micro-size regression test; runtime/replay parity | Proven |
| Baseline remains corrected fixed-size `$250` | Runtime and historical replay baseline selection tests | Proven |
| Capital allocator and risk engine enforce portfolio limits | Allocation/risk code and concentration/correlation tests | Proven |
| Settlement-aware entry and continuation | Settlement projection/coverage tests | Proven |
| Exit on edge, sign, target, basis, liquidity, or max hold | Parameterized runner tests | Proven |
| Restart-safe pending exit after a bad close book | Persisted exit-request test across serialization/recovery | Proven |
| Explain candidate/baseline rejections | Profile/reason Prometheus counters and structured decision logs | Proven |
| Daily Telegram report shows day and current-simulator totals | Telegram tests plus a DB-backed, non-sending preview prove day/total accounting, runner evidence, and separate candidate/baseline sections. A fail-closed live probe at `2026-08-14T03:32:52Z` called only Telegram `getMe/getChat`, redacts the token on every path, and proved the configured bot, exact private chat, and `00:00 Europe/Kyiv` schedule reachable from the VM without sending a message | Proven locally and live-public |

## Replay and acceptance gates

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| Event-driven deterministic replay with costs, attribution, and open-position M2M | Historical replay tests; canonical candle snapshots include unrealized two-leg PnL and accrued borrow, and the final forced-close snapshot reconciles to the event ledger | Proven |
| Same dataset/config candidate versus baseline, no look-ahead | Dataset `market-db-sha256:0745b20ed8e77c0ba02a7472ab10f6d48264e25405ad9e5d81f83e7c5c0103dc`; deterministic candidate event SHA `53e9e263f2709bb84ef4466a522ed1766e53cdd6235b019b082e9a96742c9534` | Proven |
| Historical candidate passes the economic comparison checks | Candidate `+$20.7234382435`; strict baseline `-$83.1268879021`; snapshot max drawdown `0.102224%` versus `2.000771%`; higher median monthly PnL; 2/3 profitable snapshot windows; 721 exact shared timestamps; event/snapshot PnL error below `$0.01` | Proven historical economics only |
| Historical telemetry satisfies the runtime five-minute cadence gate | The source dataset is hourly and reports its real `3600s` maximum snapshot gap. No synthetic interpolation is used, so full historical `accepted` and `evidence_ready` correctly remain false on the `300s` cadence check. Earlier v29/v30 runtime data proved sub-five-minute cadence but is diagnostic only because those windows were later invalidated | Historical cadence not claimed; v31 timed gates pending |
| Paper-only shared-feed candidate/baseline on VM | Release `funding-pnl-v2-20260814-073` from code commit `d9b6d6c`; project-scoped postflight confirmed `paper_test / live_public / paper`, distinct `v31-oos-*` profiles, healthy app, restart count 0, and the exact `2026-08-14T08:40:00Z` boundary | Proven running |
| Initial post-boundary smoke | Six exact shared v31 pairs from `08:40:03Z` through `08:42:33Z`; maximum gap `34.819379s`, zero pending snapshots, zero runtime incidents/carry-in, and candidate/baseline invariant errors `0E-18`. All five venues were healthy with fresh ticker/orderbook WebSockets, complete funding/book coverage, zero stale books, and zero runner errors. Canary correctly remained false only because 72 hours had not elapsed | Proven initial smoke |
| First live-public funding settlement | v31 baseline opened COTI with venue-specific targets: Bybit `09:00Z`, Gate `12:00Z`. The first Bybit event persisted at exactly `09:00:00Z`, rate `-0.00484701`, short notional `$250`, and funding PnL `-$1.2117525`. The raw history identity matched exactly, target delay was `0s`, all 17 funding checks passed, and incidents/restarts remained zero. Gate remains correctly unpaid until its own target | Bybit proven live-public; Gate pending |
| Telegram preview reconciles to active ledger | DB-backed, non-sending v31 previews before and after the first real funding event proved that the report changes with the ledger. After Bybit settlement, candidate day/total remained `+$0.00` with equity `$6250.00`; baseline showed day/total net PnL `-$6.09`, equity `$6243.91`, funding `-$1.21`, fees `-$0.26`, slippage `-$0.28`, two fills, and one open position. The extra loss is current mark-to-market/basis movement, not a missing fee. The report includes separate day/total sections, exact v31 versions, runner evidence, the warning not to add portfolios, and `PAPER ONLY` | Proven installed post-funding preview |
| Candidate inactivity or entry is an economic decision, not a runner fault | Candidate observed the COTI signal but repeatedly rejected entry on `settlement_cost_coverage`; baseline opened the corrected `$250` COTI control exposure and subsequently rejected duplicate exposure. HOME was rejected on `no_viable_size_quote`. Runner errors and exposure defects were zero | Proven initial economic decision |
| Funding reconciliation is durable and replayable at timed gates | Audit failure on the first v30 payment pair exposed the market-persist cadence race. Release 073 writes the exact raw event and new payment in one transaction and includes a regression for idempotent repair of an existing payment. The first v31 Bybit payment passed `--require-payments`: one payment, one exact history event, zero PnL/notional/timestamp/duplicate/orphan/leg/position-total/settled-marker errors | Proven locally and first live-public event; Gate pending |
| Final acceptance cannot pass without continuous shared telemetry | Core comparison and tracked operator audit fail closed when either comparable snapshot series is empty, max gap exceeds `300s`, or snapshot-derived risk/window sources are absent. The runtime pins every open-position market, rejects an unmarkable shared snapshot before either ledger mutates, and reports readiness from fully completed shared snapshots. Local verification for release 073 is 163 tests plus clean Ruff and mypy over 88 source files | Proven implementation; timed window pending |
| Clean 72-hour canary | Boundary `2026-08-14T08:40:00Z`; earliest audit `2026-08-17T08:41:00Z` | Pending time gate |
| 30-day out-of-sample acceptance | Same boundary; earliest audit `2026-09-13T08:41:00Z` | Pending time gate |

## Current safety boundary

- VM: Contabo `169.58.161.34` only.
- Project directory: `/opt/funding_arbitrage_paper`.
- Compose project: `funding_arbitrage_paper`.
- Project-scoped container inspection proves enforced limits: app `0.70 CPU /
  640 MiB`, PostgreSQL `0.25 CPU / 384 MiB`, and Redis `0.10 CPU / 96 MiB`.
  The observed memory snapshot was approximately `292 MiB`, `179 MiB`, and
  `5.4 MiB`, respectively; all restart counts were zero.
- Only the app API is host-bound, on loopback `127.0.0.1:8000`; PostgreSQL and
  Redis have no host port binding.
- Runtime must remain `paper_test / live_public / paper` with no live execution
  module or private exchange credentials.
- Do not restart or deploy after the clean boundary unless a material correctness
  defect is found; a restart is a persisted incident and invalidates the window.
- Do not claim expected profitability from the historical replay alone.

## Invalidated evidence window

The `v25-oos-*` window beginning `2026-08-13T21:15:00Z` is excluded from all
acceptance calculations. Its baseline opened the same Gate/Bybit COTI
instrument pair in opposite directions, cancelling funding while paying fees
twice. Release 067 prevented this with a canonical persisted exposure key.

The `v26-oos-*` window beginning `2026-08-13T23:15:00Z` is also excluded from
acceptance calculations. Its exact funding-event evidence remains useful as a
diagnostic, but open positions were not marked to market, so equity, drawdown,
and rolling validation PnL understated intra-position price and basis risk.
Release 068 adds fresh typed-ticker mark-to-market, close-time no-double-count
accounting, DB replay reconciliation, and snapshot-derived risk/window metrics.
The `v27-oos-*` ledgers therefore start empty at the later clean boundary.

The `v27-oos-*` window beginning `2026-08-14T01:15:00Z` had correct PnL
accounting, but its daily report counted global opportunity rows from earlier
simulator namespaces and could therefore contradict its zero-fill explanation.
Release 069 filters report signals by the active simulator autotrade boundary
and adds persisted runner evidence. Deploying that fix required a process start,
so v27 is excluded from timed acceptance and `v28-oos-*` starts empty at
`2026-08-14T02:00:00Z`.

The `v28-oos-*` window is also excluded. Baseline opened Gate/Bybit `COTI` at
`2026-08-14T02:07:50Z`; the bounded universe later removed a required typed
ticker for that still-open position. Mark-to-market then raised 24 persisted
`ValueError` incidents per profile from `02:11:19Z`, candidate snapshots
continued to 44 while baseline stopped at 22, and readiness incorrectly observed
the scanned rather than fully completed snapshot. Release 070 pins requested
open-position markets, prevalidates both ledgers before processing, and bases
readiness on completed shared snapshots. The resulting `v29-oos-*` boundary was
`2026-08-14T03:00:00Z`.

The `v29-oos-*` window is diagnostic only. It proved exact live Bybit/Gate
funding settlement and the corrected candidate/baseline Telegram breakdown,
but release 071 restarted the app after the v29 boundary to install that report
fix. The acceptance audit therefore correctly counts one process-start incident
per profile inside the window. Subsequent rollback verification added more v29
process starts, so no v29 result is eligible for the 72-hour or 30-day gate.

Release 072 and its failed pre-boundary validation attempts created
zero-exposure v30 telemetry snapshots and six process-start records before the
`2026-08-14T07:30:00Z` boundary. The clean window later opened one baseline COTI
position and applied two exact `08:00Z` funding payments. However, the payments
committed between periodic market-history writes, so neither exact raw exchange
event was durable in `funding_history`. The fail-closed reconciliation audit found
`exact_history_mismatch_count=2`; v30 is therefore invalid for timed acceptance.

Release 073 persists a live raw funding event and its new paper payment in the
same transaction, and repairs missing history when an already-persisted payment
is seen again without changing PnL or event count. Deploying the fix required an
app restart, so `v31-oos-*` starts at the new clean
`2026-08-14T08:40:00Z` boundary. Deployment created exactly two profile-specific
process-start records before that boundary and zero v31 positions, fills,
funding payments, or post-boundary incidents.
