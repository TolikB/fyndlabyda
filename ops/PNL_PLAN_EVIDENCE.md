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
| Exclude pre-fix data | Invalidated v25/v26/v27/v28 retained separately; clean namespace `v29-oos-*` started with zero position rows | Proven running |

## Signal, funding, borrow, and market data

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| No duplicate or reverse-route two-instrument exposure | Runtime/replay canonical key; API independently derives keys for every persisted position and gate rejects missing/mismatched keys plus any overlapping historical holding intervals | Proven locally |
| Venue-specific future funding events | Adapter schedule tests cover 1/4/8-hour intervals and settlement accounting. A fail-closed live-public probe at `2026-08-14T03:17:33Z` selected exact BTC/USDT-family perpetuals on Binance, Bybit, Gate, OKX, and Hyperliquid; every current next timestamp was future/present, history was ordered and non-future, and recent event deltas matched venue metadata within `5s`. Observed universes were Binance `1/4/8h`, Bybit `1/4/8h`, Gate `1/4/8h`, OKX `4/8h`, and Hyperliquid `1h` | Proven locally and live-public |
| Robust median/EWMA, persistence, sign changes, two-sided outliers | Funding statistics/forecast code and scanner tests | Proven |
| Time-synchronized cross-venue differential | Historical-window projection and no-look-ahead replay tests | Proven |
| No unsupported short spot; hourly borrow accrual where enabled | Scanner gate, borrow config, and runner accrual test | Proven |
| Liquidity/staleness/history filters before confirmation | Opportunity filters, candidate history routing, and funnel evidence | Proven |
| WebSocket tickers/order books primary; REST recovery | Collector stream tests and VM stream-age metrics | Proven |
| Concurrent venue collection, limits, circuit breakers | Collector/adapters and failure-path tests | Proven |

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
| Daily Telegram report shows day and current-simulator totals | Telegram tests plus DB-backed read-only report preview | Proven |

## Replay and acceptance gates

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| Event-driven deterministic replay with costs, attribution, and open-position M2M | Historical replay tests; canonical candle snapshots include unrealized two-leg PnL and accrued borrow, and the final forced-close snapshot reconciles to the event ledger | Proven |
| Same dataset/config candidate versus baseline, no look-ahead | Dataset `market-db-sha256:0745b20ed8e77c0ba02a7472ab10f6d48264e25405ad9e5d81f83e7c5c0103dc`; deterministic candidate event SHA `53e9e263f2709bb84ef4466a522ed1766e53cdd6235b019b082e9a96742c9534` | Proven |
| Historical candidate passes the economic comparison checks | Candidate `+$20.7234382435`; strict baseline `-$83.1268879021`; snapshot max drawdown `0.102224%` versus `2.000771%`; higher median monthly PnL; 2/3 profitable snapshot windows; 721 exact shared timestamps; event/snapshot PnL error below `$0.01` | Proven historical economics only |
| Historical telemetry satisfies the runtime five-minute cadence gate | The source dataset is hourly and reports its real `3600s` maximum snapshot gap. No synthetic interpolation is used, so full historical `accepted` and `evidence_ready` correctly remain false on the `300s` cadence check. The initial authoritative v29 VM smoke observed seven exact shared post-boundary snapshots with a `63.862267s` maximum gap | Historical cadence not claimed; initial VM cadence proven, timed gates pending |
| Paper-only shared-feed candidate/baseline on VM | Release `funding-pnl-v2-20260814-070`; healthy independent postflight, restart count 0, `v29-oos-*` namespaces empty, all five venues ready, and readiness tied to the same fully completed shared snapshot | Proven running |
| Initial post-boundary smoke | Seven exact `v29` shared snapshot pairs from `03:00:25Z` to `03:03:59Z`; both ledgers advanced together with zero pending snapshots, invariant/replay error `0E-18`, non-start incidents zero, one expected process-start record per profile, restart count 0, and no new error logs. All five venues had complete funding/book coverage and fresh two-sample WebSocket evidence | Proven initial smoke |
| First live-public funding settlement | Exact venue event audit is ready; obtain a fresh `v29` payment after the clean boundary | Pending live event |
| Telegram preview reconciles to active ledger | A DB-backed, non-sending `v29-oos-candidate` preview after the boundary reported day/total net PnL `+$0.00`, equity `$6250.00`, zero fills/opens/closes, 55 snapshots, zero cycle failures, one expected process start, and the paper-only/no-edge explanation. Telegram remains enabled on the `Europe/Kyiv` midnight schedule without exposing credentials | Proven installed preview |
| Candidate inactivity or entry is an economic decision, not a runner fault | The post-boundary v29 funnel observed 32 raw candidates, zero eligible/confirmed, best raw net APR `-0.6973858539`, and rejected all 32 on net APR (two also unstable). Runner errors/incidents and exposure defects were zero | Proven initial economic rejection |
| Funding reconciliation is durable and replayable at timed gates | The tracked `scripts/funding_payment_audit.py` post-boundary smoke returned `ok=true` with all 16 checks true and every mismatch/duplicate/orphan count zero. There were no positions or payments yet, so a real `v29` event is still required | Proven tooling/smoke; v29 event pending |
| Final acceptance cannot pass without continuous shared telemetry | Core comparison and tracked operator audit fail closed when either comparable snapshot series is empty, max gap exceeds `300s`, or snapshot-derived risk/window sources are absent. Release 070 additionally pins every open-position market before universe limiting, rejects an unmarkable shared snapshot before either ledger mutates, and reports readiness from fully completed shared snapshots. The operator audit now retries only bounded transient `503` readiness transitions while the peer ledger finishes the same snapshot; persistent or non-503 failures still abort. Local verification is 150 tests plus Ruff/mypy, and the updated live audit observed 22 exact post-boundary pairs | Proven implementation; timed window pending |
| Clean 72-hour canary | Boundary `2026-08-14T03:00:00Z`; earliest audit `2026-08-17T03:01:00Z` | Pending time gate |
| 30-day out-of-sample acceptance | Same boundary; earliest audit `2026-09-13T03:01:00Z` | Pending time gate |

## Current safety boundary

- VM: Contabo `169.58.161.34` only.
- Project directory: `/opt/funding_arbitrage_paper`.
- Compose project: `funding_arbitrage_paper`.
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
readiness on completed shared snapshots. The clean `v29-oos-*` boundary is
`2026-08-14T03:00:00Z`.
