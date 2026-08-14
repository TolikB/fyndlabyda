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
| Exclude pre-fix data | Invalidated v25/v26/v27 retained separately; clean namespace `v28-oos-*` started with zero position rows | Proven running |

## Signal, funding, borrow, and market data

| Requirement | Authoritative evidence | State |
| --- | --- | --- |
| No duplicate or reverse-route two-instrument exposure | Runtime/replay canonical key; API independently derives keys for every persisted position and gate rejects missing/mismatched keys plus any overlapping historical holding intervals | Proven locally |
| Venue-specific future funding events | Adapter schedule tests for 1/4/8-hour intervals and settlement engine tests | Proven |
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
| Event-driven deterministic replay with costs and attribution | Historical replay tests and portable dataset digest | Proven |
| Same dataset/config candidate versus baseline, no look-ahead | Dataset `market-db-sha256:0745b20ed8e77c0ba02a7472ab10f6d48264e25405ad9e5d81f83e7c5c0103dc`; deterministic candidate event SHA `53e9e263f2709bb84ef4466a522ed1766e53cdd6235b019b082e9a96742c9534` | Proven |
| Historical candidate beats baseline acceptance checks | Candidate `+$20.7234382435`; strict baseline `-$83.1268879021`; lower drawdown, higher median monthly PnL, 2/3 profitable windows | Historical evidence only |
| Paper-only shared-feed candidate/baseline on VM | Release `funding-pnl-v2-20260814-069`; healthy pre-boundary postflight, restart count 0, `v28-oos-*` namespaces empty, all five venues ready | Proven running |
| Initial post-boundary smoke | `runtime_safe=true`; 7 exact shared snapshot pairs from `02:00:24Z` to `02:03:20Z`; max gap `36.346767s`; snapshot risk and all validation windows sourced from `portfolio_snapshots`; carry-in, incidents, accounting/replay errors, and exposure defects zero; all venue/coverage/two-sample WS checks true | Proven |
| First live-public funding settlement | Exact venue event audit is ready; obtain a fresh `v28` payment after the clean boundary | Pending live event |
| Telegram preview reconciles to active ledger | Installed release-069 preview uses only `v28-oos-candidate` PnL/fills/snapshots and opportunities at or after its autotrade boundary. Post-boundary DB-backed proof: 39 snapshots, zero cycle failures, zero eligible/confirmed signals, correct zero day/total PnL, `STARTED` process evidence, and no false attribution from earlier namespaces. Telegram remains enabled on the `Europe/Kyiv` midnight schedule without exposing credentials | Proven installed and live read-only |
| Candidate inactivity is an economic decision, not a runner fault | Initial v28 funnel: 17 raw candidates, all rejected on negative net APR after costs; best COTI Gate/Bybit net APR `-0.681520%` and best `$100` quote expected `-$0.186718`; runner errors/incidents zero | Proven initial snapshot; continue OOS |
| Funding reconciliation is durable and replayable at timed gates | Tracked `scripts/funding_payment_audit.py` verifies every payment and every raw event inside each holding interval, exact venue timestamp/rate, perpetual leg/side, notional, signed PnL, target grace, uniqueness, settlement markers/event count, and position total. Initial v28 no-payment smoke returned `ok=true`, all 17 checks true, and every mismatch count zero; a real v28 event is still required | Proven tooling and zero-event smoke; v28 event pending |
| Final acceptance cannot pass without continuous shared telemetry | Core comparison and tracked operator audit fail closed when either comparable snapshot series is empty, max gap exceeds `300s`, or snapshot-derived risk/window sources are absent. The operator audit samples WS heartbeats twice to survive a transient resubscription reset while retaining all counters/coverage from the latest sample. Initial v28 live proof passed with 7 exact pairs and `36.346767s` max gap | Proven live compatibility; timed window pending |
| Clean 72-hour canary | Boundary `2026-08-14T02:00:00Z`; earliest audit `2026-08-17T02:01:00Z` | Pending time gate |
| 30-day out-of-sample acceptance | Same boundary; earliest audit `2026-09-13T02:01:00Z` | Pending time gate |

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
