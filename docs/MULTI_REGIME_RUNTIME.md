# Multi-regime runtime

The application consumes the same durable canonical market events in live-public,
paper, and replay flows. The decision path is:

    canonical event commit
      -> exact PostgreSQL journal-row catch-up
      -> 1m to completed 15m/1h candle aggregation
      -> technical/order-flow/structure/derivatives features
      -> hysteretic regime classification
      -> one typed StrategySuiteRequest
      -> breakout/sweep plus available supplemental strategy evaluations
      -> normalized, deterministic strategy records with raw audit payloads
      -> optional intent-bound ML/RL/LLM veto or risk reduction
      -> TTL/deduplication/conflict orchestration across emitted intents
      -> execution-capability gate
      -> portfolio risk authorization
      -> expiring execution plan
      -> multi_regime_decision_batches + risk_decisions
      -> PAPER-only deterministic one- or multi-leg order-book fills
      -> OMS/fill/position projections + paper checkpoint
      -> stop, target, time-stop, or failed/partial-leg compensation

`StrategySuite` is declarative and has no order or sizing authority. It normalizes
directional, funding/basis, lead-lag/stat-arb, dated-futures basis, options,
passive-market-making, Martingale, grid, and loss-averaging evaluations. Context
mode/timestamp mismatches, duplicate contexts, duplicate signal identities, and
future inputs fail closed. The complete normalized suite is embedded in the durable
decision batch, so PostgreSQL replay and the read-only `/strategies` endpoint expose
the same evaluation evidence.

AI support is a separate immutable boundary. A meta-label, RL decision, or audited
LLM result must be fingerprint-bound to the exact `SignalIntent`. It may veto the
intent or lower a dedicated portfolio-risk multiplier. Positive RL sizing actions
are explicitly ignored, and every AI model says `execution_authorized=false` by
schema. Malformed identity, future/stale timing, duplicate support, or support for a
non-actionable signal fails the canonical consumer closed.

Historical startup replay never calls an AI provider. Feature state is rebuilt
without evaluating strategies, then persisted source-event-bound decision batches
restore signal deduplication and active allocation state. A mismatch in mode,
signal identity, priority, correlation group, or allocation fails startup. The main
application currently leaves model providers unwired until versioned artifact
loading and synchronized inference features are available.

The active runtime rebuilds a strict as-of multi-instrument view for every canonical
decision. Rows newer than the source event, stale/crossed books, incomplete venues,
funding history fetched after decision time, missing settlement timestamps, missing
depth, and insufficient virtual venue balances fail closed. That view supplies exact
funding schedules, two-independent-venue lead-lag fair value, perpetual-versus-dated
future carry, passive market-making inputs, and executable public option top-of-book
data. Bybit and OKX option instruments, quotes, implied volatility, visible size,
open interest, volume, contract multipliers, and trading increments are normalized at
typed adapter boundaries. The collector keeps only complete call/put pairs across a
bounded nearest-expiry/nearest-ATM universe and publishes every accepted quote to the
canonical journal before exposing it to the runtime. Missing, stale, future, crossed,
duplicate-conflicting, or incomplete option data produces no context; no option quote
is fabricated from perpetual data.
USD-quoted options may use a USD, USDC, or USDT underlying hedge only with an
explicit parity conversion rate retained in the signal evidence. Non-parity and any
other cross-quote pair are rejected; advanced live option execution remains disabled.

The configured `MULTI_REGIME_ASSETS` are the always-on core universe. A separate
dynamic liquid-altcoin projection evaluates active perpetuals using quote-currency
24h notional, two-sided depth within 25 bps, executable $10k slippage, open-interest
notional, exact funding history, funding persistence, data coverage, venue coverage,
listing-history evidence, and current stream quality. Bybit turnover, OKX derivative
base volume, and MEXC `amount24` are normalized to one quote-notional unit before
cross-venue ranking. The earliest stored funding observation is a conservative
listing-history lower bound; missing history can never make a newly listed asset look
older. Selection is hourly by default with entry/retention hysteresis and a bounded
number of new assets. Every result is committed as a typed
`UNIVERSE_SELECTION_SNAPSHOT` before it changes runtime eligibility, and restart or
replay restores the same selected assets from the canonical journal. Stale, future,
crossed, incomplete, low-depth, low-OI, low-volume, or insufficient-history
candidates remain excluded. Dynamic selection never grants live execution authority.

The mature legacy paper pipeline remains the sole funding execution and settlement
owner. Canonical funding contexts are evaluated and retained as evidence, but their
normalized runtime intent is suppressed so the same opportunity cannot create a
second position or count one funding payment twice. Every authoritative non-zero
funding payment is atomically projected into the canonical hash-chained double-entry
ledger in the same database transaction as its raw-history-backed payment. The
funding event natural key is the ledger idempotency key: an exact retry returns the
durable transaction, while a changed amount, asset, strategy, or posting fails the
runner closed. Startup performs a bounded backfill and rejects missing, orphaned, or
total-divergent funding projections before a new cycle. Funding capital is capped
across both legs: a `$100` limit means at most `$50 + $50`, not `$100` per venue.
Lead-lag, dated-basis, and market-making intents continue through the advanced PAPER
boundary. No projection grants live operator authority.

The consumer is downstream of the raw-event commit. Parallel WebSocket callbacks do
not define execution order: the runtime catches up by the canonical event table row
ID and checkpoints that same cursor atomically with paper OMS/fill/position changes.
Catch-up and warm-up use bounded keyset pages rather than loading the full journal
into memory. Crash-gap decisions are joined to replay rows by exact source event ID,
not by exchange timestamp. A late exchange event remains journaled and checkpointed
but cannot rewind feature, mark, order, or PnL state.
A database failure, identity collision, cursor regression, invalid ordering, or
consumer failure marks the runtime unhealthy and blocks new entries.

At process start, MULTI_REGIME_RESTORE_HOURS bounds feature warm-up. Historical risk
and strategy/AI decisions are not recomputed because portfolio/model state is
time-dependent; persisted decision batches remain authoritative. PAPER positions
are restored from their latest durable projection, and only canonical rows after
the paper checkpoint are replayed.

Multi-regime PAPER execution is enabled only when all of these conditions hold:

- effective mode is PAPER;
- MULTI_REGIME_PAPER_EXECUTION_ENABLED=true;
- PAPER_AUTOTRADE=true;
- the shared entry-health gate and portfolio risk authority approve the signal.

Directional fills use fresh canonical books, venue-specific maker/taker fees, bounded
participation, latency, spread, and nonlinear impact. Aggressive fills walk visible
L2 levels with a displayed-depth participation cap. Every partial market exit is a
separate deterministic child IOC order, and realized PnL is accrued on each exit fill.
A partially entered position is protected immediately: stop breach cancels the entry
remainder and starts a reduce-only flatten. Realized price PnL is calculated from
actual simulated entry/exit prices, so spread and impact are already embedded in
price PnL and are exposed as attribution fields rather than subtracted twice.

Advanced funding, cross-venue stat-arb, dated-basis, options-volatility, and
passive-market-making intents have a separate multi-leg PAPER planner and broker.
The planner is bound to one content-addressed execution snapshot containing the
exact per-leg L2 book, venue fee schedule, quantity/tick rules, and data-quality
state. Missing, future, stale, crossed, or insufficient-depth books fail closed.
Post-only instructions require an explicit non-crossing limit price and simulated
maker fills require matching trade evidence; a book snapshot alone cannot fill a
passive quote. Entry expiry cancels unfilled quantities and automatically flattens
every filled orphan leg. Orders, fills, positions, and the event-consumer checkpoint
are committed atomically and restored without resubmitting historical plans. Option
quotes directly trigger only the options family after the underlying feature state is
ready; an individual call or put is ignored until its matching fresh pair exists.
Matching requires the same venue, quote, settlement asset, expiry, and strike.
PAPER fills use the actual option bid/ask, visible quantity, venue fee schedule, and
contract multiplier for notional, fees, exposure, and PnL. Bybit and OKX option
trading fees use the venue formula
`min(account fee rate * underlying index, fee cap * option premium) * size`;
the default non-VIP maker/taker rates and 7% cap are configurable because the
effective account tier and region can differ. Every entry and exit fill uses the
underlying index observed for that fill, rather than reusing the entry index. The
approved package size is capped by the most restrictive scaled delta, gamma, vega,
daily-theta, stress-loss, and visible-liquidity limit retained in the intent. The
runtime does not represent an
expiry exercise/delivery charge as zero: new entries without the configured
pre-expiry exit buffer are rejected, and PAPER holding time ends at least 15 minutes
before expiry by default.

For RUN_MODE=live, the multi-regime pipeline is forcibly downgraded to
SHADOW. It cannot construct a paper broker or submit exchange orders. Existing
authenticated funding execution is a separate path. Directional live execution
remains unavailable until paper/shadow acceptance evidence and an explicit later
authorization exist.

One-leg breakout and sweep/reversion intents use the directional planner and broker.
The synchronized advanced signal types use the multi-leg planner and broker only in
PAPER. Runtime funding execution is additionally suppressed while the legacy funding
pipeline owns settlement. An advanced intent cannot reach planning without
an exact execution snapshot and an approved multi-leg portfolio-risk decision; a
post-risk planning failure is stored as an explicit execution block. The default
runtime projects funding, lead-lag, dated-basis, options-volatility, and
passive-market-making contexts from synchronized evidence. Public option collection
does not grant account authority: LIMITED_LIVE and LIVE suppress advanced execution,
the options strategy receives no live operator authorization, and SAFE_MODE suppresses
every intent. Research-only Martingale, grid, and loss-averaging strategies remain
non-executable in every mode.

Read-only inspection endpoints:

- GET /multi-regime/status
- GET /multi-regime/paper/summary
- GET /multi-regime/paper/positions?limit=100
- GET /regimes
- GET /strategies
- GET /signals
- GET /risk

`GET /strategies` retains the original top-level directional fields and ordering
(including `score`) and appends advanced suite rows with family/context/evaluation
identity and raw audit payload. This is additive for existing readers.

Candidate output uses simulator version v32-multi-regime-candidate so its totals do
not mix with v31 data. Directional and advanced positions, OMS orders, fills, and
checkpoints are also isolated by simulator version. Portfolio snapshots have explicit legacy and
combined scopes: funding-runner restore consumes legacy balances, while Telegram,
analytics, and replay prefer the authoritative combined scope whenever multi-regime
PAPER execution is enabled. This prevents a later funding-only cycle from hiding
multi-regime PnL. Combined funding plus multi-regime snapshots feed the daily
Telegram human-readable DAILY and ALL TIME sections. Directional and advanced fills,
fees, opens, closes, and active positions are included in the totals; embedded
spread/impact remains cost attribution and is not subtracted twice.

The Telegram report deliberately excludes simulator versions, signal counters,
cycle diagnostics, reconciliation internals, and other system telemetry. Those
details remain available in logs and metrics; the user-facing message contains
only financial results, trade counts, costs, balance, and open positions.

Local integration tests prove the canonical-event-to-snapshot-to-risk-plan-to-
durable PAPER position lifecycle, including restart recovery and multi-leg
compensation. `scripts/multi_regime_paper_probe.py` extends the directional proof to the deployment
PostgreSQL engine without touching application rows: it requires an unarmed
`paper_test/mock/PAPER` host with no private exchange credentials, creates a unique
temporary PostgreSQL database from `template0`, directs every probe dependency to
that database, drives a synthetic risk-approved entry through a target-triggered
protective exit, restarts from the durable checkpoint, reconciles quantity, OMS,
fills, fees, gross/net PnL and the equity invariant, then disconnects, drops, and
verifies removal of that exact database.

Run it only in the isolated validation Compose project:

    python scripts/multi_regime_paper_probe.py \
      --confirm I_UNDERSTAND_THIS_WRITES_SYNTHETIC_PAPER_DATA \
      --output artifacts/multi-regime-paper-probe.json

The probe never constructs exchange adapters and fails closed when host paper
autotrade, live arming/autotrade, or any private exchange credential is present. It
does not claim exchange-hosted directional protective orders, directional
limited-live/live execution, or completed shadow/paper acceptance windows.

The exact PostgreSQL acceptance result and its source/image hashes are retained in
`evidence/runtime/`. That evidence closes the runtime lifecycle requirement only;
the elapsed Shadow, Paper, and Limited-Live gates remain separate and incomplete.
