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
      -> PAPER-only deterministic order-book fills
      -> OMS/fill/position projections + paper checkpoint
      -> stop, target, time-stop, or partial-entry flatten

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

The active runtime currently projects a conservative passive-market-making context
from the canonical L2 book, order-flow snapshot, ATR, configured venue maker fee,
paper position-size cap, and directional paper inventory. It never grants live
operator authority. Other advanced contexts require their dedicated synchronized
multi-instrument projections; absence means no evaluation rather than invented
zero-cost data.

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

Directional PAPER execution is enabled only when all of these conditions hold:

- effective mode is PAPER;
- MULTI_REGIME_PAPER_EXECUTION_ENABLED=true;
- PAPER_AUTOTRADE=true;
- the shared entry-health gate and portfolio risk authority approve the signal.

Fills use fresh canonical books, venue-specific maker/taker fees, bounded
participation, latency, spread, and nonlinear impact. Aggressive fills walk visible
L2 levels with a displayed-depth participation cap. Every partial market exit is a
separate deterministic child IOC order, and realized PnL is accrued on each exit fill.
A partially entered position is protected immediately: stop breach cancels the entry
remainder and starts a reduce-only flatten. Realized price PnL is calculated from
actual simulated entry/exit prices, so spread and impact are already embedded in
price PnL and are exposed as attribution fields rather than subtracted twice.

For RUN_MODE=live, the multi-regime directional pipeline is forcibly downgraded to
SHADOW. It cannot construct a paper broker or submit exchange orders. Existing
authenticated funding execution is a separate path. Directional live execution
remains unavailable until paper/shadow acceptance evidence and an explicit later
authorization exist.

Only one-leg breakout and sweep/reversion intents currently have a verified planner
and directional PAPER broker in this runtime. In PAPER, LIMITED_LIVE, and LIVE,
accepted raw outcomes from every other strategy family are normalized as
`execution_planner_unavailable` and cannot enter orchestration. In SHADOW/BACKTEST/
REPLAY they may enter orchestration for measurement, but the decision batch records
an explicit execution block before risk or planning. SAFE_MODE suppresses every
intent. This separation prevents an implemented research strategy from being
mistaken for an executable venue path.

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
not mix with v31 data. Directional positions, OMS orders, fills, and checkpoints are
also isolated by simulator version. Portfolio snapshots have explicit legacy and
combined scopes: funding-runner restore consumes legacy balances, while Telegram,
analytics, and replay prefer the authoritative combined scope whenever directional
PAPER execution is enabled. This prevents a later funding-only cycle from hiding
directional PnL. Combined funding plus directional snapshots feed the existing daily
Telegram human-readable DAILY and ALL TIME sections; directional fills, fees,
opens, and closes
are included, while embedded spread/impact is called out explicitly.

The Telegram report deliberately excludes simulator versions, signal counters,
cycle diagnostics, reconciliation internals, and other system telemetry. Those
details remain available in logs and metrics; the user-facing message contains
only financial results, trade counts, costs, balance, and open positions.

This slice now proves canonical-event-to-risk-plan-to-durable PAPER position and PnL
lifecycle. `scripts/multi_regime_paper_probe.py` extends that proof to the deployment
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
