# V1 strategy-suite contract

`funding_arbitrage.services.strategy_suite` is the common evaluation boundary for
all implemented V1 strategy families. It solves a specific authority problem: a
strategy may explain a thesis and emit an expiring `SignalIntent`, but it cannot
approve capital, create exchange instructions, or submit an order.

## Boundary

Every request carries a canonical source-event ID, trading mode, UTC decision time,
one or more typed contexts, and deterministic request identity. The suite validates:

- every context has the same trading mode as the request;
- no context is newer than decision time;
- identical contexts are not evaluated twice;
- every evaluation has exactly one outcome: intent or rejection;
- strategy, mode, intent, and timestamp identities agree;
- evaluation and signal IDs are unique;
- evaluation identity includes the complete raw payload, not only intent/rejection;
- replaying identical input produces byte-equivalent JSON output.

Each normalized record retains the strategy's raw JSON evaluation as audit evidence.
This preserves metrics such as forecast settlements, carry, volatility risk, quote
proposal, and rejection details even when the execution-capability gate suppresses
the raw intent.

## Execution authority

The suite's output flows through signal orchestration, portfolio risk, an explicit
strategy-specific planner, durable OMS, and finally a mode-matched adapter. No stage
may be skipped.

Optional ML/RL/LLM output crosses `services.decision_support` before orchestration.
The support envelope includes the exact signal ID, SHA-256 fingerprint of the full
intent, UTC evaluation time, and each model's native audited decision. The gate has
only three effects:

- reject the intent when a meta-label rejects, RL requests `CLOSE`, or LLM rejects;
- reduce the portfolio-risk multiplier for RL/LLM reduction actions;
- pass without increasing risk. An RL increase request is retained in audit but
  ignored.

For LLM output, the boundary also recomputes the canonical request hash and checks
that audit action, reason, fallback flag, schema versions, prompt version, and UTC
timestamp match the bound request and decision. A self-consistent support checksum
cannot hide a mismatched request/audit pair.

The multiplier is applied by `PortfolioRiskAuthority` after every ordinary order,
position, asset, strategy, venue, correlation, cash, liquidity, volatility, margin,
and stop-risk cap. AI therefore cannot manufacture an intent, approve a rejected
trade, increase size, construct an order, or authorize execution. LLM inference
remains asynchronous upstream; the synchronous canonical event engine consumes only
the already schema-, budget-, latency-, model-allowlist-, and audit-checked result.
It is combined with an existing risk-context reduction using the smaller value, so
optional decision support cannot relax a prior conservative limit.

Every durable batch validates the complete `intent -> support -> orchestration ->
risk -> plan -> instruction` chain. A plan must reference an approved same-signal
risk decision and may not exceed the instrument, side, hedge ratio, or quantity of
the approved intent. The PAPER broker repeats the authority checks before creating
an order so even a malformed legacy or in-memory batch cannot bypass an AI veto.

The directional planner supports one-leg `ORDERFLOW_BREAKOUT` and
`LIQUIDITY_SWEEP_REVERSION`. A separate synchronized planner supports PAPER intents
for funding, cross-exchange stat-arb, dated-futures basis, options volatility, and
passive market making. It requires a content-addressed snapshot with a fresh exact
L2 book, data quality, venue fees, and quantity/tick rules for every leg. The
snapshot, intent fingerprint, approved risk decision, plan, and each instruction
are identity-bound. A missing/stale/crossed book, insufficient displayed depth,
invalid post-only price, or changed intent is stored as an explicit block and no
order is created.

The advanced PAPER broker repeats the full authority-chain validation, models
partial fills, requires trade evidence for passive maker fills, and automatically
compensates filled orphan legs after entry failure. Its OMS/fill/position projection
shares the canonical event checkpoint transaction, so restart cannot silently
resubmit an accepted plan. LIMITED_LIVE and LIVE still suppress these advanced
paths, and SAFE_MODE suppresses all intents. Adding a signal type to an allowlist
alone therefore cannot make it executable.

Funding execution remains on its existing, independently guarded two-leg funding
pipeline as the single settlement owner. The canonical suite evaluates the same
as-of funding evidence but suppresses its runtime intent, preventing duplicate
exposure and funding attribution while exact legacy settlement and reconciliation
remain authoritative.

Startup rebuilds market/features without invoking strategy or AI providers for
historical events. Persisted source-event-bound batches restore the orchestrator's
seen and active state, and current policy/config must reproduce their priority,
correlation, and allocation projections. This keeps restart behavior deterministic
and prevents changed model artifacts or exhausted budgets from rewriting history.

The application does not yet instantiate a live meta-label, RL, or LLM provider.
Their typed policies, audit gateway, bounded support contract, persistence, and
restart path are implemented, but runtime activation still requires versioned model
artifact loading and synchronized feature projection. Until then assessments are
empty rather than fabricated, and no AI claim is made for deployed behavior.

## Runtime projection

The production application supplies funding, cross-venue lead-lag, dated-basis, and
passive-market-making evaluation inputs from one strict as-of projection. Funding
uses robust history and exact per-venue settlement timestamps; lead-lag requires two
independent fresh reference venues; dated basis requires an actual future expiry and
same-venue perpetual; market making uses canonical L2/order flow, ATR, inventory, and
venue fees. Missing evidence produces no context. A missing ATR disables only market
making, not unrelated synchronized strategies.

Every executable advanced intent still requires an independent content-addressed
per-leg snapshot and the multi-leg portfolio-risk boundary. Runtime funding intents
are intentionally execution-suppressed while the legacy funding pipeline is the sole
position/settlement owner. Options contexts remain empty until a real synchronized
public options chain is integrated. Live operator authorization is always false.
