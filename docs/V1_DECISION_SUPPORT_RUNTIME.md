# V1 decision-support runtime

The canonical runtime can opt into local meta-label and constrained RL inference.
Both components are disabled by default, consume only the canonical as-of feature
projection, and can only reject an intent or reduce its risk multiplier. They cannot
create a signal, approve risk, construct an order, or authorize execution.

## Artifact activation

Runtime artifacts use one JSON-only `decision-support-artifacts-v1` bundle. Pickle,
joblib, shared libraries, and executable model formats are intentionally unsupported.
Activation requires all of the following:

1. the bundle resides below `DECISION_SUPPORT_ARTIFACT_ROOT`;
2. `DECISION_SUPPORT_ARTIFACT_BUNDLE_FILE` is a relative path;
3. the raw file matches `DECISION_SUPPORT_ARTIFACT_SHA256`;
4. the bundle, meta-label artifact, and RL artifact pass their own canonical SHA-256
   checksums and strict schema validation; unknown fields, duplicate keys, and
   non-standard numeric constants are rejected;
5. the file is regular, bounded by
   `DECISION_SUPPORT_ARTIFACT_MAXIMUM_BYTES`, and on POSIX is owned by root or the
   service user and is not group/world writable; every path component is traversed
   descriptor-relative without following symlinks;
6. every requested feature belongs to the fixed runtime feature schema, and an RL
   artifact uses `runtime-decision-support-state-v1`.

Enable the outer boundary and at least one component:

```dotenv
DECISION_SUPPORT_ENABLED=true
DECISION_SUPPORT_META_LABEL_ENABLED=true
DECISION_SUPPORT_RL_ENABLED=false
DECISION_SUPPORT_ARTIFACT_ROOT=/opt/funding_arbitrage/models/decision-support
DECISION_SUPPORT_ARTIFACT_BUNDLE_FILE=runtime-bundle.json
DECISION_SUPPORT_ARTIFACT_SHA256=<sha256-of-exact-file>
```

Any missing, stale, drifted, future, incomplete, tampered, or schema-incompatible
meta-label input deterministically rejects the associated intent. RL has a constrained
`HOLD/REDUCE_25/REDUCE_50/CLOSE` runtime action set; every runtime guardrail fallback
is `CLOSE`, so unavailable RL evidence vetoes a new intent instead of passing it at
full size. Positive position changes are never permitted. Drawdown is measured from
the durable portfolio high-water equity and restored across process restarts. Every
persisted local decision-support envelope includes the exact bundle checksum. Live RL
authorization is not exposed through application settings.

Freshness is evaluated per source rather than with one global TTL. Intent and
order-flow features use `MULTI_REGIME_STALE_AFTER_SECONDS`; technical features use
`MULTI_REGIME_STRATEGY_INTERVAL_SECONDS + MULTI_REGIME_SOURCE_INTERVAL_SECONDS`;
regime features use
`MULTI_REGIME_REGIME_INTERVAL_SECONDS + MULTI_REGIME_SOURCE_INTERVAL_SECONDS`; and
derivatives features use `FUNDING_SNAPSHOT_STALE_SECONDS`. The RL state timestamp is
set to the evaluation time only after every selected feature passes its own source
quality and freshness check. A slower valid regime update therefore remains usable,
while a stale order book or order-flow observation still fails closed immediately.
In live-shadow mode, drawdown also requires an equity observation no older than the
larger of three live-loop intervals or two request timeouts. Every newly observed
equity high-water is persisted immediately, even between periodic account snapshots,
so a restart cannot silently reset the peak used by the drawdown guardrail.

LLM inference remains asynchronous and outside the synchronous canonical event
engine. `GuardedLLMGateway` enforces schema, model allowlist, latency, token, spend,
confidence, and live-authorization limits; only its already audited result may cross
`BoundDecisionSupport`. The application performs no implicit network LLM call and
has no LLM credential setting.

## Operations and telemetry

On-call needs to answer:

1. Which local components were activated from a trusted bundle?
2. How many decisions accepted, rejected, reduced, closed, or used fallback?
3. Are feature projections becoming incomplete?
4. Is local inference latency changing?

The corresponding bounded-cardinality metrics are:

- `funding_decision_support_artifact_loaded{component}`;
- `funding_decision_support_decisions_total{component,outcome,fallback}`;
- `funding_decision_support_projection_failures_total{component}`;
- `funding_decision_support_inference_duration_seconds{component}`.

Activation logs include only a stable event name, a short bundle checksum correlation
ID, bundle version, and enabled-component flags. Model parameters, features, API keys,
and full artifact payloads are never logged. These diagnostics stay in application
logs/metrics and are not included in the human Telegram trading report.
