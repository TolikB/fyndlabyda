# V1 performance and reliability SLO

This gate measures the in-process critical path with the same canonical models and state machines used by runtime code. It never opens a socket, loads private credentials, or submits an exchange order.

## Representative workload

The default CI run publishes 20,000 canonical L2 events through `CanonicalEventRouter`, including deterministic sequence gaps and snapshot recovery. It then prepares 5,000 unique paper decisions through `StrictSignalValidator`, the risk and plan contracts, and `DurableOMS`; every valid order is persisted-before-submit, filled, and replayed once as an idempotent duplicate report. Release and CI runs use `JsonlOMSJournal`, so every create, submit-preparation, and fill transition is flushed and fsynced; the in-memory journal is available only through the explicit unit-test profile.

Failure injection is part of the pass criterion: expired signals and plans exceeding risk-authorized quantity must be rejected, every injected book gap must recover from a snapshot, journal sequence must remain contiguous, and unexpected or invariant failures must stay at zero.

## Budgets

The versioned V1 in-process budgets are:

| Stage | P99 budget |
| --- | ---: |
| canonical event ingest | 10 ms |
| decision validation, risk, plan, and OMS create | 20 ms |
| OMS submit-preparation and fill application | 10 ms |
| decision start to terminal OMS fill | 30 ms |

These are application budgets, not exchange round-trip targets. Venue WebSocket age and authenticated submission latency remain separate Prometheus SLOs. A run passes only when every P99 budget and every reliability invariant pass.

Run locally:

```bash
PYTHONPATH=src python scripts/load_slo.py --output artifacts/load-slo.json
```

CI runs the default representative workload on Ubuntu. Smaller counts are permitted only in unit tests; they are not release evidence. Real acceptance still requires the elapsed shadow and paper windows in GATE-001 and GATE-002.