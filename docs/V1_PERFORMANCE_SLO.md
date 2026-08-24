# V1 performance and reliability SLO

This gate measures the in-process canonical decision and OMS contract path. It never opens a socket, loads private credentials, or submits an exchange order. It is a component-level release SLO, not an end-to-end benchmark of the funding paper or live runners: those use transactional PostgreSQL journals and are covered by their integration, restart, reconciliation, and elapsed canary gates.

`SqliteOMSJournal` is the deployable standalone `DurableOMS` backend and the release-harness backend. Historical replay deliberately substitutes the same journal protocol with an in-memory implementation because the replay dataset is already immutable and versioned.

## Representative workload

The default CI run publishes 20,000 canonical L2 events through `CanonicalEventRouter`, including deterministic sequence gaps and snapshot recovery. It then prepares 5,000 unique paper decisions through `StrictSignalValidator`, the risk and plan contracts, and `DurableOMS`; every valid order is persisted-before-submit, filled, and replayed once as an idempotent duplicate report. Release and CI runs use `SqliteOMSJournal` in WAL mode with `synchronous=FULL`, so create, submit-preparation, and fill are separate durable transactions. The in-memory journal is available only through the explicit unit-test profile.

Failure injection is part of the pass criterion: expired signals and plans exceeding risk-authorized quantity must be rejected, every injected book gap must recover from a snapshot, journal sequence must remain contiguous, and unexpected or invariant failures must stay at zero.

## Budgets

The versioned V1 in-process budgets are:

| Stage | P99 budget |
| --- | ---: |
| canonical event ingest | 10 ms |
| decision validation, risk, plan, and OMS create | 20 ms |
| OMS submit-preparation durable transaction | 10 ms |
| OMS fill-application durable transaction | 10 ms |
| full OMS submit + fill + duplicate-report handling | 10 ms |
| decision start through terminal fill and duplicate-report idempotency check | 30 ms |

The full OMS 10 ms gate preserves the original aggregate budget; the split submit and fill distributions make regressions attributable without relaxing it. These are application budgets, not exchange round-trip targets. Venue WebSocket age and authenticated submission latency remain separate Prometheus SLOs. A run passes only when every P99 budget and every reliability invariant pass.

Run locally:

```bash
PYTHONPATH=src python scripts/load_slo.py --output artifacts/load-slo.json
```

CI runs the default representative workload on Ubuntu. Smaller counts are permitted only in unit tests; they are not release evidence. Real acceptance still requires the elapsed shadow and paper windows in GATE-001 and GATE-002.
