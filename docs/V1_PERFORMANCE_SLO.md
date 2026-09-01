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

The plain local JSON is diagnostic output, not release evidence. A local commit-bound envelope can be generated for inspection from an exact clean commit:

```bash
PYTHONPATH=src python scripts/load_slo.py \
  --release-evidence \
  --revision 0123456789abcdef0123456789abcdef01234567 \
  --output artifacts/load-slo.json
```

Evidence mode verifies that `--revision` equals the checked-out Git `HEAD` and refuses a dirty working tree. It fails closed unless the exact V1 workload, durable SQLite OMS, and fixed latency budgets are used. It records the 40-hex code revision, UTC measurement time, operating system, architecture, Python implementation/version, and execution source. Evidence files are created exclusively and are never overwritten. The JSON is canonicalized and accompanied by `load-slo.json.sha256`; the bounded regular-file loader rejects missing or mismatched sidecars, symbolic-link final paths, duplicate JSON keys, non-finite numbers, extra schema fields, inconsistent counters/pass claims, and revision mismatches.

CI runs the exact representative workload on Ubuntu, requires the CLI identity to match the trusted GitHub runner environment, and rechecks Git state immediately after measurement. Every initialized attempt retains run metadata and a diagnostic log (which can be empty when setup fails) under a collision-free commit/run/attempt artifact name for 30 days. A completed exact-profile measurement additionally contains the JSON and checksum; a failed benchmark can retain only its run metadata and log. Main-branch pushes pass the successful artifact to a separate trusted-push job with elevated permissions. For public repositories GitHub creates a signed SLSA provenance attestation for the JSON and checksum, and image publication depends on successful completion of that job; private repositories still pass through the trusted job without making an unsupported public attestation. Pull-request code receives read-only contents permission only. The adjacent SHA-256 sidecar detects corruption but is not, by itself, proof of origin; provenance comes from the GitHub workflow artifact/attestation context. Smaller counts are permitted only in unit tests; they cannot be wrapped as release evidence. QA-004 remains partial until a successful external CI run produces that artifact. Real acceptance still requires the elapsed shadow and paper windows in GATE-001 and GATE-002.
