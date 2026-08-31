# V1 elapsed Shadow and Paper evidence

`GATE-001` and `GATE-002` are elapsed operational gates, not source-code labels.
They remain incomplete until an exact release produces a self-verifying evidence
bundle that passes `scripts/acceptance_window.py verify`.

## Fixed minimum policies

| Gate | Mode | Minimum window | Maximum sample gap | Additional proof |
| --- | --- | ---: | ---: | --- |
| GATE-001 | SHADOW | 72 hours | 300 seconds | configured cycle is at most 10s with at most one missed cycle per sample interval; decisions are suppressed; no simulated or real fills |
| GATE-002 | PAPER | 30 days | 300 seconds | same cadence plus at least 30 fills, 15 closes, two fill venues, exact book reconciliation, three cost components, and 29 daily reports |

Every sample is tied to one full Git revision, immutable image digest, redacted
configuration hash, process-start identity, ledger hash, runtime-state hash, and
authoritative source watermark. Samples are strictly ordered and SHA-256 chained.
The enclosing bundle is also checksummed and cannot mix releases or configurations.
The first sample must come from a fresh namespace with zero counters and zero
cost carry-in. Source watermarks are unique, and ledger/runtime hashes must advance
whenever their corresponding cumulative state changes.

Both gates fail closed on a readiness failure, missing venue, stale stream,
accounting drift above `$0.01`, restart, real-order submission, withdrawal request,
runner error, risk breach, unresolved reconciliation item, unknown order,
unprotected position, or data-quality incident. Cumulative counters and execution
costs must be monotonic. Each sample carries interval maxima and cumulative
incident counters, so an outage between two otherwise healthy point samples is
not hidden. The sealed runtime contract carries cycle and market/orderbook/funding
staleness settings. Those values must remain identical for the full window and be
no weaker than the verifier maxima of 10/30/120/180 seconds. For each sample gap,
the verifier derives the expected cycle count from the configured interval and
allows at most one missed cycle; canonical-market and strategy-evaluation counters
must meet the same progress floor. A mostly stalled process cannot pass by making
one large counter jump at the end.

The exact release must also provide passing artifacts for dependency outage,
partial fill, restart recovery, rate limiting, reconciliation drift, stale data,
unknown submission outcome, and WebSocket gap recovery. A deterministic replay of
an immutable dataset covering at least 30 days, 10,000 events, and all eight CEX
must produce identical result hashes twice. Dataset manifest, replay runner,
command, and result digests plus stable artifact references make the replay
rerunnable by an independent verifier. Failure and
replay evidence are bound to the same Git revision, image digest, and configuration
hash as the elapsed samples. Recovery budgets and the minimum of three injections
per scenario are fixed by verifier code; the evidence producer cannot relax them.
A scenario passes only when every injected fault is detected and recovered, no
unexpected effect occurs, and recovery stays within its fixed scenario budget.

Verification also compares the latest sealed timestamp with the current UTC clock.
Evidence dated more than five seconds into the future fails closed instead of
satisfying an elapsed-time requirement early. Naive date-times are rejected;
every evidence timestamp must carry an explicit timezone offset.

Each sealed bundle records `acceptance-policy-v1` and the canonical SHA-256 of its
complete verifier policy. A policy update invalidates an old bundle instead of
silently changing its result under new thresholds.

## Operator workflow

A deployment-specific collector must write a raw `AcceptanceWindowSealInput` JSON
document. It must not contain secrets or private exchange payloads. This change
adds the contract, sealer, and verifier; wiring the collector to an exact runtime
namespace is still required before an elapsed gate can begin. Raw input must use
`document_kind: acceptance-window-seal-input` and `schema_version: 1`; the
loader dispatches on both values. The checked-in schema is
`config/schemas/acceptance-window-seal-input-v1.json` and its drift check is:

```bash
PYTHONPATH=src python scripts/export_acceptance_schema.py --check
```

The schema encodes enforceable digest, revision, identity, timezone-offset, and
venue uniqueness patterns. The loader additionally requires strict UTF-8, rejects
duplicate keys and non-finite numbers, caps files at 64 MiB and nesting at 128
levels, and uses a single no-follow file descriptor on Linux to avoid path races.

Seal one completed
or checkpoint input once:

```bash
PYTHONPATH=src python scripts/acceptance_window.py seal \
  --input evidence/runtime/gate-001-raw.json \
  --output evidence/runtime/gate-001-sealed.json
```

Verify a sealed checkpoint or final window:

```bash
PYTHONPATH=src python scripts/acceptance_window.py verify \
  --bundle evidence/runtime/gate-001-sealed.json
```

For `verify`, exit code `0` is reserved for a fully accepted gate. Until trusted
signed collector/CI provenance, independent artifact resolution/replay, and an
external anchor are wired, `accepted` is always false and a summary-clean bundle
returns exit code `3`. The result separately reports `evidence_summary_satisfied`,
`independent_replay_verified`, `policy_satisfied`, and `trusted_provenance`.
Therefore caller-provided hashes, counts, dates, or artifact labels cannot produce
even `policy_satisfied=true`. The blocker list includes every failed summary check
plus missing independent replay/provenance. Exit code `2` means malformed,
mixed, non-immutable, or tampered evidence. Validation diagnostics never echo
rejected input values. `seal` returns `0` after writing a structurally valid
immutable bundle even when `accepted` is false. Existing evidence paths are never
overwritten.

The SHA-256 chain detects changes after sealing; it is not an operator identity or
CI signature. Consequently both gates intentionally remain `partial`. The
deployment collector, trusted signature verification, and external transparency
anchor still need to be wired before either gate can become accepted. A trusted
artifact resolver must also verify dataset/runner/command hashes, rerun the fixed
replay, and reconcile individual fills, books, and cost components; opaque evidence
references are intentionally insufficient.

This contract only makes evidence machine-verifiable. It does not grant order or
withdrawal authority and it does not make Limited Live eligible by itself.
