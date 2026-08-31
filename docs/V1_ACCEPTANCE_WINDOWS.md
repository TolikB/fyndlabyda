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
rerunnable by an independent verifier. The built-in verifier resolves only a
path-safe dataset reference below an explicitly trusted root; arbitrary commands
from evidence are never executed. It verifies the Parquet manifest, schema,
file bytes, release/config identity, per-venue temporal coverage, at least 30 fills
and 15 closes, fresh book linkage for every fill, and fees/spread/slippage before
running the versioned audit twice. Failure and replay evidence are bound to the
same Git revision, image digest, and configuration hash as the elapsed samples.
Recovery budgets and the minimum of three injections
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

A deployment-specific collector now writes a durable append-only JSONL journal
from the exact paper runner. It blocks entries until a clean eight-venue first
checkpoint, refuses private exchange credentials and reused namespaces, and
permanently blocks entries after any acceptance violation. The collector never
stores secrets or private exchange payloads. `scripts/runtime_acceptance.py`
binds it to the root-owned release identity and assembles the journal plus
independent failure/replay attachments into `AcceptanceWindowSealInput` JSON.
The full operator procedure is `ops/ACCEPTANCE_RUNTIME_RUNBOOK.md`. Raw input uses
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

Verify a sealed checkpoint without promoting it:

```bash
PYTHONPATH=src python scripts/acceptance_window.py verify \
  --bundle evidence/runtime/gate-001-sealed.json
```

Final verification additionally requires the immutable replay root, a collector
envelope signed by a trusted Ed25519 collector key, and a receipt signed by a
different trusted external-anchor key:

```bash
PYTHONPATH=src python scripts/acceptance_window.py verify \
  --bundle evidence/runtime/gate-001-sealed.json \
  --artifact-root evidence/immutable-replay \
  --collector-envelope evidence/provenance/collector-envelope.json \
  --anchor-receipt evidence/provenance/anchor-receipt.json \
  --trust-policy-id production-release-001
```

The verifier does not generate, read, or store private signing keys. Collector and
anchor roles must use different canonical Ed25519 public-key bytes, have explicit
validity windows and gate scopes, and sign domain-separated canonical payloads.
CLI callers cannot provide keyrings, pins, or trust-root paths.
`--trust-policy-id` resolves only a reviewed public policy under
`config/acceptance_trust/`; that release-bundled policy fixes the code, image,
configuration, environment, deployment, keyrings, independently reviewed replay
cost schedule, exact verifier implementation/dependency digest, and exact next
external-anchor sequence/head. No policy ships by default, so the gate fails closed
until one is reviewed and committed for a release.

Final provenance also requires the root-owned fixed-path runtime measurement at
`/run/funding-arbitrage/release-identity.json`. Its observed Git revision, image
digest, effective configuration hash, and complete application/verifier source plus
dependency digest must match the trust policy and the sealed window. CLI callers
cannot override this path. The file and every ancestor directory must be owned by
root and must not be group- or world-writable; loading is descriptor-relative and
does not follow symbolic links.

The replay runner digest includes the actual verifier module bytes, not only an
entrypoint label. Artifact input is copied through no-follow regular-file
descriptors into a private bounded snapshot before Parquet parsing. Final trusted
verification is Linux-only and requires descriptor-relative `openat`,
`O_DIRECTORY`, and `O_NOFOLLOW`; the root itself is opened component-by-component
from a pinned filesystem-root descriptor and all descendants reuse that pinned
root. Other platforms fail closed. Replay is limited to
25,000 rows, 64 parts, 64 MiB per part, 128 MiB compressed total, and 2 KiB per
result payload. The reader preflights Parquet metadata, then streams 64-row
batches while enforcing 128 MiB of actual Arrow buffers and 256 MiB of cumulative
decoded-object volume. Arrow string offsets are checked before conversion to
Python, and audit state plus deterministic hashes are updated incrementally; the
full replay is never materialized in Arrow or Python.
Every fill is tied to venue, instrument, order, position, and a
preceding fresh book; fee, borrow, gas/transfer, spread, and slippage economics are
recomputed from the release-bundled cost policy. That policy also declares which
instruments require borrow, gas, or transfer costs, so replay-controlled zero usage
cannot suppress required costs. Required borrow is reconciled independently for
every filled position at close, required gas for every fill, and required transfer
fees for every position lifecycle. Gas units come from the externally trusted cost
schedule rather than replay rows. Closed position IDs cannot be reused. Closes must
reference the same venue/instrument position, and fills must span at least two
venues.

For `verify`, exit code `0` means the summary, independent replay, collector
signature, and external anchor all passed. Without any one of them, `accepted`
is false and a structurally valid bundle returns exit code `3`. The result
separately reports `evidence_summary_satisfied`,
`independent_replay_verified`, `policy_satisfied`, and `trusted_provenance`.
Therefore caller-provided hashes, counts, dates, or artifact labels cannot produce
even `policy_satisfied=true`. The blocker list includes every failed summary check
plus missing independent replay/provenance. Exit code `2` means malformed,
mixed, non-immutable, or tampered evidence. Validation diagnostics never echo
rejected input values. `seal` returns `0` after writing a structurally valid
immutable bundle even when `accepted` is false. Existing evidence paths are never
overwritten.

The SHA-256 chain alone is not an operator identity or CI signature. The code now
contains the runtime collector plus independent artifact, collector-signature,
and anchor-receipt verification, but both gates intentionally remain `partial`
until the required real elapsed windows and separate external anchor produce
accepted evidence for one exact release.

This contract only makes evidence machine-verifiable. It does not grant order or
withdrawal authority and it does not make Limited Live eligible by itself.
