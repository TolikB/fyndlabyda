# V1 handover: what is done, what is open, and who can close it

This states the manifest's true position and separates what remains engineering
work from what only the operator can authorize. Nothing here is a promise about
a requirement that is not already provable from `config/v1_acceptance.yaml`.

Run the audit yourself rather than trusting this summary:

```bash
PYTHONPATH=src python scripts/v1_acceptance_audit.py
```

## Position

70 requirements: **29 validated, 34 implemented, 5 partial, 2 missing, 0 accepted.**

`validated` means the requirement's recorded verification commands were run and
passed against this revision. `accepted` additionally requires sealed evidence
digests and, for the gates, elapsed operational proof. No requirement is
`accepted` yet, and all three audit stages fail by design while that is true.

The distinction matters: `implemented` is a claim about code, `validated` is a
claim about a command that ran, and `accepted` is a claim about evidence bound
to an immutable revision. Only the third is releasable.

## The five partial requirements

| Requirement | What is missing | Who closes it |
| --- | --- | --- |
| QA-002 | CI artifacts from a run on a **public** repository: SBOM attestation and keyless signing are gated on `repository.visibility == 'public'` | Operator, by making the repository public, or by accepting these as permanently unavailable |
| QA-003 | The infrastructure evidence exists per-run but is not committed as sealed evidence | Engineering, once a release candidate is chosen |
| QA-004 | The load SLO envelope must come from a successful external CI run, not a local one | Engineering, from a green run on the release commit |
| GATE-001 | 72 hours of unbroken SHADOW samples | **Running now** — see below |
| GATE-002 | 30 days of unbroken PAPER samples, with ≥30 fills, 15 closes, 29 daily reports | Operator, after GATE-001 |

## The two missing requirements

**GATE-003 — Limited Live approval.** This is the authorization to configure
real-money trading. It requires a protected `workflow_dispatch` with an operator
confirmation phrase, and all 68 other requirements accepted first. This is the
operator's decision alone. It has not been run, and no part of this work
attempts to satisfy it.

**GATE-004 — completion audit.** Terminal, and blocked until everything above is
accepted.

## GATE-001 is running

A window opened **2026-09-23T00:25:58Z** on revision
`c76c591daca0ffc6c499f7fac4ff0f5022a1fc78` and completes no earlier than
**2026-09-26T00:26Z**.

Check it without touching the process:

```bash
tail -n 3 /srv/funding-arbitrage-v1/acceptance/gate-001-c76c591/gate-001-c76c591.jsonl
```

Always supply the revision to the window script as `$(git rev-parse HEAD)`. An
earlier attempt was discarded because a hand-typed 40-hex revision shared the
real commit's 7-character prefix but named a commit that does not exist; gate
evidence is bound to the revision by Git-object equality, so it would have run
the full 72 hours and then failed verification. After starting, confirm that
`/run/funding-arbitrage/release-identity.json` reports exactly `git rev-parse HEAD`.

The gate fails closed on the **first** not-ready sample, so a window that has
failed shows it immediately and every later sample reports
`data_quality_valid: false` regardless of real health. Only the first such
sample is a signal.

**Do not restart the container.** The journal is opened `O_EXCL` and a restart
cannot continue a window; it has to start again with a new window ID and a
fresh database.

If it fails, `ops/V1_VPS_ACCEPTANCE_RUNBOOK.md` covers starting another, and the
analysis scripts left on the VM (`/tmp/whyfail.py`, `/tmp/cycles.py`,
`/tmp/tail.py`) each take the journal path and report per-sample readiness,
cycle progress against the verifier rule, and the collection-pass distribution.

## What the operator alone can decide

1. **Limited Live (GATE-003).** Nobody else can authorize real-money trading.
2. **Repository visibility**, which determines whether QA-002's attestation
   evidence can ever exist.
3. **The acceptance trust policy.** Final gate verification needs a reviewed
   policy under `config/acceptance_trust/`, an Ed25519 collector key, and an
   anchor receipt signed by a *different* key. None ships, and generating them
   is deliberately left to the operator — a self-signed chain would prove
   nothing.
4. **Starting the 30-day PAPER window (GATE-002)**, which needs Telegram
   configured and `PAPER_AUTOTRADE=true`.

## Safety posture

Unchanged throughout this work and independent of the gates:

- Every dangerous capability needs both its own `*_ENABLED` flag **and** its
  canonical name in `DANGEROUS_CAPABILITY_AUTHORIZATION`. One alone does nothing.
- The running window has no dangerous capability enabled, `TRADING_MODE=SHADOW`
  and `PAPER_AUTOTRADE=false`. It cannot submit an order.
- No private key material is read, generated or stored by any of this.
