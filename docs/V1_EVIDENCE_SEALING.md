# V1 evidence sealing and recorded verification

`config/v1_acceptance.yaml` is the machine-checked source of V1 delivery status.
Two tools keep it honest: one seals what the manifest claims, the other proves
the claim by running it.

## What each status means

| Status | Meaning | What produces it |
| --- | --- | --- |
| `missing` | no acceptable implementation evidence exists | — |
| `partial` | reusable code exists, the full requirement is not proved | reviewed edit |
| `implemented` | code and focused tests exist | reviewed edit |
| `validated` | plus integration, replay, sandbox, or failure-injection evidence | `scripts/acceptance_verification.py --promote` |
| `accepted` | plus every configured threshold and elapsed-time gate | reviewed edit after the external gates pass |

No tool in this repository promotes anything to `accepted`. That still requires
the elapsed Shadow and Paper windows, the external CI artifacts, the reviewed
trust policy, and the protected Limited Live approval.

## Sealing

```bash
PYTHONPATH=src python scripts/seal_acceptance_evidence.py
```

Sealing rewrites the manifest deterministically and does three things:

1. **Expands directory evidence into explicit files.** `accepted` requires
   regular files, so a broad directory could never be sealed. Compiled artifacts
   are excluded, and the result is sorted and deduplicated.
2. **Derives the `verification` commands** each requirement needs, from the test
   modules it already lists as evidence. Two delivery requirements whose proof is
   not a focused test module carry a reviewed override instead.
3. **Records `evidence_sha256`** for every `validated` and `accepted`
   requirement. `implemented` requirements stay unsealed: they claim code and
   focused tests, not a reviewed evidence bundle.

Sealing never changes a status and never invents evidence. Drift is a build
failure:

```bash
PYTHONPATH=src python scripts/seal_acceptance_evidence.py --check
```

That reports every requirement whose sealed digests no longer match content,
whose digests do not cover its evidence, whose verification is out of date, or
which carries digests it is not entitled to.

## Recorded verification

```bash
PYTHONPATH=src python scripts/acceptance_verification.py \
  --output evidence/verification/local-verification.json
```

The runner executes every distinct verification command once, records its exit
code and duration, and attributes the result to each requirement that names it.
The artifact is bound to one Git revision and one manifest digest, so a recorded
run cannot be reused across releases.

Commands are executed without a shell. A command that would need shell quoting is
refused rather than silently split, and commands that need an authenticated CLI
(`gh`) are recorded as `external` instead of being executed, so a local run can
never claim to have proved something it did not run.

## Mechanical promotion

`--promote` moves an `implemented` requirement to `validated` only when both hold:

- every verification command it names passed in this run, and
- it carries integration-grade evidence — a test module that stands up a real
  database engine or applies the migration chain, or a committed artifact under
  `evidence/`.

Passing unit tests alone never promote a requirement. A requirement whose proof
is entirely in-process stays `implemented`, which is the accurate description of
what it has.

## Order of operations

1. Change code and tests.
2. Update the evidence lists for anything new.
3. Seal, so paths and digests match content.
4. Run recorded verification, optionally with `--promote`.
5. Seal again, so a newly promoted requirement records its digests.
6. `PYTHONPATH=src python scripts/v1_acceptance_audit.py` for the structural check.
