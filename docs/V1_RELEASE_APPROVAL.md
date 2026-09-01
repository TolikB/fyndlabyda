# V1 release approval

Limited Live approval is a configuration-only, fail-closed GitHub Environment
gate. It does not arm execution, install credentials, submit orders, or authorize
withdrawals.

`scripts/manual_live_gate.py` accepts only a manual workflow dispatch on `main`,
an immutable 40-character commit, the exact confirmation phrase, the expected
workflow/run identity, and the protected `limited-live-approval` environment.
Every V1 manifest requirement except the two
terminal release gates must already have status `accepted`; `implemented`,
`validated`, `partial`, and `missing` all block approval. This includes accepted
Shadow and Paper elapsed-window evidence, research, failure-injection, security,
infrastructure, restore, and load-SLO gates.
The restore CI job emits a typed, checksummed, transient envelope for the exact
commit and sealed candidate image; a successful console message alone is not
evidence. That envelope proves exact authoritative PostgreSQL state, backup-age
bounds, database-restore duration, crash-stage recovery, a stopped application,
and plaintext cleanup. It explicitly does **not** prove end-to-end service
recovery, ClickHouse projection rebuilding, independent attestation, or durable
evidence retention. Consequently it cannot, by itself, satisfy restore acceptance
or authorize a release. Accepted restore evidence additionally requires separately
authorized external retention/attestation plus verified service recovery and
ClickHouse rebuild. Security-sensitive Redis loss also requires operator-controlled
JWT-secret rotation and kill-switch reassertion before startup.
The load-SLO prerequisite must identify the same sealed candidate image and source
commit that passed the other release jobs; source-checkout-only performance output
is diagnostic and cannot satisfy release approval.

The requirement ID set is fixed to the complete 70-item V1 contract. Removing a
requirement, adding an unreviewed substitute, or omitting either terminal gate
blocks approval.

The emitted canonical JSON attestation binds the repository, commit, workflow actor,
workflow/run identity, protected environment, ref, exact manifest-file digest,
and the fact that it caused no real-order side effect. `GITHUB_ACTOR` is recorded as
the workflow actor and is never mislabelled as the protected-environment reviewer;
the actual reviewer identity remains authoritative in GitHub's deployment audit.
The JSON and adjacent SHA-256 sidecar are retained under a commit/run/attempt-unique
artifact name together with the successful 68-requirement prerequisite-audit JSON.
Public-repository runs additionally receive GitHub/Sigstore provenance over all three
files.
A successful
attestation is only a prerequisite for a separately armed Limited Live deployment;
all runtime credential, risk, reconciliation, and operator interlocks remain
mandatory.

Before the protected environment approval, CI runs the 68-requirement prerequisite
stage from `docs/V1_COMPLETION_AUDIT.md` against the same immutable commit. The
approval attestation can then become evidence for the separate final-candidate and
all-70 completion stages; no gate approves itself.

The current repository intentionally does not satisfy this gate: elapsed Shadow
and Paper evidence and several external delivery validations are still pending.
Their absence must remain visible rather than being converted into synthetic
approval.
