# V1 completion audit

The audit has three non-circular stages, all bound to an immutable commit:

1. `--require-release-prerequisites` requires the 68 non-terminal requirements
   accepted before protected Limited Live configuration approval.
2. `--require-final-candidate` requires those 68 plus `GATE-003` accepted before
   issuing the terminal `GATE-004` completion evidence.
3. `--require-accepted` requires all 70 requirements accepted and is the final V1
   state check.

Every stage also requires `--expected-revision <40-character-commit>`.

The manifest is complete only when every one of its 70 requirements has status
`accepted`. An accepted requirement must contain non-empty verification identifiers
and an exact SHA-256 digest for every evidence path. Accepted evidence must name
regular files; broad directories must be expanded to explicit reviewable files.
Digests bind path and content. Absolute paths, parent traversal, symbolic links,
missing paths, non-file evidence, and digest mismatches are rejected.

Each gated stage additionally requires a clean Git repository, exact `HEAD`
equality with the supplied immutable revision, and Git-object equality for the
manifest and every accepted evidence file against that commit. The resulting JSON
includes the stage, revision, and manifest digest. No
status is promoted automatically and no verification result is inferred from a
file merely existing.

The protected Limited Live workflow runs the 68-requirement prerequisite stage
before its manual configuration approval. A later immutable candidate containing
that approval evidence can run the final-candidate stage and, after `GATE-004` is
accepted, the all-70 state check. The current V1 manifest intentionally fails all
three stages while elapsed Shadow/Paper and external CI, infrastructure, restore,
and load evidence are incomplete.
