# V1 authoritative data retention and incident reconstruction

V1 treats `canonical_events`, `canonical_journal_profiles`,
`ledger_transactions`, `ledger_postings`, `reconciliation_audits`, and
`immutable_audit_log` as authoritative evidence.
PostgreSQL rejects `UPDATE`, `DELETE`, and `TRUNCATE` on these tables. Normal
application roles must have insert/select only; a schema owner is reserved for
migrations and documented break-glass recovery.
Migration downgrade refuses to remove journal-profile evidence once profile or
profile-bound checkpoint rows exist. Restore validation requires both immutable
profile triggers, and the restore drill checks a seeded profile row in the exact
critical-state checksum and proves that mutation remains rejected after restore.

## Retention contract

- Keep authoritative rows online for at least 730 days.
- Keep immutable, object-locked archives for at least 2,555 days with at least
  two independent replicas.
- Keep PostgreSQL point-in-time recovery for at least 35 days.
- Never run automatic deletion against authoritative tables. A future disposal
  requires an approved legal/retention change, a verified archive, and a
  separately audited break-glass migration.
- Build replay datasets with the versioned Parquet writer. Preserve the
  manifest, schema fingerprint, file checksums, code SHA, and config hash.

## Incident workflow

1. Freeze the affected time window and record a repeatable-read PostgreSQL
   snapshot ID, deployed code SHA, and redacted config hash.
2. Export canonical events and all relevant ledger/audit rows. Seal them as an
   `IncidentEvidenceBundle`; verification must pass before analysis.
3. Export the same window to immutable Parquet and verify all part, dataset,
   manifest, schema, cardinality, and replay-order checksums.
4. Restore into an isolated environment, replay in authoritative event order,
   and reconcile fills, balances, positions, funding, ledger, and control-plane
   actions. Do not use Redis or mutable projection tables as evidence authority.
5. Store the evidence bundle and Parquet dataset in object-locked storage, then
   record the archive object versions in the incident report.

Quarterly restore drills must prove that a randomly selected window can be
verified and replayed from both PostgreSQL backup and object-locked Parquet.
