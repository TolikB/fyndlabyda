# Encrypted backup and restore runbook

## Backup contract

Backups are PostgreSQL custom dumps streamed directly into `age`; plaintext is
never written to disk. Every archive has a SHA-256 sidecar and JSON manifest with
commit, Alembic head, size, timestamp, and exact Compose project. The backup root
must be a dedicated mode-0700 directory containing the exact marker
`funding-arbitrage-v1` in `.funding-backup-root`.
Database tools derive `PGPASSWORD` from `POSTGRES_PASSWORD` only inside the
PostgreSQL container. The password value is never copied into the host shell or
placed on a command line.

Configure an offline age recipient and run from the immutable checkout:

```bash
export AGE_RECIPIENT='age1...'
export BACKUP_ROOT=/var/backups/funding-arbitrage-v1
export COMPOSE_PROJECT_NAME=funding_arbitrage_v1
sudo --preserve-env=AGE_RECIPIENT,BACKUP_ROOT,COMPOSE_PROJECT_NAME \
  bash scripts/backup_state.sh
```

Copy the encrypted archive and sidecars to versioned object storage with object
lock and retention. Test the off-host copy checksum. The script never prunes old
backups; retention deletion is a separately reviewed storage policy.

## Restore drill

Restore is destructive and must be performed first on a disposable isolated VM.
Select the archive and verify its off-host provenance. Immediately before the
restore, create a distinct current-state backup with `scripts/backup_state.sh`;
by default the restore refuses a safety backup older than 15 minutes or whose
Alembic head differs from the currently running database. Stop only the exact
application service after that backup; keep PostgreSQL running. The restore
script refuses broad paths, wrong project names, missing
markers/sidecars, bad checksums, a running app, missing safety backup, or an
inexact confirmation phrase.

```bash
sudo systemctl stop funding-arbitrage-v1.service
export BACKUP_ROOT=/var/backups/funding-arbitrage-v1
export COMPOSE_PROJECT_NAME=funding_arbitrage_v1
export PRE_RESTORE_BACKUP=/var/backups/funding-arbitrage-v1/funding-v1-postgres-NEW.dump.age
export AGE_IDENTITY_FILE=/root/.config/age/funding-v1-backup-identity.txt
export CONFIRM_RESTORE=RESTORE_FUNDING_V1_POSTGRES_AND_KEEP_APP_STOPPED
export RESTORE_CHANGE_TICKET=DRILL-2026-001
export MAX_PRE_RESTORE_BACKUP_AGE_SECONDS=900
sudo --preserve-env=BACKUP_ROOT,COMPOSE_PROJECT_NAME,PRE_RESTORE_BACKUP,AGE_IDENTITY_FILE,CONFIRM_RESTORE,RESTORE_CHANGE_TICKET,MAX_PRE_RESTORE_BACKUP_AGE_SECONDS \
  bash scripts/restore_state.sh \
  /var/backups/funding-arbitrage-v1/funding-v1-postgres-TARGET.dump.age
```

`pg_restore` uses `--single-transaction --exit-on-error`; after restore, Alembic
is advanced and critical tables are queried. The application intentionally stays
stopped. `AGE_IDENTITY_FILE` is mandatory, resolved to a regular file, must be
owned by the restore operator, and must not expose any group/world permission
bits. Run reconciliation, ledger invariants, deterministic replay, and report
checks before an explicit operator restart.

## Schedule and evidence

- Encrypted backup: daily and before every migration/release.
- Off-host object-lock replication: after each successful backup.
- Checksum sampling: weekly.
- Full disposable-VM restore drill: quarterly.
- Record archive hash, source commit, migration head, duration, row/invariant
  checks, operator, and ticket. A backup is not accepted until a restore drill has
  proven it usable.