# Encrypted backup and restore runbook

## Backup contract

Backups are PostgreSQL custom dumps streamed directly into `age`; plaintext is
never written to disk. Every archive has a SHA-256 sidecar and JSON manifest with
commit, Alembic head, size, timestamp, and exact Compose project. The backup root
must be a dedicated mode-0700 directory containing the exact marker
`funding-arbitrage-v1` in `.funding-backup-root`.
Each run holds a non-blocking lock for this backup root. Publication is atomic:
the encrypted archive, checksum, and manifest are moved into place before a
`.complete` checksum set for both the encrypted archive and JSON manifest is
published last. Restores reject incomplete or internally inconsistent sets and
backups whose Alembic head changed during the dump.
Database tools derive `PGPASSWORD`, `PGUSER`, and `PGDATABASE` from the running
PostgreSQL container only. Passwords and deployment-specific database names are
never copied into the host shell or placed on a command line.
The source commit is mandatory: an immutable archive supplies `.release-sha`, a
Git checkout supplies `HEAD`, or the operator supplies `RELEASE_COMMIT_SHA`. The
sealed CI candidate artifact includes `.release-sha`. The running app image must
carry the same `org.opencontainers.image.revision` label. All available sources
must match exactly, tracked Git changes are rejected, and provenance is checked
both before and after `pg_dump`; `unknown` provenance is rejected. These hashes
detect corruption and mismatch but do not replace off-host object lock or an
independent signature for authenticity.

Configure an offline age recipient and run from the immutable checkout:

```bash
export AGE_RECIPIENT='age1...'
export BACKUP_ROOT=/var/backups/funding-arbitrage-v1
export COMPOSE_PROJECT_NAME=funding_arbitrage_v1
sudo --preserve-env=AGE_RECIPIENT,BACKUP_ROOT,COMPOSE_PROJECT_NAME,RELEASE_COMMIT_SHA \
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
Alembic head differs from the currently running database. Create the exact
root-owned restore fence before stopping the systemd unit. The unit has an
`ExecCondition` that prevents an automatic restart while this marker exists.
Because stopping the unit stops the whole Compose project, start only PostgreSQL
again before invoking restore. The restore script disables the stopped app
container restart policy, locks the exact maintenance marker for the full
operation, and refuses broad paths, wrong project names, missing markers or
sidecars, bad checksums, a concurrent restore, a running app, missing safety
backup, or an inexact confirmation phrase.

```bash
cd /opt/funding-arbitrage-v1
export RESTORE_CHANGE_TICKET=DRILL-2026-001
export RESTORE_MAINTENANCE_MARKER=/opt/funding-arbitrage-v1/.restore-maintenance
sudo sh -c 'set -C; umask 077; printf "%s\n" "$1" > "$2"' sh \
  "funding-arbitrage-v1-restore:${RESTORE_CHANGE_TICKET}" \
  "$RESTORE_MAINTENANCE_MARKER"
sudo systemctl stop funding-arbitrage-v1.service
sudo docker compose --project-name funding_arbitrage_v1 --env-file .env.live \
  --file docker-compose.yml up --detach postgres
export BACKUP_ROOT=/var/backups/funding-arbitrage-v1
export COMPOSE_PROJECT_NAME=funding_arbitrage_v1
export PRE_RESTORE_BACKUP=/var/backups/funding-arbitrage-v1/funding-v1-postgres-NEW.dump.age
export AGE_IDENTITY_FILE=/root/.config/age/funding-v1-backup-identity.txt
export CONFIRM_RESTORE=RESTORE_FUNDING_V1_POSTGRES_AND_KEEP_APP_STOPPED
export MAX_PRE_RESTORE_BACKUP_AGE_SECONDS=900
sudo --preserve-env=BACKUP_ROOT,COMPOSE_PROJECT_NAME,PRE_RESTORE_BACKUP,AGE_IDENTITY_FILE,CONFIRM_RESTORE,RESTORE_CHANGE_TICKET,RESTORE_MAINTENANCE_MARKER,MAX_PRE_RESTORE_BACKUP_AGE_SECONDS \
  bash scripts/restore_state.sh \
  /var/backups/funding-arbitrage-v1/funding-v1-postgres-TARGET.dump.age
```

`pg_restore` uses `--single-transaction --exit-on-error`; after restore, Alembic
is advanced and critical tables are queried. The application remains stopped and
fenced. `AGE_IDENTITY_FILE` is mandatory, resolved to a regular file, must be
owned by the restore operator, must have a non-writable operator-owned parent,
and must not expose any group/world permission bits. Run reconciliation, ledger
invariants, deterministic replay, and report checks. The restore deliberately
sets the stopped app container policy to `no`; explicitly restore the declared
Compose policy before removing the fence. Only then remove the exact fence and
restart the unit:

```bash
app_container_id="$(sudo docker compose --project-name funding_arbitrage_v1 \
  --env-file .env.live --file docker-compose.yml ps --all --quiet app)"
sudo docker update --restart=unless-stopped "$app_container_id"
test "$(sudo docker inspect "$app_container_id" --format '{{.HostConfig.RestartPolicy.Name}}')" = unless-stopped

sudo grep -Fxq "funding-arbitrage-v1-restore:${RESTORE_CHANGE_TICKET}" "$RESTORE_MAINTENANCE_MARKER"
sudo rm -- "$RESTORE_MAINTENANCE_MARKER"
sudo systemctl start funding-arbitrage-v1.service
```

Legacy backups containing `git_commit: "unknown"` or the former one-line
`.complete` marker are intentionally rejected by automated restore. Recreate a
current-format encrypted backup from the original environment when possible; if
that is impossible, use a separately reviewed offline recovery procedure. Never
edit a manifest or sidecar to bypass the gate.

## Schedule and evidence

- Encrypted backup: daily and before every migration/release.
- Off-host object-lock replication: after each successful backup.
- Checksum sampling: weekly.
- Full disposable-VM restore drill: quarterly.
- Record archive hash, source commit, migration head, duration, row/invariant
  checks, operator, and ticket. A backup is not accepted until a restore drill has
  proven it usable.