# V1 acceptance host provisioning

`GATE-001` and `GATE-002` are elapsed operational gates. They need a Linux host
with Docker, a root-owned release identity, and an uninterrupted process for 72
hours and 30 days respectively. This runbook prepares such a host and starts one
window. It does not enable exchange orders, withdrawals, or private keys, and it
never authorizes Limited Live.

Read `ops/ACCEPTANCE_RUNTIME_RUNBOOK.md` first: it owns the collector contract
and the exact env profiles. This document covers only the host around it.

## Host requirements

| Requirement | Why |
| --- | --- |
| Ubuntu 24.04, x86_64 | Matches the CI runner and the sealed candidate image |
| Docker Engine 27+ with Compose v2 | The overlay uses `pull_policy: never` and a measured image ID |
| Uninterrupted uptime for the window | The journal is opened `O_EXCL`; a restart cannot continue a window |
| UTC clock with `chrony` synchronized | Every sample carries a timezone-explicit timestamp |
| >= 8 GiB RAM, >= 10 GiB free disk | Enforced by `scripts/host_preflight.sh` |
| A `funding` system user at UID 10001 | The container runs unprivileged as `10001:10001` |

Final trusted verification is Linux-only: it needs descriptor-relative `openat`,
`O_DIRECTORY`, and `O_NOFOLLOW`. Other platforms fail closed.

## Shared-host boundary

A host may carry unrelated projects. Everything below is scoped to this project
and nothing else may be touched:

- Use a dedicated Compose project name, `funding_arbitrage_v1`.
- Use dedicated directories under `/opt`, `/srv`, and `/var/backups` — never a
  broad system path. `infra/terraform/main.tf` enforces this with lifecycle
  preconditions.
- Never run a bare `docker system prune`, `docker compose down` without `-p`,
  or `docker stop $(docker ps -q)`. Those reach other projects.
- Publish nothing to a host port: PostgreSQL, Redis, and ClickHouse stay on
  loopback, and `scripts/host_preflight.sh` fails if 5432, 9108, or 9109 are
  reachable from anywhere else.
- Pick a window ID that is unique across every project on the host, because the
  evidence directory and journal path are derived from it.

## Prepare the host

```bash
sudo apt-get update && sudo apt-get install --yes chrony age jq
sudo timedatectl set-timezone UTC
sudo chronyc waitsync 10 0.1
```

Create the unprivileged runtime user the container maps to:

```bash
sudo groupadd --gid 10001 funding || true
sudo useradd --uid 10001 --gid 10001 --system --no-create-home funding || true
```

## Prepare immutable paths

Replace the window ID with a new, never-reused value. The evidence directory is
writable only by container UID 10001; the release identity is public metadata,
root-owned, read-only, and contains no credentials.

```bash
export WINDOW_ID=gate-001-release-001
sudo install -d -o 10001 -g 10001 -m 0700 "/srv/funding-arbitrage-v1/acceptance/${WINDOW_ID}"
sudo install -d -o root -g root -m 0755 /run/funding-arbitrage
```

## Build and measure the candidate

The window must run one immutable image built from a clean checkout.

```bash
cd /opt/funding-arbitrage-v1
test -z "$(git status --porcelain)"
export RELEASE_REVISION="$(git rev-parse HEAD)"
test "${#RELEASE_REVISION}" -eq 40
docker build --pull=false --tag funding-arbitrage-acceptance-candidate .
export ACCEPTANCE_IMAGE="$(docker image inspect --format '{{.Id}}' funding-arbitrage-acceptance-candidate)"
test "${ACCEPTANCE_IMAGE#sha256:}" != "$ACCEPTANCE_IMAGE"
```

Write the acceptance env profile from `ops/ACCEPTANCE_RUNTIME_RUNBOOK.md`, then
create the identity once inside that exact image. The command has no network and
does not load a live-credential env file:

```bash
sudo docker run --rm --network none --read-only --user 0:0 \
  --env-file "/srv/funding-arbitrage-v1/acceptance/${WINDOW_ID}.env" \
  --mount type=bind,src=/run/funding-arbitrage,dst=/run/funding-arbitrage \
  "$ACCEPTANCE_IMAGE" python scripts/runtime_acceptance.py identity \
  --code-revision "$RELEASE_REVISION" \
  --image-digest "$ACCEPTANCE_IMAGE" \
  --output /run/funding-arbitrage/release-identity.json
sudo chown root:root /run/funding-arbitrage/release-identity.json
sudo chmod 0444 /run/funding-arbitrage/release-identity.json
```

The application independently recalculates the effective configuration and the
complete runner digest. A mismatched identity aborts startup.

## Start the window

```bash
export ACCEPTANCE_EVIDENCE_DIR="/srv/funding-arbitrage-v1/acceptance/${WINDOW_ID}"
export ACCEPTANCE_RELEASE_IDENTITY_FILE=/run/funding-arbitrage/release-identity.json
export APP_ENV_FILE="/srv/funding-arbitrage-v1/acceptance/${WINDOW_ID}.env"
export APP_RUNTIME_SECRETS_ENV_FILE=/dev/null
export APP_TELEGRAM_SECRETS_ENV_FILE=/dev/null
docker compose -p funding_arbitrage_v1 \
  -f docker-compose.yml -f docker-compose.acceptance.yml config --quiet
docker compose -p funding_arbitrage_v1 \
  -f docker-compose.yml -f docker-compose.acceptance.yml up -d app
```

Do not pass `--build`, do not replace the image, and do not restart the
container for the whole window. `ACCEPTANCE_IMAGE` must stay the measured
`sha256:...` ID.

## While the window runs

Check progress without touching the process:

```bash
docker compose -p funding_arbitrage_v1 ps app
tail -n 3 "/srv/funding-arbitrage-v1/acceptance/${WINDOW_ID}/${WINDOW_ID}.jsonl"
```

The collector blocks entries until a clean eight-venue first checkpoint and
permanently blocks them after any acceptance violation, so a failed window is
visible in the journal rather than silently continuing.

## Assemble, seal, verify

```bash
PYTHONPATH=src python scripts/runtime_acceptance.py assemble \
  --journal "/srv/funding-arbitrage-v1/acceptance/${WINDOW_ID}/${WINDOW_ID}.jsonl" \
  --attachments "evidence/runtime/${WINDOW_ID}-attachments.json" \
  --output "evidence/runtime/${WINDOW_ID}-raw.json"

PYTHONPATH=src python scripts/acceptance_window.py seal \
  --input "evidence/runtime/${WINDOW_ID}-raw.json" \
  --output "evidence/runtime/${WINDOW_ID}-sealed.json"
```

A locally sealed bundle does not complete either gate. Final verification
additionally requires the immutable replay root, a collector envelope signed by a
trusted Ed25519 collector key, an anchor receipt signed by a *different* trusted
key, and a reviewed trust policy under `config/acceptance_trust/`. No policy
ships by default; see `docs/V1_ACCEPTANCE_WINDOWS.md`.

## What this runbook does not do

- It does not enable exchange orders. Both acceptance modes leave the
  application unable to submit one.
- It does not enable withdrawals, DEX, MEV, or any other dangerous capability.
  Those need their flag *and* `DANGEROUS_CAPABILITY_AUTHORIZATION`.
- It does not make Limited Live eligible. `GATE-003` is a separate protected
  approval described in `docs/V1_RELEASE_APPROVAL.md`.
