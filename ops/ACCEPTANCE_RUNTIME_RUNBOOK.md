# Runtime acceptance collector

This runbook starts a new elapsed `GATE-001` Shadow or `GATE-002` Paper
measurement. It does not enable exchange orders and it must never reuse a
database, simulator version, window ID, journal path, or evidence file from an
earlier run.

## Safety boundary

- Use a dedicated PostgreSQL database with all migrations applied and no paper,
  OMS, ledger, report, reconciliation, withdrawal, or live-order rows.
- Leave every private exchange API credential empty. The process refuses to
  start the collector if any is present.
- Use exactly `bybit,gate,okx,binance,hyperliquid,mexc,kucoin,htx` with
  `MARKET_DATA_MODE=live_public` and `EXECUTION_MODE=paper`.
- The app remains unable to submit exchange orders in both acceptance modes.
  Entry simulation is blocked until the first healthy eight-venue checkpoint;
  any later runtime, accounting, venue, freshness, or journal failure disables
  entries permanently for that process.
- Do not restart the container. The journal is created with `O_EXCL`; a restart
  cannot continue or overwrite a window and therefore requires a new window.

## Prepare immutable paths

Run as root on the Linux host. Replace the example window and host paths with
new values. The evidence directory is writable only by container UID `10001`;
the release identity is public metadata, root-owned, read-only, and contains no
credentials.

```bash
install -d -o 10001 -g 10001 -m 0700 /srv/funding-arbitrage-v1/acceptance/gate-001-release-001
install -d -o root -g root -m 0755 /run/funding-arbitrage
```

Build the candidate once and capture its immutable local image ID. Configure a
dedicated acceptance env file before measuring its configuration hash, then
create the identity once inside that exact image. The command has no network,
does not load a live-credential env file, and uses the same acceptance env file
that Compose will load for the app:

```bash
test -z "$(git status --porcelain)"
export RELEASE_REVISION="$(git rev-parse HEAD)"
test "${#RELEASE_REVISION}" -eq 40
docker build --pull=false --tag funding-arbitrage-acceptance-candidate .
export ACCEPTANCE_IMAGE="$(docker image inspect --format '{{.Id}}' funding-arbitrage-acceptance-candidate)"
test "${ACCEPTANCE_IMAGE#sha256:}" != "$ACCEPTANCE_IMAGE"

docker run --rm --network none --read-only --user 0:0 \
  --env-file /srv/funding-arbitrage-v1/acceptance/gate-001-release-001.env \
  --mount type=bind,src=/run/funding-arbitrage,dst=/run/funding-arbitrage \
  "$ACCEPTANCE_IMAGE" python scripts/runtime_acceptance.py identity \
  --code-revision "$RELEASE_REVISION" \
  --image-digest "$ACCEPTANCE_IMAGE" \
  --output /run/funding-arbitrage/release-identity.json
chown root:root /run/funding-arbitrage/release-identity.json
chmod 0444 /run/funding-arbitrage/release-identity.json
```

The app independently recalculates the effective configuration and complete
runner digest. A mismatched identity aborts startup.

## Shadow configuration

```dotenv
RUN_MODE=paper_test
TRADING_MODE=SHADOW
MARKET_DATA_MODE=live_public
EXECUTION_MODE=paper
PAPER_AUTOTRADE=false
PAPER_COMPARISON_ENABLED=false
PAPER_AUTO_INIT_DATABASE=false
ACCEPTANCE_COLLECTOR_ENABLED=true
ACCEPTANCE_WINDOW_ID=gate-001-release-001
ACCEPTANCE_JOURNAL_PATH=/var/lib/funding-arbitrage/acceptance/gate-001-release-001.jsonl
ACCEPTANCE_SAMPLE_INTERVAL_SECONDS=240
```

## Paper configuration

Use a different clean database and immutable identifiers. Paper additionally
requires `PAPER_AUTOTRADE=true` and configured daily Telegram reporting so the
30-day gate can prove at least 29 reports. Private exchange credentials remain
empty.

```dotenv
RUN_MODE=paper_test
TRADING_MODE=PAPER
MARKET_DATA_MODE=live_public
EXECUTION_MODE=paper
PAPER_AUTOTRADE=true
PAPER_COMPARISON_ENABLED=false
PAPER_AUTO_INIT_DATABASE=false
TELEGRAM_ENABLED=true
ACCEPTANCE_COLLECTOR_ENABLED=true
ACCEPTANCE_WINDOW_ID=gate-002-release-001
ACCEPTANCE_JOURNAL_PATH=/var/lib/funding-arbitrage/acceptance/gate-002-release-001.jsonl
ACCEPTANCE_SAMPLE_INTERVAL_SECONDS=240
```

Start with the acceptance overlay and explicit host paths:

```bash
export ACCEPTANCE_EVIDENCE_DIR=/srv/funding-arbitrage-v1/acceptance/gate-001-release-001
export ACCEPTANCE_RELEASE_IDENTITY_FILE=/run/funding-arbitrage/release-identity.json
export APP_ENV_FILE=/srv/funding-arbitrage-v1/acceptance/gate-001-release-001.env
export APP_RUNTIME_SECRETS_ENV_FILE=/dev/null
export APP_TELEGRAM_SECRETS_ENV_FILE=/dev/null
docker compose -f docker-compose.yml -f docker-compose.acceptance.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.acceptance.yml up -d app
```

`ACCEPTANCE_IMAGE` must remain the measured `sha256:...` image ID exported
above. The overlay uses `pull_policy: never`; do not pass `--build` or replace
the image during the elapsed window. For Paper, put only the Telegram values in
the dedicated acceptance env file (or an equally dedicated Telegram env file)
and keep every exchange credential empty.

## Assemble and verify

Failure-injection and deterministic-replay jobs produce one reviewed
`acceptance-runtime-attachments` JSON document bound to the same revision,
image, and configuration digest. Assembly validates every identity and creates
the raw seal input without overwriting an existing path:

```bash
PYTHONPATH=src python scripts/runtime_acceptance.py assemble \
  --journal /srv/funding-arbitrage-v1/acceptance/gate-001-release-001/gate-001-release-001.jsonl \
  --attachments evidence/runtime/gate-001-release-001-attachments.json \
  --output evidence/runtime/gate-001-release-001-raw.json

PYTHONPATH=src python scripts/acceptance_window.py seal \
  --input evidence/runtime/gate-001-release-001-raw.json \
  --output evidence/runtime/gate-001-release-001-sealed.json
```

Use the final verification command in `docs/V1_ACCEPTANCE_WINDOWS.md` only after
the independent collector signature, external anchor receipt, immutable replay
root, and release-bundled trust policy exist. A locally assembled or sealed file
alone does not complete either gate.
