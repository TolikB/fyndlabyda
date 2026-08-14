# Live trading runbook

This branch can submit real IOC orders. It remains inert unless all live mode
interlocks and venue credentials are present. Use dedicated subaccounts with no
manual orders, positions, deposits, or withdrawals while the process runs.

## Account preparation

1. Create a dedicated subaccount on every enabled venue. Transfer only the
   intended small canary collateral.
2. Enable one-way position mode on the CCXT-backed venues, isolated margin, and
   1x leverage. MEXC uses Hedge Mode because its authenticated order API has
   explicit open-long/open-short/close-long/close-short sides. The service
   verifies/configures the venue-specific mode before its first order.
3. Pay fees in the quote/settlement currency. Do not keep BNB, GT, OKB, HYPE,
   or other fee-token inventory in the dedicated accounts: it complicates
   authoritative USD equity accounting. A spot-buy fee taken from the acquired
   base asset is detected and deducted from hedge quantity. Every unexplained
   spot balance fails dedicated-account reconciliation.
4. Bind each API credential to the fixed server IP. Grant read and trade only;
   never grant withdrawal, transfer, wallet-management, or API-key-management
   permissions.
5. Do not reuse credentials from another bot. Do not paste secrets into source,
   Git, issue trackers, logs, or chat.

For MEXC, complete KYC before enabling futures API trading. Create a dedicated
API key with spot/contract account read and spot/contract deal read+write only.
Disable withdrawals and transfers, bind the key to the server IP, and verify
that API futures trading is available for the account/region. The adapter reads
MEXC's exact account fee and contract size during preflight; a missing market,
permission, KYC status, or private response stops live readiness.

MEXC paper mode needs no key. It uses public spot and futures REST/WebSocket
feeds, exact per-contract funding cycles, next settlement timestamps, and
contract-size conversion. MEXC live mode uses the authenticated spot V3 and
contract V1 APIs with deterministic client order IDs and REST reconciliation.

## Configuration and preflight

Copy `.env.live.example` to `.env`, replace the database password, choose the
smallest useful `LIVE_VENUES` subset, and insert only those venue credentials.
The exact confirmation phrase and `LIVE_ARMED=true` are mandatory. Keep the
initial $100 per-leg limit and 1x leverage for the canary.

Before startup, ensure `.runtime/LIVE_DISABLED` does not exist. Then validate
without starting the service:

```bash
docker compose config --quiet
docker compose build app
docker compose run --rm app alembic upgrade head
```

The startup sequence loads private markets/balances/positions/orders and
reconciles them with PostgreSQL. `/health/ready` remains HTTP 503 unless this
passes. Inspect safe state (never secrets) through:

```bash
curl -fsS http://127.0.0.1:8000/system/live
curl -fsS http://127.0.0.1:8000/health/ready
curl -fsS http://127.0.0.1:8000/metrics | grep funding_live
```

## Emergency stop

Block all new entries immediately with:

```bash
docker compose exec app sh -c \
  "printf 'operator emergency stop\\n' > /app/.runtime/LIVE_DISABLED"
```

The dedicated `runtime_state` volume is writable by the otherwise read-only,
unprivileged app container and survives container recreation. To inspect the
switch without exposing credentials, run
`docker compose exec app ls -l /app/.runtime/LIVE_DISABLED`.

With the default `LIVE_LIQUIDATE_ON_PAUSE=true`, the persistent switch blocks
new entries and requests bounded, exact risk-reducing closes as soon as fresh
depth is available. If an order becomes `UNKNOWN`, a second leg is not
filled, reconciliation differs, or an unwind is incomplete, the service creates
the same switch automatically and sends a Telegram safety alert.

Do not clear the switch until every venue's orders, positions, balances, and the
database ledger have been inspected. Resolve `MANUAL_INTERVENTION` records and
run a fresh reconciliation before deleting the file. Never blindly restart into
an unknown order state.

Only after that inspection, clear it with
`docker compose exec app rm /app/.runtime/LIVE_DISABLED` and restart the app.

## Canary sequence

1. Start with two venues, BTC/ETH only, 1x leverage, $100 per leg, one position.
2. Observe at least one complete open, funding settlement, and two-leg close.
3. Compare exchange statements with `live_orders`, `live_positions`,
   `live_account_snapshots`, and the Telegram equity report.
4. Run at least 72 hours without reconciliation failures before changing one
   risk limit at a time.

Daily and total PnL are calculated from authenticated account equity, including
venue fees and unrealized PnL. External transfers contaminate equity-delta PnL;
do not move funds during a reporting period without recording a new baseline.

MEXC does not provide a separate exchange sandbox through this service. Test
the same strategy in `RUN_MODE=paper_test`, `MARKET_DATA_MODE=live_public`, and
`EXECUTION_MODE=paper`; only the explicit live interlocks above can activate
authenticated order submission.
