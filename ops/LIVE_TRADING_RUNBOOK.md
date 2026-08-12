# Live trading runbook

This branch can submit real IOC orders. It remains inert unless all live mode
interlocks and venue credentials are present. Use dedicated subaccounts with no
manual orders, positions, deposits, or withdrawals while the process runs.

## Account preparation

1. Create a dedicated subaccount on every enabled venue. Transfer only the
   intended small canary collateral.
2. Enable one-way position mode, isolated margin, and 1x leverage. The service
   verifies/configures these before its first order.
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
mkdir -p .runtime
printf 'operator emergency stop\n' > .runtime/LIVE_DISABLED
```

With the default `LIVE_LIQUIDATE_ON_PAUSE=true`, the persistent switch blocks
new entries and requests bounded, exact risk-reducing closes as soon as fresh
depth is available. If an order becomes `UNKNOWN`, a second leg is not
filled, reconciliation differs, or an unwind is incomplete, the service creates
the same switch automatically and sends a Telegram safety alert.

Do not clear the switch until every venue's orders, positions, balances, and the
database ledger have been inspected. Resolve `MANUAL_INTERVENTION` records and
run a fresh reconciliation before deleting the file. Never blindly restart into
an unknown order state.

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
