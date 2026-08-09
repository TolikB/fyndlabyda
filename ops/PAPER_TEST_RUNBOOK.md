# Paper-test deployment runbook

This deployment behaves like a continuously running production service while
using public market-data adapters and `PaperTradingExecutor` only. It never
uses exchange trading credentials and never sends a live order. A fully
offline deterministic mock profile is also available.

## Required VM prerequisites

- Linux VM with Docker Engine and Docker Compose v2.
- At least 2 CPU, 4 GB RAM, and persistent disk for PostgreSQL.
- TCP access to port `8000` for the API/dashboard, and optionally `9090` and
  `3000` for Prometheus/Grafana. Keep database and Redis ports private.

## Start

```bash
cd /path/to/funding-bot
cp .env.paper-live-data.example .env
docker compose up -d --build
docker compose ps
```

The app runs Alembic migrations before Uvicorn. The paper runner starts in the
background and performs a cycle every 15 seconds with real public data from
Bybit, Gate, OKX, Binance, and Hyperliquid. All five venues receive $1,000 of
tradable virtual balance plus a separate reserve.

## Verify

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/health/ready
curl http://127.0.0.1:8000/portfolio
curl http://127.0.0.1:8000/analytics/paper
curl http://127.0.0.1:8000/metrics | grep funding_paper_runner
docker compose logs -f app
```

After the first cycle, `/health/ready` becomes `ready`. After confirmation and
the configured hold/settlement intervals, `/analytics/paper` shows fills,
funding payments, closed positions, fees, and the equity curve.

## Telegram daily report

Set these values in `.env` when the bot credentials are ready:

```dotenv
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=<bot-token>
TELEGRAM_CHAT_ID=<chat-id>
TELEGRAM_TIMEZONE=Europe/Kyiv
TELEGRAM_REPORT_HOUR=0
TELEGRAM_REPORT_MINUTE=0
```

The report is generated for the previous local calendar day and sent once per
day. The database ledger prevents duplicate reports after restarts. Until the
token and chat ID are set, no Telegram request is made.

## Stop without deleting data

```bash
docker compose stop
docker compose start
```

Do not use `docker compose down -v` unless the PostgreSQL paper history should
be intentionally deleted.

## Configuration knobs

- `PAPER_INITIAL_BALANCE_USD`: virtual starting equity.
- `PAPER_POSITION_SIZE_USD`: virtual capital per opportunity.
- `PAPER_MAX_HOLD_SECONDS`: automatic paper close time.
- `PAPER_SETTLEMENT_INTERVAL_SECONDS`: accelerated mock funding event interval.
- `PAPER_LOOP_INTERVAL_SECONDS`: runner cadence.
- `PAPER_MAX_OPEN_POSITIONS`: portfolio cap.

The accelerated settlement interval changes wall-clock test speed only; it does
not enable real funding or real trading.
