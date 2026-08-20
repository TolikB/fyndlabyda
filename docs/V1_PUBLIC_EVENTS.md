# V1 canonical public events

The production-shaped paper and guarded live runners persist supplemental public
market events for Binance, Bybit, Gate, OKX, Hyperliquid, MEXC, KuCoin, and HTX.
This pipeline never sends orders and never requires private credentials.

## Data contract

- Native adapters are authoritative for exact funding rate, settlement timestamp,
  mark price, and index price. Incomplete funding observations are rejected.
- CCXT Pro 4.5.73 supplies bounded WebSocket trades plus one-minute OHLCV and
  open-interest REST recovery. Contract quantities are converted to base units;
  venue-specific open-interest units are explicit in each profile.
- Public liquidation collection uses WebSocket on Binance, Bybit, and OKX, and
  REST on Gate and HTX. Hyperliquid, MEXC, and KuCoin expose no supported unified
  public liquidation endpoint in the pinned client, so capability is reported as
  unavailable and no synthetic events are created.
- Gate and MEXC open interest falls back to normalized native ticker observations
  because their pinned unified clients do not expose `fetchOpenInterest`.
- Every event has exchange and receive timestamps, deterministic identity,
  instrument type, source, sequence identity, and data-quality state.

The runner selects at most `PUBLIC_EVENT_SYMBOL_LIMIT_PER_PROFILE` instruments
for each spot or derivative account by normalized 24-hour volume. Stream failures
reconnect with bounded exponential backoff; REST polling is controlled by
`PUBLIC_EVENT_REST_INTERVAL_SECONDS`. The canonical event writer remains the
single durable sink and its existing failure gate blocks new entries.

Prometheus exports `funding_public_event_capability` for the explicit venue/account
matrix and `funding_public_events_total` by exchange, stream, and source. Invalid
records increment `funding_market_data_dropped_total` without logging payloads.

## Verification

`tests/test_public_events.py` covers contract conversion, exact funding fields,
OI fallback and de-duplication, fail-closed malformed events, and all eight venue
profiles. `tests/test_event_journal.py` proves liquidation events survive a full
append/replay round trip.
