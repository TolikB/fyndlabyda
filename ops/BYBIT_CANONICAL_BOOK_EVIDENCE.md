# Bybit Canonical Order-Book Evidence

## 2026-08-16 public mainnet probe

Command:

```powershell
python scripts/bybit_canonical_book_probe.py
```

Safety boundary: public REST and public WebSocket only; no API key, private
stream, order submission, position mutation, or VM access.

Observed result:

```json
{"best_ask":"62981.60","best_bid":"62981.50","book_sequence":63001416,"event_id":"evt_e14c8897ff8bbf7b1c763d58eb3751ba79df9eb3d0f43bb9d2e4a15e02972e61","event_kind":"BOOK_SNAPSHOT","event_sequence":"u:63001416:seq:770225913175","event_source":"BYBIT.PUBLIC.ORDERBOOK.50","exchange_timestamp":"2026-08-16T09:50:24.127000+00:00","quality":"VALID","receive_timestamp":"2026-08-16T09:50:24.551065+00:00","status":"ok","symbol":"BTCUSDT"}
```

Verified:

- official instrument discovery supplied canonical BTC/USDT perpetual metadata;
- an official `orderbook.50.BTCUSDT` snapshot parsed with `u`, `seq`, and `cts`;
- the canonical event existed before the legacy `OrderBook` was returned;
- event and returned book shared update ID `63001416`;
- reconstructed top of book was uncrossed and marked `VALID`;
- deterministic event ID and both exchange/receive timestamps were present.

Primary contracts:

- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- https://bybit-exchange.github.io/docs/v5/market/orderbook

This is live-schema evidence for Bybit only. It is not evidence for the other
venues, private streams, sustained cadence, restart recovery, or trading safety.

## 2026-08-16 update-sequence continuity check

A second read-only sample captured twelve consecutive native
`orderbook.50.BTCUSDT` frames directly from the public WebSocket:

```text
type: snapshot, u: 63005941, seq: 770232251756
type: delta,    u: 63005942 ... 63005952
```

The native `u` values advanced by exactly one across the sample while `seq`
advanced by varying amounts. The adapter therefore uses `u` for per-topic gap
detection and retains `seq` in canonical event metadata for cross-depth
correlation. No API key, private stream, or trading endpoint was used.
