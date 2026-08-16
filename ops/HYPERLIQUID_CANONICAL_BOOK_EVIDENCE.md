# Hyperliquid Canonical Order-Book Evidence

Each public `l2Book` message is a complete L2 snapshot. It is now normalized
into a deterministic canonical `BOOK_SNAPSHOT` with the native millisecond
`time` as sequence and exchange timestamp, plus receive timestamp, instrument
identity, and data quality before strategy use.

Public probe:

```powershell
python scripts/hyperliquid_canonical_book_probe.py
```

The probe uses public metadata and public WebSocket depth only. No wallet,
private stream, order, position, API key, or VM is used.

Primary contract:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions

Observed on 2026-08-16:

```json
{"best_ask":"62962.0","best_bid":"62961.0","book_sequence":1786875910529,"event_id":"evt_18be505e0bd7b004db6698ffb2c890265eb7a2946c8d6df7884bfb8c5203a3e9","event_kind":"BOOK_SNAPSHOT","event_sequence":"time:1786875910529","event_source":"HYPERLIQUID.PUBLIC.L2BOOK","exchange_timestamp":"2026-08-16T10:25:10.529000+00:00","quality":"VALID","receive_timestamp":"2026-08-16T10:25:10.927679+00:00","status":"ok","symbol":"BTC"}
```

This proves the current public snapshot schema, not sustained cadence, wallet
reconciliation, or trading safety.
