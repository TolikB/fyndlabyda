# Gate Canonical Order-Book Evidence

Gate's subscribed `spot.order_book` and `futures.order_book` channels provide
complete requested-depth snapshots. Each update now becomes a deterministic
canonical `BOOK_SNAPSHOT` carrying the native `lastUpdateId`/`id`, exchange
timestamp, receive timestamp, instrument identity, and data quality before the
book is exposed to strategy code.

Public probe:

```powershell
python scripts/gate_canonical_book_probe.py
```

The probe uses public REST discovery and public WebSocket depth only. No API
key, private stream, order, position, or VM is used.

Primary contract:

- https://www.gate.com/docs/developers/apiv4/ws/en/#order-book-channel

Observed on 2026-08-16:

```json
{"best_ask":"62960.1","best_bid":"62960","book_sequence":121680483199,"event_id":"evt_18962cb1730541684e88c3add932113f4b638d2b74b0f11c9daee14e5f862f89","event_kind":"BOOK_SNAPSHOT","event_sequence":"snapshot:121680483199","event_source":"GATE.PUBLIC.FUTURES.ORDER_BOOK","exchange_timestamp":"2026-08-16T10:20:41.866000+00:00","quality":"VALID","receive_timestamp":"2026-08-16T10:20:42.025717+00:00","status":"ok","symbol":"BTC_USDT"}
```

This proves the current public perpetual snapshot schema, not sustained cadence,
private streams, or trading safety.
