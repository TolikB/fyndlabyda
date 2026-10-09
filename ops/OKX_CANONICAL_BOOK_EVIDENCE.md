# OKX Canonical Order-Book Evidence

## Contract implemented

- public `books` channel, with a 400-level initial snapshot and 100 ms
  incremental updates;
- `prevSeqId` must match the prior `seqId`; documented sequence resets are
  applied without discarding the reconstructed book;
- zero quantities delete levels;
- empty same-sequence heartbeat updates preserve liveness without creating a
  fake depth event;
- the deprecated checksum is not used because OKX fixes it to `0` as of
  2026-06-23 and requires `seqId`/`prevSeqId` validation instead.

Primary contracts:

- https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel
- https://www.okx.com/en-us/help/okx-order-book-channels-checksum-field-deprecation

## Public mainnet probe

Run:

```powershell
python scripts/okx_canonical_book_probe.py
```

The probe uses public instrument discovery and public WebSocket market data
only. It requires an initial canonical snapshot followed by a canonical delta
before reporting success. No API key, private stream, order, position, or VM is
used.

Observed on 2026-08-16:

```json
{"best_ask":"62952.2","best_bid":"62952.1","book_sequence":334174705983,"delta_events":1,"event_id":"evt_72aad00873d762b656b6fe8489e79a48eda0480a3298addb38cf9c6c48ba9f9f","event_kind":"BOOK_DELTA","event_sequence":"prev:334174705847:seq:334174705983","event_source":"OKX.PUBLIC.ORDERBOOK.BOOKS","exchange_timestamp":"2026-08-16T10:05:49.306000+00:00","quality":"VALID","receive_timestamp":"2026-08-16T10:05:49.448071+00:00","snapshot_events":1,"status":"ok","symbol":"BTC-USDT-SWAP"}
```

This proves the current public schema and the initial snapshot-to-delta path for
OKX. It does not yet prove sustained cadence, reconnect recovery, private
streams, or trading safety.
