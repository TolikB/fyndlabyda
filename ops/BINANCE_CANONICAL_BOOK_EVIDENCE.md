# Binance Canonical Order-Book Evidence

## Contract implemented

- subscribe to public spot/USD-M `diff depth` before requesting a REST depth
  snapshot, wait for the subscription ACK, and preserve any depth events that
  arrive before that ACK;
- preserve 1,000 levels locally while exposing only the requested strategy
  depth, preventing top-of-book holes after deletes;
- spot continuity uses the documented `U <= local_id + 1 <= u` range rule;
- USD-M uses the same bootstrap bridge, then requires each `pu` to equal the
  preceding `u`;
- quantities of zero delete price levels; any gap is journaled and forces a
  reconnect plus fresh REST bootstrap.

Primary contracts:

- https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly
- https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly

The REST spot depth response has no exchange timestamp. Its canonical bootstrap
event is explicitly sourced as `BINANCE.PUBLIC.ORDERBOOK.REST_BOOTSTRAP` and uses
the response-observation timestamp. Strategy output is withheld until a timed
WebSocket delta bridges the snapshot sequence.

## Public mainnet probe

Run:

```powershell
python scripts/binance_canonical_book_probe.py
```

The probe uses only public REST and public WebSocket market data. It requires a
canonical bootstrap snapshot followed by a synchronized canonical delta before
reporting success. No API key, private stream, order, position, or VM is used.

Observed on 2026-08-16 after adding the subscription ACK barrier:

```json
{"best_ask":"62955.60","best_bid":"62955.50","book_sequence":11297846585476,"delta_events":3,"event_id":"evt_2264bba49fd8c81e7323809dbdb94a24c808e09f6c57b7678ee90c95b6147b89","event_kind":"BOOK_DELTA","event_sequence":"U:11297846580902:u:11297846585476:pu:11297846580757","event_source":"BINANCE.PUBLIC.ORDERBOOK.DIFF_DEPTH","exchange_timestamp":"2026-08-16T10:15:53.094000+00:00","quality":"VALID","receive_timestamp":"2026-08-16T10:15:53.219103+00:00","snapshot_events":1,"status":"ok","symbol":"BTCUSDT"}
```

The single bootstrap snapshot confirms that this run did not require a gap
reconnect. This proves the current public USD-M schema and snapshot-to-delta
path, not sustained cadence, private streams, or trading safety.
