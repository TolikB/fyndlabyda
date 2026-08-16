# MEXC, KuCoin, and HTX Canonical Order-Book Evidence

MEXC futures uses its documented incremental protocol: the adapter disables
merged updates, waits for subscription acknowledgement, buffers early WebSocket
frames, obtains an authoritative REST snapshot, and then applies absolute level
updates only when every version is exactly consecutive. A duplicate is
journalled but not re-emitted; a gap is journalled as `GAP`, blocks the book, and
forces complete resynchronization. Contract quantities are converted to base
asset quantities at the venue boundary.

MEXC spot, KuCoin and HTX feeds publish requested-depth snapshots. Their shared
boundary validates venue and instrument identity, requires a native snapshot
identifier (KuCoin spot uses its documented snapshot timestamp), creates a
deterministic canonical `BOOK_SNAPSHOT`, and awaits durable journal publication
before returning the legacy book to strategy consumers.

Sources:

- `MEXC.PUBLIC.FUTURES.DEPTH.REST_BOOTSTRAP`
- `MEXC.PUBLIC.FUTURES.DEPTH.INCREMENTAL`
- `MEXC.PUBLIC.SPOT.LIMIT_DEPTH`
- `KUCOIN.PUBLIC.SPOT.LEVEL2DEPTH50` / `KUCOIN.PUBLIC.FUTURES.LEVEL2DEPTH50`
- `HTX.PUBLIC.SPOT.DEPTH.STEP0` / `HTX.PUBLIC.FUTURES.DEPTH.STEP0`

Authoritative protocols:

- MEXC contract REST depth, incremental WebSocket depth, and strict
  version/recovery rules:
  <https://mexcdevelop.github.io/apidocs/contract_v1_en/>
- KuCoin spot level-50 snapshots:
  <https://www.kucoin.com/docs-new/3470070w0>
- KuCoin futures level-50 snapshots:
  <https://www.kucoin.com/docs-new/3470097w0>

Public combined probe:

```powershell
python scripts/remaining_cex_canonical_book_probe.py
```

The probe uses public instrument discovery, public WebSocket data, and no API
keys, private streams, orders, positions, or VM.

Verified public result on 2026-08-16:

```text
MEXC futures BTC_USDT version 40758604065
  source=MEXC.PUBLIC.FUTURES.DEPTH.INCREMENTAL quality=VALID
  bid=62979.2 ask=62979.3
MEXC spot BTCUSDT sequence 78957073418
  source=MEXC.PUBLIC.SPOT.LIMIT_DEPTH quality=VALID
  bid=63019.94 ask=63019.95
KuCoin spot BTC-USDT sequence 1786877180267
  source=KUCOIN.PUBLIC.SPOT.LEVEL2DEPTH50 quality=VALID
  bid=62997.6 ask=62997.7
HTX perpetual BTC-USDT sequence 100126171615083
  source=HTX.PUBLIC.FUTURES.DEPTH.STEP0 quality=VALID
  bid=62972.1 ask=62972.2
```
