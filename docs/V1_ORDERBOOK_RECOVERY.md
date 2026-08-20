# V1 order-book reconstruction and recovery

The V1 contract never converts a full-book snapshot into an invented delta. Every venue uses one of two explicit native modes:

- `SNAPSHOT_DELTA`: an authoritative snapshot plus venue deltas are reconstructed in `LocalOrderBook`; a gap or checksum mismatch makes the book non-tradable until a new authoritative snapshot.
- `SNAPSHOT_REPLACEMENT`: every message is an authoritative bounded L2 snapshot. It atomically replaces prior levels, so replaying synthetic deltas would be less accurate than consuming the venue payload directly.

L1 is the best bid/ask view of the validated L2 state. A crossed, empty, stale, gapped, invalid, or unavailable L2 state cannot produce a tradable L1.

The canonical multi-regime runtime independently replays every BookSnapshot and
BookDelta through LocalOrderBook; it never assumes that an adapter's legacy
OrderBook projection is authoritative. Source-invalid events are journaled but
cannot mutate the runtime book. Duplicate snapshots are idempotent, conflicting
snapshot identities fail closed, and regressed snapshot timestamps cannot rewind
the last authoritative state. A sequence gap keeps the last levels only for
diagnosis and marks them non-tradable until a fresh valid snapshot arrives.

## Venue matrix

| Venue | Selected V1 feed behavior | Continuity | Recovery |
| --- | --- | --- | --- |
| Binance spot/perpetual | REST snapshot + diff depth | `U/u` update range (`pu` where supplied) | buffer/replay, reconnect and bootstrap |
| Bybit spot/perpetual | WebSocket snapshot then delta | native update/version identity | wait for a fresh stream snapshot |
| Gate spot/perpetual | bounded full snapshot channel | native snapshot update ID | authoritative replacement on every update |
| OKX spot/perpetual | WebSocket books snapshot then update | `prevSeqId/seqId` | wait for a fresh stream snapshot |
| Hyperliquid spot/perpetual | L2 snapshot on each eligible block | venue timestamp | authoritative replacement/reconnect |
| MEXC spot | partial-depth snapshot | venue timestamp | authoritative replacement/reconnect |
| MEXC perpetual | REST depth + versioned deltas | exact consecutive `version` | reconnect and bootstrap |
| KuCoin spot/perpetual | level2 depth-50 snapshot | timestamp/native snapshot ID | authoritative replacement/reconnect |
| HTX spot/perpetual | step0 full snapshot | native `id/mrid` | authoritative replacement/reconnect |

This matches the selected endpoints, not every feed a venue happens to offer. Gate and KuCoin also expose incremental alternatives; V1 deliberately uses their documented bounded snapshot streams for those adapters. Hyperliquid documents `l2Book` as a snapshot feed. No currently selected feed publishes a supported checksum, and OKX deprecated its checksum in favor of sequence continuity. The generic L2 engine still validates an adapter-supplied checksum transactionally: a failed snapshot or delta checksum does not mutate the last authoritative levels and forces snapshot recovery.

Protocol references:

- Gate full snapshot and incremental channel: https://www.gate.com/docs/apiv4/ws/en/index.html
- KuCoin depth-50 direct snapshot semantics and incremental alternatives: https://www.kucoin.com/docs-new/3470221w0
- Hyperliquid `WsBook` snapshot semantics: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- HTX snapshot and incremental depth guidance: https://huobiapi.github.io/docs/spot/v1/en/

The machine-readable source of truth is `market_data/orderbook_protocols.py`. Contract tests require an explicit spot and perpetual policy for all eight V1 CEX venues and verify that unknown combinations fail closed.