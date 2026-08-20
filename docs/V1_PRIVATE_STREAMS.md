# V1 authenticated private streams

## Implemented contract

Live mode creates authenticated CCXT Pro account clients only after the existing
strict live-mode configuration and REST-adapter checks pass. The account topology
covers Binance, Bybit, Gate, OKX, Hyperliquid, MEXC, KuCoin, and HTX. Binance,
Gate, MEXC, KuCoin, and HTX use separate spot and derivative profiles where the
venue APIs require distinct account channels; Bybit, OKX, and Hyperliquid use
unified private accounts.

Every supported WebSocket update is normalized to the immutable canonical event
journal:

- orders become `ORDER_UPDATE`, preserving client/exchange IDs, cumulative base
  quantity, status, side, type, prices, and reduce-only state;
- personal trades become `FILL`, preserving fill/order identity, base quantity,
  execution price, fee asset/cost, and maker/taker role;
- positions become signed base-unit `POSITION_SNAPSHOT` events with entry, mark,
  PnL, leverage, liquidation price, and margin where supplied;
- balances become per-asset `BALANCE_SNAPSHOT` events with total, available,
  locked, and borrowed amounts.

Derivative contract counts are converted with the venue market's `contractSize`.
Unknown instruments, non-finite numbers, invalid sides, missing fill identity, or
positions without a usable mark are rejected rather than guessed. Exchange
timestamps are retained; a payload with no venue timestamp is explicitly marked
`RECOVERING` and uses receipt time.

## Recovery and safety

Private WebSockets reduce latency but are not the accounting authority. Startup
and every periodic authenticated REST reconciliation write matching balance,
position, and open-order snapshots to the same journal. This closes reconnect
gaps and provides the position recovery path for MEXC and HTX, whose pinned CCXT
Pro 4.5.73 adapters do not advertise `watchPositions`.

New live entries are blocked when any channel is reconnecting or stopped, the
REST checkpoint is absent/stale, or canonical event persistence fails. Stream
loss never triggers automatic resubmission of an order with an unknown outcome.
`/health/ready` fails closed and `/system/live` exposes only channel health and
redacted error types. Prometheus, Grafana, and Alertmanager cover stream health,
normalized event counts, and normalization failures.

## Verification

`tests/test_private_streams.py` validates deterministic normalization, contract-
to-base conversion, reconnect health, stale checkpoints, REST position recovery,
and the pinned capability matrix for all eight CEX. Live execution, private
credentials, and network calls are not used by these tests. An authenticated
Linux preflight with read-only/trade-only keys is still required before any
Limited-Live acceptance gate; it must not grant withdrawal or transfer access.