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

`CANONICAL_HIGH_FREQUENCY_MARKET_EVENTS_ENABLED` defaults to `true` and keeps
the complete raw trade, order-book, and option-quote journal required by the
full replay profile. A positive
`CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS` enables a bounded
paper profile: trades, complete collector book snapshots, and option quotes are
forwarded at most once per kind and instrument in that interval. Repeated
funding and open-interest observations use the same bound; paper funding
payments and ledger accounting remain event-exact. Native book
deltas are not journaled in this mode because omitting intermediate deltas
would create an invalid replay sequence; live local books still drive scanning
and execution. Capacity-constrained paper deployments may instead set the
feature flag to `false` to suppress all three high-frequency canonical copies.
Sampled snapshots, completed candles, liquidations, universe changes,
decisions, orders, fills, positions, ledger rows, and audits remain durable.
When the feature flag is `false`, readiness requires fresh canonical funding
rather than a book stream that is intentionally not journaled. Operators must
record either bounded setting with replay evidence because such a run cannot
claim raw trade- or L2-complete replay coverage. Bounded profiles also require
`MULTI_REGIME_ENABLED=false`; the legacy funding paper loop may continue, but
order-flow and multi-regime decisions are fail-closed rather than evaluated on
statistically invalid sparse inputs. `RUN_MODE=live` rejects both bounded
variants and requires the complete canonical journal.

Every paper/live process first acquires one database-scoped writer lease and
holds it until all event producers stop. Only the lease owner may append an
immutable `canonical_journal_profiles` boundary, so overlapping deployments
cannot interleave recording contracts. The boundary records the canonical row
tip, timestamp, simulation versions, release commit, cadence/universe/options
settings, and deterministic non-secret config hash. Exact-profile restarts may
replay only the latest contiguous compatible row-ID window; wall-clock process
time never substitutes for journal insertion order. Cross-profile and unlabeled
canonical ranges are rejected. Raw incident reads use an explicitly forensic
API, while strategy replay requires a profile argument. The current contract and
its active boundary are visible at `GET /system/canonical-journal`.
PostgreSQL uses a session advisory lock; ordinary file-backed SQLite uses a
sidecar OS file lock derived from the resolved database path, while private
in-memory SQLite databases use an engine-local lock. SQLite `file:` URI writer
configurations are rejected because aliases cannot prove one lock identity. A checkpoint references the latest
compatible boundary whose row marker is strictly lower than the checkpoint row;
a same-tip restart therefore cannot relabel historical state with a boundary
that only applies to future rows.
The first upgrade from a pre-lease release is necessarily stop-before-start:
operators must fence and stop the old writer before migrating or starting this
version; it is not a rolling-upgrade boundary.

Prometheus exports `funding_public_event_capability` for the explicit venue/account
matrix and `funding_public_events_total` by exchange, stream, and source. Invalid
records increment `funding_market_data_dropped_total` without logging payloads;
sampled-out rows increment
`funding_canonical_high_frequency_events_sampled_out_total` by event kind.

## Verification

`tests/test_public_events.py` covers contract conversion, exact funding fields,
OI fallback and de-duplication, fail-closed malformed events, and all eight venue
profiles. `tests/test_event_journal.py` proves liquidation events survive a full
append/replay round trip.
