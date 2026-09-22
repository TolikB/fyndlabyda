# V1 market-data freshness

`MARKET_DATA_STALE_SECONDS` is the contract for how old a price may be before the
system refuses to trade on it. Three collector behaviours keep that contract
true rather than nominal.

## REST revalidation is bounded by the staleness budget

`MarketDataCollector` keeps WebSocket tickers primary and revalidates them
against REST periodically. The configured `rest_validation_seconds` is an upper
bound only: the effective interval is `min(configured, stale_after_seconds // 2)`.

A revalidation interval at or above the staleness budget cannot keep cached REST
tickers inside it, so any market the stream does not cover would age past the
budget and the venue would never be healthy. Halving the budget leaves headroom
for the fetch itself, which the venue matrix measures in the low seconds.

## A stale ticker is not market data

Venues publish a last-trade timestamp per market, so a thin market can report a
price that is hours old while the endpoint itself is perfectly healthy. Measured
on the eight-venue set, KuCoin's USDC-margined contracts routinely report ages of
minutes to over a day, and Binance reports a long tail on inactive markets.

`_usable_tickers` therefore drops any ticker older than the staleness budget, or
dated more than `_MAX_TICKER_CLOCK_SKEW_SECONDS` into the future, alongside the
existing price and volume sanity checks. Drops are counted on
`market_data_dropped_total{reason="stale_ticker"}` and logged as
`stale_tickers_dropped`.

This runs before the universe limiter, which ranks partly on funding rate and so
otherwise pulls exactly these dead markets into the tradeable set. For the same
reason the limiter now ranks only assets that have a usable ticker, plus any
asset pinned by an open position: an asset with funding but no priceable market
would spend a universe slot and starve the venue of tradeable markets.

## The snapshot boundary can still repair a venue

Venues are collected concurrently, so a slow venue lets an earlier venue's
tickers age before the snapshot closes. `_VenueCollection.non_ticker_complete`
records the completeness of everything the boundary refresh cannot re-verify —
funding, requested order books, funding history — so that when the refresh
re-fetches a required market's ticker successfully it can restore the venue to
operationally complete, instead of only ever demoting it.

## What this does not do

Dropping a stale ticker does not hide a venue outage. A venue with no usable
tickers has no ticker ages at all, which fails the readiness check in
`qa/runtime_acceptance.py` exactly as an outage does, and a required market that
is still stale after the boundary refresh leaves the venue incomplete.
