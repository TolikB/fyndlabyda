# V1 market-data freshness

`MARKET_DATA_STALE_SECONDS` is the contract for how old a price may be before the
system refuses to trade on it. Three collector behaviours keep that contract
true rather than nominal.

## REST revalidation is bounded by the staleness budget

`MarketDataCollector` keeps WebSocket tickers primary and revalidates them
against REST periodically. The configured `rest_validation_seconds` is an upper
bound only: the static bound is `min(configured, stale_after_seconds // 2)`, and
`_ticker_revalidation_deadline` narrows it further at runtime.

A revalidation interval at or above the staleness budget cannot keep cached REST
tickers inside it, so any market the stream does not cover would age past the
budget and the venue would never be healthy. But halving the budget is not
enough on its own: a cached page fetched when a venue is collected still has to
be inside the budget when the snapshot closes, and a whole eight-venue pass
measures around 12 seconds. With a 30-second budget and a 15-second interval,
Gate's entire page expired mid-pass on 57% of measured passes. The deadline
therefore reserves the last measured pass duration plus the boundary margin,
decayed so one quick cycle cannot shrink the reserve for the next.

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

## Freshness is decided at the snapshot boundary

Venues are collected concurrently, but the whole pass takes seconds, so a ticker
that was inside the budget when its venue was collected can be outside it by the
time the snapshot closes. Readiness measures every age against `captured_at`, so
the collector does too: the boundary re-ages and re-filters every venue's tickers
against the shared `observed_at` before the snapshot is assembled. A venue left
with no usable ticker, or whose open-position mark is still stale, gets one bulk
re-fetch.

`_VenueCollection.non_ticker_complete` records the completeness of everything
that re-fetch cannot re-verify — funding, requested order books, funding history
— so a successful re-fetch can restore the venue to operationally complete,
instead of only ever demoting it.

The re-fetch decision looks `stale_after_seconds / 6` ahead, because a venue that
is merely close to the budget when the decision is made would be over it once the
fetch returns. A fetch takes time of its own, so the venues are re-aged
immediately afterwards and repaired again, up to `_MAX_TICKER_REPAIR_ROUNDS`;
otherwise a venue that expires during someone else's re-fetch reaches the
snapshot with nothing and fails the window.

## A window opens only after sustained health

`ACCEPTANCE_WARMUP_SNAPSHOTS` consecutive clean eight-venue snapshots are
required before `RuntimeAcceptanceCollector` opens a window. One clean snapshot
from a process whose caches are still filling is not evidence it can hold eight
venues, and because a single not-ready sample fails a window permanently, a
window that opens cold simply fails on its next sample. Any venue dropping out
during warm-up restarts the streak.

## The collection pass needs enough CPU to stay inside the budget

One eight-venue collection pass measures about 12 seconds of wall clock, most of
it normalizing several thousand tickers per venue and validating the selected
order books. Measured on the acceptance host, the app container ran pinned at
~95% of a single CPU for the whole window, which is what stretched the pass and
pushed data past the budget in the first place.

The app service is therefore sized at 2 CPUs and 3 GiB, and
`scripts/host_preflight.sh` requires an 8 GiB host so the full compose stack
still fits with headroom. A window that is CPU-starved fails GATE-001 on data
age even when every venue is healthy.

## What this does not do

Dropping a stale ticker does not hide a venue outage. A venue with no usable
tickers has no ticker ages at all, which fails the readiness check in
`qa/runtime_acceptance.py` exactly as an outage does, and a required market that
is still stale after the boundary refresh leaves the venue incomplete.
