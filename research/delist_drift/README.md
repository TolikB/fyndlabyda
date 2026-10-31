# Short after a Binance delisting announcement (round 15, research only)

## Question

"Binance Will Delist X on <date>" forces Binance-only holders to sell. After the first drop, does
X's USDT-M perp keep falling for days?

## Rules

The rules are in the `bt_delist.py` docstring. They were written on 2026-10-31, before any price
around an announcement was looked at.

- **Entry:** short at the first whole hour at least 15 minutes after the announcement.
- **Exit:** after 24 or 72 hours, or one hour before the perp itself stops trading.
- **Costs:** taker fee plus slippage tier, on both sides, plus funding.
- **Book:** 5 slots of 20%.
- **Trials:** 2.

`selftest_delist.py` checks the title parsing on real catalog titles, the entry hour, the perp's
end, the costs and the book.

## Data

- **Announcements:** `download_announcements.py` fetched all 446 announcements of Binance's
  "Delisting" catalog. Of these, 43 are spot delistings named "Binance Will Delist … on
  <date>"; the catalog starts in 2022-03.
- **Development sample** (before 2025-07): 24 announcements and 93 tickers. Only 26 tickers
  have a perp, and 21 of those traded at the entry hour.
- **Holdout** (2025-07..2026-09): 13 announcements, 30 tickers with a perp.
- **Prices:** the Binance 1h archive.

## Result (development): FAIL, the holdout stays closed

| Trial | Trades | Mean 1x | t (announcements) | Win | Worst | Funding | Monthly mean | Max DD |
|---|---|---|---|---|---|---|---|---|
| hold 24h | 21 | +5.5% | 0.47 | 62% | −43.7% | −1.6% | +0.83% | 12.4% |
| hold 72h | 21 | −6.5% | −0.66 | 76% | −358.8% | −4.7% | −0.97% | 71.8% |

**The typical trade wins.** Over 72 hours the median trade makes +9.8%, and the trades other
than ALPACA average +11.2%.

**One squeeze takes it all.** In April 2025, ALPACAUSDT rose 298% within the 72 hours, and its
shorts paid 60% in funding. That is −359% of the notional on one trade, or −72% of equity at a
20% slot.

**The short is crowded.** Shorts pay 1.6% (24h) to 4.7% (72h) in funding on average: the market
already knows this trade and charges for it.

**Too rare for the target.** There is about 0.6 announcement a month. Even +10% a trade at a
20% slot is 1–2% a month, far from 5%.

**Optimistic costs.** Every entry used the 25 bp fallback slippage tier, because these coins
have no tier on the entry day. Real fills in a falling, delisted coin are worse.

A stop-loss version would be chosen after seeing ALPACA, so it is not a valid trial. Such a
version would also face the same crowding and rarity.

## Registry

`/w/pnl_round15.parquet` adds 2 trials: 548 recorded plus 24 unrecorded, 572 in all.

## Files

- `bt_delist.py`, `selftest_delist.py`, `download_announcements.py`
- `results_dev.json`, `dev.log`
