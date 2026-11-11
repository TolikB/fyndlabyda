# Does the same stock pay different funding on Binance and on xyz? (round 39, research only)

## Question

Binance sets the interest term of 231 of its 235 stock, fund and metal perps to zero. Hyperliquid's xyz dex funds
hourly with a multiplier of 0.5 and the usual interest term (see [binance-tradfi-perps facts](../tradfi_lag/README.md)).
So a long can pay different amounts for the same stock on the two venues.

The trade is a hedged pair: long the stock where funding is cheap, short it where it is dear. It keeps the difference
for as long as the two prices move together. The stage 4 plan (D2) put the ceiling near 2% a month with leverage, so
this round asks whether a steady base exists, not whether it reaches the target on its own.

## Files

| File | What it is |
|---|---|
| `download.py` | Public history of both venues for the 99 pairs, run from the workstation so the VM's Hyperliquid budget stays with the collectors |
| `test_carry.py` | The rules (docstring), the dev run and the one holdout look |
| `selftest_carry.py` | Known-answer checks on synthetic pairs and files; prints `selftest OK` |

## Data

`download.py` fetches, up to 2026-11-11 00:00 UTC:

- **Binance:** `fundingRate` history and 1h klines.
- **Hyperliquid:** `fundingHistory` (hourly) and 1h candles. Hyperliquid serves only the latest 5,000 candles, so xyz
  prices start about 2026-04-16 and the test starts on 04-20.

The pairs come from `tradfi_lag`'s pairs file of 2026-11-11: 99 pairs less 2 PREMARKET. That makes 81 US equities and
funds, 8 commodities, 3 Korean, 3 Hong Kong and 2 Chinese equities. A pair delisted before 11-11 is missing.

The download printed row counts only. No rate or price was read before the rules and selftest were committed.

## Rules in short

The full rules are in the docstring of `test_carry.py`.

- **Each day at 01:00 UTC**, for each pair:
  - G = the funding gap over the last L days, per 8 h: xyz funding less Binance funding.
  - Eligible when both venues already funded on the window's first day and both prices exist.
- **The book:** the 5 pairs with the largest |G| ≥ THETA.
  - G > 0: long Binance, short xyz.
  - G < 0: the reverse.
  - Each pair holds 0.2 × s of equity on each leg, s ∈ {1, 2, 3}.
- **Costs:** a leg opened or closed pays a taker fee of 0.05% on Binance and 0.09% on xyz (growth mode off, since its past
  dates are unknown). A pair kept in the same direction costs nothing.
- **Liquidation:** a venue whose margin (half the equity plus its legs' open PnL) falls under 5% of its notional at an
  hourly close fails the trial at that s.
- **Trials:** 6 = THETA {0.005%, 0.015%, 0.03% per 8 h} × L {3, 14 days}.
- **Bar on development** (decisions 2026-04-20 .. 08-31):
  - t ≥ 2.5 over weekly sums;
  - ≥ 5% a month at the smallest s*, drawdown ≤ 35% and no liquidation there;
  - > 0 at 2x fees;
  - no week carries more than half the net.
- **Holdout:** 09-01 .. 11-09, one look.

The funding part and the price part are reported apart, so a pass carried by price drift rather than funding shows
up.

## Checks

**Mutation check, 2026-11-11.** Each of 40 one-line mutations of `test_carry.py` must make the selftest fail. They
include changed limits, fees and windows, dropped conditions, a swapped side or leg, missing costs, a wrong week
boundary, and broken holdout gates. The first selftest caught 39. The survivor dropped the check that a window starts
inside the grid; it mattered only when a negative slice wraps to the grid's end. A case for that was added, and the
final selftest catches all 40. The registered numbers (fees, limits, thresholds, windows) are asserted literally.

## Run

```bash
python download.py pairs.json data 2026-11-11T00:00     # workstation
# copy data/ and pairs.json to /opt/funding_arbitrage_research/data/tradfi_carry/ on the VM, then:
cd /opt/funding_arbitrage_research
docker run --rm --memory 2g -v /opt/funding_arbitrage_research:/w -w /w/tradfi_carry hl-liqmap:py312 python selftest_carry.py
docker run --rm --memory 2g -v /opt/funding_arbitrage_research:/w -w /w/tradfi_carry hl-liqmap:py312 python test_carry.py dev
```
