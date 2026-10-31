# Funding snipe: does the price give back the coupon? (descriptive, research only)

**Question.** A position open at the settlement instant collects the whole interval's
funding, however briefly it was held. Could the receiving side open a few minutes before the
settlement, collect the funding, and close one minute after?

**Answer.** No. The perp behaves like a bond going ex-coupon, and the price takes most of the
coupon back.

`study_snipe.py` uses 1-minute bars from `research/wick_trap/`. It looks at 507,295
settlements of 469 symbols on development days only (2022-01..2025-06, ranks 30–150). The
2025-07..2026-10 minute window was not touched.

## What `study_snipe.txt` shows

All figures are in basis points.

**Typical rates.** For rates of 3–10 bp, the receiving side loses 4–5 bp in price between 5
minutes before and 1 minute after the settlement. That is about the whole rate.

**Extreme rates.** For settled rates of at least 30 bp, the receiver keeps +16..+31 bp
before fees, for entries 1–15 minutes before the settlement. The price gives back about 63%
of the rate. This bucket is sorted by the settled rate, which is known only near the
settlement, so it is an upper estimate.

**The version a trader could actually run.** Here the trade is picked by the previous
settlement's rate, which is known in advance, with entry 5 minutes before:

| Previous rate | Events | Net after costs | Share of positive events |
|---|---|---|---|
| 10–30 bp | 3,845 | −47 bp | 14% |
| ≥ 30 bp | 1,028 | −44 bp | 23% |

Costs are a 10 bp taker round trip plus twice the slippage tier. The slippage tier is a
median 25 bp on these coins.

**Maker fills.** Even with maker fills and no slippage, the edge is only about +8 bp, on
about 300 events a year.

## Verdict

There is no trade here. No trial was defined, so the registry is unchanged at 564. The
development figures here were descriptive, and the minute holdout is still unspent.

The idea differs from round 4 (`settle_bt.py`). Round 4 entered only after the settlement and
never collected the funding.
