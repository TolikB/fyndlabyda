"""Round 6: positioning (Binance metrics) and low-volatility factors (research only).

Pre-registered on 2026-10-30, before any of these series was computed; 8 trials:
  lowvol|30, lowvol|90   long the lowest-vol quintile, short the highest (trailing N days)
  crowd|level, crowd|chg7  top-trader position long/short ratio: 3-day mean, or its 7-day
                           change; short the most crowded-long quintile, long the opposite
  retail|level           all-accounts long/short ratio, 3-day mean, same contrarian rule
  taker|mom, taker|rev   taker buy/sell volume ratio, 3-day mean, long high (mom) or low (rev)
  oi|chg7                7-day OI value change; short the fastest-growing quintile, long
                         the fastest-shrinking
Common to all: top-60 universe (bt.universe), metrics of day t known at its close, trade
at the open of t+1, quintiles, inverse-vol weights, 50/50 dollar-neutral, vol target 20%,
real funding, tiered costs; the 2x-cost run is reported, not selected on.
The holdout (2026-03-30..2026-09-30) is not printed here. It is opened once, only for
trials whose development run has Sharpe >= 1.0 at 1x costs, >= 0.7 at 2x costs and at
least 60% positive quarters; anything that passes there goes to the forward paper test
as a new series, never into the frozen combo5/combo45.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

import bt
import evalx

ROOT = "/w"


def metric(frame, column, index, columns):
    wide = frame.pivot_table(index="date", columns="symbol", values=column, aggfunc="last")
    wide.index = pd.to_datetime(wide.index)
    return wide.reindex(index=index, columns=columns)


def xs_book(score, live, opn, quantile=0.2, long_high=True):
    score = score.where(live)
    rank = score.rank(axis=1, pct=True)
    top, bottom = rank >= 1 - quantile, rank <= quantile
    raw = (top.astype(float) - bottom.astype(float)) * (1 if long_high else -1)
    raw = (raw / bt.asset_vol(opn)).where(live).fillna(0.0)
    pos, neg = raw.clip(lower=0), (-raw).clip(lower=0)
    return pos.div(pos.sum(axis=1), axis=0).fillna(0) * 0.5 - neg.div(neg.sum(axis=1), axis=0).fillna(0) * 0.5


def main():
    opn, close, qvol, funding = bt.load()
    live, med = bt.universe(close, qvol, 60)
    m = pd.read_parquet(f"{bt.DATA}/metrics_1d.parquet")
    get = lambda c: metric(m, c, opn.index, opn.columns)  # noqa: E731
    top_ratio = get("sum_toptrader_long_short_ratio")
    accounts = get("count_long_short_ratio")
    taker = get("sum_taker_long_short_vol_ratio")
    oi = get("oi_value")
    has_metrics = top_ratio.notna()
    live_m = live & has_metrics
    books = {
        "lowvol|30": xs_book(bt.asset_vol(opn, 30), live, opn, long_high=False),
        "lowvol|90": xs_book(bt.asset_vol(opn, 90).where(close.notna().cumsum() >= 90), live, opn,
                             long_high=False),
        "crowd|level": xs_book(top_ratio.rolling(3, min_periods=2).mean(), live_m, opn, long_high=False),
        "crowd|chg7": xs_book(np.log(top_ratio / top_ratio.shift(7)), live_m, opn, long_high=False),
        "retail|level": xs_book(accounts.rolling(3, min_periods=2).mean(), live_m, opn, long_high=False),
        "taker|mom": xs_book(taker.rolling(3, min_periods=2).mean(), live_m, opn, long_high=True),
        "taker|rev": xs_book(taker.rolling(3, min_periods=2).mean(), live_m, opn, long_high=False),
        "oi|chg7": xs_book(np.log(oi / oi.shift(7)), live_m, opn, long_high=False),
    }
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    results, series = {}, {}
    for name, raw in books.items():
        w = bt.vol_target(raw, opn, 0.2)
        for cost_mult in (1.0, 2.0):
            pnl, parts = bt.run(w, opn, funding, med, cost_mult, close=close)
            key = f"{name}|vol20|cost{cost_mult:g}x"
            series[key] = pnl
            start = "2021-02-01" if name.split("|")[0] in ("crowd", "retail", "taker", "oi") else "2020-07-01"
            dev = pnl[(pnl.index < holdout_start) & (pnl.index >= start)]
            s = bt.stats(dev)
            s["quarters_pos_pct"] = round(evalx.quarters_positive(dev), 0)
            results[key] = {"dev": s, "parts": {k: round(float(v), 3) for k, v in parts.items()}}
            print(f"{key:<30} m_mean {s['monthly_mean_pct']:>6} med {s['monthly_median_pct']:>6} "
                  f"sh {s['sharpe']:>5} dd {s['max_dd_pct']:>5} q+ {s['quarters_pos_pct']:>3} "
                  f"turn {parts['turnover_per_day']:.2f} yrs {s['by_year_pct']}", flush=True)
    with open(f"{ROOT}/results_round6.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{ROOT}/pnl_round6.parquet")


if __name__ == "__main__":
    main()
