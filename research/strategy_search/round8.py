"""Round 8: oscillators and BTC-residual reversal on hourly bars (research only).

Pre-registered on 2026-10-31, before any of these series was computed; 16 trials,
8 signals x 2 rebalance intervals, all cross-sectional (round6.xs_book: quintiles,
inverse-vol weights, 50/50 dollar-neutral) on the top-30 universe of the previous day
(round 7b showed that the top-60's slippage eats short-horizon signals):
  rsi2|rev       2-hour RSI, long the most oversold
  bb20|trend, bb20|rev        20-hour Bollinger z-score
  cci20|trend    20-hour CCI
  donch20|trend, donch20|rev  position in the 20-hour high-low channel
  resid1|rev, resid4|rev      the last 1 or 4 hours' return in excess of beta x BTC's
                 (beta: 168-hour rolling regression); long the laggards, short the
                 leaders. This is also the "BTC leads, alts catch up" idea.
Rebalance every 4 or 8 hours (bt_hourly.hold_every). Signal at the close of hour h,
trade at the open of h+1, hourly vol target 20%, real funding by settlement hour, tiered
costs at 1x and 2x, a 0x run for the pre-cost Sharpe. Returns are compounded into days
for the registry; development window and bar as in round 7.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

import bt
import bt_hourly as bh
import features as f
import round7
from round6 import xs_book


def resid(close, k, beta_n=168):
    r1 = close.pct_change(fill_method=None)
    btc = r1["BTCUSDT"]
    var = btc.rolling(beta_n, min_periods=beta_n // 2).var()
    beta = r1.rolling(beta_n, min_periods=beta_n // 2).cov(btc).div(var, axis=0)
    rk = close / close.shift(k) - 1.0
    return rk - beta.mul(rk["BTCUSDT"], axis=0)


def main():
    _, close_d, qvol_d, _ = bt.load()
    live_d, med = bt.universe(close_d, qvol_d, 30)
    traded = set(live_d.columns[live_d.any()]) | {"BTCUSDT"}
    px = bh.load(("open", "high", "low", "close"), symbols=traded)
    opn, close = px["open"], px["close"]
    cols = opn.columns
    live = bh.from_daily(live_d.astype(float), opn.index, cols).fillna(0.0) > 0.5
    live &= opn.notna()
    slip = bh.from_daily(bt.slippage(med), opn.index, cols)
    funding = bh.funding_hourly(opn.index, cols)
    end = close_d.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    h, lo = px["high"], px["low"]
    signals = {
        "rsi2|rev": (f.rsi(close, 2), False),
        "bb20|trend": (f.bollinger_z(close, 20), True),
        "bb20|rev": (f.bollinger_z(close, 20), False),
        "cci20|trend": (f.cci(h, lo, close, 20), True),
        "donch20|trend": (f.donchian(h, lo, close, 20), True),
        "donch20|rev": (f.donchian(h, lo, close, 20), False),
        "resid1|rev": (resid(close, 1), False),
        "resid4|rev": (resid(close, 4), False),
    }
    sleeves = round7.sleeve_matrix()
    results, series = {}, {}
    print(f"{len(signals) * 2} trials, hours {opn.index.min()}..{opn.index.max()}, symbols {len(cols)}, "
          f"dev {round7.DEV_START}..{holdout_start.date()} (holdout not shown)", flush=True)
    for name, (score, long_high) in signals.items():
        raw = xs_book(score, live, opn, long_high=long_high)
        for every in (4, 8):
            w = bh.vol_target(bh.hold_every(raw, every), opn, 0.2)
            key0 = f"{name}|h{every}|top30"
            gross, _ = bh.run(w, opn, funding, slip, 0.0, close=close)
            _, s0 = round7.evaluate(bh.daily(gross), holdout_start)
            stats = {}
            for cost_mult in (1.0, 2.0):
                pnl, parts = bh.run(w, opn, funding, slip, cost_mult, close=close)
                pnl = bh.daily(pnl)
                key = f"{key0}|vol20|cost{cost_mult:g}x"
                series[key] = pnl
                dev, s = round7.evaluate(pnl, holdout_start)
                stats[cost_mult] = s
                results[key] = {"dev": s, "pre_cost_sharpe": s0["sharpe"],
                                "corr_sleeves": sleeves.reindex(dev.index).corrwith(dev).round(2).to_dict(),
                                "parts": {k: round(float(v), 3) for k, v in parts.items()}}
            s1, s2 = stats[1.0], stats[2.0]
            flag = "PASS" if round7.passes(s1, s2) else ""
            results[f"{key0}|vol20|cost1x"]["passes_bar"] = bool(flag)
            print(f"{key0:<24} m {s1['monthly_mean_pct']:>6}/{s2['monthly_mean_pct']:>6} "
                  f"sh {s1['sharpe']:>5}/{s2['sharpe']:>5} pre {s0['sharpe']:>5} dd {s1['max_dd_pct']:>5} "
                  f"q+ {s1['quarters_pos_pct']:>3} worst {s1['worst_year_pct']:>6} "
                  f"turn {results[f'{key0}|vol20|cost1x']['parts']['turnover_per_day']:.2f} {flag}", flush=True)
        del raw
    with open(f"{round7.ROOT}/results_round8.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{round7.ROOT}/pnl_round8.parquet")


if __name__ == "__main__":
    np.seterr(all="ignore")
    main()
