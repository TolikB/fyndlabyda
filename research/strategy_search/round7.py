"""Round 7: classic oscillators and indicators on daily bars (research only).

Pre-registered on 2026-10-31, before any of these series was computed; 56 trials,
14 indicators x 2 forms x 2 directions, textbook parameters (features.py):
  rsi14, rsi2, stoch14 (%D), willr14, cci20, macd (12/26/9), bb20 (z-score), keltner20,
  donchian20, di14 (+DI vs -DI), obv20, mfi14, taker7 (taker share of quote volume),
  fundz (7-day funding against its 90-day history)
Each indicator becomes a centred score in [-1, 1] (SCORES below); the unbounded ones
(macd, taker7) are first z-scored against the symbol's own 90-day history.
Forms:
  xs  cross-sectional: top-60 universe, quintiles of the score, inverse-vol weights,
      50/50 dollar-neutral (round6.xs_book)
  ts  time series: top-20 universe, each coin holds its score times its inverse vol,
      over the inverse-vol sum of the live coins (gross <= 1, scaled by conviction)
Directions: trend (long a high score) and rev (long a low score).
Common: signal at the close of day t, trade at the open of t+1, vol target 20%, real
funding, tiered costs at 1x and 2x (2x is reported, not selected on); a 0x run gives the
pre-cost Sharpe as a diagnostic, not as a trial.
Development 2020-07-01 up to the holdout (2026-03-30..2026-09-30), which is not printed.
Development bar, the same for every round from 7 on: Sharpe >= 1.0 at 1x and >= 0.7 at
2x costs, >= 60% positive quarters, no calendar year below -15%. A trial that clears it
is rerun at half and double its lookback (counted as trials); both must be positive
before it joins the candidate pool, which meets the holdout once, all together.
"""

from __future__ import annotations

import glob
import json

import numpy as np
import pandas as pd

import bt
import evalx
import features as f
from round6 import xs_book

ROOT = "/w"
DEV_START = "2020-07-01"
SLEEVES = ["listshort|d1|h30|stop0.5|hedgetop20", "xsmom|top20|vol20|cost1x",
           "carry|top20|vol20|cost1x", "pumplong|top60|j0.3|h5|stop0.3|hedgeBTC"]


def own_z(x, n=90):
    mean, sd = x.rolling(n, min_periods=n // 2).mean(), x.rolling(n, min_periods=n // 2).std()
    return ((x - mean) / sd).where(sd > 0)


def scores(px, funding, scale=1.0):
    """Centred indicator scores; `scale` stretches every lookback (robustness reruns)."""
    h, lo, c, qv = px["high"], px["low"], px["close"], px["quote_volume"]
    n = lambda base: max(2, int(round(base * scale)))  # noqa: E731
    pdi, mdi, _ = f.directional(h, lo, c, n(14))
    raw = {
        "rsi14": (f.rsi(c, n(14)) - 50) / 50,
        "rsi2": (f.rsi(c, n(2)) - 50) / 50,
        "stoch14": (f.stochastic(h, lo, c, n(14)) - 50) / 50,
        "willr14": (f.williams_r(h, lo, c, n(14)) + 50) / 50,
        "cci20": f.cci(h, lo, c, n(20)) / 200,
        "macd": own_z(f.macd_hist(c, n(12), n(26), n(9))) / 2,
        "bb20": f.bollinger_z(c, n(20)) / 2,
        "keltner20": f.keltner(h, lo, c, n(20), n(14)) / 2,
        "donchian20": (f.donchian(h, lo, c, n(20)) - 0.5) * 2,
        "di14": ((pdi - mdi) / (pdi + mdi)).where((pdi + mdi) > 0),
        "obv20": f.obv_ratio(c, qv, n(20)),
        "mfi14": (f.mfi(h, lo, c, qv, n(14)) - 50) / 50,
        "taker7": own_z(f.taker_ratio(px["taker_buy_quote_volume"], qv, n(7))) / 2,
        "fundz": f.funding_z(funding.where(c.notna()), n(7)) / 2,
    }
    return {k: v.clip(-1, 1) for k, v in raw.items()}


def ts_book(score, live, opn, long_high=True):
    inv = (1.0 / bt.asset_vol(opn)).where(live)
    raw = score.where(live) * inv * (1 if long_high else -1)
    return raw.div(inv.sum(axis=1), axis=0).fillna(0.0)


def sleeve_matrix():
    frames = [pd.read_parquet(p) for p in sorted(glob.glob(f"{ROOT}/pnl_round*.parquet"))]
    matrix = pd.concat(frames, axis=1)
    matrix = matrix.loc[:, ~matrix.columns.duplicated()]
    return matrix[SLEEVES]


def evaluate(pnl, holdout_start, start=None):
    start = start or DEV_START                  # looked up per call: round 10 starts later
    dev = pnl[(pnl.index < holdout_start) & (pnl.index >= start)]
    s = bt.stats(dev)
    s["quarters_pos_pct"] = round(evalx.quarters_positive(dev), 0)
    s["worst_year_pct"] = min(s["by_year_pct"].values())
    return dev, s


def passes(s1, s2):
    return (s1["sharpe"] >= 1.0 and s2["sharpe"] >= 0.7 and s1["quarters_pos_pct"] >= 60
            and s1["worst_year_pct"] >= -15)


def run_books(books, opn, close, funding, med, holdout_start, sleeves, round_name, target=0.2):
    """target=None: the books are already vol-targeted."""
    results, series = {}, {}
    for name, raw in books.items():
        w = bt.vol_target(raw, opn, target) if target else raw
        gross_pnl, _ = bt.run(w, opn, funding, med, 0.0, close=close)
        _, s0 = evaluate(gross_pnl, holdout_start)
        stats = {}
        for cost_mult in (1.0, 2.0):
            pnl, parts = bt.run(w, opn, funding, med, cost_mult, close=close)
            key = f"{name}|vol20|cost{cost_mult:g}x"
            series[key] = pnl
            dev, s = evaluate(pnl, holdout_start)
            stats[cost_mult] = s
            corr = sleeves.reindex(dev.index).corrwith(dev).round(2).to_dict()
            results[key] = {"dev": s, "pre_cost_sharpe": s0["sharpe"], "corr_sleeves": corr,
                            "parts": {k: round(float(v), 3) for k, v in parts.items()}}
        s1, s2 = stats[1.0], stats[2.0]
        flag = "PASS" if passes(s1, s2) else ""
        results[f"{name}|vol20|cost1x"]["passes_bar"] = bool(flag)
        print(f"{name:<24} m {s1['monthly_mean_pct']:>6}/{s2['monthly_mean_pct']:>6} "
              f"sh {s1['sharpe']:>5}/{s2['sharpe']:>5} pre {s0['sharpe']:>5} dd {s1['max_dd_pct']:>5} "
              f"q+ {s1['quarters_pos_pct']:>3} worst {s1['worst_year_pct']:>6} "
              f"turn {results[f'{name}|vol20|cost1x']['parts']['turnover_per_day']:.2f} {flag}", flush=True)
    with open(f"{ROOT}/results_{round_name}.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{ROOT}/pnl_{round_name}.parquet")
    return results


def main():
    opn, close, _, funding = bt.load()
    px = f.load_ohlcv()
    px = {k: v.reindex(index=opn.index, columns=opn.columns) for k, v in px.items()}
    live60, med = bt.universe(close, px["quote_volume"], 60)
    live20, _ = bt.universe(close, px["quote_volume"], 20)
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    books = {}
    for ind, score in scores(px, funding).items():
        for direction, long_high in (("trend", True), ("rev", False)):
            books[f"{ind}|xs|{direction}"] = xs_book(score, live60, opn, long_high=long_high)
            books[f"{ind}|ts|{direction}"] = ts_book(score, live20, opn, long_high=long_high)
    print(f"{len(books)} trials, dev {DEV_START}..{holdout_start.date()} (holdout not shown)", flush=True)
    run_books(books, opn, close, funding, med, holdout_start, sleeve_matrix(), "round7")


if __name__ == "__main__":
    main()
