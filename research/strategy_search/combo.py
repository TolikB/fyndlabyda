"""Round 3: combine sleeves and add a portfolio risk overlay (research only).

Each sleeve's daily PnL is rescaled to 1 unit of ex-ante vol (trailing 60-day
realised vol, known the day before), sleeves are equal-risk weighted, and the sum
is scaled to a target vol. The drawdown brake cuts the scale to `brake` while the
equity is more than `dd_trigger` below its peak (decided on yesterday's equity).
PnL-level combination ignores netting between sleeves, so costs are a bit
overstated; leverage is capped at `max_lev` x each sleeve's own book.
"""

from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

import bt
import evalx

ROOT = "/w"


def unit_vol(pnl: pd.Series, lookback: int = 60) -> pd.Series:
    vol = pnl.rolling(lookback, min_periods=30).std().shift(1) * np.sqrt(365)
    # A sleeve that has not traded yet has no vol: it contributes nothing.
    vol = vol.where(vol > 0.01)
    return pnl / vol


def overlay(pnl: pd.Series, target: float, dd_trigger: float | None, brake: float, max_lev: float):
    vol = pnl.rolling(60, min_periods=30).std().shift(1) * np.sqrt(365)
    lev = (target / vol.where(vol > 0.01)).clip(upper=max_lev).fillna(0.0)
    out = np.zeros(len(pnl))
    equity, peak = 1.0, 1.0
    for i, (r, k) in enumerate(zip(pnl.fillna(0.0).to_numpy(), lev.to_numpy())):
        if dd_trigger is not None and equity < peak * (1 - dd_trigger):
            k *= brake
        out[i] = k * r
        equity *= 1 + out[i]
        peak = max(peak, equity)
    return pd.Series(out, index=pnl.index)


def main(sleeve_keys: list[str]):
    frames = [pd.read_parquet(f"{ROOT}/pnl_round{n}.parquet") for n in ("1", "2", "2c", "2d")]
    pnl = pd.concat(frames, axis=1)
    end = pnl.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    sleeves = pd.DataFrame({k: unit_vol(pnl[k]) for k in sleeve_keys}).dropna(how="all")
    sleeves = sleeves[sleeves.index >= "2020-07-01"]
    print("sleeve correlation (dev):")
    print(sleeves[sleeves.index < holdout_start].corr().round(2).to_string())
    base = sleeves.mean(axis=1, skipna=True)
    trials = {}
    for target in (0.3, 0.45, 0.6, 0.8, 1.0):
        for dd_trigger, brake in ((None, 1.0), (0.15, 0.5), (0.2, 0.25)):
            key = f"combo|vol{int(target*100)}|dd{dd_trigger}|brake{brake}"
            trials[key] = overlay(base, target, dd_trigger, brake, max_lev=3.0)
    matrix = pd.DataFrame(trials)
    dev = matrix[matrix.index < holdout_start]
    dev = dev[dev.index >= dev.ne(0).idxmax().max()]
    out = {}
    for key in matrix:
        s = bt.stats(dev[key])
        s["quarters_pos_pct"] = round(evalx.quarters_positive(dev[key]), 0)
        out[key] = s
        print(f"{key:<36} m_mean {s['monthly_mean_pct']:>6} med {s['monthly_median_pct']:>6} "
              f"cagr {s['cagr_pct']:>7} sh {s['sharpe']:>5} dd {s['max_dd_pct']:>5} "
              f"q+ {s['quarters_pos_pct']:>3} p10 {s['p_month_ge_10pct']:>3} yrs {s['by_year_pct']}")
    with open(f"{ROOT}/results_round3.json", "w") as handle:
        json.dump(out, handle, indent=1, default=str)


if __name__ == "__main__":
    main(sys.argv[1:])
