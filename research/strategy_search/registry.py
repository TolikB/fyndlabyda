"""Trial registry: every configuration ever run, deflated Sharpe and PBO (research only).

Reads every /w/pnl_round*.parquet (one column of daily net returns per trial),
keeps the development period (before the holdout), and reports the best trials,
the deflated Sharpe of the best one given all trials, and the CSCV probability of
backtest overfitting over the whole registry.
"""

from __future__ import annotations

import glob
import json

import numpy as np
import pandas as pd

import bt
import evalx

ROOT = "/w"
# The first listing-short grid (36 runs, sizes 0.1/0.2, holds up to 60 days) was
# overwritten by the hedged grid; 24 of its runs are not repeated there.
UNRECORDED_TRIALS = 24


def main():
    frames = [pd.read_parquet(path) for path in sorted(glob.glob(f"{ROOT}/pnl_round*.parquet"))]
    matrix = pd.concat(frames, axis=1)
    matrix = matrix.loc[:, ~matrix.columns.duplicated()]
    end = matrix.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    dev = matrix[(matrix.index < holdout_start) & (matrix.index >= "2020-07-01")].fillna(0.0)
    daily_sharpe = dev.mean() / dev.std().replace(0, np.nan)
    annual = (daily_sharpe * np.sqrt(365)).sort_values(ascending=False)
    print(f"trials {matrix.shape[1]} recorded + {UNRECORDED_TRIALS} unrecorded, dev {dev.index.min().date()}..{dev.index.max().date()}")
    print("top 12 by dev Sharpe:")
    rows = []
    for key in annual.index[:12]:
        s = bt.stats(dev[key])
        rows.append((key, round(annual[key], 2), s["monthly_mean_pct"], s["max_dd_pct"],
                     round(evalx.quarters_positive(dev[key]), 0)))
        print(f"  {key:<44} sh {annual[key]:5.2f} m_mean {s['monthly_mean_pct']:>6} "
              f"dd {s['max_dd_pct']:>5} q+ {rows[-1][-1]:>4}")
    best = annual.index[0]
    dsr = evalx.deflated_sharpe(dev[best], daily_sharpe.dropna().to_numpy(), UNRECORDED_TRIALS)
    pbo = evalx.pbo(dev[annual.dropna().index], blocks=12)
    print(f"best {best}: deflated Sharpe {dsr:.3f} (needs >= 0.95); PBO {pbo:.2f} (needs < 0.5)")
    with open(f"{ROOT}/registry.json", "w") as handle:
        json.dump({"trials": int(matrix.shape[1]), "best": best, "dsr": dsr, "pbo": pbo,
                   "top": rows}, handle, indent=1, default=str)


if __name__ == "__main__":
    main()
