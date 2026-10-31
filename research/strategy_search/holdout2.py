"""Open the holdout once for the whole candidate pool of rounds 7-10 (research only).

Rule fixed on 2026-10-31, while rounds 8-10 were still running and before any of their
results or any holdout number of the pool was seen:
- pool: every 1x-cost trial in results_round{7b,8,9,10}.json flagged passes_bar that
  also passed its robustness reruns (round 7c for 7b; a round-9 filter replaces its
  unfiltered book only if it beats it, as round 9's docstring says). The pool is passed
  on the command line, so the log shows exactly what was opened;
- osc: the equal-weight mean of the pool members, each scaled to unit ex-ante vol
  (combo.unit_vol), so no member is picked over another;
- combo5s: osc plus the four frozen combo sleeves, equal-risk;
- each portfolio gets the round-3 overlay (DD trigger 0.2, brake 0.25, max lev 3) at the
  highest vol target in 0.2..1.2 (step 0.1) whose development max drawdown is <= 35%;
- success: holdout monthly mean >= 5% with holdout max drawdown <= 35%. Anything else is
  reported as it is; nothing is re-tuned after this run.
"""

from __future__ import annotations

import glob
import json
import sys

import numpy as np
import pandas as pd

import bt
import combo
import evalx
import round7

TARGETS = [round(x, 1) for x in np.arange(0.2, 1.21, 0.1)]


def describe(part):
    s = bt.stats(part)
    s["quarters_pos_pct"] = evalx.quarters_positive(part)
    s["months_pct"] = {str(k.date())[:7]: round(v * 100, 1)
                       for k, v in ((1 + part).resample("ME").prod() - 1).items()}
    return s


def main(pool):
    frames = [pd.read_parquet(p) for p in sorted(glob.glob(f"{round7.ROOT}/pnl_round*.parquet"))]
    pnl = pd.concat(frames, axis=1)
    pnl = pnl.loc[:, ~pnl.columns.duplicated()]
    end = pd.Timestamp("2026-09-30")
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    pnl = pnl[(pnl.index >= round7.DEV_START) & (pnl.index <= end)]
    members = pd.DataFrame({k: combo.unit_vol(pnl[k]) for k in pool})
    old = pd.DataFrame({k: combo.unit_vol(pnl[k]) for k in round7.SLEEVES})
    osc = members.mean(axis=1, skipna=True)
    portfolios = {"osc": osc, "combo5s": pd.concat([old, osc.rename("osc")], axis=1).mean(axis=1, skipna=True)}
    dev_mask = pnl.index < holdout_start
    print("pool:", pool)
    print("dev correlation of the pool and the old sleeves:")
    print(pd.concat([members, old], axis=1)[dev_mask].corr().round(2).to_string())
    out = {"pool": pool}
    for name, base in portfolios.items():
        chosen, series = None, None
        for target in TARGETS:
            s = combo.overlay(base, target, 0.2, 0.25, max_lev=3.0)
            if bt.stats(s[dev_mask])["max_dd_pct"] <= 35:
                chosen, series = target, s
        if chosen is None:
            print(f"{name}: no vol target keeps development drawdown <= 35%")
            continue
        dev, hold = describe(series[dev_mask]), describe(series[~dev_mask])
        ok = hold["monthly_mean_pct"] >= 5 and hold["max_dd_pct"] <= 35
        out[name] = {"vol_target": chosen, "development": dev, "holdout": hold, "success": ok}
        print(f"{name} vol {chosen}: dev m {dev['monthly_mean_pct']} med {dev['monthly_median_pct']} "
              f"sh {dev['sharpe']} dd {dev['max_dd_pct']} | holdout m {hold['monthly_mean_pct']} "
              f"sh {hold['sharpe']} dd {hold['max_dd_pct']} months {hold['months_pct']} "
              f"{'SUCCESS' if ok else 'no'}", flush=True)
    print("holdout per pool member and old sleeve (raw 1x PnL, % total, Sharpe):")
    for key in list(pool) + round7.SLEEVES:
        h = pnl[key][~dev_mask]
        print(f"  {key:<44} {((1 + h).prod() - 1) * 100:7.1f}%  sh {bt.stats(h)['sharpe']}")
    with open(f"{round7.ROOT}/holdout2.json", "w") as handle:
        json.dump(out, handle, indent=1, default=str)


if __name__ == "__main__":
    main(sys.argv[1:])
