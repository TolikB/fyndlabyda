"""Round 4: trade the price drift right after funding settlements (research only).

Events come from settle_study.py (top-60 universe, previous settlement's rate known
long before T). Long when prev > +thr, short when prev < -thr, enter at the open of
hour T (just after the settlement), exit at the open of T+k. Exiting before the next
settlement means no funding is paid when the interval exceeds k hours; events whose
interval is <= k hours pay the next settlement at `rate` (a cost for the side that pays).
Each event takes 5% of equity; one hour's events are capped at 100% gross.
Costs: 5 bps taker fee plus the liquidity-tiered slippage on both sides.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

import bt
import evalx

ROOT = "/w"


def main():
    ev = pd.read_parquet(f"{ROOT}/settle_events.parquet")
    _, close, qvol, _ = bt.load()
    _, med = bt.universe(close, qvol, 60)
    slip = bt.slippage(med).shift(1)
    day = ev["T"].dt.normalize()
    stacked = slip.stack()
    ev["slip"] = stacked.reindex(pd.MultiIndex.from_arrays([day, ev["symbol"]])).fillna(0.0025).to_numpy()
    end = ev["T"].max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    results, series = {}, {}
    for side_name, side in (("long", 1), ("short", -1), ("both", 0)):
        for thr in (0.0003, 0.001):
            for k in (1, 2, 4):
                if side == 1:
                    pick = ev["prev"] > thr
                elif side == -1:
                    pick = ev["prev"] < -thr
                else:
                    pick = ev["prev"].abs() > thr
                e = ev[pick].copy()
                direction = np.sign(e["prev"]) if side == 0 else side
                move = e[f"post{k}"] / 1e4
                funding_paid = np.where(e["hours"] <= k, direction * e["rate"], 0.0)
                e["net"] = direction * move - funding_paid - 2 * (bt.FEE + e["slip"])
                e["gross"] = direction * move
                per_hour = e.groupby("T")["net"].transform("size")
                weight = np.minimum(0.05, 1.0 / per_hour)
                e["pnl"] = weight * e["net"]
                daily = e.groupby(e["T"].dt.normalize())["pnl"].sum()
                daily = daily.reindex(pd.date_range("2020-01-01", end.normalize(), freq="D"), fill_value=0.0)
                key = f"settle|{side_name}|thr{thr*1e4:g}bp|k{k}"
                series[key] = daily
                dev = daily[daily.index < holdout_start]
                s = bt.stats(dev)
                dev_e = e[e["T"] < holdout_start]
                results[key] = {"dev": s, "events": len(dev_e)}
                print(f"{key:<30} n {len(dev_e):>6} gross {dev_e['gross'].mean()*1e4:6.1f}bp "
                      f"med {dev_e['gross'].median()*1e4:6.1f}bp net {dev_e['net'].mean()*1e4:6.1f}bp "
                      f"sh {s['sharpe']:>5} cagr {s['cagr_pct']:>6} dd {s['max_dd_pct']:>5} "
                      f"q+ {evalx.quarters_positive(dev):4.0f} yrs {s['by_year_pct']}", flush=True)
    with open(f"{ROOT}/results_round4.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{ROOT}/pnl_round4.parquet")


if __name__ == "__main__":
    main()
