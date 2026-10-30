"""Open the holdout once for the candidate chosen on development data only.

Candidate (rule fixed before this run): the round-3 combination with the highest
development monthly mean among those with development max drawdown <= 35%:
combo|vol45|dd0.2|brake0.25 over four sleeves.
"""

import json

import pandas as pd

import bt
import combo
import evalx

SLEEVES = ["listshort|d1|h30|stop0.5|hedgetop20", "xsmom|top20|vol20|cost1x",
           "carry|top20|vol20|cost1x", "pumplong|top60|j0.3|h5|stop0.3|hedgeBTC"]

frames = [pd.read_parquet(f"/w/pnl_round{n}.parquet") for n in ("1", "2", "2c", "2d")]
pnl = pd.concat(frames, axis=1)
end = pnl.index.max()
holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
sleeves = pd.DataFrame({k: combo.unit_vol(pnl[k]) for k in SLEEVES})
sleeves = sleeves[sleeves.index >= "2020-07-01"]
series = combo.overlay(sleeves.mean(axis=1, skipna=True), 0.45, 0.2, 0.25, max_lev=3.0)
out = {}
for name, part in (("development", series[series.index < holdout_start]),
                   ("holdout", series[series.index >= holdout_start])):
    s = bt.stats(part)
    s["quarters_pos_pct"] = evalx.quarters_positive(part)
    monthly = ((1 + part).resample("ME").prod() - 1).round(4)
    s["months_pct"] = {str(k.date())[:7]: round(v * 100, 1) for k, v in monthly.items()}
    out[name] = s
    print(name, json.dumps(s, default=str))
print("holdout per sleeve (raw sleeve PnL, % total):")
for k in SLEEVES:
    h = pnl[k][pnl.index >= holdout_start]
    print(f"  {k:<44} total {((1 + h).prod() - 1) * 100:6.1f}%  sharpe {bt.stats(h)['sharpe']}")
json.dump(out, open("/w/holdout.json", "w"), indent=1, default=str)
