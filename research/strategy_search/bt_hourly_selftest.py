"""Synthetic checks for bt_hourly: a once-a-day book reproduces bt.run exactly."""

import os
import tempfile

import numpy as np
import pandas as pd

import bt
import bt_hourly as bh

rng = np.random.default_rng(5)
n_days = 40
days = pd.date_range("2023-01-01", periods=n_days, freq="D")
hours = pd.date_range(days[0], periods=n_days * 24, freq="h")
cols = ["AUSDT", "BUSDT"]

# Prices move only inside hour 12 of each day, so hourly open-to-open returns sum to
# the daily open-to-open return and the comparison can be exact.
moves = rng.normal(0, 0.04, (n_days, 2))
day_open = 100 * np.vstack([np.ones(2), np.cumprod(1 + moves, axis=0)[:-1]])
h_open = np.repeat(day_open, 24, axis=0)
after_noon = np.tile(np.arange(24) >= 13, n_days)
h_open[after_noon] *= np.repeat(1 + moves, 24, axis=0)[after_noon]
opn_h = pd.DataFrame(h_open, hours, cols)
close_h = opn_h.shift(-1).fillna(opn_h)                 # close of hour h = open of h+1
opn_d = pd.DataFrame(day_open, days, cols)
close_d = pd.DataFrame(day_open * (1 + moves), days, cols)

# BUSDT's data ends at the close of day 25.
dead = 25
opn_h.loc[hours >= days[dead + 1], "BUSDT"] = np.nan
close_h.loc[hours >= days[dead + 1], "BUSDT"] = np.nan
opn_d.loc[days[dead + 1]:, "BUSDT"] = np.nan
close_d.loc[days[dead + 1]:, "BUSDT"] = np.nan

w_d = pd.DataFrame(rng.normal(0, 0.5, (n_days, 2)), days, cols)
w_d.loc[days[dead]:, "BUSDT"] = 0.0
w_h = pd.DataFrame(np.nan, hours, cols)
w_h.loc[w_d.index + pd.Timedelta(hours=23)] = w_d.to_numpy()
w_h = bh.hold_every(w_h, 24)

# Funding settles at 00:00, 08:00 and 16:00; both attributions go through the code paths
# the backtests use (bt.load's daily rule copied here, bt_hourly.funding_hourly on a file).
settle = pd.date_range(days[0] + pd.Timedelta(hours=8), days[-1] + pd.Timedelta(days=1), freq="8h")
rows = [{"symbol": s, "calc_time": int(t.value // 10**6) + 7, "last_funding_rate": rng.normal(1e-4, 2e-4)}
        for t in settle for s in cols]
fr = pd.DataFrame(rows)
fr["date"] = (pd.to_datetime(fr["calc_time"], unit="ms") - pd.Timedelta(minutes=1)).dt.normalize()
fund_d = fr.groupby(["date", "symbol"])["last_funding_rate"].sum().unstack().reindex(index=days, columns=cols).fillna(0.0)
with tempfile.TemporaryDirectory() as tmp:
    fr.drop(columns="date").to_parquet(os.path.join(tmp, "fundingRate_all.parquet"))
    saved, bt.DATA = bt.DATA, tmp
    fund_h = bh.funding_hourly(hours, cols)
    bt.DATA = saved

med = pd.DataFrame({"AUSDT": 2e9, "BUSDT": 1e8}, index=days)          # 2 bp and 10 bp
slip_h = bh.from_daily(bt.slippage(med), hours, cols)

# 1. A once-a-day book: per-day sums of the hourly PnL equal bt.run, including funding,
#    costs and the delisting penalty.
for cost_mult in (0.0, 1.0, 2.0):
    pnl_d, _ = bt.run(w_d, opn_d, fund_d, med, cost_mult, close=close_d)
    pnl_h, _ = bh.run(w_h, opn_h, fund_h, slip_h, cost_mult, close=close_h)
    per_day = pnl_h.groupby(pnl_h.index.normalize()).sum()
    assert np.allclose(per_day.to_numpy(), pnl_d.to_numpy(), atol=1e-12), (cost_mult, (per_day - pnl_d).abs().max())

# 2. Timing: foresight of the next hour wins, the hour just seen does not.
walk = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.01, (2000, 2)), axis=0)),
                    pd.date_range("2023-01-01", periods=2000, freq="h"), cols)
zero = 0.0 * walk
future = np.sign(walk.shift(-2) / walk.shift(-1) - 1)
pnl, _ = bh.run(future, walk, zero, zero, 0.0)
assert pnl.mean() > 0.005, pnl.mean()
stale = np.sign(walk / walk.shift(1) - 1)
pnl, _ = bh.run(stale, walk, zero, zero, 0.0)
assert abs(pnl.mean()) < 0.001, pnl.mean()

# 3. A quiet hour inside a symbol's life is filled flat, not settled as a delisting.
gappy = walk.copy()
gappy.iloc[500:503, 0] = np.nan
filled = bh.fill_interior(gappy)
assert filled.iloc[500:503, 0].eq(gappy.iloc[499, 0]).all()
gappy.iloc[1500:, 1] = np.nan
assert bh.fill_interior(gappy).iloc[1500:, 1].isna().all()

# 4. hold_every changes weights only after hours k-1, 2k-1, ...
held = bh.hold_every(future, 4)
changes = held.index[(held.diff().abs().sum(axis=1) > 0)]
assert ((changes.hour % 4) == 3).all()

# 5. The universe and slippage of an hour come from the previous day.
assert slip_h.loc["2023-01-05 13:00", "BUSDT"] == bt.slippage(med).loc["2023-01-04", "BUSDT"]
print("bt_hourly self-test ok")
