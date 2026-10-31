"""Hourly portfolio backtests with the rules of bt.run (research only).

- weights decided at the close of hour h trade at the open of h+1 and earn
  open-to-open returns; `hold_every` keeps them for k hours between rebalances;
- the universe and the slippage tier of an hour on day D come from day D-1, so both
  are known before the hour (bt.universe, bt.slippage);
- costs: taker fee plus that slippage on every unit of turnover;
- funding: each settlement is paid by the position held in the hour before it
  (calc_time minus one minute);
- a symbol whose data ends is settled at its last close with bt.DELIST_PENALTY;
  hours missing inside a symbol's life are filled flat, so a quiet hour is not a delisting.
`bt_hourly_selftest.py` checks that a once-a-day book reproduces bt.run exactly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import bt

HOURS_PER_YEAR = 365 * 24


def fill_interior(frame: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill gaps between a symbol's first and last bar, never past the last."""
    return frame.ffill().where(frame.bfill().notna())


def load(columns=("open", "close"), symbols=None):
    """Wide hourly tables; `symbols` limits memory to the ones a study can trade."""
    h = pd.read_parquet(f"{bt.DATA}/klines_1h.parquet", columns=["symbol", "open_time", *set(columns) | {"quote_volume"}],
                        filters=[("symbol", "in", sorted(symbols))] if symbols is not None else None)
    h = h[(h["quote_volume"] > 0) & ~h["symbol"].isin(bt.EXCLUDE)]
    h["t"] = pd.to_datetime(h["open_time"].astype("int64"), unit="ms")
    h = h.drop_duplicates(["symbol", "t"])
    out = {}
    for c in columns:
        wide = h.pivot(index="t", columns="symbol", values=c).astype("float32")
        out[c] = wide
    index = pd.date_range(min(v.index.min() for v in out.values()),
                          max(v.index.max() for v in out.values()), freq="h")
    for c in columns:
        wide = out[c].reindex(index)
        out[c] = fill_interior(wide) if c in ("open", "close") else wide
    return out


def funding_hourly(index, columns):
    f = pd.read_parquet(f"{bt.DATA}/fundingRate_all.parquet")
    f = f[f["symbol"].isin(columns)]
    f["t"] = (pd.to_datetime(f["calc_time"].astype("int64"), unit="ms") - pd.Timedelta(minutes=1)).dt.floor("h")
    return f.groupby(["t", "symbol"])["last_funding_rate"].sum().unstack().reindex(
        index=index, columns=columns).fillna(0.0)


def from_daily(daily: pd.DataFrame, index, columns, lag_days=1):
    """The daily value of day D - lag_days for every hour of day D."""
    days = index.normalize() - pd.Timedelta(days=lag_days)
    out = daily.reindex(columns=columns).reindex(days)
    out.index = index
    return out


def hold_every(weights: pd.DataFrame, k: int) -> pd.DataFrame:
    """Rebalance only at the close of hours k-1, 2k-1, ... of the day."""
    if k == 1:
        return weights
    keep = (weights.index.hour % k) == (k - 1)
    return weights[keep].reindex(weights.index).ffill()


def run(weights, opn, funding, slip, cost_mult=1.0, close=None):
    """bt.run on hourly bars; `slip` is already causal per bar, so it is not shifted."""
    w = weights.fillna(0.0)
    held = w.shift(1).fillna(0.0)
    ret_oo = opn.shift(-1) / opn - 1.0
    ends = opn.notna() & opn.shift(-1).isna()
    last_move = (close / opn - 1.0) if close is not None else 0.0 * opn
    ret_oo = ret_oo.where(~ends, last_move)
    gross = (held * ret_oo.fillna(0.0)).sum(axis=1) - (held.abs() * ends).sum(axis=1) * bt.DELIST_PENALTY
    fund = -(held * funding.fillna(0.0)).sum(axis=1)
    turnover = (held - held.shift(1).fillna(0.0)).abs()
    costs = (turnover * (bt.FEE + slip.fillna(0.0025)) * cost_mult).sum(axis=1)
    pnl = gross + fund - costs
    return pnl, {"gross": gross.sum(), "funding": fund.sum(), "costs": costs.sum(),
                 "turnover_per_day": turnover.sum(axis=1).mean() * 24}


def asset_vol(opn, lookback=24 * 30):
    r = opn.pct_change(fill_method=None)
    return r.rolling(lookback, min_periods=lookback // 2).std() * np.sqrt(HOURS_PER_YEAR)


def vol_target(raw, opn, target_ann, cap_gross=3.0, lookback=24 * 60):
    """bt.vol_target on hourly bars."""
    rets = opn.shift(-1) / opn - 1.0
    port = (raw.shift(1).fillna(0.0) * rets.fillna(0.0)).sum(axis=1)
    realised = port.rolling(lookback, min_periods=lookback // 3).std() * np.sqrt(HOURS_PER_YEAR)
    scale = (target_ann / realised).clip(upper=10.0).shift(1)
    scaled = raw.mul(scale.fillna(0.0), axis=0)
    over = (scaled.abs().sum(axis=1) / cap_gross).clip(lower=1.0)
    return scaled.div(over, axis=0)


def daily(pnl: pd.Series) -> pd.Series:
    """Hourly returns compounded into calendar days, for bt.stats and the registry."""
    return (1 + pnl).groupby(pnl.index.normalize()).prod() - 1
