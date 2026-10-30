"""Daily portfolio backtests for crypto perpetual strategies (research only).

Honesty rules built in:
- signals use data up to the close of day t; positions trade at the open of t+1
  and earn open-to-open returns (no look-ahead);
- point-in-time universe: top N by trailing 30-day median quote volume, at least
  90 days of history, including symbols that were later delisted;
- costs: taker fee 5 bps plus liquidity-tiered slippage on every unit of turnover;
- funding: positions pay/receive the funding that actually settled each day;
- a symbol whose data ends is closed at its last open with a 2% penalty;
- parameter sets are fixed ensembles, not fitted; the last HOLDOUT_MONTHS are kept
  out of every development run and opened once for the final candidate.
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

DATA = sys.argv[1] if len(sys.argv) > 1 else "/w/data"
HOLDOUT_MONTHS = 6
FEE = 0.0005
EXCLUDE = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT", "FDUSDUSDT", "BTCDOMUSDT",
           "DEFIUSDT", "BLUEBIRDUSDT", "FOOTBALLUSDT", "USDEUSDT"}


def load():
    k = pd.read_parquet(f"{DATA}/klines_1d.parquet")
    k = k[~k["symbol"].isin(EXCLUDE)]
    k["date"] = pd.to_datetime(k["open_time"].astype("int64"), unit="ms").dt.normalize()
    k = k.drop_duplicates(["symbol", "date"]).sort_values(["date", "symbol"])
    # The archive keeps emitting flat zero-volume bars after a delisting: drop them,
    # so a dead symbol ends on its last traded day.
    k = k[k["quote_volume"] > 0]
    px = {c: k.pivot(index="date", columns="symbol", values=c).astype(float)
          for c in ("open", "close", "quote_volume")}
    if not os.path.exists(f"{DATA}/fundingRate_all.parquet"):
        print("WARNING: no funding file, funding is zero (debug runs only)", flush=True)
        return px["open"], px["close"], px["quote_volume"], 0.0 * px["open"].fillna(0.0)
    f = pd.read_parquet(f"{DATA}/fundingRate_all.parquet")
    f = f[~f["symbol"].isin(EXCLUDE)]
    # Rebalances trade a moment after the 00:00 settlement, so that settlement is paid
    # by the position held over the previous day.
    f["date"] = (pd.to_datetime(f["calc_time"].astype("int64"), unit="ms")
                 - pd.Timedelta(minutes=1)).dt.normalize()
    funding = f.groupby(["date", "symbol"])["last_funding_rate"].sum().unstack().reindex(
        index=px["open"].index, columns=px["open"].columns).fillna(0.0)
    return px["open"], px["close"], px["quote_volume"], funding


def universe(close, qvol, top):
    med = qvol.rolling(30, min_periods=20).median()
    age = close.notna().cumsum()
    eligible = med.where(age >= 90)
    rank = eligible.rank(axis=1, ascending=False)
    return rank <= top, med


def slippage(med_qvol):
    s = pd.DataFrame(0.0025, index=med_qvol.index, columns=med_qvol.columns)
    s[med_qvol > 50e6] = 0.0010
    s[med_qvol > 200e6] = 0.0005
    s[med_qvol > 1e9] = 0.0002
    return s


DELIST_PENALTY = 0.02


def run(weights, opn, funding, med_qvol, cost_mult=1.0, close=None):
    """weights: target weights decided at close of day t (index t)."""
    w = weights.fillna(0.0)
    held = w.shift(1).fillna(0.0)          # position during day t (open t -> open t+1)
    ret_oo = opn.shift(-1) / opn - 1.0     # open t -> open t+1
    ends = opn.notna() & opn.shift(-1).isna()
    # A symbol whose data ends is settled at its last close (flat when no close is
    # given), and pays DELIST_PENALTY whatever the side.
    last_move = (close / opn - 1.0) if close is not None else 0.0 * opn
    ret_oo = ret_oo.where(~ends, last_move)
    gross = (held * ret_oo.fillna(0.0)).sum(axis=1) - (held.abs() * ends).sum(axis=1) * DELIST_PENALTY
    fund = -(held * funding.shift(-0).fillna(0.0)).sum(axis=1)
    turnover = (held - held.shift(1).fillna(0.0)).abs()
    costs = (turnover * (FEE + slippage(med_qvol).shift(1).fillna(0.0025)) * cost_mult).sum(axis=1)
    pnl = gross + fund - costs
    return pnl, {"gross": gross.sum(), "funding": fund.sum(), "costs": costs.sum(),
                 "turnover_per_day": turnover.sum(axis=1).mean()}


def vol_target(raw, opn, target_ann, cap_gross=3.0, lookback=60):
    """Scale a raw weight book so its ex-ante (trailing realised) vol hits target."""
    rets = opn.shift(-1) / opn - 1.0
    port = (raw.shift(1).fillna(0.0) * rets.fillna(0.0)).sum(axis=1)
    realised = port.rolling(lookback, min_periods=20).std() * np.sqrt(365)
    scale = (target_ann / realised).clip(upper=10.0).shift(1)  # known before the day
    scaled = raw.mul(scale.fillna(0.0), axis=0)
    gross = scaled.abs().sum(axis=1)
    over = (gross / cap_gross).clip(lower=1.0)
    return scaled.div(over, axis=0)


def asset_vol(opn, lookback=30):
    r = opn.pct_change(fill_method=None)
    return r.rolling(lookback, min_periods=15).std() * np.sqrt(365)


def tsmom(opn, close, live, lookbacks=(10, 20, 40, 80, 160)):
    sig = sum(np.sign(close / close.shift(L) - 1.0) for L in lookbacks) / len(lookbacks)
    raw = sig / asset_vol(opn)
    raw = raw.where(live)
    return raw.div(raw.abs().sum(axis=1), axis=0)


def xsmom(opn, close, live, lookbacks=(7, 14, 28), quantile=0.2):
    score = sum((close.shift(1) / close.shift(L + 1) - 1.0).rank(axis=1, pct=True)
                for L in lookbacks) / len(lookbacks)
    score = score.where(live)
    long = score >= 1 - quantile
    short = score <= quantile
    inv = 1.0 / asset_vol(opn)
    raw = (long.astype(float) - short.astype(float)) * inv
    raw = raw.where(live).fillna(0.0)
    pos = raw.clip(lower=0)
    neg = (-raw).clip(lower=0)
    return pos.div(pos.sum(axis=1), axis=0).fillna(0) * 0.5 - neg.div(neg.sum(axis=1), axis=0).fillna(0) * 0.5


def carry(opn, close, funding, live, window=7, quantile=0.2, trend_filter=True):
    avg = funding.rolling(window, min_periods=3).mean().where(live)
    score = avg.rank(axis=1, pct=True)
    short = score >= 1 - quantile          # rich funding: shorts get paid
    long = score <= quantile               # negative funding: longs get paid
    raw = long.astype(float) - short.astype(float)
    if trend_filter:
        trend = np.sign(close / close.shift(40) - 1.0)
        raw = raw.where(np.sign(raw) == trend, 0.0)
    raw = (raw / asset_vol(opn)).where(live).fillna(0.0)
    pos = raw.clip(lower=0)
    neg = (-raw).clip(lower=0)
    return pos.div(pos.sum(axis=1), axis=0).fillna(0) * 0.5 - neg.div(neg.sum(axis=1), axis=0).fillna(0) * 0.5


def stats(pnl):
    pnl = pnl.dropna()
    eq = (1 + pnl).cumprod()
    dd = 1 - eq / eq.cummax()
    monthly = (1 + pnl).resample("ME").prod() - 1
    years = (1 + pnl).resample("YE").prod() - 1
    sharpe = pnl.mean() / pnl.std() * np.sqrt(365) if pnl.std() > 0 else 0.0
    return {
        "months": len(monthly),
        "monthly_mean_pct": round(monthly.mean() * 100, 2),
        "monthly_median_pct": round(monthly.median() * 100, 2),
        "cagr_pct": round((eq.iloc[-1] ** (365 / len(pnl)) - 1) * 100, 1),
        "vol_pct": round(pnl.std() * np.sqrt(365) * 100, 1),
        "sharpe": round(sharpe, 2),
        "max_dd_pct": round(dd.max() * 100, 1),
        "pos_months_pct": round((monthly > 0).mean() * 100, 0),
        "p_month_ge_10pct": round((monthly >= 0.10).mean() * 100, 0),
        "by_year_pct": {str(k.year): round(v * 100, 1) for k, v in years.items()},
    }


def main():
    opn, close, qvol, funding = load()
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=HOLDOUT_MONTHS)).normalize()
    print(f"data {opn.index.min().date()} .. {end.date()}, symbols {opn.shape[1]}, "
          f"holdout from {holdout_start.date()}")
    results, series = {}, {}
    for top in (20, 40):
        live, med = universe(close, qvol, top)
        books = {
            "tsmom": tsmom(opn, close, live),
            "xsmom": xsmom(opn, close, live),
            "carry": carry(opn, close, funding, live),
        }
        books["combo"] = (books["tsmom"] + books["xsmom"] + books["carry"]) / 3
        for name, raw in books.items():
            for target in (0.2, 0.4, 0.6, 0.8, 1.0):
                w = vol_target(raw, opn, target)
                for cost_mult in (1.0, 2.0):
                    pnl, parts = run(w, opn, funding, med, cost_mult, close=close)
                    dev = pnl[pnl.index < holdout_start]
                    key = f"{name}|top{top}|vol{int(target*100)}|cost{cost_mult:g}x"
                    series[key] = pnl
                    results[key] = {"dev": stats(dev), "parts": {k: round(float(v), 3) for k, v in parts.items()}}
                    s = results[key]["dev"]
                    print(f"{key:<34} m_mean {s['monthly_mean_pct']:>6} med {s['monthly_median_pct']:>6} "
                          f"cagr {s['cagr_pct']:>7} vol {s['vol_pct']:>6} sh {s['sharpe']:>5} "
                          f"dd {s['max_dd_pct']:>5} pos {s['pos_months_pct']:>3} p10 {s['p_month_ge_10pct']:>3}",
                          flush=True)
    with open(f"{DATA}/../results_round1.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{DATA}/../pnl_round1.parquet")


if __name__ == "__main__":
    main()
