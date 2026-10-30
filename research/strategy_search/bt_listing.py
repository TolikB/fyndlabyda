"""Round 2: short newly listed Binance perpetuals (research only).

A listing is known on its first daily bar L. The short opens at the open of
L + delay (decided at the close of the bar before), is held for `hold` days and is
closed earlier when the day's high reaches entry * (1 + stop): that day returns
entry-to-stop plus 0.5% stop slippage, then the weight drops to zero. Each trade
gets `size` of equity; total gross is capped at `gross_cap` by scaling all shorts
down evenly. Funding, fees and tiered slippage come from bt.run.
"""

from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

import bt

STOP_SLIPPAGE = 0.005


def load_high(column="high"):
    k = pd.read_parquet(f"{bt.DATA}/klines_1d.parquet", columns=["symbol", "open_time", column])
    k = k[~k["symbol"].isin(bt.EXCLUDE)]
    k["date"] = pd.to_datetime(k["open_time"].astype("int64"), unit="ms").dt.normalize()
    k = k.drop_duplicates(["symbol", "date"])
    return k.pivot(index="date", columns="symbol", values=column).astype(float)


def listing_book(opn, high, first_day, *, delay, hold, stop, size, gross_cap, min_qvol, qvol,
                 side=-1, low=None):
    """Short (side -1, stop on the high) or long (side +1, stop on the low) per event."""
    weights = pd.DataFrame(0.0, index=opn.index, columns=opn.columns)
    override = pd.DataFrame(np.nan, index=opn.index, columns=opn.columns)
    index = {day: i for i, day in enumerate(opn.index)}
    trades = []
    events = first_day.items() if hasattr(first_day, "items") else first_day
    for symbol, listed in events:
        start = index[listed] + delay                      # first held day
        if start >= len(opn.index) - 1:
            continue
        decided = start - 1
        # Liquidity known at the decision: median quote volume of the bars so far.
        seen = qvol[symbol].iloc[index[listed] : decided + 1].median()
        if not np.isfinite(seen) or seen < min_qvol:
            continue
        entry = opn[symbol].iloc[start]
        if not np.isfinite(entry):
            continue
        exit_reason, last = "time", min(start + hold - 1, len(opn.index) - 2)
        for i in range(start, last + 1):
            px = opn[symbol].iloc[i]
            if not np.isfinite(px):
                exit_reason, last = "delisted", i - 1
                break
            weights.iloc[i - 1, weights.columns.get_loc(symbol)] = side * size
            if stop is None:
                continue
            if side < 0 and high[symbol].iloc[i] >= entry * (1 + stop):
                fill = max(px, entry * (1 + stop)) * (1 + STOP_SLIPPAGE)
            elif side > 0 and low[symbol].iloc[i] <= entry * (1 - stop):
                fill = min(px, entry * (1 - stop)) * (1 - STOP_SLIPPAGE)
            else:
                continue
            override.iloc[i, override.columns.get_loc(symbol)] = fill / px - 1
            exit_reason, last = "stop", i
            break
        exit_px = opn[symbol].iloc[last + 1] if exit_reason == "time" else np.nan
        trades.append({"symbol": symbol, "entry_day": str(opn.index[start].date()),
                       "exit": exit_reason, "days": last - start + 1,
                       "move_pct": round((exit_px / entry - 1) * 100, 1) if np.isfinite(exit_px) else None})
    gross = weights.abs().sum(axis=1)
    weights = weights.div((gross / gross_cap).clip(lower=1.0), axis=0)
    return weights, override, trades


def hedge(weights, basket_live, kind):
    """Offset the book's net notional with BTC or the top-20 basket, decided the same day."""
    if kind is None:
        return weights
    offset = -weights.sum(axis=1)
    out = weights.copy()
    if kind == "BTC":
        out["BTCUSDT"] = out["BTCUSDT"] + offset
        return out
    basket = (basket_live & (weights == 0)).astype(float)
    return out + basket.div(basket.sum(axis=1).replace(0, np.nan), axis=0).fillna(0.0).mul(offset, axis=0)


def run_with_override(weights, override, opn, funding, med, close=None):
    pnl, parts = bt.run(weights, opn, funding, med, close=close)
    held = weights.shift(1).fillna(0.0)
    ret_oo = (opn.shift(-1) / opn - 1.0).fillna(0.0)
    # Replace the open-to-open return by open-to-stop on stop days.
    fix = (held * (override - ret_oo)).where(override.notna()).sum(axis=1)
    return pnl + fix, parts


def main():
    opn, close, qvol, funding = bt.load()
    high = load_high().reindex_like(opn).where(opn.notna())  # same traded days as opn
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    archive_start = opn.index.min() + pd.Timedelta(days=60)
    first = opn.apply(lambda col: col.first_valid_index())
    first = first[first > archive_start]
    _, med = bt.universe(close, qvol, 40)
    print(f"listings after {archive_start.date()}: {len(first)}; holdout from {holdout_start.date()}")
    top20, _ = bt.universe(close, qvol, 20)
    results, series = {}, {}
    grid = [(d, h, st, hk) for d in (1, 3) for h in (14, 30) for st in (0.5, 1.0, None)
            for hk in (None, "BTC", "top20")]
    for delay, hold, stop, hedge_kind in grid:
        w, ov, trades = listing_book(opn, high, first, delay=delay, hold=hold, stop=stop,
                                     size=0.1, gross_cap=1.0, min_qvol=20e6, qvol=qvol)
        w = hedge(w, top20, hedge_kind)
        pnl, parts = run_with_override(w, ov, opn, funding, med, close)
        dev = pnl[(pnl.index < holdout_start) & (pnl.index >= archive_start)]
        s = bt.stats(dev)
        dev_trades = [t for t in trades if t["entry_day"] < str(holdout_start.date())]
        stops = sum(t["exit"] == "stop" for t in dev_trades)
        key = f"listshort|d{delay}|h{hold}|stop{stop}|hedge{hedge_kind}"
        series[key] = pnl
        results[key] = {"dev": s, "parts": {k: round(float(v), 3) for k, v in parts.items()},
                        "trades": len(dev_trades), "stops": stops}
        print(f"{key:<40} n {len(dev_trades):>4} stop {stops:>3} m_mean {s['monthly_mean_pct']:>6} "
              f"cagr {s['cagr_pct']:>7} vol {s['vol_pct']:>6} sh {s['sharpe']:>5} "
              f"dd {s['max_dd_pct']:>5} pos {s['pos_months_pct']:>3} yrs {s['by_year_pct']}", flush=True)
    with open(f"{bt.DATA}/../results_round2.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{bt.DATA}/../pnl_round2.parquet")


if __name__ == "__main__":
    main()
