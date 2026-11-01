"""Does Hyperliquid lag Binance by enough seconds to pay a taker? (round 20, research only)

Pre-registered on 2026-11-01, when the collector had run for minutes and no row had been analysed.
Nothing is tuned after the first run. No orders are placed anywhere.

Data: collector.py on the research VM, 100 ms buckets on the host's own clock.
  bn   Binance USDT-M aggTrades (last price per bucket, local receive time)
  hl   Hyperliquid best bid/ask per bucket (HL block time h_last, local receive time)
  rtt  REST round trips each minute; the day's median HL round trip / 2 is the one-way order delay
Coins: HL coins whose Binance symbol (collector.binance_symbol) appears in the bn data.

Signal, per coin, on the local 100 ms grid (prices forward-filled within a day)
  r_bn = Binance last price now / 1 s ago - 1; r_hl = HL mid now / 1 s ago - 1; gap = r_bn - r_hl
  fire when |r_bn| >= 0.4%, gap has the sign of r_bn and |gap| >= 0.2%, the HL quote was updated in the last
  10 s, and the top of book on the side we take holds at least 2,500 USDT; then the coin rests 60 s
Trade (direction of r_bn), trials H5 (exit after 5 s) and H30 (30 s)
  entry  the order reaches HL at signal time + one-way delay; it fills at the ask (long) or bid (short) of the
         first HL row whose block time is at or after that arrival
  exit   the same at signal time + H + one-way delay, on the other side of the book
  costs  HL taker 0.045% on each side (2x: 0.09%); funding is ignored over at most 30 s
Book: at most 5 open trades, each 0.2 x s of equity, s in {1, 2, 3}; a trade's return lands on its exit day.

Bar on development (2026-11-01 13:00 .. 2026-11-15 00:00 UTC)
  - mean net return a trade > 0 with t >= 2.5, t over the hourly sums of trade returns (hours with trades)
  - monthly figure = mean daily book return x 30 >= 5% at the smallest such s*, max drawdown <= 35% at s*
  - monthly figure at s* > 0 at 2x costs
Holdout: 2026-11-15 .. 2026-11-29 00:00 UTC, one look, only for trials that pass. Descriptive only (no
trial): how far HL lags Binance after large Binance moves, and the subset of trades on listing-jump coins.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_leadlag.py dev        results_dev.json, /w/pnl_round20.parquet
  python test_leadlag.py holdout    results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import json
import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
import bt  # noqa: E402  (stats)

from collector import binance_symbol  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("OUT", f"{HERE}/data")
B = 100
LOOK = 10                  # buckets in 1 s
THETA, GAP = 0.004, 0.002
STALE = 100                # buckets (10 s)
DEPTH = 2500.0
REST = 600                 # buckets (60 s)
FEE = 0.00045
SLOT, OPEN_MAX, SIZES = 0.2, 5, (1, 2, 3)
TRIALS = {"H5": 5_000, "H30": 30_000}
PERIODS = {"dev": ("2026-11-01 13:00", "2026-11-15"), "holdout": ("2026-11-15", "2026-11-29")}
DAY = 86_400_000


def ms(x):
    return int(pd.Timestamp(x, tz="UTC").value // 1_000_000)


def read(table, lo, hi):
    days = pd.date_range(pd.Timestamp(lo, unit="ms").normalize(), pd.Timestamp(hi - 1, unit="ms").normalize())
    parts = [p for d in days for p in sorted(glob.glob(f"{DATA}/{table}/{d:%Y-%m-%d}/*.parquet"))]
    if not parts:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    key = "b" if "b" in df.columns else "t"
    return df[(df[key] >= lo) & (df[key] < hi)]


def grid_ffill(idx, values, n):
    """Forward-fill values placed at grid positions idx (sorted) onto a grid of n; NaN before the first."""
    out = np.full(n, np.nan)
    out[idx] = values
    pos = np.where(~np.isnan(out), np.arange(n), -1)
    pos = np.maximum.accumulate(pos)
    return np.where(pos >= 0, out[np.maximum(pos, 0)], np.nan), pos


def signals(bn_last, bid, ask, bid_sz, ask_sz, hl_pos):
    """Grid arrays for one coin-day -> list of (grid index, side)."""
    n = len(bn_last)
    mid = (bid + ask) / 2
    out, rest_until = [], -1
    with np.errstate(invalid="ignore", divide="ignore"):
        r_bn = np.r_[np.full(LOOK, np.nan), bn_last[LOOK:] / bn_last[:-LOOK] - 1]
        r_hl = np.r_[np.full(LOOK, np.nan), mid[LOOK:] / mid[:-LOOK] - 1]
    gap = r_bn - r_hl
    cand = np.flatnonzero((np.abs(r_bn) >= THETA) & (np.sign(gap) == np.sign(r_bn)) & (np.abs(gap) >= GAP))
    for i in cand:
        if i <= rest_until or hl_pos[i] < 0 or i - hl_pos[i] > STALE:
            continue
        side = 1 if r_bn[i] > 0 else -1
        depth = ask[i] * ask_sz[i] if side > 0 else bid[i] * bid_sz[i]
        if not np.isfinite(depth) or depth < DEPTH:
            continue
        out.append((int(i), side))
        rest_until = i + REST
    return out


def fill(h_sorted, px_sorted, arrival):
    """Price of the first HL row with block time >= arrival, else None."""
    j = int(np.searchsorted(h_sorted, arrival))
    return float(px_sorted[j]) if j < len(h_sorted) else None


def trade_return(side, entry, exit_, mult):
    gross = exit_ / entry - 1 if side > 0 else -(exit_ / entry - 1)
    return gross - 2 * FEE * mult


def coin_day(bn, hl, day0, one_way, trials):
    """bn, hl: one coin's rows within one UTC day. -> trade rows."""
    n = DAY // B
    bi = ((bn["b"].to_numpy(np.int64) - day0) // B).clip(0, n - 1)
    hi_ = ((hl["b"].to_numpy(np.int64) - day0) // B).clip(0, n - 1)
    bn_last, _ = grid_ffill(bi, bn["last"].to_numpy(float), n)
    bid, hl_pos = grid_ffill(hi_, hl["bid"].to_numpy(float), n)
    ask, _ = grid_ffill(hi_, hl["ask"].to_numpy(float), n)
    bid_sz, _ = grid_ffill(hi_, hl["bid_sz"].to_numpy(float), n)
    ask_sz, _ = grid_ffill(hi_, hl["ask_sz"].to_numpy(float), n)
    h = hl.sort_values("h_last")
    hs, hb, ha = h["h_last"].to_numpy(np.int64), h["bid"].to_numpy(float), h["ask"].to_numpy(float)
    rows = []
    for i, side in signals(bn_last, bid, ask, bid_sz, ask_sz, hl_pos):
        t_sig = day0 + i * B + B          # the bucket's end on the local clock
        e = fill(hs, ha if side > 0 else hb, t_sig + one_way)
        if e is None:
            continue
        for key, H in trials.items():
            x = fill(hs, hb if side > 0 else ha, t_sig + H + one_way)
            if x is None:
                continue
            rows.append({"trial": key, "t_sig": t_sig, "exit_t": t_sig + H, "side": side, "entry": e, "exit": x,
                         "ret1": trade_return(side, e, x, 1.0), "ret2": trade_return(side, e, x, 2.0)})
    return rows


def run(period, trials=TRIALS):
    lo, hi = (ms(x) for x in PERIODS[period])
    rtt = read("rtt", lo, hi)
    rows = []
    for d0 in range(lo // DAY * DAY, hi, DAY):
        a, b_ = max(lo, d0), min(hi, d0 + DAY)
        bn, hl = read("bn", a, b_), read("hl", a, b_)
        if bn.empty or hl.empty:
            continue
        r = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "hl")]["ms"]
        one_way = int(round(r.median() / 2)) if len(r) else 150
        listed = set(bn["sym"].unique())
        for coin, g in hl.groupby("coin"):
            sym = binance_symbol(coin, listed)
            if sym is None:
                continue
            for x in coin_day(bn[bn["sym"] == sym], g, d0, one_way, trials):
                rows.append({"coin": coin, "one_way_ms": one_way, **x})
    return pd.DataFrame(rows)


def book(tr, col, s):
    tr = tr.sort_values("t_sig")
    open_exits, keep = [], []
    for idx, et, xt in zip(tr.index, tr["t_sig"].to_numpy(), tr["exit_t"].to_numpy()):
        open_exits = [x for x in open_exits if x > et]
        if len(open_exits) < OPEN_MAX:
            open_exits.append(xt)
            keep.append(idx)
    acc = tr.loc[keep]
    day = pd.to_datetime(acc["exit_t"], unit="ms").dt.normalize()
    return acc, (SLOT * s * acc[col]).groupby(day).sum()


def evaluate(trades, period):
    lo, hi = PERIODS[period]
    days = pd.date_range(pd.Timestamp(lo).normalize(), pd.Timestamp(hi) - pd.Timedelta(days=1))
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        r = {}
        for mult, col in (("1x", "ret1"), ("2x", "ret2")):
            acc, _ = book(g, col, 1)
            hourly = acc.groupby(acc["t_sig"] // 3_600_000)[col].sum()
            t = float(hourly.mean() / hourly.std() * math.sqrt(len(hourly))) if len(hourly) > 2 and hourly.std() > 0 else 0.0
            r[mult] = {"trades": int(len(acc)), "mean_pct": round(float(acc[col].mean() * 100), 4),
                       "median_pct": round(float(acc[col].median() * 100), 4), "t_hours": round(t, 2),
                       "win_pct": round(float((acc[col] > 0).mean() * 100), 1), "coins": int(acc["coin"].nunique()),
                       "sizes": {}}
            for s in SIZES:
                _, daily = book(g, col, s)
                daily = daily.reindex(days, fill_value=0.0)
                st = bt.stats(daily)
                r[mult]["sizes"][str(s)] = {"monthly_figure_pct": round(float(daily.mean() * 30 * 100), 2),
                                            "max_dd_pct": st["max_dd_pct"]}
                if mult == "1x" and s == 1:
                    pnl[f"leadlag|{key}"] = daily
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def passes(r, size=None):
    a, b = r["1x"], r["2x"]
    if not (a["mean_pct"] > 0 and a["t_hours"] >= 2.5):
        return False, None
    if size is None:
        size = next((s for s in SIZES if a["sizes"][str(s)]["monthly_figure_pct"] >= 5), None)
    if size is None:
        return False, None
    ok = (a["sizes"][str(size)]["monthly_figure_pct"] >= 5 and a["sizes"][str(size)]["max_dd_pct"] <= 35
          and b["sizes"][str(size)]["monthly_figure_pct"] > 0)
    return bool(ok), size


def main():
    mode = sys.argv[1]
    path = f"{HERE}/results_{mode}.json"
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trades = run("holdout", {k: TRIALS[k] for k in ok})
    else:
        trades = run("dev")
    out, pnl = evaluate(trades, mode)
    for key in out:
        out[key]["passes"], out[key]["size"] = passes(out[key], None if mode == "dev" else dev[key]["size"])
    print(json.dumps(out, indent=1))
    trades.to_parquet(f"{HERE}/trades_{mode}.parquet")
    if mode == "dev":
        pnl.to_parquet("/w/pnl_round20.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
