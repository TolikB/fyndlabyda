"""Does Lighter lag Binance by more than its 300 ms taker delay? (round 25, research only)

Pre-registered on 2026-11-05, before the development window opened; the collected rows have only been counted, and
this file was checked on synthetic data only (selftest_lag.py). No orders are placed anywhere.

Why: round 20 asks the same of Hyperliquid, with a 0.045% fee to clear and so a 0.4% move to wait for. Lighter
charges standard accounts nothing and quotes majors under 1 bp wide, so far smaller moves could pay, and they come
far more often. Its price for that is a 300 ms delay on taker orders, while resting orders can be pulled faster.

Data, all on the host's own clock
  bn   hl_leadlag: Binance USDT-M aggTrades, last price per 100 ms bucket
  lt   collector.py: Lighter best bid and ask per 100 ms bucket, with the local receive time r_last
  rtt  collector.py: REST round trips to Lighter each minute; ONE_WAY is the day's median / 2
Coins: Lighter markets whose <coin>USDT or 1000<coin>USDT is in the Binance data.

Signal, per coin, on the local 100 ms grid (prices carried forward within a day)
  r_bn = Binance last now / 1 s ago - 1; r_lt = Lighter mid now / 1 s ago - 1; gap = r_bn - r_lt
  fire when |r_bn| >= THETA, gap has the sign of r_bn with |gap| >= THETA / 2, the Lighter quote changed in the last
  10 s and the top of the book on the side we take holds at least 2,500 USDT; then the coin rests 5 s
Trade in the direction of r_bn. An order sent at the end of the signal's bucket reaches the engine after ONE_WAY +
300 ms. The host sees that book ONE_WAY later still, so on the local clock:
  entry  ask (long) or bid (short) of the first row with r_last >= t_sig + 2 x ONE_WAY + 300 ms
  exit   the same at t_sig + H + 2 x ONE_WAY + 300 ms, on the other side of the book
Costs: fees are zero and both fills cross the spread. The 2x run takes a further half spread on each side, at
that side's own quote. Funding is ignored over at most 30 s.
Book: at most 5 open trades, each 0.2 x s of equity, s in {1, 2, 3}; a trade's return lands on its exit day.

Trials: 9 = THETA {0.05%, 0.1%, 0.2%} x H {2 s, 5 s, 30 s}.

Bar on development (2026-11-06 .. 2026-11-20 00:00 UTC)
  - mean net return a trade > 0 with t >= 2.5, t over the hourly sums of trade returns (hours with trades)
  - monthly figure = mean daily book return x 30 >= 5% at the smallest such s*, max drawdown <= 35% at s*
  - monthly figure at s* > 0 at 2x costs
Holdout: 2026-11-20 .. 2026-12-04 00:00 UTC, one look, only for trials that pass. A pass earns only a forward paper
series. Rows from before 2026-11-06 are the pilot and are left out of both.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_lag.py dev        results_lag_dev.json, /w/pnl_round25.parquet
  python test_lag.py holdout    results_lag_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
LT_DATA = os.environ.get("OUT", f"{HERE}/data")
BN_DATA = os.environ.get("BN_OUT", "/w/hl_leadlag/data")
B = 100
DAY = 86_400_000
LOOK = 10                  # buckets in 1 s
STALE = 100                # buckets (10 s)
DEPTH = 2500.0
REST = 50                  # buckets (5 s)
TAKER_DELAY = 300
SLOT, OPEN_MAX, SIZES = 0.2, 5, (1, 2, 3)
THETAS = {"t5": 0.0005, "t10": 0.001, "t20": 0.002}
HOLDS = {"h2": 2_000, "h5": 5_000, "h30": 30_000}
TRIALS = [f"{a}|{b}" for a in THETAS for b in HOLDS]
PERIODS = {"dev": ("2026-11-06", "2026-11-20"), "holdout": ("2026-11-20", "2026-12-04")}


def ms(x):
    return int(pd.Timestamp(x, tz="UTC").value // 1_000_000)


def read(root, table, lo, hi):
    days = pd.date_range(pd.Timestamp(lo, unit="ms").normalize(), pd.Timestamp(hi - 1, unit="ms").normalize())
    parts = [p for d in days for p in sorted(glob.glob(f"{root}/{table}/{d:%Y-%m-%d}/*.parquet"))]
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


def binance_symbol(coin, listed):
    for name in (f"{coin}USDT", f"1000{coin}USDT"):
        if name in listed:
            return name
    return None


def signals(bn_last, bid, ask, bid_sz, ask_sz, pos, theta):
    """Grid arrays for one coin-day -> list of (grid index, side)."""
    mid = (bid + ask) / 2
    out, rest_until = [], -1
    with np.errstate(invalid="ignore", divide="ignore"):
        r_bn = np.r_[np.full(LOOK, np.nan), bn_last[LOOK:] / bn_last[:-LOOK] - 1]
        r_lt = np.r_[np.full(LOOK, np.nan), mid[LOOK:] / mid[:-LOOK] - 1]
    gap = r_bn - r_lt
    cand = np.flatnonzero((np.abs(r_bn) >= theta) & (np.sign(gap) == np.sign(r_bn)) & (np.abs(gap) >= theta / 2))
    for i in cand:
        if i <= rest_until or pos[i] < 0 or i - pos[i] > STALE:
            continue
        side = 1 if r_bn[i] > 0 else -1
        depth = ask[i] * ask_sz[i] if side > 0 else bid[i] * bid_sz[i]
        if not np.isfinite(depth) or depth < DEPTH:
            continue
        out.append((int(i), side))
        rest_until = i + REST
    return out


def quote_at(r, bid, ask, t):
    """(bid, ask) of the first row with local receive time >= t, else None."""
    j = int(np.searchsorted(r, t))
    return (float(bid[j]), float(ask[j])) if j < len(r) else None


def trade_returns(side, q_in, q_out):
    """Net return at 1x and 2x costs; q are (bid, ask)."""
    entry, exit_ = (q_in[1], q_out[0]) if side > 0 else (q_in[0], q_out[1])
    gross = side * (exit_ / entry - 1)
    extra = (q_in[1] - q_in[0]) / 2 / entry + (q_out[1] - q_out[0]) / 2 / exit_
    return gross, gross - extra


def coin_day(bn, lt, day0, one_way, trials):
    """bn, lt: one coin's rows within one UTC day. trials: {key: (theta, H)}. -> trade rows."""
    n = DAY // B
    bi = ((bn["b"].to_numpy(np.int64) - day0) // B).clip(0, n - 1)
    li = ((lt["b"].to_numpy(np.int64) - day0) // B).clip(0, n - 1)
    bn_last, _ = grid_ffill(bi, bn["last"].to_numpy(float), n)
    bid, pos = grid_ffill(li, lt["bid"].to_numpy(float), n)
    ask, _ = grid_ffill(li, lt["ask"].to_numpy(float), n)
    bid_sz, _ = grid_ffill(li, lt["bid_sz"].to_numpy(float), n)
    ask_sz, _ = grid_ffill(li, lt["ask_sz"].to_numpy(float), n)
    l = lt.sort_values("r_last")
    r, b_, a_ = l["r_last"].to_numpy(np.int64), l["bid"].to_numpy(float), l["ask"].to_numpy(float)
    delay = 2 * one_way + TAKER_DELAY
    rows = []
    by_theta = {}
    for key, (theta, H) in trials.items():
        by_theta.setdefault(theta, []).append((key, H))
    for theta, keys in by_theta.items():
        for i, side in signals(bn_last, bid, ask, bid_sz, ask_sz, pos, theta):
            t_sig = day0 + i * B + B          # the bucket's end on the local clock
            q_in = quote_at(r, b_, a_, t_sig + delay)
            if q_in is None:
                continue
            for key, H in keys:
                q_out = quote_at(r, b_, a_, t_sig + H + delay)
                if q_out is None:
                    continue
                ret1, ret2 = trade_returns(side, q_in, q_out)
                rows.append({"trial": key, "t_sig": t_sig, "exit_t": t_sig + H, "side": side, "ret1": ret1, "ret2": ret2})
    return rows


def run(period, trials):
    lo, hi = (ms(x) for x in PERIODS[period])
    rtt = read(LT_DATA, "rtt", lo, hi)
    rows = []
    for d0 in range(lo, hi, DAY):
        bn, lt = read(BN_DATA, "bn", d0, d0 + DAY), read(LT_DATA, "lt", d0, d0 + DAY)
        if bn.empty or lt.empty:
            continue
        rt = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "lighter")]["ms"]
        one_way = int(round(rt.median() / 2)) if len(rt) else 150
        listed = set(bn["sym"].unique())
        for coin, g in lt.groupby("coin", observed=True):
            sym = binance_symbol(coin, listed)
            if sym is None:
                continue
            for x in coin_day(bn[bn["sym"] == sym], g, d0, one_way, trials):
                rows.append({"coin": coin, "one_way_ms": one_way, **x})
        print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d} {len(rows)} trades", flush=True)
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


def drawdown(daily):
    eq = (1 + daily).cumprod()
    return float((1 - eq / eq.cummax()).max())


def evaluate(trades, period):
    lo, hi = PERIODS[period]
    days = pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1))
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        r = {}
        for mult, col in (("1x", "ret1"), ("2x", "ret2")):
            acc, _ = book(g, col, 1)
            hourly = acc.groupby(acc["t_sig"] // 3_600_000)[col].sum()
            t = float(hourly.mean() / hourly.std() * math.sqrt(len(hourly))) if len(hourly) > 2 and hourly.std() > 0 else 0.0
            r[mult] = {"trades": int(len(acc)), "mean_bp": round(float(acc[col].mean() * 1e4), 3),
                       "median_bp": round(float(acc[col].median() * 1e4), 3), "t_hours": round(t, 2),
                       "win_pct": round(float((acc[col] > 0).mean() * 100), 1), "coins": int(acc["coin"].nunique()),
                       "sizes": {}}
            for s in SIZES:
                _, daily = book(g, col, s)
                daily = daily.reindex(days, fill_value=0.0)
                r[mult]["sizes"][str(s)] = {"monthly_figure_pct": round(float(daily.mean() * 30 * 100), 2),
                                            "max_dd_pct": round(drawdown(daily) * 100, 2)}
                if mult == "1x" and s == 1:
                    pnl[f"lag|{key}"] = daily
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def passes(r, size=None):
    a, b = r["1x"], r["2x"]
    if not (a["mean_bp"] > 0 and a["t_hours"] >= 2.5):
        return False, None
    if size is None:
        size = next((s for s in SIZES if a["sizes"][str(s)]["monthly_figure_pct"] >= 5), None)
    if size is None:
        return False, None
    ok = (a["sizes"][str(size)]["monthly_figure_pct"] >= 5 and a["sizes"][str(size)]["max_dd_pct"] <= 35
          and b["sizes"][str(size)]["monthly_figure_pct"] > 0)
    return bool(ok), size


def trial_map(keys=None):
    return {k: (THETAS[k.split("|")[0]], HOLDS[k.split("|")[1]]) for k in (keys or TRIALS)}


def main():
    mode = sys.argv[1]
    path = f"{HERE}/results_lag_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_lag_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trades = run("holdout", trial_map(ok))
    else:
        trades = run("dev", trial_map())
    out, pnl = evaluate(trades, mode)
    for key in out:
        out[key]["passes"], out[key]["size"] = passes(out[key], None if dev is None else dev[key]["size"])
    print(json.dumps(out, indent=1))
    if mode == "dev":
        pnl.to_parquet("/w/pnl_round25.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
