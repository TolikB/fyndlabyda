"""Does a sharp move on Binance's tape keep going for 5 to 60 seconds? (round 38, research only)

Pre-registered on 2026-11-11, before any price in the collector's rows was read. Rounds 20 and 25 read the same Binance
rows on 2026-11-16 and 2026-11-20; this file and its selftest were committed before either. Of the rows only file sizes
and the collector's counters were looked at, and this file was checked on synthetic data only
(selftest_jump_follow.py). No orders are placed anywhere, and no keys are used.

Why: in the listing jumps of round 16b half the move came 1.1 s after the post and more of it over the next seconds, so
news does not reach every bot at once. Round 11 lost money buying deep wicks with limit orders because moves went on
more often than a random walk would. If a sub-second move on a burst of volume keeps going for seconds, the tape itself
is the signal, on every coin, with no notice feed. It has to clear Binance's taker fee twice and the slip of a market
order in a fast market.

Data: ../hl_leadlag/collector.py (container hl-leadlag since 2026-11-01 13:00 UTC), on the host's own clock
  bn   Binance USDT-M aggTrades in 100 ms buckets by local receive time: last, hi, lo, buy_q and sell_q (quote volume
       taken by buyers and by sellers), e_last (Binance event time of the bucket's last trade), r_last (local receive)
  rtt  a REST round trip to Binance each minute; ONE_WAY = the day's median / 2
Symbols: every symbol in the day's rows.

Signal, per symbol, on the local 100 ms grid (last prices carried forward within the day), at bucket i:
  r = last now / last 1 s ago - 1, with |r| >= THETA
  the quote volume of the last 1 s is >= 20,000 USDT and >= 10 x the mean 1 s volume of the 15 minutes before that second
  taker buys less taker sells over that second have the sign of r
  the price 1 s ago comes from a row at most 2 s older than that
  then the symbol rests 60 s (separately for each THETA)
Silences: the collector runs 100 symbols to a connection, sorted by name. A connection whose symbols sent no row for over
2 s was down; so was one whose rows of the day begin or end over 2 s from the day's bounds. A signal needs its connection
up for the whole 15 minutes and 1 s before it, so that the volume baseline and the reference price are real. No signal
is taken in the first 15 minutes and 1 s of a UTC day, whose baseline would reach into the day before (the collector
also changes its connections five seconds after midnight), or after 23:58 UTC.
Trade, in the direction of r. An order sent at the end of the signal's bucket (t_sig) reaches Binance ONE_WAY later.
  entry  the first row whose Binance event time (running maximum of e_last) is at or after t_sig + ONE_WAY: a long pays
         that row's hi and a short gets its lo, so a market order in a fast market is charged the bucket's whole range
  exit   the same at t_sig + H + ONE_WAY, on the other side: a long gets the lo and a short pays the hi
  When no such row comes within 5 s of the arrival, the trade is dropped and counted.
Costs: Binance USDT-M taker fee 0.05% a side (the regular tier, no BNB discount). The 2x run pays 0.10% a side. Funding
is ignored over at most 60 s.
Book: at most 5 open trades, each 0.2 x s of equity, s in {1, 2, 3}; trades at the same time are taken in symbol order;
a trade's return lands on its exit day.

Trials: 9 = THETA {0.3%, 0.6%, 1.2%} x H {5 s, 15 s, 60 s}.

Bar on development (2026-11-01 13:00 .. 2026-11-15 00:00 UTC): the bar of round 20 and one more rule
  - mean net return a trade > 0 with t >= 2.5, t over the hourly sums of trade returns
  - monthly figure = mean daily book return x 30 >= 5% at the smallest such s*, max drawdown <= 35% at s*
  - monthly figure at s* > 0 at 2x costs
  - no UTC day carries more than half of the net sum (book at s = 1, 1x costs)
Holdout: 2026-11-15 .. 2026-11-29 00:00 UTC, one look, only for trials that pass, the same bar at the dev s*. A pass earns
only a forward paper series. A significantly negative mean is not a pass for the reverse trade, which would need a round
of its own.
Reported, not part of the bar: the drift (signed change of the last price from the signal to the exit row, no costs), the
gross mean, the dropped trades, the share of trades in a UTC second with signals on 5 or more symbols, and the net by
day.

Known limits: rows hold trades by local receive time, so a fill row can carry trades from just before the order arrived,
and its range is charged as slip; no queue or book depth is modelled. ONE_WAY is half a round trip that opens a new
connection each time. Connections are rebuilt from the day's symbols, which can differ from the collector's list by a
symbol that never traded. Binance's clock is taken to be this host's, which chrony held within a millisecond of NTP time.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_jump_follow.py dev        results_jump_follow_dev.json, /w/pnl_round38.parquet
  python test_jump_follow.py holdout    results_jump_follow_holdout.json (refuses a second time)
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
sys.path.insert(0, os.path.join(HERE, "..", "xvenue"))
import test_lag as lag  # noqa: E402

DATA = os.environ.get("BN_OUT", "/w/hl_leadlag/data")
PNL = "/w/pnl_round38.parquet"
DAY, B = lag.DAY, lag.B
LOOK = 10                    # buckets in 1 s
BASE = 9_000                 # buckets in 15 min
BURST, FLOOR = 10.0, 20_000.0
FRESH = 20                   # buckets (2 s)
REST = 600                   # buckets (60 s)
GAP_MS = 2_000
PER_CONN = 100
END_MS = DAY - 120_000
SLACK = 5_000
ONE_WAY_DEFAULT = 130
FEE = 0.0005
SLOT, OPEN_MAX, SIZES = 0.2, 5, (1, 2, 3)
BEST_DAY = 0.5
THETAS = {"t03": 0.003, "t06": 0.006, "t12": 0.012}
HOLDS = {"h5": 5_000, "h15": 15_000, "h60": 60_000}
TRIALS = {f"{a}|{h}": (THETAS[a], HOLDS[h]) for a in THETAS for h in HOLDS}
PERIODS = {"dev": ("2026-11-01 13:00", "2026-11-15"), "holdout": ("2026-11-15", "2026-11-29")}
COLS = ["b", "sym", "last", "hi", "lo", "buy_q", "sell_q", "e_last", "r_last"]


def connections(syms):
    """The collector's connections: the day's symbols sorted by name, PER_CONN to a connection -> {symbol: index}."""
    return {s: k // PER_CONN for k, s in enumerate(sorted(syms))}


def silences(buckets, lo, hi):
    """Bucket starts with a row on one connection -> (starts, ends) in local ms of the stretches of over GAP_MS without
    a row, counting the stretch before the day's first row and after its last."""
    b = np.r_[lo - B, np.unique(np.asarray(buckets, dtype=np.int64)), hi]
    k = np.flatnonzero(np.diff(b) > GAP_MS)
    return b[k] + B, b[k + 1]


def up_mask(n, day0, starts, ends):
    """Grid mask of the buckets whose connection had no silence in the LOOK + BASE buckets before them or in them."""
    down = np.zeros(n, dtype=np.int64)
    for s, e in zip(starts, ends):
        down[max(0, int((s - day0) // B)):max(0, min(n, int((e - day0) // B)))] = 1
    cd = np.r_[0, np.cumsum(down)]
    i = np.arange(n)
    return (cd[i + 1] - cd[np.maximum(i - LOOK - BASE, 0)]) == 0


def features(g, day0):
    """One symbol's rows of one day, sorted by (b, r_last) -> grid arrays: last price carried forward, grid index of
    its row, return over 1 s, quote volume of the last 1 s, taker imbalance of the last 1 s, mean 1 s volume of the
    15 minutes before, and whether the reference price is fresh."""
    n = DAY // B
    bi = (g["b"].to_numpy(np.int64) - day0) // B
    lastrow = np.r_[bi[1:] != bi[:-1], True]
    last, pos = lag.grid_ffill(bi[lastrow], g["last"].to_numpy(float)[lastrow], n)
    buy, sell = g["buy_q"].to_numpy(float), g["sell_q"].to_numpy(float)
    cq = np.r_[0.0, np.cumsum(np.bincount(bi, buy + sell, minlength=n))]
    ci = np.r_[0.0, np.cumsum(np.bincount(bi, buy - sell, minlength=n))]
    i = np.arange(n)
    j = np.maximum(i - LOOK, 0)                      # the bucket 1 s ago
    with np.errstate(invalid="ignore", divide="ignore"):
        r = last / last[j] - 1
    v1 = cq[i + 1] - cq[j + 1]                       # buckets i-9 .. i
    m1 = ci[i + 1] - ci[j + 1]
    base = (cq[j + 1] - cq[np.maximum(j + 1 - BASE, 0)]) / (BASE / LOOK)
    ok = (i - LOOK - BASE >= 0) & (pos[j] >= j - FRESH) & np.isfinite(r)
    return {"last": last, "r": r, "v1": v1, "m1": m1, "base": base, "ok": ok}


def signals(f, allowed, theta):
    """Feature arrays of one symbol-day and a grid mask of allowed buckets -> [(grid index, side)] after the rest."""
    r = f["r"]
    hit = (f["ok"] & allowed & (np.abs(r) >= theta) & (f["v1"] >= FLOOR) & (f["v1"] >= BURST * f["base"])
           & (np.sign(f["m1"]) == np.sign(r)))
    out, rest_until = [], -1
    for k in np.flatnonzero(hit):
        if k <= rest_until:
            continue
        out.append((int(k), 1 if r[k] > 0 else -1))
        rest_until = k + REST
    return out


def fill_row(em, arrival):
    """Index of the first row whose running-max event time is at or after arrival, or None when none comes within
    SLACK of it."""
    j = int(np.searchsorted(em, arrival))
    if j == len(em) or em[j] - arrival > SLACK:
        return None
    return j


def symbol_day(g, day0, one_way, allowed, trials):
    """g: one symbol's rows of one UTC day, sorted by (b, r_last). allowed: grid mask of buckets where a signal may be
    taken. trials: {key: (theta, H)}. -> (trade rows, {key: dropped trades})."""
    f = features(g, day0)
    em = np.maximum.accumulate(g["e_last"].to_numpy(np.int64))
    hi, lo, lp = (g[c].to_numpy(float) for c in ("hi", "lo", "last"))
    by_theta = {}
    for key, (theta, H) in trials.items():
        by_theta.setdefault(theta, []).append((key, H))
    rows, dropped = [], {}
    for theta, keys in by_theta.items():
        for i, side in signals(f, allowed, theta):
            t_sig = day0 + (i + 1) * B                   # the bucket's end on the local clock
            a = fill_row(em, t_sig + one_way)
            for key, H in keys:
                x = fill_row(em, t_sig + H + one_way)
                if a is None or x is None:
                    dropped[key] = dropped.get(key, 0) + 1
                    continue
                entry = hi[a] if side > 0 else lo[a]
                exit_ = lo[x] if side > 0 else hi[x]
                gross = side * (exit_ / entry - 1)
                rows.append({"trial": key, "t_sig": t_sig, "exit_t": t_sig + H, "side": side, "entry": float(entry),
                             "exit": float(exit_), "gross": gross, "drift": side * (lp[x] / f["last"][i] - 1),
                             "ret1": gross - 2 * FEE, "ret2": gross - 4 * FEE})
    return rows, dropped


def read_day(day0):
    """One UTC day of bn rows, sorted by (sym, b, r_last). A part is filed under the date it was written on, so the
    day's last rows are in the first parts of the next date."""
    day, after = (f"{pd.Timestamp(d, unit='ms'):%Y-%m-%d}" for d in (day0, day0 + DAY))
    parts = sorted(glob.glob(f"{DATA}/bn/{day}/*.parquet")) + sorted(glob.glob(f"{DATA}/bn/{after}/000*.parquet"))
    frames = []
    for p in parts:
        df = pd.read_parquet(p, columns=COLS)
        frames.append(df[(df["b"] >= day0) & (df["b"] < day0 + DAY)])
    if not frames:
        return pd.DataFrame(columns=COLS)
    return pd.concat(frames, ignore_index=True).sort_values(["sym", "b", "r_last"], kind="stable", ignore_index=True)


def day_rows(df, d0, one_way, trials, lo, hi):
    """One UTC day of rows sorted by (sym, b, r_last); lo, hi: the period's bounds in ms. -> (trade rows,
    {key: dropped trades}, silences per connection)."""
    n = DAY // B
    t_end = d0 + (np.arange(n) + 1) * B
    when = (t_end <= d0 + END_MS) & (t_end >= lo) & (t_end < hi)
    conn = connections(df["sym"].unique())
    up, gaps = {}, {}
    for k in sorted(set(conn.values())):
        names = [s for s, c in conn.items() if c == k]
        st, en = silences(df.loc[df["sym"].isin(names), "b"].to_numpy(), d0, d0 + DAY)
        up[k], gaps[k] = up_mask(n, d0, st, en) & when, int(len(st))
    rows, dropped = [], {}
    for sym, g in df.groupby("sym", sort=True):
        got, drop = symbol_day(g, d0, one_way, up[conn[sym]], trials)
        rows += [{"coin": sym, **x} for x in got]
        for key, c in drop.items():
            dropped[key] = dropped.get(key, 0) + c
    return rows, dropped, [gaps[k] for k in sorted(gaps)]


def run(period, trials):
    lo, hi = (lag.ms(x) for x in PERIODS[period])
    rtt = lag.read(DATA, "rtt", lo, hi)
    rows, days, dropped = [], [], {}
    for d0 in range(lo // DAY * DAY, hi, DAY):
        df = read_day(d0)
        if df.empty:
            continue
        rt = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "binance")]["ms"] if len(rtt) else []
        one_way = int(round(rt.median() / 2)) if len(rt) else ONE_WAY_DEFAULT
        got, drop, gaps = day_rows(df, d0, one_way, trials, lo, hi)
        rows += got
        for key, c in drop.items():
            dropped[key] = dropped.get(key, 0) + c
        days.append({"day": f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d}", "symbols": int(df["sym"].nunique()),
                     "silences": gaps, "one_way_ms": one_way, "trades_so_far": len(rows)})
        print(json.dumps(days[-1]), flush=True)
    return pd.DataFrame(rows), days, dropped


def book(tr, col, s):
    """At most OPEN_MAX open trades, taken by signal time and then symbol -> (kept trades, book return by exit day)."""
    tr = tr.sort_values(["t_sig", "coin"], kind="stable")
    open_exits, keep = [], []
    for idx, et, xt in zip(tr.index, tr["t_sig"].to_numpy(), tr["exit_t"].to_numpy()):
        open_exits = [x for x in open_exits if x > et]
        if len(open_exits) < OPEN_MAX:
            open_exits.append(xt)
            keep.append(idx)
    acc = tr.loc[keep]
    day = pd.to_datetime(acc["exit_t"], unit="ms").dt.normalize()
    return acc, (SLOT * s * acc[col]).groupby(day).sum()


def evaluate(trades, period, dropped):
    lo, hi = PERIODS[period]
    days = pd.date_range(pd.Timestamp(lo).normalize(), pd.Timestamp(hi).normalize() - pd.Timedelta(days=1))
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
                                            "max_dd_pct": round(lag.drawdown(daily) * 100, 2)}
                if mult == "1x" and s == 1:
                    pnl[f"jumpfollow|{key}"] = daily
        d1 = pnl[f"jumpfollow|{key}"]
        total = float(d1.sum())
        acc, _ = book(g, "ret1", 1)
        busy = g.groupby(g["t_sig"] // 1000)["coin"].transform("nunique") >= 5
        r.update({"best_day_share": round(float(d1.max() / total), 3) if total > 0 else None,
                  "by_day_pct": {f"{d:%m-%d}": round(float(v * 100), 3) for d, v in d1.items()},
                  "gross_bp": round(float(acc["gross"].mean() * 1e4), 3),
                  "drift_bp": round(float(acc["drift"].mean() * 1e4), 3),
                  "signals": int(len(g)) + dropped.get(key, 0), "dropped": dropped.get(key, 0),
                  "clustered_pct": round(float(busy.mean() * 100), 1)})
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def passes(r, size=None):
    a, b = r["1x"], r["2x"]
    if not (a["mean_bp"] > 0 and a["t_hours"] >= 2.5):
        return False, size
    if size is None:
        size = next((s for s in SIZES if a["sizes"][str(s)]["monthly_figure_pct"] >= 5), None)
    if size is None:
        return False, None
    share = r.get("best_day_share")
    ok = (a["sizes"][str(size)]["monthly_figure_pct"] >= 5 and a["sizes"][str(size)]["max_dd_pct"] <= 35
          and b["sizes"][str(size)]["monthly_figure_pct"] > 0 and share is not None and share <= BEST_DAY)
    return bool(ok), size


def main():
    mode = sys.argv[1]
    if mode not in PERIODS:
        raise SystemExit("usage: test_jump_follow.py dev|holdout")
    path = f"{HERE}/results_jump_follow_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_jump_follow_dev.json"))
        ok = [k for k in TRIALS if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trials = {k: TRIALS[k] for k in ok}
    else:
        trials = TRIALS
    trades, days, dropped = run(mode, trials)
    if trades.empty:
        raise SystemExit("no trades at all: a data bug until shown otherwise")
    out, pnl = evaluate(trades, mode, dropped)
    for key in trials:
        if key not in out:
            out[key] = {"trades": 0, "signals": dropped.get(key, 0), "passes": False, "size": None}
            continue
        out[key]["passes"], out[key]["size"] = passes(out[key], None if dev is None else dev[key]["size"])
    out["_days"] = days
    if mode == "dev":
        pnl.reindex(columns=[f"jumpfollow|{k}" for k in trials], fill_value=0.0).to_parquet(PNL)
    trades.to_parquet(f"{HERE}/trades_jump_follow_{mode}.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
