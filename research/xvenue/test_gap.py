"""Do price gaps between venues close by more than the costs? (round 24, research only)

Pre-registered on 2026-11-05, before the development window opened. Only the pilot hours of 11-05 have been read
(pilot.txt, spreads and how often a gap appears, never a trade result), and this file was checked on synthetic data only
(selftest_gap.py). No orders are placed anywhere.

Idea: one coin's perp on two venues should cost the same. On liquid coins it does. The pilot shows gaps on a few volatile
small caps, and a position of about 1,000 USDT a leg is too small for a fund to bother with. Buying the cheap leg and
selling the dear one is market neutral, so the drawdown should be low if the gap closes.

Quotes: collector.py, per coin on a 3 s grid (GRID_MS), carried at most 2 minutes. Prices are per one coin (see
pilot.canon). Pairs: two venues listing a coin, each with >= 1M USDT of 24 h quote volume in the last `slow` reading
before the UTC day. KuCoin has no volume feed and stays out.
Baseline: the median of the mid gap (mid_b / mid_a - 1) over 5-minute blocks, then the median of the last 72 finished
blocks (6 h), at least 12. A pair whose baseline is beyond +-3% is two tokens, or two contract sizes, and is skipped.
Costs: taker fee per venue (FEES; MEXC per contract from that day's schedule), on all four fills, plus half the quoted
spread again on every fill, because a real order walks the book and these prices are only the top. COST below is
the fees of the four fills.
Signal, either direction: edge = bid_b / ask_a - 1 - baseline >= THETA + COST on two grid bins in a row, i and i + 1.
Entry: at bin i + 2, buying a at its ask and selling b at its bid, plus the slip above.
Exit: at the bin after the mid gap first falls back to the entry baseline, or after the time limit below.
Funding: never paid, because every position is flat by minute 59 of its UTC hour and no entry is taken from minute 55.
Settlements on these venues fall on the hour, so a gap that exists only to pay a coupon is not captured here either.
A trade's return is on one leg's notional: the sum of the two legs' price moves, less costs.
Book: at most 5 open positions, one per coin, each with a leg notional of 1,000 x s USDT on 2,500 USDT of capital, s in
{1, 2, 3}. Returns land on the exit day. 2x costs doubles fees and slip.

Trials: 6 = THETA {10, 20, 40 bp} x venues {all seven, all but Gate, whose ticker call takes ~3 s}.

Periods: development 2026-11-07..2026-11-21; holdout 2026-11-21..2026-12-05. Rows before 11-07 are the pilot and stay out.
Bar on development
  - mean net return a trade > 0 with t >= 2.5, t over the hourly sums of trade returns (hours with trades)
  - monthly figure = mean daily book return x 30 >= 5% at the smallest such s*, max drawdown <= 35% at s*
  - monthly figure at s* > 0 at 2x costs
Holdout: the same bar with s* from development, one look, only for trials that pass. A pass earns only a forward paper
series with real top-of-book sizes.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research/xvenue mounted at /w/xvenue)
  python test_gap.py grid 2026-11-07    builds grid/<day>.npz from the raw tables (run once per day)
  python test_gap.py dev                results_gap_dev.json, pnl_round24.parquet
  python test_gap.py holdout            results_gap_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import gzip
import json
import math
import os
import sys

import numpy as np
import pandas as pd

import pilot

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("OUT", f"{HERE}/data")
GRIDS = f"{HERE}/grid"
GRID_MS = 3_000
DAY = 86_400_000
HOUR = 3_600_000
N = DAY // GRID_MS
BLOCK = 100                 # bins in 5 minutes
LOOK_BLOCKS, MIN_BLOCKS = 72, 12
STALE = 40                  # bins (2 minutes)
MIN_QV = 1e6
MAX_BASE = 0.03
THETAS = {"t10": 0.0010, "t20": 0.0020, "t40": 0.0040}
SETS = {"all": ("binance", "bybit", "okx", "gate", "bitget", "mexc", "lighter"),
        "nogate": ("binance", "bybit", "okx", "bitget", "mexc", "lighter")}
FEES = {"binance": 0.0005, "bybit": 0.00055, "okx": 0.0005, "gate": 0.0005, "bitget": 0.0006, "lighter": 0.0}
LIMITS_BINS = {"h60": 1200}
OPEN_MAX, LEG, CAPITAL, SIZES = 5, 1000.0, 2500.0, (1, 2, 3)
PERIODS = {"dev": ("2026-11-07", "2026-11-21"), "holdout": ("2026-11-21", "2026-12-05")}
TRIALS = [f"{t}|{s}" for t in THETAS for s in SETS]


def ms(x):
    return int(pd.Timestamp(x, tz="UTC").value // 1_000_000)


def carry(x, limit=STALE):
    """Forward-fill along the last axis, at most `limit` bins past a real value."""
    n = x.shape[-1]
    pos = np.where(np.isnan(x), -1, np.arange(n))
    pos = np.maximum.accumulate(pos, axis=-1)
    out = np.take_along_axis(x, np.maximum(pos, 0), axis=-1)
    out[(pos < 0) | (np.arange(n) - pos > limit)] = np.nan
    return out


def mexc_fees(day):
    """{coin: taker fee} from that day's raw schedule (or the latest one before it)."""
    days = sorted(p for p in glob.glob(f"{DATA}/meta/*/mexc.json.gz") if p.split("/")[-2] <= day)
    if not days:
        return {}
    with gzip.open(days[-1], "rt") as fh:
        d = json.load(fh)["data"]
    return {pilot.canon("mexc", x["symbol"])[0]: float(x.get("takerFeeRate") or 0.0) for x in d
            if x.get("quoteCoin") == "USDT" and x.get("apiAllowed")}


def eligible(day):
    """{(venue, raw symbol)} with enough volume at the last reading before the day."""
    t0 = ms(day)
    frames = [pd.read_parquet(p, columns=["t", "v", "s", "qv"]) for d in (pd.Timestamp(day) - pd.Timedelta(days=1),)
              for p in sorted(glob.glob(f"{DATA}/slow/{d:%Y-%m-%d}/*.parquet"))[-8:]]
    s = pd.concat(frames, ignore_index=True).astype({"v": object, "s": object})
    s = s[s["t"] < t0].sort_values("t").groupby(["v", "s"], as_index=False)["qv"].last()
    s = s[s["qv"] >= MIN_QV]
    return set(zip(s["v"], s["s"]))


def build_grid(day):
    """Raw tables of one UTC day -> grid/<day>.npz: bid, ask per coin unit, shape (series, N), float32."""
    t0 = ms(day)
    keep = eligible(day)
    bid, ask = {}, {}

    def put(key, bins, b, a):
        if key not in bid:
            bid[key], ask[key] = np.full(N, np.nan, "float32"), np.full(N, np.nan, "float32")
        bid[key][bins], ask[key][bins] = b, a

    for p in sorted(glob.glob(f"{DATA}/bbo/{day}/*.parquet")):
        f = pd.read_parquet(p, columns=["t", "v", "s", "bid", "ask"])
        for (v, s), g in f.groupby(["v", "s"], observed=True, sort=False):
            if (v, s) in keep:
                unit = pilot.canon(v, s)[1]
                put((v, s), ((g["t"].to_numpy() - t0) // GRID_MS).clip(0, N - 1), g["bid"].to_numpy() / unit,
                    g["ask"].to_numpy() / unit)
    for p in sorted(glob.glob(f"{DATA}/lt/{day}/*.parquet")):
        f = pd.read_parquet(p, columns=["coin", "bid", "ask", "r_last"]).sort_values("r_last")
        for s, g in f.groupby("coin", observed=True, sort=False):
            if ("lighter", s) in keep:
                unit = pilot.canon("lighter", s)[1]
                put(("lighter", s), ((g["r_last"].to_numpy() - t0) // GRID_MS).clip(0, N - 1), g["bid"].to_numpy() / unit,
                    g["ask"].to_numpy() / unit)
    keys = sorted(bid)
    os.makedirs(GRIDS, exist_ok=True)
    np.savez(f"{GRIDS}/{day}.npz", keys=np.array([f"{v}|{s}" for v, s in keys]),
             bid=carry(np.stack([bid[k] for k in keys])), ask=carry(np.stack([ask[k] for k in keys])))
    print(day, len(keys), "series", flush=True)


def baseline(gap):
    """gap over two days of bins -> baseline per bin from finished 5-minute blocks only (NaN where too few)."""
    n = len(gap) // BLOCK
    with np.errstate(all="ignore"):
        blocks = np.nanmedian(gap[: n * BLOCK].reshape(n, BLOCK), axis=1) if n else np.array([])
    base = pd.Series(blocks).rolling(LOOK_BLOCKS, min_periods=MIN_BLOCKS).median().to_numpy()
    out = np.full(len(gap), np.nan)
    for i in range(n):                  # block i is usable from the first bin of block i + 1
        out[(i + 1) * BLOCK: (i + 2) * BLOCK] = base[i]
    return out


def hour_end(i):
    """Last bin (minute 59 of the UTC hour) at or after bin i, counted from a day start."""
    per = HOUR // GRID_MS
    return (i // per) * per + per - 20


def legs_return(ba_in, bb_in, ba_out, bb_out, slip):
    """Buy a at ask, sell b at bid, reverse at exit. ba/bb are (bid, ask) tuples. -> return on one leg's notional."""
    a_in, b_in = ba_in[1] * (1 + slip), bb_in[0] * (1 - slip)
    a_out, b_out = ba_out[0] * (1 - slip), bb_out[1] * (1 + slip)
    return (a_out / a_in - 1) + (1 - b_out / b_in)


def half_spread(q):
    return (q[1] - q[0]) / (q[1] + q[0])


def scan(bid_a, ask_a, bid_b, ask_b, base, cost, theta, start, limit):
    """One direction of one pair: buy a, sell b. Arrays cover two days; `start` is the first bin of the day scored.
    -> list of (i_in, i_out)."""
    n = len(bid_a)
    with np.errstate(all="ignore"):
        edge = bid_b / ask_a - 1 - base
        mid_gap = ((bid_b + ask_b) / (bid_a + ask_a)) - 1
    hot = edge >= theta + cost
    per = HOUR // GRID_MS
    out, free = [], start
    for i in np.flatnonzero(hot[start:-3] & hot[start + 1:-2]) + start:
        j = i + 2
        if j < free or (j - start) % per >= per - 100 or abs(base[i]) > MAX_BASE:
            continue
        stop = min(j + limit, start + hour_end(j - start), n - 2)
        x = stop
        with np.errstate(all="ignore"):
            back = np.flatnonzero(mid_gap[j + 1: stop] <= base[i])
        if len(back):
            x = j + 1 + int(back[0]) + 1
        out.append((j, x))
        free = x + 1
    return out


def pair_trades(q_a, q_b, fee, start, thetas):
    """q_x: (bid, ask) arrays over two days. -> trade dicts for both directions and each theta."""
    gap = ((q_b[0] + q_b[1]) / (q_a[0] + q_a[1])) - 1
    base = baseline(gap.astype(float))
    cost = 2 * fee
    rows = []
    for key, theta in thetas.items():
        for way, (a, b, bs) in {"ab": (q_a, q_b, base), "ba": (q_b, q_a, -base)}.items():
            for i, x in scan(a[0], a[1], b[0], b[1], bs, cost, theta, start, LIMITS_BINS["h60"]):
                qa_in, qb_in = (float(a[0][i]), float(a[1][i])), (float(b[0][i]), float(b[1][i]))
                qa_out, qb_out = (float(a[0][x]), float(a[1][x])), (float(b[0][x]), float(b[1][x]))
                if not np.isfinite(qa_in + qb_in + qa_out + qb_out).all():
                    continue
                slip = half_spread(qa_in) + half_spread(qb_in) + half_spread(qa_out) + half_spread(qb_out)
                gross = legs_return(qa_in, qb_in, qa_out, qb_out, 0.0)
                rows.append({"theta": key, "i": int(i), "x": int(x), "gross": gross, "fee": cost, "slip": slip})
    return rows


def day_trades(day, sets):
    prev = f"{GRIDS}/{pd.Timestamp(day) - pd.Timedelta(days=1):%Y-%m-%d}.npz"
    now = np.load(f"{GRIDS}/{day}.npz")
    keys = [tuple(k.split("|", 1)) for k in now["keys"]]
    old = np.load(prev) if os.path.exists(prev) else None
    old_ix = {tuple(k.split("|", 1)): i for i, k in enumerate(old["keys"])} if old is not None else {}
    d0 = ms(day)
    fees = mexc_fees(day)
    coins = {}
    for i, (v, s) in enumerate(keys):
        coins.setdefault(pilot.canon(v, s)[0], []).append((v, s, i))

    def series(v, s, i):
        b, a = now["bid"][i].astype(float), now["ask"][i].astype(float)
        j = old_ix.get((v, s))
        pb = old["bid"][j].astype(float) if j is not None else np.full(N, np.nan)
        pa = old["ask"][j].astype(float) if j is not None else np.full(N, np.nan)
        return np.r_[pb, b], np.r_[pa, a]

    rows = []
    for coin, legs in coins.items():
        for n, (va, sa, ia) in enumerate(legs):
            for vb, sb, ib in legs[n + 1:]:
                fa = fees.get(coin) if va == "mexc" else FEES[va]
                fb = fees.get(coin) if vb == "mexc" else FEES[vb]
                if fa is None or fb is None:
                    continue
                for r in pair_trades(series(va, sa, ia), series(vb, sb, ib), fa + fb, N, THETAS):
                    for set_name, members in sets.items():
                        if va in members and vb in members:
                            t_in, t_out = d0 + (r["i"] - N) * GRID_MS, d0 + (r["x"] - N) * GRID_MS
                            rows.append({"trial": f"{r['theta']}|{set_name}", "coin": coin, "a": va, "b": vb, "t_in": t_in,
                                         "t_out": t_out, "ret1": r["gross"] - r["fee"] - r["slip"],
                                         "ret2": r["gross"] - 2 * r["fee"] - 2 * r["slip"]})
    return rows


def book(tr, col, s):
    tr = tr.sort_values("t_in")
    open_rows, keep = [], []
    for idx, ti, to, coin in zip(tr.index, tr["t_in"].to_numpy(), tr["t_out"].to_numpy(), tr["coin"].to_numpy()):
        open_rows = [x for x in open_rows if x[0] > ti]
        if len(open_rows) < OPEN_MAX and coin not in {x[1] for x in open_rows}:
            open_rows.append((to, coin))
            keep.append(idx)
    acc = tr.loc[keep]
    day = pd.to_datetime(acc["t_out"], unit="ms").dt.normalize()
    return acc, (LEG * s / CAPITAL * acc[col]).groupby(day).sum()


def drawdown(daily):
    eq = (1 + daily).cumprod()
    return float((1 - eq / eq.cummax()).max())


def evaluate(trades, period, keys):
    lo, hi = PERIODS[period]
    days = pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1))
    out, pnl = {}, {}
    for key in keys:
        g = trades[trades["trial"] == key]
        r = {}
        for mult, col in (("1x", "ret1"), ("2x", "ret2")):
            acc, _ = book(g, col, 1)
            hourly = acc.groupby(acc["t_in"] // HOUR)[col].sum()
            t = float(hourly.mean() / hourly.std() * math.sqrt(len(hourly))) if len(hourly) > 2 and hourly.std() > 0 else 0.0
            r[mult] = {"trades": int(len(acc)), "mean_bp": round(float(acc[col].mean() * 1e4), 2) if len(acc) else 0.0,
                       "median_bp": round(float(acc[col].median() * 1e4), 2) if len(acc) else 0.0, "t_hours": round(t, 2),
                       "win_pct": round(float((acc[col] > 0).mean() * 100), 1) if len(acc) else 0.0,
                       "coins": int(acc["coin"].nunique()), "sizes": {}}
            for s in SIZES:
                _, daily = book(g, col, s)
                daily = daily.reindex(days, fill_value=0.0)
                r[mult]["sizes"][str(s)] = {"monthly_figure_pct": round(float(daily.mean() * 30 * 100), 2),
                                            "max_dd_pct": round(drawdown(daily) * 100, 2)}
                if mult == "1x" and s == 1:
                    pnl[f"gap|{key}"] = daily
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


def run(period):
    lo, hi = PERIODS[period]
    rows = []
    for d in pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1)):
        rows += day_trades(f"{d:%Y-%m-%d}", SETS)
        print(f"{d:%Y-%m-%d} {len(rows)} rows", flush=True)
    return pd.DataFrame(rows)


def main():
    mode = sys.argv[1]
    if mode == "grid":
        build_grid(sys.argv[2])
        return
    path = f"{HERE}/results_gap_{mode}.json"
    dev = None
    keys = TRIALS
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_gap_dev.json"))
        keys = [k for k in dev if dev[k]["passes"]]
        if not keys:
            raise SystemExit("no trial passed development: the holdout stays closed")
    out, pnl = evaluate(run(mode), mode, keys)
    for key in out:
        out[key]["passes"], out[key]["size"] = passes(out[key], None if dev is None else dev[key]["size"])
    print(json.dumps(out, indent=1))
    if mode == "dev":
        pnl.to_parquet(f"{HERE}/pnl_round24.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
