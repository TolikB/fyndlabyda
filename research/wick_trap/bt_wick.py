"""Wick trap: resting limit orders far from the price on mid-cap Binance perps (research only).

Pre-registered on 2026-10-31, before any 1-minute data was downloaded. This is Track A of the
2,500 USDT small-capacity plan. Nothing is tuned after the first run.

Idea: liquidation cascades on mid-cap alts print 1-minute wicks that partly revert. Orders that
rest far from the price fill only on such a wick. They fill as a maker, at a size a fund cannot use.

Data
  Binance USDT-M perpetual 1-minute klines from data.binance.vision, 2022-01..2026-10.
  Daily klines and funding come from the forward archive (fwd/data).

Universe
  Each UTC day: symbols ranked 30..150 by the previous day's quote volume, among those with at
  least 30 days of history, minus bt.EXCLUDE. Zero-volume bars are dropped.

Each minute t (only when minute t-1 exists, i.e. there is no gap)
  P, sigma  P is the close of minute t-1. sigma is the std of 1-minute log returns over the
            previous 1,440 bars (at least 1,000 of them).
  orders    a buy limit at P*exp(-k*sigma) and a sell limit at P*exp(+k*sigma), k in {8, 12, 16}
  fill      only when the minute's low (high) trades through the level by at least one tick. A
            tick is the smallest price step seen in that symbol-month. The fill price is the level.
            If both levels trade in the same minute, take the side that is worse at the close.
  take-profit  a limit at half the wick retraced, fill*exp(+-k*sigma/2). It is live from the next
            minute and fills (as a maker) only when traded through by a tick, or when a minute
            opens through it.
  stop      fill*exp(-+k*sigma), executed as a taker.
            A later minute that opens through it exits at that open.
            A minute that touches it exits at the stop.
            A minute that reaches both the stop and the take-profit counts as the stop.
            If the entry minute itself reaches the stop, the exit is the worse of the stop and that
            minute's close. (The plan said "closes beyond the stop"; that reading would be
            optimistic, so it was tightened before any data was seen.)
  timeout   a taker exit at the close of the last minute within H minutes of the entry, H in {15, 60}
  one position per symbol at a time

Book
  2,500 USDT, 5 slots of 20% of equity each, no leverage, first come first served.
  Daily returns are compounded.

Costs
  maker 0.02%, taker 0.05%, plus bt.slippage of the 30-day median daily quote volume (to the
  previous day) on taker exits. Funding is charged for each settlement inside a position.
  Everything is rerun at 2x fees and slippage.

Trials
  6 (k x H). Long and short are a breakdown of the same trades, not extra trials.

Bar (development 2022-01-01..2025-06-30), which must hold at 1x and at 2x costs
  - mean net return a trade > 0, with day-clustered t >= 3
  - monthly mean >= 5% with max drawdown <= 35%
  - >= 60% positive months
  - the neighbouring k at the same H has a positive mean

Holdout (2025-07-01..2026-10-30)
  One look, only for at most 2 configurations that pass, chosen by development Sharpe.
  Earlier daily and hourly research has already seen the 2026-04..09 regime (holdout2.py).

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_wick.py dev        development run: results_dev.json, /w/pnl_round11.parquet
  python bt_wick.py holdout    the one holdout look: results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import heapq
import json
import math
import os
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
import bt  # noqa: E402  (EXCLUDE, slippage tiers, stats)

ROOT = "/w"
M1 = f"{ROOT}/data/klines_1m"
DAILY = f"{ROOT}/fwd/data"
HERE = os.path.dirname(os.path.abspath(__file__))
KS = (8, 12, 16)
HS = (15, 60)
MAKER, TAKER = 0.0002, 0.0005
SLOTS = 5
PERIODS = {"dev": ("2022-01-01", "2025-07-01"), "holdout": ("2025-07-01", "2026-10-31")}
SIG_WIN, SIG_MIN = 1440, 1000
RANK_LO, RANK_HI, MIN_AGE = 30, 150, 30
MIN = 60_000
DAY = 86_400_000


# ---------------------------------------------------------------- inputs

def universe():
    """-> (live: date x symbol bool, slip: date x symbol taker slippage), both known by the day before."""
    k = pd.read_parquet(f"{DAILY}/klines_1d.parquet", columns=["symbol", "open_time", "quote_volume"])
    k = k[~k["symbol"].isin(bt.EXCLUDE) & (k["quote_volume"].astype(float) > 0)]
    k["date"] = pd.to_datetime(k["open_time"].astype("int64"), unit="ms").dt.normalize()
    qv = k.drop_duplicates(["symbol", "date"]).pivot(index="date", columns="symbol", values="quote_volume")
    qv = qv.astype(float).asfreq("D")
    age = qv.notna().cumsum()
    prev = qv.shift(1).where(age.shift(1) >= MIN_AGE)
    rank = prev.rank(axis=1, ascending=False)
    live = (rank >= RANK_LO) & (rank <= RANK_HI)
    med = qv.rolling(30, min_periods=20).median().shift(1)
    return live, bt.slippage(med)


def funding_events():
    f = pd.read_parquet(f"{DAILY}/fundingRate_all.parquet")
    f = f[~f["symbol"].isin(bt.EXCLUDE)].sort_values("calc_time")
    return {s: (g["calc_time"].astype("int64").to_numpy(), g["last_funding_rate"].astype(float).to_numpy())
            for s, g in f.groupby("symbol")}


def load_symbol(sym):
    files = sorted(glob.glob(f"{M1}/{sym}/*.parquet"))
    if not files:
        return None
    df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    df = df.drop_duplicates("t").sort_values("t")
    return df[df["qv"] > 0].reset_index(drop=True)


# ---------------------------------------------------------------- core

def month_ticks(t, o, h, l, c):
    """Smallest positive price step per calendar month, mapped to every bar."""
    m = t.astype("datetime64[ms]").astype("datetime64[M]").astype(np.int64)
    tick = np.zeros(len(t))
    for mm in np.unique(m):
        sel = m == mm
        u = np.unique(np.concatenate([o[sel], h[sel], l[sel], c[sel]]))
        d = np.diff(u)
        d = d[d > 1e-9 * u[1:]]  # ignore float noise between equal prices
        tick[sel] = d.min() if len(d) else 0.0
    return tick


def prepare(t, o, h, l, c, live_days, start_ms, end_ms):
    """-> P, sigma, tick, valid (an order may rest in minute i)."""
    gap = np.empty(len(t), bool)
    gap[0] = True
    gap[1:] = np.diff(t) != MIN
    lr = np.empty(len(t))
    lr[0] = np.nan
    lr[1:] = np.diff(np.log(c))
    lr[gap] = np.nan
    sig = pd.Series(lr).rolling(SIG_WIN, min_periods=SIG_MIN).std().shift(1).to_numpy()
    P = np.empty(len(t))
    P[0] = np.nan
    P[1:] = c[:-1]
    P[gap] = np.nan
    valid = (np.isin(t // DAY, live_days) & np.isfinite(P) & np.isfinite(sig) & (sig > 0)
             & (t >= start_ms) & (t < end_ms))
    return P, sig, month_ticks(t, o, h, l, c), valid


def trades_for(t, o, h, l, c, P, sig, tick, valid, k, H):
    """-> [(i, j, d, fill, exit_px, taker)]: entry bar i, exit bar j, direction d (+1 long)."""
    lb = P * np.exp(-k * sig)
    ls = P * np.exp(k * sig)
    long_hit = valid & (l < lb - tick)
    short_hit = valid & (h > ls + tick)
    out, free, n = [], -1, len(t)
    for i in np.flatnonzero(long_hit | short_hit):
        if i <= free:
            continue
        if long_hit[i] and short_hit[i]:
            d = 1 if (c[i] / lb[i] - 1) < (1 - c[i] / ls[i]) else -1
        else:
            d = 1 if long_hit[i] else -1
        fill = lb[i] if d > 0 else ls[i]
        step = k * sig[i]
        stop = fill * math.exp(-d * step)
        tp = fill * math.exp(d * step / 2)
        tk = tick[i]
        if (d > 0 and l[i] <= stop) or (d < 0 and h[i] >= stop):
            out.append((i, i, d, fill, min(stop, c[i]) if d > 0 else max(stop, c[i]), True))
            free = i
            continue
        res, j, end_t = None, i + 1, t[i] + H * MIN
        while j < n and t[j] <= end_t:
            if d > 0:
                if o[j] <= stop:
                    res = (o[j], True)
                elif o[j] > tp + tk:
                    res = (tp, False)
                elif l[j] <= stop:
                    res = (stop, True)
                elif h[j] > tp + tk:
                    res = (tp, False)
            else:
                if o[j] >= stop:
                    res = (o[j], True)
                elif o[j] < tp - tk:
                    res = (tp, False)
                elif h[j] >= stop:
                    res = (stop, True)
                elif l[j] < tp - tk:
                    res = (tp, False)
            if res is not None:
                break
            j += 1
        if res is None:
            j -= 1
            res = (c[j], True)
        out.append((i, j, d, fill, res[0], res[1]))
        free = j
    return out


def costs(d, fill, exit_px, taker, slip, funding_sum, mult):
    """Net simple return of one trade."""
    gross = d * (exit_px / fill - 1.0)
    fee_out = (TAKER + slip) if taker else MAKER
    return gross - mult * (MAKER + fee_out) - d * funding_sum


def book(trades):
    """trades: DataFrame(entry_t, exit_t, sym, ret) -> (accepted trades, daily returns)."""
    tr = trades.sort_values(["entry_t", "sym"])
    active, keep = [], []
    for idx, et, xt in zip(tr.index, tr["entry_t"].to_numpy(), tr["exit_t"].to_numpy()):
        while active and active[0] < et:
            heapq.heappop(active)
        if len(active) < SLOTS:
            heapq.heappush(active, xt)
            keep.append(idx)
    acc = tr.loc[keep]
    day = pd.to_datetime(acc["exit_t"], unit="ms").dt.normalize()
    daily = (acc["ret"] / SLOTS).groupby(day).sum()
    return acc, daily


# ---------------------------------------------------------------- run

G = {}


def init(period):
    live, slip = universe()
    G["live"], G["slip"], G["fund"] = live, slip, funding_events()
    G["start"], G["end"] = (int(pd.Timestamp(x).value // 1_000_000) for x in PERIODS[period])
    G["configs"] = G.get("configs") or [(k, h) for k in KS for h in HS]


def one_symbol(sym):
    df = load_symbol(sym)
    if df is None or len(df) < SIG_MIN + 2:
        return None
    col = G["live"][sym].to_numpy(dtype=bool)
    live_days = G["live"].index[col].asi8 // 1_000_000 // DAY
    t = df["t"].to_numpy(np.int64)
    o, h, l, c = (df[x].to_numpy(float) for x in ("o", "h", "l", "c"))
    P, sig, tick, valid = prepare(t, o, h, l, c, live_days, G["start"], G["end"])
    if not valid.any():
        return None
    slip_by_day = G["slip"][sym]
    ft, fr = G["fund"].get(sym, (np.array([], np.int64), np.array([])))
    rows = []
    for k, H in G["configs"]:
        for i, j, d, fill, px, taker in trades_for(t, o, h, l, c, P, sig, tick, valid, k, H):
            lo, hi = np.searchsorted(ft, t[i], "right"), np.searchsorted(ft, t[j] + MIN, "left")
            fsum = float(fr[lo:hi].sum())
            s = float(slip_by_day.get(pd.Timestamp(t[i] // DAY * DAY, unit="ms"), 0.0025))
            rows.append((f"k{k}_h{H}", sym, int(t[i]), int(t[j]), d, fill, px, taker, s, fsum,
                         costs(d, fill, px, taker, s, fsum, 1.0), costs(d, fill, px, taker, s, fsum, 2.0)))
    return pd.DataFrame(rows, columns=["cfg", "sym", "entry_t", "exit_t", "dir", "fill", "exit", "taker",
                                       "slip", "funding", "ret1", "ret2"])


def edge(acc, ret):
    if acc.empty:
        return {"trades": 0}
    day = pd.to_datetime(acc["entry_t"], unit="ms").dt.normalize()
    per_day = acc[ret].groupby(day).mean()
    t = per_day.mean() / per_day.std() * math.sqrt(len(per_day)) if len(per_day) > 2 and per_day.std() > 0 else 0.0
    side = {name: {"trades": int((acc["dir"] == s).sum()),
                   "mean_bp": round(float(acc.loc[acc["dir"] == s, ret].mean() * 1e4), 1) if (acc["dir"] == s).any() else None}
            for name, s in (("long", 1), ("short", -1))}
    return {"trades": int(len(acc)), "days": int(len(per_day)), "mean_bp": round(float(acc[ret].mean() * 1e4), 1),
            "t_day": round(float(t), 2), "win_pct": round(float((acc[ret] > 0).mean() * 100), 1),
            "taker_exit_pct": round(float(acc["taker"].mean() * 100), 1), **side}


def evaluate(trades, period):
    start, end = PERIODS[period]
    days = pd.date_range(start, pd.Timestamp(end) - pd.Timedelta(days=1), freq="D")
    out, pnl = {}, {}
    for cfg in sorted(trades["cfg"].unique()):
        sub = trades[trades["cfg"] == cfg]
        out[cfg] = {}
        for mult, ret in (("1x", "ret1"), ("2x", "ret2")):
            acc, daily = book(sub.assign(ret=sub[ret]))
            daily = daily.reindex(days, fill_value=0.0)
            out[cfg][mult] = {"edge": edge(acc, ret), "book": bt.stats(daily)}
            if mult == "1x":
                pnl[f"wick_{cfg}"] = daily
    return out, pd.DataFrame(pnl)


def passes(res, cfg):
    k, h = cfg.split("_")
    kk = int(k[1:])
    neighbours = [f"k{x}_{h}" for x in KS if abs(x - kk) == 4]
    for mult in ("1x", "2x"):
        e, b = res[cfg][mult]["edge"], res[cfg][mult]["book"]
        if not (e.get("trades") and e["mean_bp"] > 0 and e["t_day"] >= 3 and b["monthly_mean_pct"] >= 5
                and b["max_dd_pct"] <= 35 and b["pos_months_pct"] >= 60):
            return False
        if not all(res[n][mult]["edge"].get("mean_bp", -1) > 0 for n in neighbours):
            return False
    return True


def run(period, configs=None):
    G["configs"] = configs
    init(period)
    syms = sorted(s for s in G["live"].columns if G["live"][s].any() and os.path.isdir(f"{M1}/{s}"))
    print(f"{period}: {len(syms)} symbols, configs {G['configs']}", flush=True)
    parts = []
    with Pool(2) as pool:  # forked after init(), so the workers share G
        for n, part in enumerate(pool.imap_unordered(one_symbol, syms, chunksize=4)):
            if part is not None and len(part):
                parts.append(part)
            if (n + 1) % 50 == 0:
                print(f"  {n + 1}/{len(syms)} symbols", flush=True)
    trades = pd.concat(parts, ignore_index=True)
    trades.to_parquet(f"{HERE}/trades_{period}.parquet")
    return evaluate(trades, period)


def show(res):
    for cfg, r in res.items():
        for mult in ("1x", "2x"):
            e, b = r[mult]["edge"], r[mult]["book"]
            print(f"{cfg:<8} {mult}  trades {e.get('trades', 0):>6}  mean {e.get('mean_bp')} bp  t {e.get('t_day')}  "
                  f"win {e.get('win_pct')}%  long {e.get('long')}  short {e.get('short')}\n"
                  f"{'':<12}month mean {b['monthly_mean_pct']}%  median {b['monthly_median_pct']}%  "
                  f"dd {b['max_dd_pct']}%  pos {b['pos_months_pct']}%  sharpe {b['sharpe']}  years {b['by_year_pct']}",
                  flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        res, pnl = run("dev")
        for cfg in res:
            res[cfg]["passes"] = passes(res, cfg)
        show(res)
        print("passing:", [c for c in res if res[c]["passes"]], flush=True)
        pnl.to_parquet(f"{ROOT}/pnl_round11.parquet")
        with open(f"{HERE}/results_dev.json", "w") as fh:
            json.dump(res, fh, indent=1, default=str)
    elif mode == "holdout":
        out_path = f"{HERE}/results_holdout.json"
        if os.path.exists(out_path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = sorted((c for c in dev if dev[c]["passes"]), key=lambda c: -dev[c]["1x"]["book"]["sharpe"])[:2]
        if not ok:
            raise SystemExit("no configuration passed development: the holdout stays closed")
        res, _ = run("holdout", [(int(c.split("_")[0][1:]), int(c.split("_")[1][1:])) for c in ok])
        show(res)
        with open(out_path, "w") as fh:
            json.dump(res, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
