"""Round 34: can a resting quote collect round 33's reversal instead of paying the spread for it? (research only)

Pre-registered on 2026-11-06, before the development window opened; the collected rows have only been counted, and this file
was checked on synthetic data only (selftest_quote_xs.py). No orders are placed anywhere.

Why: round 33 found that the coins that moved most against the market over 15 minutes reverse by 2.2 bp a period gross, on 2024
to mid-2025 Binance bars, with 54% of periods positive, against a cost of 2.7 bp for crossing the spread four times. At a maker
fee of 0 a quote is paid the spread rather than paying it, if the fills that come are not the bad ones. Round 33 saw these
numbers, so this rule was written after looking at development bars of round 33; the windows below hold new data.

Data: as test_quote.py. Clock, queue, through model and Side are that file's.

Every 15 minutes on the UTC grid, from 00:15 on: each contract's mid against its mid 15 minutes ago, less the mean of all
contracts. The N lowest get a resting buy, the N highest a resting sell, at the touch.
  Entry: the quote follows the touch for W seconds, then is pulled. No fill, no trade.
  Exit: held to 15 minutes after the signal, then rest the exit at the far touch for 30 s, then take the near touch at the first
  snapshot received from signal + 15 min + 30 s + 2 x ONE_WAY.
Costs: 0/0. The report also gives the mean less 2 bp on taker exits.
Book: each position holds s / (2 x N) of equity, s in {1, 2, 3}.

Trials: 4 = N {5, 10} x W {60 s, 300 s}.

Bar on development (2026-11-08 .. 2026-11-22 00:00 UTC): as round 25, with the through model as the 2x run.
Holdout: 2026-11-22 .. 2026-12-06 00:00 UTC, one look, only for trials that pass. A pass earns only a forward paper series.
Rows from before 2026-11-08 are a rehearsal of this code and are left out of both.

Known limits: as test_quote.py. Fifteen minutes are a short test of a small edge, and 2,500 USDT over 10 or 20 positions is
small at the touch, which suits it.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_quote_xs.py rehearsal   results_quote_xs_rehearsal.json: code check only, no reading of the numbers
  python test_quote_xs.py dev         results_quote_xs_dev.json, /w/pnl_round34.parquet
  python test_quote_xs.py holdout     results_quote_xs_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import math
import multiprocessing as mp
import os
import sys

import numpy as np
import pandas as pd

import test_quote as q

lag = q.lag
HERE = q.HERE
DATA = q.DATA
DAY = q.DAY
GRID = 15 * 60_000
PASSIVE_EXIT = 30_000
NS = {"n5": 5, "n10": 10}
WS = {"w60": 60_000, "w300": 300_000}
TRIALS = [f"{a}|{b}" for a in NS for b in WS]
SIZES = lag.SIZES
PERIODS = q.PERIODS
WORKERS = q.WORKERS


def mids_on_grid(mx, d0):
    """mx: one day's rows. -> frame of the latest mid at each grid point, by contract, indexed by ms; NaN before a first quote."""
    x = mx.sort_values("r_last")
    points = d0 + np.arange(0, DAY // GRID + 1) * GRID
    out = {}
    for sym, g in x.groupby("sym", observed=True):
        r, m = g["r_last"].to_numpy(np.int64), ((g["bid"] + g["ask"]) / 2).to_numpy(float)
        i = np.searchsorted(r, points, "right") - 1
        out[sym] = np.where(i >= 0, m[np.maximum(i, 0)], np.nan)
    return pd.DataFrame(out, index=points)


def picks(mids, N):
    """-> {grid time: (contracts to buy, contracts to sell)}, for grid points with 2 x N contracts quoted."""
    r = (mids / mids.shift(1) - 1).iloc[1:]
    r = r.sub(r.mean(axis=1), axis=0)
    out = {}
    for t, row in r.iterrows():
        x = row.dropna()
        if len(x) >= 2 * N:
            x = x.sort_values()
            out[int(t)] = (list(x.index[:N]), list(x.index[-N:]))
    return out


def leg(ev, r, t, buy, W, delay, model):
    """One signal on one contract. ev, r: its events and their times. -> position dict, or None without a fill."""
    i = int(np.searchsorted(r, t))
    n = len(ev)
    entry_side = q.Side(buy, model)
    stop = t + W
    entry, t_in = None, 0
    while i < n:
        kind, rr, a, b, c, d = ev[i]
        rr = int(rr)
        i += 1
        if rr >= stop + delay:
            return None
        if kind == 0:
            entry_side.roll(rr)
            if rr < stop:
                entry_side.want(a if buy else b, c if buy else d, rr, delay)
            else:
                entry_side.pull(rr, delay)
        else:
            got = entry_side.hit(a, b, int(c), rr)
            if got is not None:
                entry, t_in = got, rr
                break
    if entry is None:
        return None
    exit_at = t + GRID
    leave = exit_at + PASSIVE_EXIT + delay
    out_side = q.Side(not buy, model)
    while i < n:
        kind, rr, a, b, c, d = ev[i]
        rr = int(rr)
        i += 1
        if kind == 0:
            if rr >= leave:
                return {"t_in": t_in, "t_out": rr, "buy": buy, "entry": entry, "exit": a if buy else b, "taker": True}
            out_side.roll(rr)
            if rr >= exit_at:
                if rr < exit_at + PASSIVE_EXIT:
                    out_side.want(b if buy else a, d if buy else c, rr, delay)
                else:
                    out_side.pull(rr, delay)
        elif rr >= exit_at:
            got = out_side.hit(a, b, int(c), rr)
            if got is not None:
                return {"t_in": t_in, "t_out": rr, "buy": buy, "entry": entry, "exit": got, "taker": False}
    return None


def signal_returns(job):
    """One day's worth for one trial's picks, per contract."""
    sym, ev, jobs, delay = job
    r = ev[:, 1].astype(np.int64)
    rows = []
    for key, t, buy, W, model in jobs:
        x = leg(ev, r, t, buy, W, delay, model)
        if x:
            s = 1 if x["buy"] else -1
            rows.append({"trial": key, "model": model, "coin": sym, "t_sig": t, "t_in": x["t_in"], "exit_t": x["t_out"],
                         "taker": x["taker"], "ret": s * (x["exit"] / x["entry"] - 1)})
    return rows


def day_jobs(mx, mt, d0, one_way):
    mids = mids_on_grid(mx, d0)
    per_sym = {}
    for key in TRIALS:
        n, w = key.split("|")
        for t, (longs, shorts) in picks(mids, NS[n]).items():
            for model in q.MODELS:
                for sym in longs:
                    per_sym.setdefault(sym, []).append((key, t, True, WS[w], model))
                for sym in shorts:
                    per_sym.setdefault(sym, []).append((key, t, False, WS[w], model))
    out = []
    for sym, jobs in per_sym.items():
        a, b = mx[mx["sym"] == sym], mt[mt["sym"] == sym]
        if len(a) and len(b):
            out.append((sym, q.events(a, b), jobs, 2 * one_way))
    return out


def run(period):
    lo, hi = (lag.ms(x) for x in PERIODS[period])
    rtt = lag.read(DATA, "rtt", lo, hi)
    rows = []
    with mp.Pool(WORKERS) as pool:
        for d0 in range(lo, hi, DAY):
            mx, mt = lag.read(DATA, "mx", d0, d0 + DAY), lag.read(DATA, "mt", d0, d0 + DAY)
            if mx.empty or mt.empty:
                continue
            rt = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "mexc")]["ms"]
            one_way = int(round(rt.median() / 2)) if len(rt) else 150
            for part in pool.imap_unordered(signal_returns, day_jobs(mx, mt, d0, one_way)):
                rows.extend(part)
            print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d} {len(rows)} positions", flush=True)
    return pd.DataFrame(rows)


def evaluate(trades, period):
    lo, hi = PERIODS[period]
    days = pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1))
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        weight = 1 / (2 * NS[key.split("|")[0]])
        r = {}
        for mult, model in (("1x", "queue"), ("2x", "through")):
            m = g[g["model"] == model]
            if m.empty:
                r[mult] = {"trades": 0, "mean_bp": 0.0, "t_hours": 0.0, "sizes": {str(s): {"monthly_figure_pct": 0.0, "max_dd_pct": 0.0} for s in SIZES}}
                continue
            hourly = m.groupby(m["t_sig"] // 3_600_000)["ret"].sum()
            t = float(hourly.mean() / hourly.std() * math.sqrt(len(hourly))) if len(hourly) > 2 and hourly.std() > 0 else 0.0
            r[mult] = {"trades": int(len(m)), "mean_bp": round(float(m["ret"].mean() * 1e4), 3), "median_bp": round(float(m["ret"].median() * 1e4), 3),
                       "t_hours": round(t, 2), "win_pct": round(float((m["ret"] > 0).mean() * 100), 1),
                       "taker_exit_pct": round(float(m["taker"].mean() * 100), 1),
                       "mean_bp_at_0_2": round(float((m["ret"] - 2e-4 * m["taker"]).mean() * 1e4), 3), "sizes": {}}
            day = pd.to_datetime(m["exit_t"], unit="ms").dt.normalize()
            for s in SIZES:
                daily = (weight * s * m["ret"]).groupby(day).sum().reindex(days, fill_value=0.0)
                r[mult]["sizes"][str(s)] = {"monthly_figure_pct": round(float(daily.mean() * 30 * 100), 2),
                                            "max_dd_pct": round(lag.drawdown(daily) * 100, 2)}
                if mult == "1x" and s == 1:
                    pnl[f"quotexs|{key}"] = daily
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def main():
    mode = sys.argv[1]
    path = f"{HERE}/results_quote_xs_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_quote_xs_dev.json"))
        if not any(v["passes"] for v in dev.values()):
            raise SystemExit("no trial passed development: the holdout stays closed")
    trades = run(mode)
    if dev is not None:
        trades = trades[trades["trial"].isin([k for k, v in dev.items() if v["passes"]])]
    out, pnl = evaluate(trades, mode)
    for key in out:
        out[key]["passes"], out[key]["size"] = lag.passes(out[key], None if dev is None else dev[key]["size"])
    if mode == "dev":
        pnl.to_parquet("/w/pnl_round34.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    if mode != "rehearsal":
        print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
