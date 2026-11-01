"""Short a new Binance USDT-M perp during its first day of trading (round 18, research only).

Pre-registered on 2026-11-01, before any 1-minute price of a launch day was looked at. Only the daily
archive's listing dates were inspected. Nothing is tuned after the first run. No orders are placed.

Idea: a new perp often opens with a premium, as airdrop holders hedge and late longs chase. The existing
listing-short sleeve (code/bt_listing.py) enters at the earliest at the next daily open, so the first
day itself has never been tested. The launch time is announced hours ahead, so no speed race is needed.

Events
  listing   every USDT-M perp (minus bt.EXCLUDE) whose first daily bar with volume lies in the period;
            t0 = the open time of its first 1-minute bar with volume
  renames   skipped when the symbol's base, stripped of a leading 1000 / 1000000 / 1M, matches a perp
            that traded before t0 (a rename or relisting, not a new coin)

Trade (1-minute daily archives of the launch day and the next two days)
  entry     short at the open of the minute t0 + D, D in {15, 60} minutes; skipped without a bar with
            volume at that minute
  exit      the open of the minute entry + 24 h (the next bar within 60 minutes if missing), or the
            buy-stop at 1.5 x entry: the first minute whose high reaches it exits at max(stop, its open)
  costs     taker 0.05% plus 25 bp slippage on each side (a new perp has no 30-day volume tier);
            funding settlements in (entry, exit], received by the short when positive; everything is
            rerun at 2x fees and slippage

Book: each launch gets 0.2 x s of equity, at most 5 open at once (later ones skipped), s in {1, 2, 3};
a trade's return lands on its exit day; bt.stats on the daily series.

Trials: 2 (D = 15, 60)

Bar on development (launches 2020-01-01..2025-06-30)
  - mean net return a trade > 0 with t >= 2.5 over trades, at 1x costs
  - s* = the smallest s with a monthly mean >= 5% at 1x; max drawdown at s* <= 35%
  - monthly mean at s* > 0 at 2x costs
Holdout: launches 2025-07-01..2026-10-28, one look, only for trials that pass, same bar and s*.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_launch.py events            launches per period (no prices)
  python bt_launch.py download dev      1-minute archives of the launch days
  python bt_launch.py dev               results_dev.json, /w/pnl_round18.parquet
  python bt_launch.py holdout           results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import math
import os
import re
import sys
import urllib.parse

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
sys.path.insert(0, "/w/listing_pump")
import bt  # noqa: E402
import bt_spotlist as sl  # noqa: E402  (archive download, minute bars, funding)

ROOT = "/w"
HERE = os.path.dirname(os.path.abspath(__file__))
M1 = f"{ROOT}/data/launch/m1"
MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000
TAKER, SLIP = 0.0005, 0.0025
HOLD = 24 * HOUR
STOP = 1.5
SLOT = 0.2
OPEN_MAX = 5
SIZES = (1, 2, 3)
DELAYS = {"D15": 15 * MIN, "D60": 60 * MIN}
PERIODS = {"dev": ("2020-01-01", "2025-07-01"), "holdout": ("2025-07-01", "2026-10-29")}
PREFIX = re.compile(r"^(1000000|1000|1M)(?=[A-Z])")


def base(sym):
    return PREFIX.sub("", sym[:-4] if sym.endswith("USDT") else sym)


def listings(period):
    """-> DataFrame(sym, day) of new perps whose first daily bar is in the period, renames skipped."""
    k = pd.read_parquet(f"{sl.DAILY}/klines_1d.parquet", columns=["symbol", "open_time", "quote_volume"])
    k = k[(k["quote_volume"].astype(float) > 0) & ~k["symbol"].isin(bt.EXCLUDE) & k["symbol"].str.endswith("USDT")]
    first = k.groupby("symbol")["open_time"].min().astype("int64").sort_values()
    lo, hi = (int(pd.Timestamp(x).value // 1_000_000) for x in PERIODS[period])
    rows, seen = [], {}
    for sym, t in first.items():
        b = base(sym)
        renamed = b in seen and seen[b] < t
        seen.setdefault(b, t)
        if lo <= t < hi:
            rows.append({"sym": sym, "day": int(t), "renamed": renamed})
    return pd.DataFrame(rows)


def days_for(day):
    return [pd.Timestamp(day + i * DAY, unit="ms").strftime("%Y-%m-%d") for i in range(3)]


def one_day(job):
    sym, d = job
    path = f"{M1}/{sym}/{d}.parquet"
    if os.path.exists(path) or os.path.exists(path + ".missing"):
        return "kept"
    # symbols such as 币安人生USDT need a percent-encoded path
    blob = sl.fetch(urllib.parse.quote(f"{sl.FILES}/{sym}/1m/{sym}-1m-{d}.zip", safe=":/"))
    os.makedirs(f"{M1}/{sym}", exist_ok=True)
    if blob is None:
        open(path + ".missing", "w").close()
        return "missing"
    sl.parse_zip(blob).to_parquet(path + ".tmp", index=False)
    os.replace(path + ".tmp", path)
    return "new"


def download(period):
    ls = listings(period)
    ls = ls[~ls["renamed"]]
    jobs = [(s, d) for s, day in zip(ls["sym"], ls["day"]) for d in days_for(int(day))]
    counts = {"kept": 0, "new": 0, "missing": 0}
    with cf.ThreadPoolExecutor(6) as pool:
        for res in pool.map(one_day, jobs):
            counts[res] += 1
    print(f"{period}: {len(ls)} launches, {len(jobs)} symbol-days {counts}", flush=True)


def bars(sym, day):
    frames = [pd.read_parquet(f"{M1}/{sym}/{d}.parquet") for d in days_for(day)
              if os.path.exists(f"{M1}/{sym}/{d}.parquet")]
    if not frames:
        return None
    b = pd.concat(frames, ignore_index=True).drop_duplicates("t").sort_values("t")
    b = {c: b[c].to_numpy(np.int64 if c == "t" else float) for c in ("t", "o", "h", "l", "c", "qv")}
    return b


def trade(b, delay):
    """-> (t0, entry_t, entry_px, exit_t, exit_px, stopped) or None."""
    traded = np.flatnonzero(b["qv"] > 0)
    if len(traded) == 0:
        return None
    t0 = int(b["t"][traded[0]])
    et = t0 + delay
    i = sl.bar_at(b, et)
    if i is None or b["qv"][i] <= 0:
        return None
    j = sl.bar_at(b, et + HOLD, sl.EXIT_GRACE)
    if j is None:
        return None
    px = b["o"][i]
    hit = np.flatnonzero(b["h"][i:j] >= STOP * px)
    if len(hit):
        k = i + int(hit[0])
        return t0, et, px, int(b["t"][k]), max(STOP * px, b["o"][k]), True
    return t0, et, px, int(b["t"][j]), b["o"][j], False


def net(px_in, px_out, fsum, mult):
    return -(px_out / px_in - 1.0) + fsum - mult * 2 * (TAKER + SLIP)


def run(period, trials=tuple(DELAYS)):
    ls = listings(period)
    ls = ls[~ls["renamed"]]
    fund = sl.funding_events()
    rows = []
    for sym, day in zip(ls["sym"], ls["day"]):
        b = bars(sym, int(day))
        if b is None:
            continue
        ft, fr = fund.get(sym, (np.array([], np.int64), np.array([])))
        for key in trials:
            got = trade(b, DELAYS[key])
            if got is None:
                continue
            t0, et, px_in, xt, px_out, stopped = got
            fsum = float(fr[np.searchsorted(ft, et, "right"):np.searchsorted(ft, xt, "right")].sum())
            rows.append({"trial": key, "sym": sym, "t0": t0, "entry_t": et, "exit_t": xt, "entry": px_in, "exit": px_out,
                         "stopped": stopped, "funding": fsum, "ret1": net(px_in, px_out, fsum, 1.0),
                         "ret2": net(px_in, px_out, fsum, 2.0)})
    return pd.DataFrame(rows)


def book(tr, col, s):
    tr = tr.sort_values("entry_t")
    open_exits, keep = [], []
    for idx, et, xt in zip(tr.index, tr["entry_t"].to_numpy(), tr["exit_t"].to_numpy()):
        open_exits = [x for x in open_exits if x > et]
        if len(open_exits) < OPEN_MAX:
            open_exits.append(xt)
            keep.append(idx)
    acc = tr.loc[keep]
    day = pd.to_datetime(acc["exit_t"], unit="ms").dt.normalize()
    return acc, (SLOT * s * acc[col]).groupby(day).sum()


def evaluate(trades, period):
    start, end = PERIODS[period]
    days = pd.date_range(pd.Timestamp(start), pd.Timestamp(end) - pd.Timedelta(days=1))
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        r = {}
        for mult, col in (("1x", "ret1"), ("2x", "ret2")):
            acc, _ = book(g, col, 1)
            x = acc[col]
            t = float(x.mean() / x.std() * math.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0
            r[mult] = {"trades": int(len(acc)), "mean_pct": round(float(x.mean() * 100), 3), "t": round(t, 2),
                       "median_pct": round(float(x.median() * 100), 2), "win_pct": round(float((x > 0).mean() * 100), 1),
                       "worst_pct": round(float(x.min() * 100), 1), "stopped": int(acc["stopped"].sum()),
                       "funding_mean_pct": round(float(acc["funding"].mean() * 100), 3), "sizes": {}}
            for s in SIZES:
                _, daily = book(g, col, s)
                daily = daily.reindex(days, fill_value=0.0)
                r[mult]["sizes"][str(s)] = bt.stats(daily)
                if mult == "1x" and s == 1:
                    pnl[f"launch|{key}"] = daily
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def passes(r, size=None):
    a, b = r["1x"], r["2x"]
    if not (a["mean_pct"] > 0 and a["t"] >= 2.5):
        return False, None
    if size is None:
        size = next((s for s in SIZES if a["sizes"][str(s)]["monthly_mean_pct"] >= 5), None)
    if size is None:
        return False, None
    ok = (a["sizes"][str(size)]["monthly_mean_pct"] >= 5 and a["sizes"][str(size)]["max_dd_pct"] <= 35
          and b["sizes"][str(size)]["monthly_mean_pct"] > 0)
    return bool(ok), size


def show(out):
    for key, r in out.items():
        for mult in ("1x", "2x"):
            x = r[mult]
            print(f"{key} {mult} trades {x['trades']} mean {x['mean_pct']}% median {x['median_pct']}% t {x['t']} "
                  f"win {x['win_pct']}% worst {x['worst_pct']}% stopped {x['stopped']} funding {x['funding_mean_pct']}%",
                  flush=True)
            for s, st in x["sizes"].items():
                print(f"     s={s} month {st['monthly_mean_pct']}% dd {st['max_dd_pct']}% years {st['by_year_pct']}",
                      flush=True)
        print(f"   passes {r.get('passes')} s* {r.get('size')}", flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    if mode == "events":
        for period in PERIODS:
            ls = listings(period)
            y = pd.to_datetime(ls["day"], unit="ms").dt.year
            print(f"{period}: {len(ls)} first days, {int(ls['renamed'].sum())} renames skipped; by year "
                  f"{ls[~ls['renamed']].groupby(y[~ls['renamed']]).size().to_dict()}")
            print("  renames: " + ", ".join(ls.loc[ls["renamed"], "sym"].head(30)))
    elif mode == "download":
        download(sys.argv[2])
    elif mode == "dev":
        trades = run("dev")
        out, pnl = evaluate(trades, "dev")
        for key in out:
            out[key]["passes"], out[key]["size"] = passes(out[key])
        show(out)
        pnl.to_parquet(f"{ROOT}/pnl_round18.parquet")
        trades.to_parquet(f"{HERE}/trades_dev.parquet")
        with open(f"{HERE}/results_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{HERE}/results_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        download("holdout")
        trades = run("holdout", ok)
        out, _ = evaluate(trades, "holdout")
        for key in out:
            out[key]["passes"], out[key]["size"] = passes(out[key], dev[key]["size"])
        show(out)
        trades.to_parquet(f"{HERE}/trades_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
