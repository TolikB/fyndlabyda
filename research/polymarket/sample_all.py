"""Is round 29's gap a selection on final volume? All markets of sample days, with no volume floor (descriptive, no trial).

Rounds 27 and 29 keep markets with final volume >= 50,000, which is unknown at entry. A surprise draws trading, so a market
may clear the floor BECAUSE the favourite lost. Raising the floor cannot show this: volumes are heavy-tailed, so the same
multiplier on volume changes the odds of inclusion alike at any floor. Dropping the floor can.

For every market with a scheduled end on a sample day (the 5th of each month, 2025-09..2026-10), entry 3 days before as in
round 29. Many markets can share one end time, and Gamma's offsets stop at about 2,000, so a window of a minute keeps its first
2,000 by id. Each line gives the favourite's price, its win rate and the gap, with the markets split at the old floor.

  python sample_all.py          sample_all.parquet, then the table
  python sample_all.py table    the table alone
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

import numpy as np
import pandas as pd

import bt_bonds as b
import download_markets as dm

PATH = f"{b.DATA}/sample_all.parquet"
DAYS = f"{b.DATA}/sample_days"
URL = "https://clob.polymarket.com/prices-history?market={token}&startTs={a}&endTs={z}&fidelity=60"
HEADERS = {"User-Agent": "research"}
K = 3
WORKERS = 4
PACE = 0.45
pause_until = [0.0]
lock = threading.Lock()


def days():
    months = pd.date_range("2025-09-01", "2026-10-01", freq="MS")
    return [m + pd.Timedelta(days=4) for m in months]


def window(lo, hi):
    """dm.window_rows with no volume floor. Halving stops at a minute: markets that share one end time cannot be split by time."""
    rows, offset = {}, 0
    while True:
        data = dm.get({"closed": "true", "limit": dm.PAGE, "offset": offset, "end_date_min": f"{lo:%Y-%m-%dT%H:%M:%SZ}",
                       "end_date_max": f"{hi:%Y-%m-%dT%H:%M:%SZ}", "order": "id", "ascending": "true"})
        if not data:
            return rows, offset
        for m in data:
            r = dm.row(m)
            if r:
                rows[r["id"]] = r
        offset += len(data)
        time.sleep(dm.PAUSE)
        if offset >= dm.OFFSET_CAP:
            if hi - lo <= pd.Timedelta(minutes=1):
                print(f"capped at {lo}", flush=True)
                return rows, offset
            mid = lo + (hi - lo) / 2
            left, a = window(lo, mid)
            right, b = window(mid, hi)
            return {**left, **right}, a + b


def history(token, a, z, tries=6):
    for attempt in range(tries):
        wait = pause_until[0] - time.time()
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(URL.format(token=token, a=int(a), z=int(z)), headers=HEADERS)
            return json.loads(urllib.request.urlopen(req, timeout=60).read()).get("history") or []
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                with lock:
                    pause_until[0] = time.time() + 30
            elif exc.code in (400, 404):
                return []
            else:
                time.sleep(3 + 5 * attempt)
        except Exception:  # noqa: BLE001
            time.sleep(3 + 5 * attempt)
    return []


def one(r):
    time.sleep(PACE)
    te = r["end"] // 1000 - K * b.DAY
    h = history(r["token_yes"], te - 3 * b.HOUR, te)
    if not h:
        return None
    t, p = np.array([x["t"] for x in h]), np.array([x["p"] for x in h], float)
    py = b.last_price(t, p, te)
    return None if py is None else {"id": r["id"], "py": py}


def collect():
    frames = []
    os.makedirs(DAYS, exist_ok=True)
    for d in days():
        path = f"{DAYS}/{d:%Y-%m-%d}.parquet"
        if not os.path.exists(path):
            got, seen = window(d, d + pd.Timedelta(days=1))
            m = pd.DataFrame(got.values())
            te = m["end"] // 1000 - K * b.DAY
            keep = m["outcome_yes"].isin([0.0, 1.0]) & (m["start"] // 1000 < te) & (te < m["closed"] // 1000)
            m.loc[keep, ["id", "event", "end", "token_yes", "outcome_yes", "volume"]].to_parquet(path, index=False)
            print(f"{d:%Y-%m-%d} seen {seen} kept {len(got)} open at entry and resolved {int(keep.sum())}", flush=True)
            del got, m
        frames.append(pd.read_parquet(path))
    m = pd.concat(frames, ignore_index=True).drop_duplicates("id")
    print(f"{len(m)} markets open at entry and resolved", flush=True)
    out = []
    with cf.ThreadPoolExecutor(WORKERS) as pool:
        for n, r in enumerate(pool.map(one, m.to_dict("records"))):
            if r:
                out.append(r)
            if n % 5000 == 0:
                print(n, len(out), flush=True)
    d = m.merge(pd.DataFrame(out), on="id")
    d.to_parquet(PATH, index=False, compression="zstd")
    print(f"{len(d)} with a price at entry", flush=True)


def gap(x):
    if not len(x):
        return "0"
    se = np.sqrt(x["won"].mean() * (1 - x["won"].mean()) / len(x)) * 100
    return f"{len(x):6d} events {x['event'].nunique():6d}  price {x['fav'].mean():.3f}  won {x['won'].mean():.3f}  gap {(x['won'].mean() - x['fav'].mean()) * 100:+.2f} pp  se {se:.2f}"


def table():
    d = pd.read_parquet(PATH)
    d["yes"] = d["py"] > 0.5
    d["fav"] = np.where(d["yes"], d["py"], 1 - d["py"])
    d["won"] = np.where(d["yes"], d["outcome_yes"], 1 - d["outcome_yes"])
    print("volume quantiles", d["volume"].quantile([.1, .25, .5, .75, .9]).round(0).to_dict())
    for low, high in ((0.60, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 0.95), (0.60, 0.95)):
        x = d[(d["fav"] >= low) & (d["fav"] < high)]
        print(f"\nfavourite {low:.2f}..{high:.2f}")
        print(f"  all markets       {gap(x)}")
        print(f"  volume < 50k      {gap(x[x['volume'] < 5e4])}")
        print(f"  volume >= 50k     {gap(x[x['volume'] >= 5e4])}")
        for a, z in ((0, 1e3), (1e3, 1e4), (1e4, 5e4)):
            print(f"  {a:>7,.0f}..{z:>7,.0f}  {gap(x[(x['volume'] >= a) & (x['volume'] < z)])}")


def main():
    if len(sys.argv) < 2:
        os.makedirs(b.DATA, exist_ok=True)
        collect()
    table()


if __name__ == "__main__":
    main()
