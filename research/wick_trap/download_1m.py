"""Download Binance USDT-M 1-minute klines for the wick-trap universe (research only).

Symbol-months: every month 2022-01..2026-10 with at least one universe day for that symbol
(`bt_wick.universe`). Complete months come from monthly zips. October 2026 comes from daily zips up
to 2026-10-30.

Output: /w/data/klines_1m/<SYMBOL>/<YYYY-MM>.parquet with columns t (open time, ms), o, h, l, c
(float64) and qv (float32). Resumable. Archives that do not exist are listed in missing_1m.json.

  python download_1m.py --count   only count the symbol-months
  python download_1m.py           download them
"""

from __future__ import annotations

import concurrent.futures as cf
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

import pandas as pd

import bt_wick

FILES = "https://data.binance.vision/data/futures/um"
OUT = bt_wick.M1
LAST_DAY = pd.Timestamp("2026-10-30")
COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume"]


def fetch(url, tries=5):
    for attempt in range(tries):
        try:
            # symbols such as 币安人生USDT need a percent-encoded path
            return urllib.request.urlopen(urllib.parse.quote(url, safe=":/"), timeout=90).read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1 + 2 * attempt)
    raise RuntimeError(f"gave up on {url}")


def parse(blob):
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        raw = archive.read(archive.namelist()[0])
    header = None if raw[:1].isdigit() else 0
    df = pd.read_csv(io.BytesIO(raw), header=header, usecols=range(8))
    df.columns = COLS
    t = df["open_time"].astype("int64")
    if len(t) and t.max() > 10 ** 14:  # microsecond timestamps
        t = t // 1000
    return pd.DataFrame({"t": t, "o": df["open"].astype(float), "h": df["high"].astype(float),
                         "l": df["low"].astype(float), "c": df["close"].astype(float),
                         "qv": df["quote_volume"].astype("float32")})


def months_needed():
    live, _ = bt_wick.universe()
    live = live.loc["2022-01-01":str(LAST_DAY.date())]
    jobs = []
    for sym in live.columns:
        days = live.index[live[sym].to_numpy(dtype=bool)]
        jobs += [(sym, m) for m in sorted(set(days.strftime("%Y-%m")))]
    return jobs


def one(job):
    sym, month = job
    path = f"{OUT}/{sym}/{month}.parquet"
    if os.path.exists(path):
        return "kept"
    if month == LAST_DAY.strftime("%Y-%m"):
        blobs = [fetch(f"{FILES}/daily/klines/{sym}/1m/{sym}-1m-{d:%Y-%m-%d}.zip")
                 for d in pd.date_range(f"{month}-01", LAST_DAY)]
        frames = [parse(b) for b in blobs if b]
    else:
        blob = fetch(f"{FILES}/monthly/klines/{sym}/1m/{sym}-1m-{month}.zip")
        frames = [parse(blob)] if blob else []
    if not frames:
        return "missing"
    os.makedirs(f"{OUT}/{sym}", exist_ok=True)
    pd.concat(frames, ignore_index=True).to_parquet(path + ".tmp", compression="zstd", index=False)
    os.replace(path + ".tmp", path)
    return "new"


def main():
    jobs = months_needed()
    print(f"{len(jobs)} symbol-months, {len({s for s, _ in jobs})} symbols", flush=True)
    if "--count" in sys.argv:
        return
    counts, missing = {"kept": 0, "new": 0, "missing": 0}, []
    with cf.ThreadPoolExecutor(8) as pool:
        for n, (job, res) in enumerate(zip(jobs, pool.map(one, jobs))):
            counts[res] += 1
            if res == "missing":
                missing.append(job)
            if (n + 1) % 500 == 0 or n + 1 == len(jobs):
                print(f"  {n + 1}/{len(jobs)} {counts}", flush=True)
    with open(f"{OUT}/missing_1m.json", "w") as fh:
        json.dump(missing, fh)


if __name__ == "__main__":
    main()
