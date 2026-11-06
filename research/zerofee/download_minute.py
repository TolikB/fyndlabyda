"""Download Binance USDT-M 1-minute klines of round 33's universe, with each minute's VWAP (research only).

/w/data/klines_1m stops a coin when it leaves ranks 30-150, and /w/data/tape_1m has 7 coins and no base volume.

Output: /w/data/minute_mexc/<SYMBOL>/<YYYY-MM>.parquet with columns t (open time, ms), c (close), vw (quote volume / base volume,
or the close in a minute with no trade), both float64. Resumable.

  python download_minute.py
"""

from __future__ import annotations

import concurrent.futures as cf
import io
import json
import os
import zipfile

import pandas as pd

import bt_hourly_mexc as u
import download_tape as d

OUT = "/w/data/minute_mexc"
MONTHS = [f"{m:%Y-%m}" for m in pd.date_range("2024-01-01", "2026-10-01", freq="MS")]


def parse(blob):
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        raw = archive.read(archive.namelist()[0])
    header = None if raw[:1].isdigit() else 0
    df = pd.read_csv(io.BytesIO(raw), header=header, usecols=range(11))
    df.columns = d.COLS
    t = df["open_time"].astype("int64")
    if len(t) and t.max() > 10 ** 14:  # microsecond timestamps
        t = t // 1000
    c, base, quote = df["close"].astype(float), df["volume"].astype(float), df["quote_volume"].astype(float)
    return pd.DataFrame({"t": t, "c": c, "vw": (quote / base).where(base > 0, c)})


def one(job):
    sym, month = job
    path = f"{OUT}/{sym}/{month}.parquet"
    if os.path.exists(path):
        return "kept"
    blob = d.fetch(f"{d.FILES}/{sym}/1m/{sym}-1m-{month}.zip")
    if blob is None:
        return "missing"
    os.makedirs(f"{OUT}/{sym}", exist_ok=True)
    parse(blob).to_parquet(path + ".tmp", compression="zstd", index=False)
    os.replace(path + ".tmp", path)
    return "new"


def main():
    costs = json.load(open(u.COSTS))["contracts"]
    listed = set(pd.read_parquet(f"{u.bt.DATA}/klines_1h.parquet", columns=["symbol"])["symbol"].unique())
    symbols = sorted(u.universe(costs, listed))
    print(len(symbols), "symbols:", " ".join(symbols), flush=True)
    jobs = [(s, m) for s in symbols for m in MONTHS]
    counts = {"kept": 0, "new": 0, "missing": 0}
    with cf.ThreadPoolExecutor(6) as pool:
        for n, r in enumerate(pool.map(one, jobs)):
            counts[r] += 1
            if n % 100 == 0:
                print(n, len(jobs), counts, flush=True)
    print("done", counts, flush=True)


if __name__ == "__main__":
    main()
