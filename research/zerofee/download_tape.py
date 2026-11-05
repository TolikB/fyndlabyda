"""Download Binance USDT-M 1-minute klines with taker volume for the zero-fee tape test (research only).

The wick-trap archive (/w/data/klines_1m) has no taker volume, which round 26's order-flow signal needs.

Output: /w/data/tape_1m/<SYMBOL>/<YYYY-MM>.parquet with columns t (open time, ms), o, h, l, c (float64), qv
(quote volume) and tq (taker-buy quote volume), both float32. Resumable.

  python download_tape.py
"""

from __future__ import annotations

import concurrent.futures as cf
import io
import os
import time
import urllib.error
import urllib.request
import zipfile

import pandas as pd

FILES = "https://data.binance.vision/data/futures/um/monthly/klines"
OUT = "/w/data/tape_1m"
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT", "ADAUSDT", "ZECUSDT")
MONTHS = [f"{m:%Y-%m}" for m in pd.date_range("2022-01-01", "2026-10-01", freq="MS")]
COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count", "taker_base",
        "taker_quote"]


def fetch(url, tries=5):
    for attempt in range(tries):
        try:
            return urllib.request.urlopen(url, timeout=90).read()
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
    df = pd.read_csv(io.BytesIO(raw), header=header, usecols=range(11))
    df.columns = COLS
    t = df["open_time"].astype("int64")
    if len(t) and t.max() > 10 ** 14:  # microsecond timestamps
        t = t // 1000
    return pd.DataFrame({"t": t, "o": df["open"].astype(float), "h": df["high"].astype(float),
                         "l": df["low"].astype(float), "c": df["close"].astype(float),
                         "qv": df["quote_volume"].astype("float32"), "tq": df["taker_quote"].astype("float32")})


def one(job):
    sym, month = job
    path = f"{OUT}/{sym}/{month}.parquet"
    if os.path.exists(path):
        return "kept"
    blob = fetch(f"{FILES}/{sym}/1m/{sym}-1m-{month}.zip")
    if blob is None:
        return "missing"
    os.makedirs(f"{OUT}/{sym}", exist_ok=True)
    parse(blob).to_parquet(path + ".tmp", compression="zstd", index=False)
    os.replace(path + ".tmp", path)
    return "new"


def main():
    jobs = [(s, m) for s in SYMBOLS for m in MONTHS]
    counts = {"kept": 0, "new": 0, "missing": 0}
    with cf.ThreadPoolExecutor(6) as pool:
        for res in pool.map(one, jobs):
            counts[res] += 1
    print(counts)


if __name__ == "__main__":
    main()
