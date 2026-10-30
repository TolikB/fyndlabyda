"""Daily aggregates of Binance futures 'metrics' (OI, long/short ratios), research only.

data.binance.vision keeps these only as daily files of 5-minute rows. To keep the
download bounded, a (symbol, day) is fetched only while the symbol ranks in the top
80 by trailing 30-day median quote volume (the studies use the top 60), from 2021-01.
Per day: OI value at the last row, and the day's mean of each ratio.
"""

from __future__ import annotations

import io
import os
import sys
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

sys.argv = sys.argv[:1] + ["/w/data"]
import bt  # noqa: E402

URL = "https://data.binance.vision/data/futures/um/daily/metrics/{s}/{s}-metrics-{d}.zip"
OUT = "/w/data/metrics_1d.parquet"
RATIOS = ["sum_toptrader_long_short_ratio", "count_toptrader_long_short_ratio",
          "count_long_short_ratio", "sum_taker_long_short_vol_ratio"]


def fetch(job):
    symbol, day = job
    url = URL.format(s=symbol, d=day)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                blob = response.read()
            break
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
        except (urllib.error.URLError, TimeoutError):
            pass
        if attempt == 2:
            return None
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        frame = pd.read_csv(archive.open(archive.namelist()[0]))
    if "sum_open_interest_value" not in frame or frame.empty:
        return None
    row = {"symbol": symbol, "date": day,
           "oi_value": pd.to_numeric(frame["sum_open_interest_value"], errors="coerce").dropna().iloc[-1]
           if frame["sum_open_interest_value"].notna().any() else float("nan"),
           "rows": len(frame)}
    for column in RATIOS:
        row[column] = pd.to_numeric(frame[column], errors="coerce").mean() if column in frame else float("nan")
    return row


def main():
    _, close, qvol, _ = bt.load()
    live, _ = bt.universe(close, qvol, 80)
    live = live[live.index >= "2021-01-01"]
    stacked = live.stack()
    jobs = [(symbol, str(day.date())) for (day, symbol), flag in stacked.items() if flag]
    done = set()
    if os.path.exists(OUT):
        old = pd.read_parquet(OUT)
        done = set(zip(old["symbol"], old["date"]))
    jobs = [job for job in jobs if job not in done]
    print(f"jobs {len(jobs)} (already have {len(done)})", flush=True)
    rows = []
    with ThreadPoolExecutor(16) as pool:
        for i, row in enumerate(pool.map(fetch, jobs, chunksize=64)):
            if row:
                rows.append(row)
            if i % 10000 == 0:
                print(f"{i}/{len(jobs)} rows {len(rows)}", flush=True)
    new = pd.DataFrame(rows)
    if os.path.exists(OUT):
        new = pd.concat([pd.read_parquet(OUT), new], ignore_index=True)
    new.to_parquet(OUT)
    print(f"saved {len(new)} symbol-days to {OUT}", flush=True)


if __name__ == "__main__":
    main()
