"""Download 1h USDT-M klines for every symbol that was ever in the top 100 by volume.

Months are derived from the daily archive (first to last traded day), so no S3
listing is needed; a missing month (404) is skipped without retries.
"""

import concurrent.futures as cf
import io
import sys
import time
import urllib.error
import urllib.request
import zipfile

import pandas as pd

FILES = "https://data.binance.vision"
DATA = sys.argv[1] if len(sys.argv) > 1 else "/w/data"
COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume"]


def fetch(url, tries=4):
    for attempt in range(tries):
        try:
            return urllib.request.urlopen(url, timeout=60).read()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1 + attempt * 2)
    return None


def one(job):
    symbol, month = job
    blob = fetch(f"{FILES}/data/futures/um/monthly/klines/{symbol}/1h/{symbol}-1h-{month}.zip")
    if blob is None:
        return None
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        raw = archive.read(archive.namelist()[0]).decode()
    header = 0 if raw[:1] and not raw[0].isdigit() else None
    frame = pd.read_csv(io.StringIO(raw), header=header, usecols=range(8))
    frame.columns = COLUMNS
    frame = frame[["open_time", "open", "high", "low", "close", "quote_volume"]]
    frame["symbol"] = symbol
    return frame


def main():
    k = pd.read_parquet(f"{DATA}/klines_1d.parquet", columns=["symbol", "open_time", "quote_volume"])
    k = k[k["quote_volume"] > 0]
    k["date"] = pd.to_datetime(k["open_time"], unit="ms")
    qv = k.pivot(index="date", columns="symbol", values="quote_volume")
    rank = qv.rolling(30, min_periods=20).median().rank(axis=1, ascending=False)
    symbols = sorted(rank.columns[(rank <= 100).any()])
    jobs = []
    for symbol in symbols:
        days = k.loc[k["symbol"] == symbol, "date"]
        for month in pd.period_range(days.min(), days.max(), freq="M"):
            jobs.append((symbol, str(month)))
    print(f"{len(symbols)} symbols, {len(jobs)} monthly files", flush=True)
    frames = []
    with cf.ThreadPoolExecutor(16) as pool:
        for index, frame in enumerate(pool.map(one, jobs)):
            if frame is not None:
                frames.append(frame)
            if index % 2000 == 0:
                print(f"  {index}/{len(jobs)}", flush=True)
    data = pd.concat(frames, ignore_index=True)
    data.to_parquet(f"{DATA}/klines_1h.parquet")
    print(f"1h: {len(data)} rows, {data['symbol'].nunique()} symbols", flush=True)


if __name__ == "__main__":
    main()
