"""Download Binance USDT-M daily klines and funding history from data.binance.vision.

Includes delisted symbols (the archive keeps them), so the research universe has no
survivorship bias. Output: one parquet per kind under /data.
"""

import concurrent.futures as cf
import io
import re
import sys
import time
import urllib.request
import zipfile
from xml.etree import ElementTree

import pandas as pd

BUCKET = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
FILES = "https://data.binance.vision"
OUT = sys.argv[1] if len(sys.argv) > 1 else "/data"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def list_prefixes(prefix):
    keys, marker = [], ""
    while True:
        url = f"{BUCKET}?delimiter=/&prefix={prefix}" + (f"&marker={marker}" if marker else "")
        root = ElementTree.fromstring(urllib.request.urlopen(url, timeout=60).read())
        page = [p.find("s3:Prefix", NS).text for p in root.findall("s3:CommonPrefixes", NS)]
        page += [c.find("s3:Key", NS).text for c in root.findall("s3:Contents", NS)]
        keys += page
        truncated = root.find("s3:IsTruncated", NS).text == "true"
        if not truncated or not page:
            return keys
        marker = page[-1]


def fetch(url, tries=4):
    for attempt in range(tries):
        try:
            return urllib.request.urlopen(url, timeout=60).read()
        except Exception:  # noqa: BLE001
            if attempt == tries - 1:
                return None
            time.sleep(1 + attempt * 2)


def read_zip(blob, names):
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        raw = archive.read(archive.namelist()[0]).decode()
    first = raw.split("\n", 1)[0]
    header = 0 if first and not first[0].isdigit() else None
    frame = pd.read_csv(io.StringIO(raw), header=header)
    if header is None:
        frame.columns = names[: len(frame.columns)]
    return frame


KLINE = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
         "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]
FUNDING = ["calc_time", "funding_interval_hours", "last_funding_rate"]


def symbol_months(kind, interval=""):
    base = f"data/futures/um/monthly/{kind}/"
    symbols = [p.rstrip("/").split("/")[-1] for p in list_prefixes(base)]
    symbols = [s for s in symbols if re.fullmatch(r"[A-Z0-9]+USDT", s)]
    jobs = []
    for symbol in symbols:
        prefix = f"{base}{symbol}/{interval + '/' if interval else ''}"
        for key in list_prefixes(prefix):
            if key.endswith(".zip"):
                jobs.append((symbol, key))
    return jobs


def download(kind, interval, names):
    jobs = symbol_months(kind, interval)
    print(f"{kind}: {len(jobs)} monthly files", flush=True)
    frames = []

    def one(job):
        symbol, key = job
        blob = fetch(f"{FILES}/{key}")
        if blob is None:
            return None
        frame = read_zip(blob, names)
        frame["symbol"] = symbol
        return frame

    with cf.ThreadPoolExecutor(12) as pool:
        for index, frame in enumerate(pool.map(one, jobs)):
            if frame is not None:
                frames.append(frame)
            if index % 2000 == 0:
                print(f"  {index}/{len(jobs)}", flush=True)
    data = pd.concat(frames, ignore_index=True)
    data.to_parquet(f"{OUT}/{kind}_{interval or 'all'}.parquet")
    print(f"{kind}: {len(data)} rows, {data['symbol'].nunique()} symbols", flush=True)


download("fundingRate", "", FUNDING)
