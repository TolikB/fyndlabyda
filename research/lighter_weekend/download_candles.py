"""Download hourly candles and funding of Lighter's stock and commodity perps from its public API (research only).

One call every 1.2 s; a 429 stops the run. Market ids and start dates come from xvenue's daily snapshot of orderBookDetails.

Output: /w/data/lighter/candles_1h.parquet (symbol, t ms at the bar's start, o, h, l, c, v, V) and fund_1h.parquet
(symbol, t ms, rate as the API gives it, direction).

  python download_candles.py
"""

from __future__ import annotations

import glob
import gzip
import json
import os
import time
import urllib.error
import urllib.request

import pandas as pd

import bt_weekend as b

ROOT = "https://mainnet.zklighter.elliot.ai/api/v1"
HEADERS = {"User-Agent": "research"}
PACE = 1.2
CANDLE_PAGE = 20 * b.DAY_MS
FUND_PAGE = 30 * b.DAY_MS


def get(path):
    time.sleep(PACE)
    req = urllib.request.Request(f"{ROOT}/{path}", headers=HEADERS)
    try:
        return json.loads(urllib.request.urlopen(req, timeout=60).read())
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            raise SystemExit("429: stopped")
        raise


def pages(first, last, step):
    """Windows in ms from `last` back to `first`."""
    z = last
    while z > first:
        yield max(first, z - step), z
        z -= step


def candles(market, first, last):
    rows = []
    for a, z in pages(first, last, CANDLE_PAGE):
        rows += get(f"candles?market_id={market}&resolution=1h&start_timestamp={a // 1000}&end_timestamp={z // 1000}"
                    f"&count_back=500")["c"]
    return pd.DataFrame(rows).drop_duplicates("t") if rows else pd.DataFrame()


def fundings(market, first, last):
    rows = []
    for a, z in pages(first, last, FUND_PAGE):
        rows += get(f"fundings?market_id={market}&resolution=1h&start_timestamp={a // 1000}&end_timestamp={z // 1000}"
                    f"&count_back=750")["fundings"]
    return pd.DataFrame(rows).drop_duplicates("timestamp") if rows else pd.DataFrame()


def main():
    os.makedirs(b.DATA, exist_ok=True)
    meta = sorted(glob.glob("/w/xvenue/data/meta/*/lighter.json.gz"))[-1]
    d = pd.DataFrame(json.load(gzip.open(meta))["order_book_details"])
    d = d[d["symbol"].isin(b.UNIVERSE) & (d["status"] == "active")]
    last = int(time.time()) * 1000
    cs, fs = [], []
    for _, r in d.iterrows():
        first = int(r["created_at"])
        c, f = candles(r["market_id"], first, last), fundings(r["market_id"], first, last)
        if len(c):
            cs.append(c[["t", "o", "h", "l", "c", "v", "V"]].assign(symbol=r["symbol"]))
        if len(f):
            fs.append(pd.DataFrame({"symbol": r["symbol"], "t": f["timestamp"].astype("int64") * 1000,
                                    "rate": f["rate"].astype(float), "direction": f["direction"]}))
        print(r["symbol"], len(c), len(f), flush=True)
    pd.concat(cs, ignore_index=True).to_parquet(f"{b.DATA}/candles_1h.parquet", index=False, compression="zstd")
    pd.concat(fs, ignore_index=True).to_parquet(f"{b.DATA}/fund_1h.parquet", index=False, compression="zstd")
    print("done", flush=True)


if __name__ == "__main__":
    main()
