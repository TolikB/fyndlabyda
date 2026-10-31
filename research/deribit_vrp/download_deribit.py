"""Download Deribit option trades in the Friday entry windows and the daily delivery prices (research only).

For every Friday 2020-01-03..2026-10-23 and each of BTC and ETH, all option trades in
[08:05, 10:00) UTC come from history.deribit.com (public, no key), page by page. Output in
/w/data/deribit/:
  <CUR>_windows.parquet   friday, trade_id, timestamp, instrument_name, price, mark_price, iv,
                          index_price, direction, amount
  <CUR>_delivery.parquet  date, delivery_price (Deribit's settlement index for each day)
Resumable per currency-Friday (cache in /w/data/deribit/cache/).
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request

import pandas as pd

OUT = "/w/data/deribit"
HIST = "https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time"
DELIVERY = "https://www.deribit.com/api/v2/public/get_delivery_prices"
FIRST, LAST = "2020-01-03", "2026-10-23"
PAUSE = 0.25
COLS = ["trade_id", "timestamp", "instrument_name", "price", "mark_price", "iv", "index_price", "direction", "amount"]


def get(url, params, tries=6):
    full = url + "?" + urllib.parse.urlencode(params)
    for attempt in range(tries):
        try:
            time.sleep(PAUSE)
            return json.loads(urllib.request.urlopen(full, timeout=60).read())["result"]
        except Exception:  # noqa: BLE001
            time.sleep(2 + 3 * attempt)
    raise RuntimeError(f"gave up on {full}")


def window(cur, friday):
    start = int((friday + pd.Timedelta(hours=8, minutes=5)).value // 1_000_000)
    end = int((friday + pd.Timedelta(hours=10)).value // 1_000_000)
    seen, rows = set(), []
    while True:
        res = get(HIST, {"currency": cur, "kind": "option", "start_timestamp": start, "end_timestamp": end,
                         "count": 1000, "sorting": "asc"})
        new = [t for t in res["trades"] if t["trade_id"] not in seen]
        for t in new:
            seen.add(t["trade_id"])
            rows.append({c: t.get(c) for c in COLS})
        if not res.get("has_more") or not res["trades"]:
            break
        last = res["trades"][-1]["timestamp"]
        if not new and last == start:
            raise RuntimeError(f"stuck at {start} for {cur} {friday.date()}")
        start = last
    return rows


def delivery(cur):
    rows, offset = [], 0
    while True:
        res = get(DELIVERY, {"index_name": f"{cur.lower()}_usd", "offset": offset, "count": 1000})
        rows += res["data"]
        offset += len(res["data"])
        if not res["data"] or offset >= res["records_total"]:
            break
    return pd.DataFrame(rows).drop_duplicates("date")


def main():
    os.makedirs(f"{OUT}/cache", exist_ok=True)
    fridays = pd.date_range(FIRST, LAST, freq="W-FRI")
    for cur in ("BTC", "ETH"):
        delivery(cur).to_parquet(f"{OUT}/{cur}_delivery.parquet", index=False)
        frames = []
        for n, f in enumerate(fridays):
            path = f"{OUT}/cache/{cur}_{f.date()}.parquet"
            if not os.path.exists(path):
                df = pd.DataFrame(window(cur, f), columns=COLS)
                df.insert(0, "friday", str(f.date()))
                df.to_parquet(path + ".tmp", index=False)
                os.replace(path + ".tmp", path)
            frames.append(pd.read_parquet(path))
            if (n + 1) % 50 == 0:
                print(f"  {cur} {n + 1}/{len(fridays)} Fridays, {sum(len(x) for x in frames)} trades", flush=True)
        pd.concat(frames, ignore_index=True).to_parquet(f"{OUT}/{cur}_windows.parquet", index=False)
        print(f"{cur}: {sum(len(x) for x in frames)} trades", flush=True)


if __name__ == "__main__":
    main()
