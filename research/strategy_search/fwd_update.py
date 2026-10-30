"""Extend the research archive with Binance public REST data (forward paper test).

The archive (data.binance.vision) ends on 2026-09-30. Every run fetches the closed
daily klines and the funding settlements after the last stored point, and writes
archive + REST into <root>/data with the archive's columns, so bt.load reads the
forward data exactly like the research data. Public endpoints only, throttled well
below Binance's per-IP limits (the VM's IP is shared with the paper stacks).
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import pandas as pd

BASE = "https://fapi.binance.com"
ARCHIVE = "/w/data"
DAY_MS = 86_400_000
KLINE_COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
                 "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]
FLOATS = ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_volume",
          "taker_buy_quote_volume"]


def get(path: str, **params):
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code in (418, 429):
                # Rate limited: stop at once, the IP is shared with the paper stacks.
                raise SystemExit(f"rate limited ({error.code}) on {path}; stopping")
            if error.code == 400:
                return None                   # unknown or delisted symbol
            if attempt == 3:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == 3:
                raise
        time.sleep(2 * (attempt + 1))
    return None


def perpetual_symbols() -> dict[str, int]:
    """USDT perpetuals and their listing day (ms)."""
    info = get("/fapi/v1/exchangeInfo")
    return {s["symbol"]: int(s.get("onboardDate", 0)) // DAY_MS * DAY_MS for s in info["symbols"]
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"}


def fetch_klines(symbol: str, start: int, now_ms: int) -> list[list]:
    rows = []
    while True:
        data = get("/fapi/v1/klines", symbol=symbol, interval="1d", startTime=start, limit=99)
        time.sleep(0.15)
        bars = [list(bar[:12]) + [symbol] for bar in data or [] if int(bar[6]) < now_ms]
        rows.extend(bars)
        if not data or len(data) < 99:
            return rows
        start = int(data[-1][0]) + DAY_MS


def update_klines(root: str, now_ms: int) -> pd.DataFrame:
    path = f"{root}/rest_klines.parquet"
    rest = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame(columns=KLINE_COLUMNS + ["symbol"])
    archive = pd.read_parquet(f"{ARCHIVE}/klines_1d.parquet", columns=["symbol", "open_time"])
    archive_end = int(archive["open_time"].max())
    # Archive symbols still trading on its last day may have been delisted since;
    # they are asked too, and simply end when the API no longer knows them.
    alive = set(archive.loc[archive["open_time"] == archive_end, "symbol"])
    listed = perpetual_symbols()
    symbols = sorted(set(listed) | alive)
    in_archive = set(archive["symbol"])
    first = rest.groupby("symbol")["open_time"].min().to_dict() if len(rest) else {}
    last = rest.groupby("symbol")["open_time"].max().to_dict() if len(rest) else {}
    today_open = now_ms - now_ms % DAY_MS
    rows = []
    for symbol in symbols:
        # The archive misses a few listed symbols (non-ASCII names): fetch their whole
        # history, or the listing sleeve would take them for new listings.
        if symbol not in in_archive and listed.get(symbol, 0) and first.get(symbol, 1 << 62) > listed[symbol]:
            rows.extend(fetch_klines(symbol, listed[symbol], now_ms))
            continue
        since = max(archive_end, int(last.get(symbol, archive_end))) + DAY_MS
        if since >= today_open:
            continue
        rows.extend(fetch_klines(symbol, since, now_ms))
    if rows:
        new = pd.DataFrame(rows, columns=KLINE_COLUMNS + ["symbol"])
        new[FLOATS] = new[FLOATS].astype(float)
        for column in ("open_time", "close_time", "count", "ignore"):
            new[column] = new[column].astype("int64")
        rest = pd.concat([rest, new], ignore_index=True) if len(rest) else new
        rest = rest.drop_duplicates(["symbol", "open_time"], keep="last")
        rest.to_parquet(path)
    print(f"klines: {len(symbols)} symbols asked, {len(rows)} new bars, rest total {len(rest)}", flush=True)
    return rest


def funding_window(start: int, end: int) -> list[dict]:
    data = get("/fapi/v1/fundingRate", startTime=start, endTime=end - 1, limit=1000)
    time.sleep(1.0)
    if data is not None and len(data) >= 1000 and end - start > 60_000:
        middle = (start + end) // 2
        return funding_window(start, middle) + funding_window(middle, end)
    return data or []


def update_funding(root: str, now_ms: int) -> pd.DataFrame:
    path = f"{root}/rest_funding.parquet"
    rest = pd.read_parquet(path) if os.path.exists(path) else None
    archive_end = int(pd.read_parquet(f"{ARCHIVE}/fundingRate_all.parquet", columns=["calc_time"])["calc_time"].max())
    # Archive calc_time carries a few ms of jitter: restart on the hour after it.
    start = archive_end - archive_end % 3_600_000 + 3_600_000
    if rest is not None and len(rest):
        start = max(start, int(rest["calc_time"].max()) + 1)
    end = now_ms - now_ms % 3_600_000        # whole hours only
    rows = []
    step = 4 * 3_600_000
    for window_start in range(start, end, step):
        for item in funding_window(window_start, min(window_start + step, end)):
            rows.append({"calc_time": int(item["fundingTime"]), "funding_interval_hours": 0,
                         "last_funding_rate": float(item["fundingRate"]), "symbol": item["symbol"]})
    if rows:
        new = pd.DataFrame(rows)
        rest = pd.concat([rest, new], ignore_index=True) if rest is not None else new
        rest = rest.drop_duplicates(["symbol", "calc_time"], keep="last")
        rest.to_parquet(path)
    print(f"funding: {len(rows)} new settlements, rest total {0 if rest is None else len(rest)}", flush=True)
    return rest


def main(root: str) -> None:
    os.makedirs(f"{root}/data", exist_ok=True)
    now_ms = int(time.time() * 1000)
    klines = update_klines(root, now_ms)
    funding = update_funding(root, now_ms)
    archive_k = pd.read_parquet(f"{ARCHIVE}/klines_1d.parquet")
    merged = pd.concat([archive_k, klines[archive_k.columns]], ignore_index=True)
    merged = merged.drop_duplicates(["symbol", "open_time"], keep="last")
    merged.to_parquet(f"{root}/data/klines_1d.parquet")
    archive_f = pd.read_parquet(f"{ARCHIVE}/fundingRate_all.parquet")
    if funding is not None:
        archive_f = pd.concat([archive_f, funding[archive_f.columns]], ignore_index=True)
    archive_f.to_parquet(f"{root}/data/fundingRate_all.parquet")
    last_day = pd.to_datetime(merged["open_time"].max(), unit="ms").date()
    print(f"data through {last_day}: {len(merged)} bars, {len(archive_f)} settlements", flush=True)


if __name__ == "__main__":
    main(sys.argv[1])
