"""Record best bid and ask for USDT perps on eight venues, with local receive times (research only).

Fees are what killed most of our 593 trials: 7.5 bp a unit of turnover against a pre-cost edge that is often
smaller. Lighter charges standard accounts nothing, and MEXC charges nothing on hundreds of contracts. This
collector gathers what those two questions need, on the clock of the host a bot would run on:
  - round 24: do prices of one coin on two venues drift apart and converge by more than the costs?
  - round 25: does Lighter lag Binance by more than its 300 ms taker delay? (Binance side: hl_leadlag)

Public data only, no keys, no orders.

Feeds
  REST   one bulk ticker call per venue every POLL_S seconds: Binance, Bybit, OKX, Gate, Bitget, MEXC, KuCoin
  WS     wss://mainnet.zklighter.elliot.ai/stream: ticker/<market> for every active perp, market_stats/all

Tables, written to $OUT/<table>/<UTC date>/<HHMMSS>.parquet every FLUSH_S seconds (120)
  bbo    t (local ms at the end of the call), v, s (venue's own symbol), bid, ask, bq, aq (venue size units, NaN
         where the ticker has none), e (venue time, ms, NaN where the ticker has none). Only rows that changed
         since the same venue's previous poll: to rebuild a book, carry the last row forward.
  polls  t0, t, v, n, changed, ok: one row a poll, so a missing bbo row is known to mean "unchanged"
  lt     100 ms buckets on the local clock, one per Lighter market and bucket with a change, as hl_leadlag's hl
         table: b, coin, bid, ask, bid_sz, ask_sz, h_last (venue update time, ms), r_last (local receive, ms)
  slow   every SLOW_S seconds (300): t, v, s, qv (24 h quote volume), fr (latest funding rate as given)
  rtt    every minute: t, venue, ms (a light REST call to Lighter)
Daily, raw and gzipped: $OUT/meta/<UTC date>/<venue>.json.gz, contract sizes and fee schedules.

  python collector.py
"""

from __future__ import annotations

import asyncio
import gzip
import json
import math
import os
import threading
import time
import traceback
import urllib.request

import pandas as pd
import websockets

OUT = os.environ.get("OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
POLL_S = float(os.environ.get("POLL_S", "3"))
FLUSH_S = int(os.environ.get("FLUSH_S", "120"))
SLOW_S = 300
BUCKET = 100
PER_CONN = 75
TIMEOUT = 10
NAN = float("nan")

LT_ROOT = "https://mainnet.zklighter.elliot.ai/"
LT_API = f"{LT_ROOT}api/v1"
LT_WS = "wss://mainnet.zklighter.elliot.ai/stream"

TICKERS = {
    "binance": "https://fapi.binance.com/fapi/v1/ticker/bookTicker",
    "bybit": "https://api.bybit.com/v5/market/tickers?category=linear",
    "okx": "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    "gate": "https://api.gateio.ws/api/v4/futures/usdt/tickers",
    "bitget": "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
    "mexc": "https://contract.mexc.com/api/v1/contract/ticker",
    "kucoin": "https://api-futures.kucoin.com/api/v1/allTickers",
}
META = {
    "okx": "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
    "gate": "https://api.gateio.ws/api/v4/futures/usdt/contracts",
    "mexc": "https://contract.mexc.com/api/v1/contract/detail",
    "kucoin": "https://api-futures.kucoin.com/api/v1/contracts/active",
    "lighter": f"{LT_API}/orderBookDetails",
}
BN_24H = "https://fapi.binance.com/fapi/v1/ticker/24hr"


def now_ms():
    return int(time.time() * 1000)


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "research"})
    return json.loads(urllib.request.urlopen(req, timeout=TIMEOUT).read())


def num(x):
    """Float, or NaN for anything missing or unparsable."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return NAN


def p_binance(d):
    return [(x["symbol"], num(x["bidPrice"]), num(x["askPrice"]), num(x["bidQty"]), num(x["askQty"]), num(x.get("time")))
            for x in d if x["symbol"].endswith("USDT")]


def p_bybit(d):
    e = num(d.get("time"))
    return [(x["symbol"], num(x.get("bid1Price")), num(x.get("ask1Price")), num(x.get("bid1Size")), num(x.get("ask1Size")), e)
            for x in d["result"]["list"] if x["symbol"].endswith("USDT")]


def p_okx(d):
    return [(x["instId"], num(x.get("bidPx")), num(x.get("askPx")), num(x.get("bidSz")), num(x.get("askSz")), num(x.get("ts")))
            for x in d["data"] if x["instId"].endswith("-USDT-SWAP")]


def p_gate(d):
    return [(x["contract"], num(x.get("highest_bid")), num(x.get("lowest_ask")), num(x.get("highest_size")),
             num(x.get("lowest_size")), NAN) for x in d]


def p_bitget(d):
    return [(x["symbol"], num(x.get("bidPr")), num(x.get("askPr")), num(x.get("bidSz")), num(x.get("askSz")), num(x.get("ts")))
            for x in d["data"]]


def p_mexc(d):
    return [(x["symbol"], num(x.get("bid1")), num(x.get("ask1")), NAN, NAN, num(x.get("timestamp")))
            for x in d["data"] if x["symbol"].endswith(("_USDT", "_USDC", "_USD1", "_USD"))]


def p_kucoin(d):
    return [(x["symbol"], num(x.get("bestBidPrice")), num(x.get("bestAskPrice")), num(x.get("bestBidSize")),
             num(x.get("bestAskSize")), num(x.get("ts")) / 1e6) for x in d["data"] if x["symbol"].endswith("USDTM")]


PARSE = {"binance": p_binance, "bybit": p_bybit, "okx": p_okx, "gate": p_gate, "bitget": p_bitget, "mexc": p_mexc,
         "kucoin": p_kucoin}


def s_bybit(d):
    return [(x["symbol"], num(x.get("turnover24h")), num(x.get("fundingRate"))) for x in d["result"]["list"]]


def s_okx(d):
    return [(x["instId"], num(x.get("volCcy24h")) * num(x.get("last")), NAN) for x in d["data"]]


def s_gate(d):
    return [(x["contract"], num(x.get("volume_24h_quote")), num(x.get("funding_rate"))) for x in d]


def s_bitget(d):
    return [(x["symbol"], num(x.get("usdtVolume")), num(x.get("fundingRate"))) for x in d["data"]]


def s_mexc(d):
    return [(x["symbol"], num(x.get("amount24")), num(x.get("fundingRate"))) for x in d["data"]]


SLOW = {"bybit": s_bybit, "okx": s_okx, "gate": s_gate, "bitget": s_bitget, "mexc": s_mexc}


def valid(bid, ask):
    return bid > 0 and ask > 0 and ask >= bid and math.isfinite(bid) and math.isfinite(ask)


def changed(rows, last):
    """Keep valid rows whose bid or ask moved; last ({sym: (bid, ask)}) is updated in place."""
    out = []
    for r in rows:
        if valid(r[1], r[2]) and last.get(r[0]) != (r[1], r[2]):
            last[r[0]] = (r[1], r[2])
            out.append(r)
    return out


def lt_markets():
    """-> {market_id: symbol} for active Lighter perps."""
    d = get(f"{LT_API}/orderBookDetails")["order_book_details"]
    return {x["market_id"]: x["symbol"] for x in d if x.get("status") == "active" and x.get("market_type") == "perp"}


def lt_ticker(m):
    """-> (market_id, bid, ask, bid_sz, ask_sz, venue ms) from an update/ticker message, or None."""
    t = m.get("ticker") or {}
    a, b = t.get("a") or {}, t.get("b") or {}
    bid, ask = num(b.get("price")), num(a.get("price"))
    if not valid(bid, ask):
        return None
    market = int(str(m["channel"]).split(":")[1])
    return market, bid, ask, num(b.get("size")), num(a.get("size")), int(num(t.get("last_updated_at")) / 1000)


class Store:
    """Rows waiting for the next flush, one list per table, shared by the poll threads and the event loop."""

    def __init__(self):
        self.lock = threading.Lock()
        self.tables = {"bbo": [], "polls": [], "slow": [], "rtt": []}
        self.lt = {}

    def add(self, table, rows):
        with self.lock:
            self.tables[table].extend(rows)

    def bucket(self, coin, bid, ask, bid_sz, ask_sz, h, r):
        b = r // BUCKET * BUCKET
        self.lt[(coin, b)] = {"b": b, "coin": coin, "bid": bid, "ask": ask, "bid_sz": bid_sz, "ask_sz": ask_sz,
                              "h_last": h, "r_last": r}

    def take(self):
        with self.lock:
            tables, self.tables = self.tables, {k: [] for k in self.tables}
        lt, self.lt = list(self.lt.values()), {}
        return tables, lt


def write(table, rows, cols=None):
    if not rows:
        return
    t = pd.Timestamp.now(tz="UTC")
    d = f"{OUT}/{table}/{t:%Y-%m-%d}"
    os.makedirs(d, exist_ok=True)
    path = f"{d}/{t:%H%M%S}.parquet"
    frame = pd.DataFrame(rows, columns=cols)
    for c in ("v", "s", "coin"):
        if c in frame:
            frame[c] = frame[c].astype("category")
    frame.to_parquet(path + ".tmp", index=False, compression="zstd")
    os.replace(path + ".tmp", path)


class Collector:
    def __init__(self):
        self.store = Store()
        self.n = {"lt_msgs": 0, "lt_reconnects": 0, "errors": 0}
        self.polls = {v: {"ok": 0, "bad": 0, "ms": 0} for v in TICKERS}
        self.markets = {}

    def poll_venue(self, v):
        """Blocking loop for one venue, on its own thread: one slow venue must not delay the others."""
        last, next_slow = {}, 0.0
        while True:
            t0 = now_ms()
            ok, rows, fresh = True, [], []
            try:
                d = get(TICKERS[v])
                t = now_ms()
                rows = PARSE[v](d)
                fresh = changed(rows, last)
                self.store.add("bbo", [(t, v, *r) for r in fresh])
                if time.time() >= next_slow:
                    next_slow = time.time() + SLOW_S
                    self.slow(v, d, t)
            except Exception:  # noqa: BLE001
                ok, t = False, now_ms()
                self.n["errors"] += 1
            self.store.add("polls", [(t0, t, v, len(rows), len(fresh), ok)])
            p = self.polls[v]
            p["ok" if ok else "bad"] += 1
            p["ms"] = t - t0
            time.sleep(max(0.2, POLL_S - (t - t0) / 1000))

    def slow(self, v, d, t):
        if v in SLOW:
            self.store.add("slow", [(t, v, *r) for r in SLOW[v](d)])
        elif v == "binance":
            self.store.add("slow", [(t, v, x["symbol"], num(x.get("quoteVolume")), NAN) for x in get(BN_24H)])

    def meta_day(self):
        """Blocking loop: raw contract details for each venue once a day."""
        while True:
            day = f"{pd.Timestamp.now(tz='UTC'):%Y-%m-%d}"
            os.makedirs(f"{OUT}/meta/{day}", exist_ok=True)
            for v, url in META.items():
                try:
                    with gzip.open(f"{OUT}/meta/{day}/{v}.json.gz", "wt") as fh:
                        json.dump(get(url), fh)
                except Exception:  # noqa: BLE001
                    self.n["errors"] += 1
            time.sleep(86_400 - time.time() % 86_400 + 60)

    async def flush_loop(self):
        while True:
            await asyncio.sleep(FLUSH_S)
            tables, lt = self.store.take()
            try:
                await asyncio.to_thread(self.flush, tables, lt)
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1
                traceback.print_exc()

    def flush(self, tables, lt):
        write("bbo", tables["bbo"], ["t", "v", "s", "bid", "ask", "bq", "aq", "e"])
        write("polls", tables["polls"], ["t0", "t", "v", "n", "changed", "ok"])
        write("slow", tables["slow"], ["t", "v", "s", "qv", "fr"])
        write("rtt", tables["rtt"], ["t", "venue", "ms"])
        write("lt", lt)
        with open(f"{OUT}/status.json.tmp", "w") as fh:
            json.dump({"t": now_ms(), "lt_markets": len(self.markets), **self.n, "polls": self.polls}, fh)
        os.replace(f"{OUT}/status.json.tmp", f"{OUT}/status.json")

    async def rtt_loop(self):
        while True:
            t0 = time.monotonic()
            try:
                await asyncio.to_thread(get, LT_ROOT)
                self.store.add("rtt", [(now_ms(), "lighter", round((time.monotonic() - t0) * 1000, 1))])
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1
            await asyncio.sleep(60)

    async def stats_conn(self):
        """market_stats/all arrives about once a second; one snapshot per SLOW_S is plenty."""
        next_slow = 0.0
        while True:
            try:
                async with websockets.connect(LT_WS, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": "market_stats/all"}))
                    async for msg in ws:
                        if time.time() < next_slow:
                            continue
                        stats = json.loads(msg).get("market_stats") or {}
                        if len(stats) > 1:
                            next_slow = time.time() + SLOW_S
                            t = now_ms()
                            self.store.add("slow", [(t, "lighter", x["symbol"], num(x.get("daily_quote_token_volume")),
                                                     num(x.get("funding_rate"))) for x in stats.values()])
            except Exception:  # noqa: BLE001
                self.n["lt_reconnects"] += 1
                await asyncio.sleep(3)

    async def lt_conn(self, markets):
        while True:
            try:
                async with websockets.connect(LT_WS, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    for m in markets:
                        await ws.send(json.dumps({"type": "subscribe", "channel": f"ticker/{m}"}))
                    async for msg in ws:
                        r = now_ms()
                        m = json.loads(msg)
                        if m.get("type") != "update/ticker":
                            continue
                        row = lt_ticker(m)
                        if row is None:
                            continue
                        self.n["lt_msgs"] += 1
                        self.store.bucket(self.markets.get(row[0], str(row[0])), *row[1:], r)
            except Exception:  # noqa: BLE001
                self.n["lt_reconnects"] += 1
                await asyncio.sleep(3)

    async def run_day(self):
        """Run the Lighter feeds for one UTC day with a fresh market list, then return."""
        self.markets = await asyncio.to_thread(lt_markets)
        ids = sorted(self.markets)
        tasks = [asyncio.create_task(self.lt_conn(ids[i:i + PER_CONN])) for i in range(0, len(ids), PER_CONN)]
        tasks.append(asyncio.create_task(self.stats_conn()))
        await asyncio.sleep(86_400 - time.time() % 86_400 + 5)
        for t in tasks:
            t.cancel()

    async def main(self):
        os.makedirs(OUT, exist_ok=True)
        for v in TICKERS:
            threading.Thread(target=self.poll_venue, args=(v,), daemon=True).start()
        threading.Thread(target=self.meta_day, daemon=True).start()
        asyncio.create_task(self.flush_loop())
        asyncio.create_task(self.rtt_loop())
        while True:
            try:
                await self.run_day()
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(Collector().main())
