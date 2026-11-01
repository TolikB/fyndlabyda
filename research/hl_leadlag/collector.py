"""Record Binance USDT-M trades and Hyperliquid best bid/ask with local receive times (research only).

Round 20 asks whether Hyperliquid's perps lag Binance's by seconds after a sharp Binance move, enough for
a taker on Hyperliquid to pay its fee. That has to be judged on the clock of this host, the one a bot
would run on, so both feeds are recorded with the local time each message arrived. Public data only, no
keys, no orders.

Coins: every Hyperliquid perp that maps to a TRADING Binance USDT-M perp (k-prefix -> 1000, as in
hl_smart/fwd_smart.binance_symbol), refreshed daily.

Feeds
  Binance   wss://fstream.binance.com/market/stream?streams=<sym>@aggTrade (the old /ws route no longer
            carries aggTrade), at most 100 streams a connection
  HL        wss://api.hyperliquid.xyz/ws, one "bbo" subscription per coin

Rows are 100 ms buckets on the local clock, one per symbol and bucket with activity:
  bn   b (bucket start, local ms), sym, last, hi, lo, buy_q (quote bought), sell_q, n, e_last (Binance event
       time of the last trade), r_last (local receive time of the last trade)
  hl   b, coin, bid, ask, bid_sz, ask_sz, h_last (HL block time of the last update), r_last
  rtt  every minute: t, venue, ms (round trip of a light REST call: Binance /fapi/v1/time, HL allMids)
Parts go to $OUT/<table>/<UTC date>/<HHMMSS>.parquet every FLUSH_S seconds (120); status.json holds counters.

  python collector.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.request

import pandas as pd
import websockets

OUT = os.environ.get("OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
BUCKET = 100
FLUSH_S = int(os.environ.get("FLUSH_S", "120"))
PER_CONN = 100
HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_API = "https://api.hyperliquid.xyz/info"
BN_WS = "wss://fstream.binance.com/market/stream?streams="
BN_API = "https://fapi.binance.com"


def now_ms():
    return int(time.time() * 1000)


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())


def get(url):
    return json.loads(urllib.request.urlopen(url, timeout=20).read())


def binance_symbol(coin, listed):
    """Hyperliquid coin -> Binance USDT-M perpetual name, or None (same rule as fwd_smart.binance_symbol)."""
    if coin.startswith("k") and coin[1:].isupper():
        name = f"1000{coin[1:]}USDT"
        return name if name in listed else None
    for name in (f"{coin}USDT", f"1000{coin}USDT"):
        if name in listed:
            return name
    return None


def coin_map():
    """-> {HL coin: Binance symbol} for live HL perps with a TRADING Binance USDT-M perp."""
    info = get(f"{BN_API}/fapi/v1/exchangeInfo")
    listed = {s["symbol"] for s in info["symbols"] if s["contractType"] == "PERPETUAL"
              and s["quoteAsset"] == "USDT" and s["status"] == "TRADING"}
    meta = post(HL_API, {"type": "meta"})
    out = {}
    for u in meta["universe"]:
        if u.get("isDelisted"):
            continue
        sym = binance_symbol(u["name"], listed)
        if sym:
            out[u["name"]] = sym
    return out


class Buckets:
    """Latest state per (key, 100 ms bucket); flushed as rows."""

    def __init__(self):
        self.rows = {}

    def bn(self, sym, price, qty, buyer_maker, e, r):
        b = r // BUCKET * BUCKET
        k = (sym, b)
        x = self.rows.get(k)
        quote = price * qty
        if x is None:
            x = self.rows[k] = {"b": b, "sym": sym, "last": price, "hi": price, "lo": price, "buy_q": 0.0,
                                "sell_q": 0.0, "n": 0, "e_last": e, "r_last": r}
        x["last"], x["hi"], x["lo"] = price, max(x["hi"], price), min(x["lo"], price)
        x["sell_q" if buyer_maker else "buy_q"] += quote
        x["n"] += 1
        x["e_last"], x["r_last"] = e, r

    def hl(self, coin, bid, ask, bid_sz, ask_sz, h, r):
        b = r // BUCKET * BUCKET
        self.rows[(coin, b)] = {"b": b, "coin": coin, "bid": bid, "ask": ask, "bid_sz": bid_sz, "ask_sz": ask_sz,
                                "h_last": h, "r_last": r}

    def take(self):
        rows, self.rows = list(self.rows.values()), {}
        return rows


class Collector:
    def __init__(self):
        self.bn, self.hl, self.rtt = Buckets(), Buckets(), []
        self.n = {"bn_msgs": 0, "hl_msgs": 0, "bn_reconnects": 0, "hl_reconnects": 0, "errors": 0}
        self.coins = {}

    def write(self, table, rows):
        if not rows:
            return
        t = pd.Timestamp.now(tz="UTC")
        d = f"{OUT}/{table}/{t:%Y-%m-%d}"
        os.makedirs(d, exist_ok=True)
        path = f"{d}/{t:%H%M%S}.parquet"
        pd.DataFrame(rows).to_parquet(path + ".tmp", index=False, compression="zstd")
        os.replace(path + ".tmp", path)

    async def flush_loop(self):
        while True:
            await asyncio.sleep(FLUSH_S)
            self.write("bn", self.bn.take())
            self.write("hl", self.hl.take())
            rtt, self.rtt = self.rtt, []
            self.write("rtt", rtt)
            with open(f"{OUT}/status.json", "w") as fh:
                json.dump({"t": now_ms(), "coins": len(self.coins), **self.n}, fh)

    async def rtt_loop(self):
        while True:
            for venue, fn in (("binance", lambda: get(f"{BN_API}/fapi/v1/time")),
                              ("hl", lambda: post(HL_API, {"type": "allMids"}))):
                t0 = time.monotonic()
                try:
                    await asyncio.to_thread(fn)
                    self.rtt.append({"t": now_ms(), "venue": venue, "ms": round((time.monotonic() - t0) * 1000, 1)})
                except Exception:  # noqa: BLE001
                    self.n["errors"] += 1
            await asyncio.sleep(60)

    async def bn_conn(self, syms):
        url = BN_WS + "/".join(f"{s.lower()}@aggTrade" for s in syms)
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    async for msg in ws:
                        r = now_ms()
                        d = json.loads(msg).get("data") or {}
                        if d.get("e") != "aggTrade":
                            continue
                        self.n["bn_msgs"] += 1
                        self.bn.bn(d["s"], float(d["p"]), float(d["q"]), bool(d["m"]), int(d["E"]), r)
            except Exception:  # noqa: BLE001
                self.n["bn_reconnects"] += 1
                await asyncio.sleep(3)

    async def hl_conn(self, coins):
        while True:
            try:
                async with websockets.connect(HL_WS, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    for c in coins:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "bbo", "coin": c}}))
                    async for msg in ws:
                        r = now_ms()
                        m = json.loads(msg)
                        if m.get("channel") != "bbo":
                            continue
                        d = m["data"]
                        bid, ask = d["bbo"]
                        if not bid or not ask:
                            continue
                        self.n["hl_msgs"] += 1
                        self.hl.hl(d["coin"], float(bid["px"]), float(ask["px"]), float(bid["sz"]), float(ask["sz"]),
                                   int(d["time"]), r)
            except Exception:  # noqa: BLE001
                self.n["hl_reconnects"] += 1
                await asyncio.sleep(3)

    async def run_day(self):
        """Run the feeds for one UTC day with a fresh coin map, then return."""
        self.coins = await asyncio.to_thread(coin_map)
        syms = sorted(set(self.coins.values()))
        tasks = [asyncio.create_task(self.bn_conn(syms[i:i + PER_CONN])) for i in range(0, len(syms), PER_CONN)]
        tasks.append(asyncio.create_task(self.hl_conn(sorted(self.coins))))
        seconds = 86_400 - time.time() % 86_400 + 5
        await asyncio.sleep(seconds)
        for t in tasks:
            t.cancel()

    async def main(self):
        os.makedirs(OUT, exist_ok=True)
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
