"""Record Binance USDT-M trades and MEXC's book and trades with local receive times (research only).

Rounds 30 and 31 ask two questions of MEXC's zero-fee contracts, on the clock of the host a bot would run on:
  - round 30: does MEXC lag Binance by more than an order takes to arrive?
  - round 31: does a resting quote earn more than it loses to informed flow when the maker fee is 0?
Public data only, no keys, no orders.

Contracts: the TOP largest at 0/0 with a spread of at most MAX_SPREAD bp in ../xvenue/mexc_costs.json. The list is fixed when
the collector first starts and kept in universe.json, so the tests have one universe, with each contract's size in coins.

Feeds
  Binance   wss://fstream.binance.com/market/stream?streams=<sym>@aggTrade
  MEXC      wss://contract.mexc.com/edge: sub.depth.full (top 5, about every 280 ms) and sub.deal, a JSON ping every 15 s

Tables, in $OUT/<table>/<UTC date>/<HHMMSS>.parquet every FLUSH_S seconds:
  bn   100 ms buckets on the local clock: b, sym, last, hi, lo, buy_q, sell_q, n, e_last, r_last
  mx   100 ms buckets: b, sym, bid, ask, bid_v, ask_v (contracts at the touch), bids5_v, asks5_v (sum of five levels), e_last,
       r_last
  mt   every trade: sym, t (MEXC ms), r (local ms), p, v (contracts), side (1 a taker bought, 2 a taker sold)
  rtt  every minute: t, venue, ms, a light REST call over a kept connection
status.json holds counters.

  python collector.py
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import time
import urllib.request

import pandas as pd
import websockets

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get("OUT", os.path.join(HERE, "data"))
COSTS = os.path.join(HERE, "..", "xvenue", "mexc_costs.json")
UNIVERSE = os.path.join(HERE, "universe.json")
BUCKET = 100
FLUSH_S = int(os.environ.get("FLUSH_S", "120"))
TOP = 30
MAX_SPREAD = 3.0
PER_CONN = 15
PING_S = 15
BN_WS = "wss://fstream.binance.com/market/stream?streams="
BN_API = "https://fapi.binance.com"
MX_WS = "wss://contract.mexc.com/edge"
MX_API = "https://contract.mexc.com/api/v1/contract"
ALIAS = {"TRUMPOFFICIAL": "TRUMP", "SILVER": "XAG"}


def now_ms():
    return int(time.time() * 1000)


def get(url):
    return json.loads(urllib.request.urlopen(url, timeout=20).read())


def binance_name(sym, listed):
    """MEXC contract -> Binance USDT-M perpetual, or None."""
    base = sym[: -len("_USDT")]
    base = ALIAS.get(base, base)
    for name in (f"{base}USDT", f"1000{base}USDT"):
        if name in listed:
            return name
    return None


def pick(costs, listed, sizes):
    """-> {MEXC contract: {"binance": name or None, "size": coins a contract}}, largest first."""
    rows = [(s, c) for s, c in costs.items() if c["taker_bp"] == 0 and c["maker_bp"] == 0 and c["spread_bp"] <= MAX_SPREAD]
    rows.sort(key=lambda x: -x[1]["qv"])
    return {s: {"binance": binance_name(s, listed), "size": sizes[s]} for s, _ in rows[:TOP] if s in sizes}


def universe():
    if os.path.exists(UNIVERSE):
        return json.load(open(UNIVERSE))
    costs = json.load(open(COSTS))["contracts"]
    info = get(f"{BN_API}/fapi/v1/exchangeInfo")
    listed = {s["symbol"] for s in info["symbols"] if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"}
    sizes = {x["symbol"]: float(x["contractSize"]) for x in get(f"{MX_API}/detail")["data"]}
    u = pick(costs, listed, sizes)
    with open(UNIVERSE, "w") as fh:
        json.dump(u, fh, indent=1)
    return u


def depth_row(msg, r):
    """A push.depth.full frame -> row, or None when a side is empty."""
    d = msg["data"]
    if not d.get("bids") or not d.get("asks"):
        return None
    bids, asks = d["bids"], d["asks"]
    return {"b": r // BUCKET * BUCKET, "sym": msg["symbol"], "bid": float(bids[0][0]), "ask": float(asks[0][0]),
            "bid_v": float(bids[0][1]), "ask_v": float(asks[0][1]), "bids5_v": float(sum(x[1] for x in bids)),
            "asks5_v": float(sum(x[1] for x in asks)), "e_last": int(d.get("cts") or msg.get("ts") or 0), "r_last": r}


def deal_rows(msg, r):
    """A push.deal frame -> rows; data may be one trade or a list."""
    d = msg["data"]
    return [{"sym": msg["symbol"], "t": int(x["t"]), "r": r, "p": float(x["p"]), "v": float(x["v"]), "side": int(x["T"])}
            for x in (d if isinstance(d, list) else [d])]


class Collector:
    def __init__(self):
        self.bn, self.mx, self.mt, self.rtt = {}, {}, [], []
        self.n = {"bn_msgs": 0, "mx_depth": 0, "mx_deals": 0, "bn_reconnects": 0, "mx_reconnects": 0, "errors": 0}
        self.u = {}

    def add_bn(self, sym, price, qty, buyer_maker, e, r):
        k = (sym, r // BUCKET * BUCKET)
        x = self.bn.get(k)
        if x is None:
            x = self.bn[k] = {"b": k[1], "sym": sym, "last": price, "hi": price, "lo": price, "buy_q": 0.0, "sell_q": 0.0,
                              "n": 0, "e_last": e, "r_last": r}
        x["last"], x["hi"], x["lo"] = price, max(x["hi"], price), min(x["lo"], price)
        x["sell_q" if buyer_maker else "buy_q"] += price * qty
        x["n"] += 1
        x["e_last"], x["r_last"] = e, r

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
            try:
                bn, mx, mt, rtt = list(self.bn.values()), list(self.mx.values()), self.mt, self.rtt
                self.bn, self.mx, self.mt, self.rtt = {}, {}, [], []
                for table, rows in (("bn", bn), ("mx", mx), ("mt", mt), ("rtt", rtt)):
                    self.write(table, rows)
                with open(f"{OUT}/status.json", "w") as fh:
                    json.dump({"t": now_ms(), "contracts": len(self.u), **self.n}, fh)
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1

    async def rtt_loop(self):
        """Round trips over kept connections, as a bot's would be."""
        hosts = {"binance": ("fapi.binance.com", "/fapi/v1/time"), "mexc": ("contract.mexc.com", "/api/v1/contract/ping")}
        conns = {v: http.client.HTTPSConnection(h, timeout=20) for v, (h, _) in hosts.items()}

        def once(venue, path):
            c = conns[venue]
            c.request("GET", path)
            c.getresponse().read()

        while True:
            for venue, (host, path) in hosts.items():
                for shot in range(2):                           # the first call may have to open the connection
                    t0 = time.monotonic()
                    try:
                        await asyncio.to_thread(once, venue, path)
                        if shot:
                            self.rtt.append({"t": now_ms(), "venue": venue, "ms": round((time.monotonic() - t0) * 1000, 1)})
                    except Exception:  # noqa: BLE001
                        self.n["errors"] += 1
                        conns[venue].close()
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
                        self.add_bn(d["s"], float(d["p"]), float(d["q"]), bool(d["m"]), int(d["E"]), r)
            except Exception:  # noqa: BLE001
                self.n["bn_reconnects"] += 1
                await asyncio.sleep(3)

    async def mx_ping(self, ws):
        while True:
            await asyncio.sleep(PING_S)
            await ws.send(json.dumps({"method": "ping"}))

    async def mx_conn(self, syms):
        while True:
            pinger = None
            try:
                async with websockets.connect(MX_WS, ping_interval=None, max_size=None) as ws:
                    for s in syms:
                        await ws.send(json.dumps({"method": "sub.depth.full", "param": {"symbol": s, "limit": 5}, "gzip": False}))
                        await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": s}, "gzip": False}))
                    pinger = asyncio.create_task(self.mx_ping(ws))
                    async for raw in ws:
                        r = now_ms()
                        m = json.loads(raw)
                        c = m.get("channel")
                        if c == "push.depth.full":
                            row = depth_row(m, r)
                            if row:
                                self.mx[(row["sym"], row["b"])] = row
                                self.n["mx_depth"] += 1
                        elif c == "push.deal":
                            rows = deal_rows(m, r)
                            self.mt.extend(rows)
                            self.n["mx_deals"] += len(rows)
            except Exception:  # noqa: BLE001
                self.n["mx_reconnects"] += 1
                await asyncio.sleep(3)
            finally:
                if pinger:
                    pinger.cancel()

    async def main(self):
        os.makedirs(OUT, exist_ok=True)
        self.u = await asyncio.to_thread(universe)
        contracts = list(self.u)
        syms = sorted({x["binance"] for x in self.u.values() if x["binance"]})
        await asyncio.gather(self.flush_loop(), self.rtt_loop(), self.bn_conn(syms),
                             *(self.mx_conn(contracts[i:i + PER_CONN]) for i in range(0, len(contracts), PER_CONN)))


if __name__ == "__main__":
    asyncio.run(Collector().main())
