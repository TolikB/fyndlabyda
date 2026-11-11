"""Record Binance TradFi perps and Hyperliquid xyz markets on the host's own clock (research only).

Binance lists perps on stocks, funds and metals (contractType TRADIFI_PERPETUAL). The xyz dex on Hyperliquid lists the
same names, most of them in growth mode, where a taker pays about 0.009%: a fifth of the 0.045% that round 20 has to
clear on Hyperliquid's own perps. Round 37 asks whether xyz lags Binance by enough to pay that. The answer needs each
feed with the local time a message arrived, on the host a bot would run on, and it cannot be had after the fact.
Public data only, no keys, no orders.

Pairs: every live xyz market whose ticker is the base asset of a TRADING Binance TradFi perp, plus ALIASES for names
that differ. An alias may pair an index with its fund (xyz:XYZ100 with QQQUSDT), so only returns compare. The map is
built once each UTC day and kept in $OUT/pairs/<date>.json with what sets each market's fee that day: its growth-mode
flag and the deployer's fee scale.

Feeds
  Binance   wss://fstream.binance.com/market/stream   <sym>@aggTrade for every pair
            wss://fstream.binance.com/public/stream   <sym>@depth5@100ms for the DEPTH_TOP pairs with the largest 24 h
                                                      quote volume (bookTicker is far too busy to record)
  HL        wss://api.hyperliquid.xyz/ws              "bbo" and "trades" per coin. A subscription is answered with
                                                      a batch of past trades: a coin's first frame on a connection
                                                      is dropped, and so is any trade that arrives more than 10 s
                                                      after its own time

Rows are 100 ms buckets on the local clock, one per symbol and bucket with activity:
  bn   b (bucket start, local ms), sym, last, hi, lo, buy_q (quote bought by takers), sell_q, buy_px (price of the
       last taker buy, NaN without one), sell_px, n, e_last (Binance event time of the last trade), r_last (local
       receive time of the last trade)
  bd   b, sym, bid, ask, bid_sz, ask_sz (base units at the touch), bids5_q, asks5_q (quote on five levels), e_last,
       r_last: the last book of the bucket
  hl   b, coin, bid, ask, bid_sz, ask_sz, h_last (HL block time of the last update), r_last
  hlt  as bn for xyz trades: coin in place of sym, h_last in place of e_last; side "B" counts as bought
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
DEPTH_TOP = int(os.environ.get("DEPTH_TOP", "30"))
PER_CONN = 100
SILENT_S = 120
FRESH_MS = 10_000
NAN = float("nan")
DEX = "xyz"
HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_API = "https://api.hyperliquid.xyz/info"
BN_TRADES = "wss://fstream.binance.com/market/stream?streams="
BN_DEPTH = "wss://fstream.binance.com/public/stream?streams="
BN_API = "https://fapi.binance.com"
ALIASES = {"xyz:GOLD": "XAUUSDT", "xyz:SILVER": "XAGUSDT", "xyz:PLATINUM": "XPTUSDT", "xyz:PALLADIUM": "XPDUSDT",
           "xyz:BRENTOIL": "BZUSDT", "xyz:XYZ100": "QQQUSDT", "xyz:SP500": "SPYUSDT", "xyz:SKHX": "SKHYNIXUSDT",
           "xyz:SMSN": "SAMSUNGUSDT", "xyz:ANT": "ANTHROPICUSDT"}


def now_ms():
    return int(time.time() * 1000)


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())


def get(url):
    return json.loads(urllib.request.urlopen(url, timeout=20).read())


def pairs_from(symbols, universe, volume):
    """Binance exchangeInfo symbols, the xyz universe, {symbol: 24 h quote volume} -> {coin: facts of the pair}."""
    listed = {s["symbol"]: s for s in symbols if s.get("contractType") == "TRADIFI_PERPETUAL"
              and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}
    by_base = {s["baseAsset"]: name for name, s in listed.items()}
    out = {}
    for u in universe:
        coin = u["name"]
        sym = ALIASES.get(coin) or by_base.get(coin.split(":", 1)[-1])
        if u.get("isDelisted") or sym not in listed:
            continue
        out[coin] = {"binance": sym, "type": listed[sym].get("underlyingType"),
                     "growth": u.get("growthMode") == "enabled", "fee_scale": float(u.get("deployerFeeScale", 1.0)),
                     "max_lev": u.get("maxLeverage"),
                     "volume": float(volume.get(sym, 0.0)), "depth": False}
    for coin in sorted(out, key=lambda c: -out[c]["volume"])[:DEPTH_TOP]:
        out[coin]["depth"] = True
    return out


def load_pairs():
    info = get(f"{BN_API}/fapi/v1/exchangeInfo")
    volume = {t["symbol"]: float(t["quoteVolume"]) for t in get(f"{BN_API}/fapi/v1/ticker/24hr")}
    meta = post(HL_API, {"type": "meta", "dex": DEX})
    return pairs_from(info["symbols"], meta["universe"], volume)


def depth_cols(d):
    """A Binance partial-depth frame -> the bd columns, or None when a side is empty."""
    bids = [(float(p), float(q)) for p, q in d.get("b") or []]
    asks = [(float(p), float(q)) for p, q in d.get("a") or []]
    if not bids or not asks:
        return None
    bid, ask = max(bids), min(asks)
    return {"bid": bid[0], "ask": ask[0], "bid_sz": bid[1], "ask_sz": ask[1],
            "bids5_q": sum(p * q for p, q in bids), "asks5_q": sum(p * q for p, q in asks)}


def fresh_trades(data, seen, r):
    """An HL "trades" frame received at r -> the trades to keep: none of a coin's first frame on a connection (the batch
    of past trades that answers a subscription), and none that is more than FRESH_MS old on arrival."""
    if not data:
        return []
    coin = data[0]["coin"]
    if coin not in seen:
        seen.add(coin)
        return []
    return [t for t in data if r - int(t["time"]) <= FRESH_MS]


class Buckets:
    """Latest state per (name, 100 ms bucket) on the local clock; taken as rows."""

    def __init__(self, key, clock):
        self.key, self.clock, self.rows = key, clock, {}

    def trade(self, name, price, quote, bought, venue_t, r):
        b = r // BUCKET * BUCKET
        x = self.rows.get((name, b))
        if x is None:
            x = self.rows[(name, b)] = {"b": b, self.key: name, "last": price, "hi": price, "lo": price, "buy_q": 0.0,
                                        "sell_q": 0.0, "buy_px": NAN, "sell_px": NAN, "n": 0, self.clock: venue_t,
                                        "r_last": r}
        x["last"], x["hi"], x["lo"] = price, max(x["hi"], price), min(x["lo"], price)
        x["buy_q" if bought else "sell_q"] += quote
        x["buy_px" if bought else "sell_px"] = price
        x["n"] += 1
        x[self.clock], x["r_last"] = venue_t, r

    def quote(self, name, cols, venue_t, r):
        b = r // BUCKET * BUCKET
        self.rows[(name, b)] = {"b": b, self.key: name, **cols, self.clock: venue_t, "r_last": r}

    def take(self):
        rows, self.rows = list(self.rows.values()), {}
        return rows


class Collector:
    def __init__(self):
        self.bn, self.bd = Buckets("sym", "e_last"), Buckets("sym", "e_last")
        self.hl, self.hlt = Buckets("coin", "h_last"), Buckets("coin", "h_last")
        self.rtt, self.pairs = [], {}
        self.n = {"bn_msgs": 0, "bd_msgs": 0, "hl_msgs": 0, "hlt_msgs": 0, "hlt_dropped": 0, "bn_reconnects": 0,
                  "bd_reconnects": 0, "hl_reconnects": 0, "errors": 0}

    def on_bn_trade(self, m, r, _):
        d = m.get("data") or {}
        if d.get("e") != "aggTrade":
            return
        self.n["bn_msgs"] += 1
        p = float(d["p"])
        self.bn.trade(d["s"], p, p * float(d["q"]), not d["m"], int(d["E"]), r)

    def on_bn_depth(self, m, r, _):
        d = m.get("data") or {}
        cols = depth_cols(d) if d.get("e") == "depthUpdate" else None
        if cols:
            self.n["bd_msgs"] += 1
            self.bd.quote(d["s"], cols, int(d["E"]), r)

    def on_hl(self, m, r, seen):
        channel, d = m.get("channel"), m.get("data")
        if channel == "bbo":
            bid, ask = d["bbo"]
            if bid and ask:
                self.n["hl_msgs"] += 1
                self.hl.quote(d["coin"], {"bid": float(bid["px"]), "ask": float(ask["px"]), "bid_sz": float(bid["sz"]),
                                          "ask_sz": float(ask["sz"])}, int(d["time"]), r)
        elif channel == "trades":
            kept = fresh_trades(d, seen, r)
            self.n["hlt_dropped"] += len(d) - len(kept)
            for t in kept:
                self.n["hlt_msgs"] += 1
                p = float(t["px"])
                self.hlt.trade(t["coin"], p, p * float(t["sz"]), t["side"] == "B", int(t["time"]), r)

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
                for table in ("bn", "bd", "hl", "hlt"):
                    await asyncio.to_thread(self.write, table, getattr(self, table).take())
                rtt, self.rtt = self.rtt, []
                self.write("rtt", rtt)
                with open(f"{OUT}/status.json", "w") as fh:
                    json.dump({"t": now_ms(), "pairs": len(self.pairs),
                               "depth": sum(p["depth"] for p in self.pairs.values()), **self.n}, fh)
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1

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

    async def feed(self, name, url, subs, handle):
        """Keep one WebSocket open; reconnect after an error or SILENT_S without a message."""
        while True:
            seen = set()
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    for s in subs:
                        await ws.send(json.dumps(s))
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=SILENT_S)
                        r = now_ms()
                        try:
                            handle(json.loads(msg), r, seen)
                        except (KeyError, ValueError, TypeError, IndexError):
                            self.n["errors"] += 1
            except Exception:  # noqa: BLE001
                self.n[f"{name}_reconnects"] += 1
                await asyncio.sleep(3)

    def day_pairs(self):
        """The UTC day's pairs: read from its file when a restart finds one, else built and saved."""
        path = f"{OUT}/pairs/{time.strftime('%Y-%m-%d', time.gmtime())}.json"
        if os.path.exists(path):
            return json.load(open(path))
        pairs = load_pairs()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".tmp", "w") as fh:
            json.dump(pairs, fh, indent=1)
        os.replace(path + ".tmp", path)
        return pairs

    async def run_day(self):
        """Run the feeds for one UTC day with that day's pairs, then return."""
        self.pairs = await asyncio.to_thread(self.day_pairs)
        syms = sorted({p["binance"] for p in self.pairs.values()})
        deep = sorted({p["binance"] for p in self.pairs.values() if p["depth"]})
        subs = [{"method": "subscribe", "subscription": {"type": kind, "coin": c}}
                for c in sorted(self.pairs) for kind in ("bbo", "trades")]
        jobs = [self.feed("bn", BN_TRADES + "/".join(f"{s.lower()}@aggTrade" for s in syms[i:i + PER_CONN]), [],
                          self.on_bn_trade) for i in range(0, len(syms), PER_CONN)]
        jobs += [self.feed("bd", BN_DEPTH + "/".join(f"{s.lower()}@depth5@100ms" for s in deep[i:i + PER_CONN]), [],
                           self.on_bn_depth) for i in range(0, len(deep), PER_CONN)]
        jobs.append(self.feed("hl", HL_WS, subs, self.on_hl))
        tasks = [asyncio.create_task(j) for j in jobs]
        try:
            await asyncio.sleep(86_400 - time.time() % 86_400 + 5)
        finally:
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
