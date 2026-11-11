"""Record Hyperliquid's thin builder perp books next to their xyz twins on the host's own clock (research only).

Several builder dexes on Hyperliquid list the same stock or index as the large xyz dex, on their own order books. On
2026-11-11 three were alive: xyz (122 live markets), io (15) and para (33). The small ones quote far wider than xyz and
trade little, and all three charge a taker about 0.009% in growth mode. Round 43 asks whether a small account can earn
that width: rest quotes on the thin book around the xyz price and hedge each fill on xyz. The answer needs both books
with the local time each message arrived, on the host a bot would run on, and it cannot be had after the fact.
Public data only, no keys, no orders.

Pairs: every live market of a builder dex other than REF whose ticker (after ALIASES) is also live on REF. A pair may
be quoted in different units on the two books (a private company's valuation), so only returns compare. The map is
built once each UTC day and kept in $OUT/pairs/<date>.json with what sets each market's fee that day: its growth-mode
flag and the deployer's fee scale.

Feed  wss://api.hyperliquid.xyz/ws   "bbo" and "trades" for each thin market, "bbo" for each REF twin.
      A subscription is answered with a batch of past trades: a coin's first trades frame on a connection is dropped,
      and so is any trade that arrives more than 10 s after its own time.
      With about 30 pairs this is under 100 subscriptions of Hyperliquid's 1,000 per IP.

Rows are 100 ms buckets on the local clock, one per coin and bucket with activity:
  hl   b (bucket start, local ms), coin, bid, ask, bid_sz, ask_sz, h_last (HL block time of the last update), r_last
       (local receive time of the last update): the last book of the bucket, thin markets and twins alike
  hlt  b, coin, last, hi, lo, buy_q (quote bought by takers), sell_q, buy_px (price of the last taker buy, NaN without
       one), sell_px, n, h_last, r_last: thin markets only
  rtt  every minute: t, venue, ms (round trip of a light REST call, HL allMids)
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
SILENT_S = 300
FRESH_MS = 10_000
NAN = float("nan")
REF = "xyz"
HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_API = "https://api.hyperliquid.xyz/info"
ALIASES = {"ANTH": "ANT"}          # a thin dex's ticker -> REF's ticker for the same underlying


def now_ms():
    return int(time.time() * 1000)


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())


def live(meta):
    return [u for u in meta["universe"] if not u.get("isDelisted")]


def facts(u):
    return {"growth": u.get("growthMode") == "enabled", "fee_scale": float(u.get("deployerFeeScale", 1.0)),
            "max_lev": u.get("maxLeverage"), "isolated": bool(u.get("onlyIsolated"))}


def pairs_from(metas):
    """{dex name: meta} -> {thin coin: {"twin": REF coin, "dex", fee facts of both}} for every live market that has a
    live REF market with the same ticker."""
    ref = {u["name"].split(":", 1)[-1]: u for u in live(metas[REF])}
    out = {}
    for dex, meta in metas.items():
        if dex == REF:
            continue
        for u in live(meta):
            ticker = u["name"].split(":", 1)[-1]
            twin = ref.get(ALIASES.get(ticker, ticker))
            if twin is not None:
                out[u["name"]] = {"twin": twin["name"], "dex": dex, **facts(u),
                                  **{"twin_" + k: v for k, v in facts(twin).items()}}
    return out


def load_pairs():
    names = [d["name"] for d in post(HL_API, {"type": "perpDexs"}) if d]
    metas = {}
    for name in names:
        metas[name] = post(HL_API, {"type": "meta", "dex": name})
        time.sleep(1.0)
    return pairs_from(metas)


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
    """Latest state per (coin, 100 ms bucket) on the local clock; taken as rows."""

    def __init__(self):
        self.rows = {}

    def trade(self, coin, price, quote, bought, venue_t, r):
        b = r // BUCKET * BUCKET
        x = self.rows.get((coin, b))
        if x is None:
            x = self.rows[(coin, b)] = {"b": b, "coin": coin, "last": price, "hi": price, "lo": price, "buy_q": 0.0,
                                        "sell_q": 0.0, "buy_px": NAN, "sell_px": NAN, "n": 0, "h_last": venue_t,
                                        "r_last": r}
        x["last"], x["hi"], x["lo"] = price, max(x["hi"], price), min(x["lo"], price)
        x["buy_q" if bought else "sell_q"] += quote
        x["buy_px" if bought else "sell_px"] = price
        x["n"] += 1
        x["h_last"], x["r_last"] = venue_t, r

    def quote(self, coin, cols, venue_t, r):
        b = r // BUCKET * BUCKET
        self.rows[(coin, b)] = {"b": b, "coin": coin, **cols, "h_last": venue_t, "r_last": r}

    def take(self):
        rows, self.rows = list(self.rows.values()), {}
        return rows


def subscriptions(pairs):
    """-> the subscribe messages: bbo and trades for each thin coin, bbo for each distinct twin."""
    subs = [{"method": "subscribe", "subscription": {"type": kind, "coin": c}}
            for c in sorted(pairs) for kind in ("bbo", "trades")]
    subs += [{"method": "subscribe", "subscription": {"type": "bbo", "coin": c}}
             for c in sorted({p["twin"] for p in pairs.values()})]
    return subs


class Collector:
    def __init__(self):
        self.hl, self.hlt = Buckets(), Buckets()
        self.rtt, self.pairs = [], {}
        self.n = {"hl_msgs": 0, "hlt_msgs": 0, "hlt_dropped": 0, "hl_reconnects": 0, "errors": 0}

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
                for table in ("hl", "hlt"):
                    await asyncio.to_thread(self.write, table, getattr(self, table).take())
                rtt, self.rtt = self.rtt, []
                self.write("rtt", rtt)
                with open(f"{OUT}/status.json", "w") as fh:
                    json.dump({"t": now_ms(), "pairs": len(self.pairs),
                               "twins": len({p["twin"] for p in self.pairs.values()}), **self.n}, fh)
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1

    async def rtt_loop(self):
        while True:
            t0 = time.monotonic()
            try:
                await asyncio.to_thread(post, HL_API, {"type": "allMids", "dex": REF})
                self.rtt.append({"t": now_ms(), "venue": "hl", "ms": round((time.monotonic() - t0) * 1000, 1)})
            except Exception:  # noqa: BLE001
                self.n["errors"] += 1
            await asyncio.sleep(60)

    async def feed(self, subs):
        """Keep one WebSocket open; reconnect after an error or SILENT_S without a message."""
        while True:
            seen = set()
            try:
                async with websockets.connect(HL_WS, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    for s in subs:
                        await ws.send(json.dumps(s))
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=SILENT_S)
                        r = now_ms()
                        try:
                            self.on_hl(json.loads(msg), r, seen)
                        except (KeyError, ValueError, TypeError, IndexError):
                            self.n["errors"] += 1
            except Exception:  # noqa: BLE001
                self.n["hl_reconnects"] += 1
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
        """Run the feed for one UTC day with that day's pairs, then return."""
        self.pairs = await asyncio.to_thread(self.day_pairs)
        task = asyncio.create_task(self.feed(subscriptions(self.pairs)))
        try:
            await asyncio.sleep(86_400 - time.time() % 86_400 + 5)
        finally:
            task.cancel()

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
