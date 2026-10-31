"""Hyperliquid liquidation-map collector (research only, public read-only API, no keys).

Hyperliquid publishes every account's open positions, with their liquidation prices, through
the info endpoint (`clearinghouseState` by address). Each public trade names both users, and a
public leaderboard lists about 48k accounts. This service:

  - learns addresses from the trades stream of every live perp and from the daily leaderboard;
  - polls each address's clearinghouseState on a priority schedule (bigger positions more often,
    an address that traded since its last poll sooner);
  - writes every observation, plus 1-minute mids and 5-minute asset contexts, to parquet parts
    under OUT/<table>/<UTC date>/.

Budget: RATE_PER_MIN clearinghouseState calls (weight 2 each) against Hyperliquid's 1,200 weight a
minute per IP, which the project's paper stacks on this host share. A 429 halves the rate for
10 minutes. Nothing here signs or sends anything.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import math
import os
import signal
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor

API = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"
LEADERBOARD = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
OUT = os.environ.get("OUT", "/data")
RATE_PER_MIN = float(os.environ.get("RATE_PER_MIN", "200"))
WORKERS = 4
FLUSH_S = 300
MIDS_S = 60
CTX_S = 300
LEADERBOARD_S = 86400
QUEUE_S = 20
PRUNE_S = 14 * 86400
PENALTY_S = 600

# (minimum position notional in USD, poll interval in seconds); an account with no position is
# polled every EMPTY_S; one that traded since its last poll is due after a quarter of its interval.
TIERS = [(5e6, 900), (1e6, 1800), (1e5, 7200), (1e4, 28800), (0.0, 86400)]
EMPTY_S = 172800


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# ---------------------------------------------------------------- schedule

def interval(ntl, traded_since_poll):
    if ntl is None:
        return 0.0
    iv = EMPTY_S if ntl <= 0 else next(s for th, s in TIERS if ntl >= th)
    return max(iv / 4, 600.0) if traded_since_poll else iv


class Registry:
    """addr -> {"first", "trade", "poll", "ntl", "tn", "lb"} (times in seconds, sizes in USD)."""

    def __init__(self):
        self.a = {}

    def _get(self, addr, now):
        e = self.a.get(addr)
        if e is None:
            e = self.a[addr] = {"first": now, "trade": 0.0, "poll": 0.0, "ntl": None, "tn": 0.0, "lb": 0.0}
        return e

    def seen_trade(self, addr, t, notional):
        e = self._get(addr, t)
        e["trade"] = max(e["trade"], t)
        e["tn"] += notional

    def seed(self, addr, account_value, now):
        self._get(addr, now)["lb"] = account_value

    def polled(self, addr, now, ntl):
        e = self._get(addr, now)
        e["poll"], e["ntl"] = now, ntl

    def failed(self, addr, now):
        """Back off after a non-rate-limit error: due again after its normal interval."""
        e = self._get(addr, now)
        e["poll"] = now
        if e["ntl"] is None:
            e["ntl"] = 0.0

    def score(self, e, now):
        if e["ntl"] is None:
            # never polled: rank like a 2x-overdue account, bigger traders and accounts first
            return 2.0 + math.log10(1.0 + max(e["tn"], e["lb"])) / 10.0
        return (now - e["poll"]) / interval(e["ntl"], e["trade"] > e["poll"])

    def due(self, now, limit):
        scored = [(self.score(e, now), a) for a, e in self.a.items()]
        scored = [x for x in scored if x[0] >= 1.0]
        scored.sort(reverse=True)
        return [a for _, a in scored[:limit]], len(scored)

    def prune(self, now):
        drop = [a for a, e in self.a.items()
                if max(e["trade"], e["first"]) < now - PRUNE_S and not e["ntl"] and e["lb"] < 1e5]
        for a in drop:
            del self.a[a]
        return len(drop)

    def save(self, path):
        tmp = path + ".tmp"
        with gzip.open(tmp, "wt") as fh:
            json.dump(self.a, fh, separators=(",", ":"))
        os.replace(tmp, path)

    def load(self, path):
        if os.path.exists(path):
            with gzip.open(path, "rt") as fh:
                self.a = json.load(fh)


class Limiter:
    def __init__(self, per_min):
        self.base = per_min
        self.until = 0.0
        self.next = time.monotonic()

    def rate(self):
        return self.base / 2 if time.monotonic() < self.until else self.base

    def penalize(self):
        self.until = time.monotonic() + PENALTY_S
        self.next = max(self.next, time.monotonic() + 30.0)

    async def wait(self):
        now = time.monotonic()
        self.next = max(self.next, now)
        delay = self.next - now
        self.next += 60.0 / self.rate()
        if delay > 0:
            await asyncio.sleep(delay)


# ---------------------------------------------------------------- parsing

def parse_state(addr, ts_ms, st):
    """clearinghouseState -> (account row, [position rows], total position notional)."""
    ms = st.get("marginSummary") or {}
    pos = []
    for ap in st.get("assetPositions") or []:
        p = ap.get("position") or {}
        szi = num(p.get("szi"))
        if not szi:
            continue
        lev = p.get("leverage") or {}
        pos.append({"ts": ts_ms, "addr": addr, "coin": str(p.get("coin")), "szi": szi,
                    "entry_px": num(p.get("entryPx")), "value": num(p.get("positionValue")),
                    "liq_px": num(p.get("liquidationPx")), "lev_type": str(lev.get("type") or ""),
                    "lev": num(lev.get("value")), "margin": num(p.get("marginUsed")),
                    "upnl": num(p.get("unrealizedPnl"))})
    ntl = sum(abs(x["value"] or 0.0) for x in pos)
    acct = {"ts": ts_ms, "addr": addr, "account_value": num(ms.get("accountValue")),
            "total_ntl": ntl, "n_pos": len(pos)}
    return acct, pos, ntl


def parse_ctx(ts_ms, doc):
    meta, ctxs = doc
    rows, live = [], []
    for u, c in zip(meta.get("universe", []), ctxs):
        name = str(u.get("name"))
        if not u.get("isDelisted"):
            live.append(name)
        rows.append({"ts": ts_ms, "coin": name, "mark": num(c.get("markPx")), "oracle": num(c.get("oraclePx")),
                     "mid": num(c.get("midPx")), "funding": num(c.get("funding")),
                     "oi": num(c.get("openInterest")), "day_ntl_vlm": num(c.get("dayNtlVlm")),
                     "premium": num(c.get("premium"))})
    return rows, live


# ---------------------------------------------------------------- storage

SCHEMAS = {
    "acct": [("ts", "int64"), ("addr", "string"), ("account_value", "float64"), ("total_ntl", "float64"),
             ("n_pos", "int32")],
    "pos": [("ts", "int64"), ("addr", "string"), ("coin", "string"), ("szi", "float64"), ("entry_px", "float64"),
            ("value", "float64"), ("liq_px", "float64"), ("lev_type", "string"), ("lev", "float64"),
            ("margin", "float64"), ("upnl", "float64")],
    "mids": [("ts", "int64"), ("coin", "string"), ("mid", "float64")],
    "ctx": [("ts", "int64"), ("coin", "string"), ("mark", "float64"), ("oracle", "float64"), ("mid", "float64"),
            ("funding", "float64"), ("oi", "float64"), ("day_ntl_vlm", "float64"), ("premium", "float64")],
}


class Store:
    def __init__(self, root):
        self.root = root
        self.buf = {k: [] for k in SCHEMAS}
        self.written = {k: 0 for k in SCHEMAS}

    def add(self, table, rows):
        self.buf[table].extend(rows)

    def flush(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        stamp = time.strftime("%H%M%S", time.gmtime())
        day = time.strftime("%Y-%m-%d", time.gmtime())
        for table, rows in self.buf.items():
            if not rows:
                continue
            schema = pa.schema([(n, getattr(pa, t)()) for n, t in SCHEMAS[table]])
            d = os.path.join(self.root, table, day)
            os.makedirs(d, exist_ok=True)
            path = os.path.join(d, f"{stamp}.parquet")
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), path + ".tmp", compression="zstd")
            os.replace(path + ".tmp", path)
            self.written[table] += len(rows)
            self.buf[table] = []


# ---------------------------------------------------------------- network

def http_json(url, payload=None, timeout=30):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json",
                                                          "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
    return json.loads(body)


class Collector:
    def __init__(self, out=OUT, rate=RATE_PER_MIN):
        self.out = out
        self.reg = Registry()
        self.reg_path = os.path.join(out, "registry.json.gz")
        self.store = Store(out)
        self.limiter = Limiter(rate)
        self.pool = ThreadPoolExecutor(WORKERS + 2)
        self.queue = deque()
        self.coins = []
        self.started = time.time()
        self.stop = asyncio.Event()
        self.n = {"polls": deque(), "errors": deque(), "trades": deque()}
        self.http429 = 0
        self.backlog = 0
        self.ws_up = False

    def tick(self, key, k=1):
        self.n[key].append((time.time(), k))

    def recent(self, key, window=300):
        q, cut = self.n[key], time.time() - window
        while q and q[0][0] < cut:
            q.popleft()
        return sum(k for _, k in q)

    async def call(self, url, payload=None, timeout=30):
        return await asyncio.get_running_loop().run_in_executor(self.pool, http_json, url, payload, timeout)

    async def every(self, seconds, fn, align=False):
        while not self.stop.is_set():
            if align:
                await asyncio.sleep(seconds - time.time() % seconds)
            try:
                await fn()
            except Exception as exc:  # keep the service alive; the status file shows errors
                self.tick("errors")
                log(f"{fn.__name__}: {type(exc).__name__}: {str(exc)[:200]}")
            if not align:
                try:
                    await asyncio.wait_for(self.stop.wait(), seconds)
                except asyncio.TimeoutError:
                    pass

    async def refresh_ctx(self):
        rows, live = parse_ctx(int(time.time() * 1000), await self.call(API, {"type": "metaAndAssetCtxs"}))
        self.store.add("ctx", rows)
        self.coins = sorted(live)

    async def refresh_mids(self):
        mids = await self.call(API, {"type": "allMids"})
        ts, live = int(time.time() * 1000), set(self.coins)
        self.store.add("mids", [{"ts": ts, "coin": c, "mid": num(v)} for c, v in mids.items() if c in live])

    async def refresh_leaderboard(self):
        doc = await self.call(LEADERBOARD, None, 120)
        now, k = time.time(), 0
        for row in doc.get("leaderboardRows", []):
            addr = str(row.get("ethAddress", "")).lower()
            if addr.startswith("0x"):
                self.reg.seed(addr, num(row.get("accountValue")) or 0.0, now)
                k += 1
        del doc
        log(f"leaderboard: {k} accounts, registry {len(self.reg.a)}")

    async def refill(self):
        due, self.backlog = self.reg.due(time.time(), int(self.limiter.rate()))
        self.queue = deque(due)

    async def flush(self):
        self.store.flush()
        dropped = self.reg.prune(time.time())
        self.reg.save(self.reg_path)
        status = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "up_s": round(time.time() - self.started), "addresses": len(self.reg.a),
                  "never_polled": sum(e["ntl"] is None for e in self.reg.a.values()),
                  "backlog_due": self.backlog, "polls_5m": self.recent("polls"),
                  "errors_5m": self.recent("errors"), "trades_5m": self.recent("trades"),
                  "http429_total": self.http429, "rate_per_min": self.limiter.rate(), "ws_up": self.ws_up,
                  "coins": len(self.coins), "rows_written": self.store.written, "pruned": dropped}
        with open(os.path.join(self.out, "status.json.tmp"), "w") as fh:
            json.dump(status, fh, indent=1)
        os.replace(os.path.join(self.out, "status.json.tmp"), os.path.join(self.out, "status.json"))

    async def worker(self):
        while not self.stop.is_set():
            if not self.queue:
                await asyncio.sleep(1.0)
                continue
            addr = self.queue.popleft()
            await self.limiter.wait()
            now = time.time()
            try:
                st = await self.call(API, {"type": "clearinghouseState", "user": addr}, 20)
            except urllib.error.HTTPError as exc:
                self.tick("errors")
                if exc.code == 429:
                    self.http429 += 1
                    self.limiter.penalize()
                    log("429 from Hyperliquid: rate halved for 10 minutes")
                else:
                    self.reg.failed(addr, now)
                continue
            except Exception:
                self.tick("errors")
                self.reg.failed(addr, now)
                continue
            try:
                acct, pos, ntl = parse_state(addr, int(now * 1000), st)
            except (AttributeError, TypeError):
                self.tick("errors")
                self.reg.failed(addr, now)
                continue
            self.store.add("acct", [acct])
            self.store.add("pos", pos)
            self.reg.polled(addr, now, ntl)
            self.tick("polls")

    async def trades(self):
        import websockets
        while not self.stop.is_set():
            if not self.coins:
                await asyncio.sleep(5)
                continue
            subscribed = list(self.coins)
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=2 ** 23) as ws:
                    for c in subscribed:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": c}}))
                    self.ws_up = True
                    log(f"trades stream: {len(subscribed)} coins")
                    while not self.stop.is_set() and set(self.coins) <= set(subscribed):
                        try:
                            msg = json.loads(await asyncio.wait_for(ws.recv(), 60))
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({"method": "ping"}))
                            continue
                        if msg.get("channel") != "trades":
                            continue
                        for t in msg.get("data") or []:
                            notional = (num(t.get("px")) or 0.0) * (num(t.get("sz")) or 0.0)
                            for u in t.get("users") or []:
                                self.reg.seen_trade(str(u).lower(), (t.get("time") or 0) / 1000.0, notional)
                        self.tick("trades", len(msg.get("data") or []))
            except Exception as exc:
                log(f"trades stream: {type(exc).__name__}: {str(exc)[:200]}")
            self.ws_up = False
            await asyncio.sleep(5)

    async def run(self):
        os.makedirs(self.out, exist_ok=True)
        self.reg.load(self.reg_path)
        loop = asyncio.get_running_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, self.stop.set)
        await self.refresh_ctx()
        log(f"start: {len(self.coins)} live perps, registry {len(self.reg.a)}, rate {self.limiter.rate():.0f}/min")
        tasks = [asyncio.create_task(x) for x in (
            self.every(CTX_S, self.refresh_ctx), self.every(MIDS_S, self.refresh_mids, align=True),
            self.every(LEADERBOARD_S, self.refresh_leaderboard), self.every(QUEUE_S, self.refill),
            self.every(FLUSH_S, self.flush, align=True), self.trades(),
            *[self.worker() for _ in range(WORKERS)])]
        await self.stop.wait()
        log("stopping: final flush")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.flush()
        self.pool.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(Collector().run())
