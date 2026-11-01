"""Forward paper test of round 16b on this host's own detection time (research only, public data, no orders).

Rules, fixed on 2026-11-01 before any forward event:
  trigger  a market added to Upbit's official market list, as logged by watch_upbit.py (watch/markets.jsonl,
           first-seen time t_seen on this host); its coin = the part after "KRW-" / "BTC-" / "USDT-". Markets of
           one coin added within 72 h count once (the first).
  perp     <COIN>USDT, else 1000<COIN>USDT, TRADING on Binance USDT-M, onboarded at least 2 days before t_seen
  paper    once t_seen + 3 min has passed, REST aggTrades of the perp over [t_seen - 60 s, t_seen + 120 s]:
           entry  at t_e = t_seen + one_way (half the median Binance REST round trip of the last hour): the
                  larger of the first trade at or after t_e and the VWAP of buyer-initiated trades in the next
                  second (bt_latency.trade's floor)
           exit   the first trade at or after t_seen + 60 s
           costs  taker 0.05% both sides plus the bt.slippage tier of the 30-day median daily quote volume
                  (Binance daily klines), twice at entry and once at exit (bt_latency.net)
           also   pre_move = price at t_seen vs 10 s earlier: how much of the jump happened before we saw it
  ledger   fwd_sniper/ledger.jsonl, one line per trigger, append-only; report.json with the count, mean,
           median and the compounded equity at s = 1 (each trade the whole equity)
Verdict after at least 8 weeks (from 2026-12-27): equity >= 1 and drawdown <= 35% at s = 1.

  python fwd_sniper.py          loop: read new watcher lines every 5 s, settle due triggers
  python fwd_sniper.py report
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
import urllib.request

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
WATCH = os.environ.get("WATCH", f"{HERE}/watch")
FWD = os.environ.get("FWD", f"{HERE}/fwd_sniper")
API = "https://fapi.binance.com"
DAY, MIN = 86_400_000, 60_000
TAKER = 0.0005
TIERS = ((1e9, 0.0002), (200e6, 0.0005), (50e6, 0.0010))
DEDUPE = 72 * 3_600_000
SETTLE = 3 * MIN


def get(path):
    return json.loads(urllib.request.urlopen(API + path, timeout=20).read())


def slip(med_qv):
    return next((s for th, s in TIERS if med_qv > th), 0.0025)


def coin_of(market):
    return market.split("-", 1)[1] if "-" in market else None


def perp_for(coin, info, t):
    """-> Binance symbol TRADING and onboarded >= 2 days before t, else None."""
    by = {s["symbol"]: s for s in info}
    for name in (f"{coin}USDT", f"1000{coin}USDT"):
        s = by.get(name)
        if s and s["status"] == "TRADING" and s["contractType"] == "PERPETUAL" and s.get("onboardDate", 0) <= t - 2 * DAY:
            return name
    return None


def agg_trades(sym, lo, hi):
    """All aggTrades in [lo, hi] -> (t, p, q, buy) arrays (paginates by id)."""
    rows = get(f"/fapi/v1/aggTrades?symbol={sym}&startTime={lo}&endTime={min(hi, lo + 3_599_000)}&limit=1000")
    out = list(rows)
    while rows and len(rows) == 1000 and rows[-1]["T"] < hi:
        rows = get(f"/fapi/v1/aggTrades?symbol={sym}&fromId={rows[-1]['a'] + 1}&limit=1000")
        out += [r for r in rows if r["T"] <= hi]
        time.sleep(0.2)
    t = np.array([r["T"] for r in out], np.int64)
    return t, np.array([float(r["p"]) for r in out]), np.array([float(r["q"]) for r in out]), \
        np.array([not r["m"] for r in out])


def paper(t, p, q, buy, t_seen, one_way):
    """-> dict with entry/exit/pre_move, or None when the trades do not cover the rule."""
    t_e = t_seen + one_way
    i = int(np.searchsorted(t, t_e))
    j = int(np.searchsorted(t, t_seen + 60_000))
    k = int(np.searchsorted(t, t_seen - 10_000))
    if i >= len(t) or t[i] > t_e + 10_000 or j >= len(t) or k == 0 or i == 0:
        return None
    sel = (t >= t_e) & (t < t_e + 1000) & buy
    vwap = float((p[sel] * q[sel]).sum() / q[sel].sum()) if sel.any() else 0.0
    before_seen = int(np.searchsorted(t, t_seen)) - 1
    return {"entry_t": int(t[i]), "entry": max(float(p[i]), vwap), "exit_t": int(t[j]), "exit": float(p[j]),
            "pre_move_pct": round((p[before_seen] / p[k - 1] - 1) * 100, 3)}


def net(entry, exit_, s, mult=1.0):
    return exit_ / entry - 1 - mult * (2 * TAKER + 3 * s)


def med_qvol(sym, t):
    k = get(f"/fapi/v1/klines?symbol={sym}&interval=1d&endTime={t - DAY}&limit=30")
    return statistics.median(float(x[7]) for x in k) if len(k) >= 20 else 0.0


class State:
    def __init__(self):
        os.makedirs(FWD, exist_ok=True)
        self.path = f"{FWD}/state.json"
        s = json.load(open(self.path)) if os.path.exists(self.path) else {}
        self.offset = s.get("offset", 0)
        self.base = set(s.get("base", []))
        self.pending = s.get("pending", [])
        self.last = s.get("last", {})
        self.rtt = s.get("rtt", [])

    def save(self):
        with open(self.path + ".tmp", "w") as fh:
            json.dump({"offset": self.offset, "base": sorted(self.base), "pending": self.pending,
                       "last": self.last, "rtt": self.rtt[-60:]}, fh)
        os.replace(self.path + ".tmp", self.path)


def read_new(st):
    """New watcher lines -> triggers appended to st.pending."""
    path = f"{WATCH}/markets.jsonl"
    if not os.path.exists(path):
        return
    with open(path, "rb") as fh:
        fh.seek(st.offset)
        for raw in fh:
            if not raw.endswith(b"\n"):
                break
            st.offset += len(raw)
            r = json.loads(raw)
            if r.get("baseline"):
                st.base |= set(r.get("markets") or r.get("added") or [])
                continue
            for m in r.get("added", []):
                coin = coin_of(m)
                if m in st.base or coin is None:
                    continue
                st.base.add(m)
                if r["t"] - st.last.get(coin, -10 ** 15) < DEDUPE:
                    continue
                st.last[coin] = r["t"]
                st.pending.append({"market": m, "coin": coin, "t_seen": r["t"]})


def settle(st, now):
    due = [e for e in st.pending if now >= e["t_seen"] + SETTLE]
    if not due:
        return
    info = get("/fapi/v1/exchangeInfo")["symbols"]
    one_way = int(statistics.median(st.rtt) / 2) if st.rtt else 130
    for e in due:
        st.pending.remove(e)
        row = {**e, "one_way_ms": one_way}
        sym = perp_for(e["coin"], info, e["t_seen"])
        row["perp"] = sym
        if sym:
            try:
                t, p, q, buy = agg_trades(sym, e["t_seen"] - 60_000, e["t_seen"] + 120_000)
                tr = paper(t, p, q, buy, e["t_seen"], one_way)
                if tr:
                    s = slip(med_qvol(sym, e["t_seen"]))
                    row.update(tr, slip=s, ret1=round(net(tr["entry"], tr["exit"], s), 6),
                               ret2=round(net(tr["entry"], tr["exit"], s, 2.0), 6))
            except Exception as exc:  # noqa: BLE001
                row["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        with open(f"{FWD}/ledger.jsonl", "a") as fh:
            fh.write(json.dumps(row) + "\n")
        report()


def report():
    path = f"{FWD}/ledger.jsonl"
    rows = [json.loads(x) for x in open(path)] if os.path.exists(path) else []
    tr = [r for r in rows if "ret1" in r]
    rets = [r["ret1"] for r in tr]
    eq = np.cumprod([1 + x for x in rets]) if rets else np.array([1.0])
    dd = float(np.max(1 - eq / np.maximum.accumulate(eq))) if rets else 0.0
    out = {"generated_ms": int(time.time() * 1000), "triggers": len(rows), "trades": len(tr),
           "mean_pct": round(float(np.mean(rets)) * 100, 3) if rets else None,
           "median_pct": round(float(np.median(rets)) * 100, 3) if rets else None,
           "equity_s1": round(float(eq[-1]), 4), "max_dd_pct": round(dd * 100, 2),
           "median_one_way_ms": statistics.median(r["one_way_ms"] for r in rows) if rows else None,
           "median_pre_move_pct": statistics.median(r["pre_move_pct"] for r in tr) if tr else None}
    with open(f"{FWD}/report.json", "w") as fh:
        json.dump(out, fh, indent=1)
    return out


def loop():
    st = State()
    last_rtt = 0
    while True:
        now = int(time.time() * 1000)
        try:
            if now - last_rtt >= MIN:
                t0 = time.monotonic()
                get("/fapi/v1/time")
                st.rtt.append(round((time.monotonic() - t0) * 1000, 1))
                st.rtt = st.rtt[-60:]
                last_rtt = now
            read_new(st)
            settle(st, now)
            st.save()
        except Exception as exc:  # noqa: BLE001
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {type(exc).__name__}: {str(exc)[:200]}", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        print(json.dumps(report(), indent=1))
    else:
        loop()
