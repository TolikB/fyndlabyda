"""Does a resting quote on MEXC's zero-fee contracts earn more than it loses to informed flow? (round 31, research only)

Pre-registered on 2026-11-06, before the development window opened; the collected rows have only been counted, and this
file was checked on synthetic data only (selftest_quote.py). No orders are placed anywhere.

Why: a maker fee of 0 turns the spread into income, and a 2,500 USDT account needs little depth. On venues with a fee the
spread is gone before it starts. The usual loss is adverse selection: a quote fills just before the price moves through it.
This test measures that on the host's own clock.

Data: universe.json; mx, the touch about every 280 ms; mt, every trade; bn, Binance's last price; rtt.

Rules, per contract, on the local clock. ONE_WAY is the day's median MEXC round trip / 2, and an order or a cancel sent at
local time t acts on trades this host receives from t + 2 x ONE_WAY.
  Flat: rest a buy at the bid and a sell at the ask of the latest snapshot. When the touch moves, move the quote.
  Fill, queue model (1x): the quote joins behind everything shown at its price. A taker's trade at that price eats the
  queue, one through it fills at once. Cancels ahead of us are ignored, which is pessimistic.
  Fill, through model (2x): only a trade strictly through the price fills, so every fill is one the market ran over.
  In position: rest the exit at the far touch, moved as the touch moves, with the same fill rule. After H seconds take the
  near touch at the first snapshot received from fill + H + 2 x ONE_WAY. Then rest 1 s.
  Filter (trials marked f): post no entry quote while Binance's last has moved 5 bp or more in the past second, and pull the
  ones standing. Contracts with no Binance perp are unfiltered.

Costs: 0/0. A position's return is exit / entry - 1, signed. The report also gives the mean less 2 bp on taker exits, for a
contract that moved to 0/2.
Book: at most 5 positions, each 0.2 x s of equity, s in {1, 2, 3}, as round 25.

Trials: 6 = H {10 s, 30 s, 120 s} x filter {off, on}.

Bar on development (2026-11-08 .. 2026-11-22 00:00 UTC): as round 25, with the through model as the 2x run.
Holdout: 2026-11-22 .. 2026-12-06 00:00 UTC, one look, only for trials that pass. A pass earns only a forward paper series.
Rows from before 2026-11-08 are a rehearsal of this code and are left out of both.

Known limits: resting orders change the book they rest in, and a 280 ms snapshot misses touches in between. Quotes that
never fill are free here, though MEXC may limit order rates. Clause 17(c) of its agreement bans spoofing and quote stuffing,
and a quote that moves with every tick must stay far from that; a pass has to be read against it.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_quote.py rehearsal   results_quote_rehearsal.json: code check only, no reading of the numbers
  python test_quote.py dev         results_quote_dev.json, /w/pnl_round31.parquet
  python test_quote.py holdout     results_quote_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import math
import multiprocessing as mp
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "xvenue"))
import test_lag as lag  # noqa: E402

DATA = os.environ.get("OUT", f"{HERE}/data")
DAY = lag.DAY
HOLDS = {"h10": 10_000, "h30": 30_000, "h120": 120_000}
FILTER_BP = 5.0
FILTER_MS = 1_000
REST_MS = 1_000
MODELS = ("queue", "through")
TRIALS = [f"{h}|{f}" for h in HOLDS for f in ("nofilter", "filter")]
PERIODS = {"rehearsal": ("2026-11-06", "2026-11-08"), "dev": ("2026-11-08", "2026-11-22"), "holdout": ("2026-11-22", "2026-12-06")}
WORKERS = 3


class Quote:
    """One resting order: live for trades received from `live` on, with `queue` contracts ahead of it."""

    __slots__ = ("price", "queue", "live")

    def __init__(self, price, queue, live):
        self.price, self.queue, self.live = price, queue, live


class Side:
    """The order on one side, and the replacement on its way."""

    def __init__(self, buy, model):
        self.buy, self.model = buy, model
        self.work = self.next = None

    def clear(self):
        self.work = self.next = None

    def want(self, price, queue, sent, lag_ms):
        """Move the quote to this price; the old one stands until the new one is live."""
        if self.next is not None:
            self.next.price, self.next.queue = price, queue
        elif self.work is None or self.work.price != price:
            self.next = Quote(price, queue, sent + lag_ms)

    def pull(self, sent, lag_ms):
        """Cancel: the order can still fill until the cancel arrives."""
        if self.work is not None and self.next is None:
            self.next = Quote(None, 0.0, sent + lag_ms)

    def roll(self, r):
        if self.next is not None and r >= self.next.live:
            self.work = None if self.next.price is None else self.next
            self.next = None

    def hit(self, p, v, taker_side, r):
        """A taker's trade: -> the fill price, or None."""
        self.roll(r)
        q = self.work
        if q is None or r < q.live or taker_side != (2 if self.buy else 1):
            return None
        through = p < q.price if self.buy else p > q.price
        if through:
            return q.price
        if self.model == "queue" and p == q.price:
            q.queue -= v
            if q.queue < 0:
                return q.price
        return None


def events(mx, mt):
    """-> time-sorted arrays: kind (0 snapshot, 1 trade), r, a, b, c, d.

    Snapshot: bid, ask, bid_v, ask_v. Trade: p, v, side, 0. On equal times the snapshot goes first."""
    s = np.column_stack([np.zeros(len(mx)), mx["r_last"], mx["bid"], mx["ask"], mx["bid_v"], mx["ask_v"]])
    t = np.column_stack([np.ones(len(mt)), mt["r"], mt["p"], mt["v"], mt["side"], np.zeros(len(mt))])
    x = np.vstack([s, t])
    return x[np.lexsort((x[:, 0], x[:, 1]))]


def toxic(r, bn_r, bn_last):
    """True when Binance's last, as received by local time r, moved FILTER_BP or more over the past FILTER_MS."""
    i = int(np.searchsorted(bn_r, r, "right")) - 1
    if i < 0:
        return False
    j = int(np.searchsorted(bn_r, r - FILTER_MS, "right")) - 1
    if j < 0:
        return False
    return abs(bn_last[i] / bn_last[j] - 1) * 1e4 >= FILTER_BP


def simulate(ev, one_way, H, model, bn_r=None, bn_last=None):
    """ev: events(). -> trades: entry and exit times, side (1 long), entry and exit prices, whether the exit took the book."""
    delay = 2 * one_way
    buy, sell = Side(True, model), Side(False, model)
    pos, entry, t_in, wait = 0, 0.0, 0, 0
    leaving = None
    out = []
    for kind, r, a, b, c, d in ev:
        r = int(r)
        if kind == 0:
            bid, ask, bid_v, ask_v = a, b, c, d
            if pos == 0:
                if r < wait:
                    continue
                if bn_r is not None and toxic(r, bn_r, bn_last):
                    buy.pull(r, delay)
                    sell.pull(r, delay)
                    continue
                buy.want(bid, bid_v, r, delay)
                sell.want(ask, ask_v, r, delay)
            elif leaving is not None and r >= leaving:
                out.append({"t_in": t_in, "t_out": r, "side": pos, "entry": entry, "exit": bid if pos > 0 else ask, "taker": True})
                pos, leaving, wait = 0, None, r + REST_MS
                buy.clear()
                sell.clear()
            elif r >= t_in + H:
                leaving = t_in + H + delay
                (sell if pos > 0 else buy).pull(r, delay)
            elif pos > 0:
                sell.want(ask, ask_v, r, delay)
            else:
                buy.want(bid, bid_v, r, delay)
            buy.roll(r)
            sell.roll(r)
            continue
        p, v, taker_side = a, b, int(c)
        if pos == 0:
            if r < wait:
                continue
            got = buy.hit(p, v, taker_side, r)
            if got is not None:
                pos, entry, t_in = 1, got, r
                buy.clear()
                sell.clear()
                continue
            got = sell.hit(p, v, taker_side, r)
            if got is not None:
                pos, entry, t_in = -1, got, r
                buy.clear()
                sell.clear()
        else:
            got = (sell if pos > 0 else buy).hit(p, v, taker_side, r)
            if got is not None:
                out.append({"t_in": t_in, "t_out": r, "side": pos, "entry": entry, "exit": got, "taker": False})
                pos, leaving, wait = 0, None, r + REST_MS
                buy.clear()
                sell.clear()
    return out


def contract_day(job):
    sym, mx, mt, bn, one_way, d0 = job
    ev = events(mx, mt)
    rows = []
    for key in TRIALS:
        hold, flt = key.split("|")
        use = flt == "filter" and bn is not None and len(bn)
        for model in MODELS:
            for x in simulate(ev, one_way, HOLDS[hold], model, *((bn["r_last"].to_numpy(np.int64), bn["last"].to_numpy(float)) if use else (None, None))):
                rows.append({"trial": key, "model": model, "coin": sym, "t_sig": x["t_in"], "exit_t": x["t_out"], "taker": x["taker"],
                             "ret": x["side"] * (x["exit"] / x["entry"] - 1)})
    return rows


def run(period):
    u = json.load(open(f"{HERE}/universe.json"))
    lo, hi = (lag.ms(x) for x in PERIODS[period])
    rtt = lag.read(DATA, "rtt", lo, hi)
    rows = []
    with mp.Pool(WORKERS) as pool:
        for d0 in range(lo, hi, DAY):
            mx, mt, bn = lag.read(DATA, "mx", d0, d0 + DAY), lag.read(DATA, "mt", d0, d0 + DAY), lag.read(DATA, "bn", d0, d0 + DAY)
            if mx.empty or mt.empty:
                continue
            rt = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "mexc")]["ms"]
            one_way = int(round(rt.median() / 2)) if len(rt) else 150
            mx = mx.sort_values("r_last")
            mt = mt.sort_values("r")
            jobs = []
            for sym, x in u.items():
                a, b = mx[mx["sym"] == sym], mt[mt["sym"] == sym]
                if a.empty or b.empty:
                    continue
                c = bn[bn["sym"] == x["binance"]].sort_values("r_last") if x["binance"] else None
                jobs.append((sym, a, b, c, one_way, d0))
            for part in pool.imap_unordered(contract_day, jobs):
                rows.extend(part)
            print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d} {len(rows)} trades, one way {one_way} ms", flush=True)
    return pd.DataFrame(rows)


def evaluate(trades, period):
    lo, hi = PERIODS[period]
    days = pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1))
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        r = {}
        for mult, model in (("1x", "queue"), ("2x", "through")):
            m = g[g["model"] == model]
            if m.empty:
                r[mult] = {"trades": 0, "mean_bp": 0.0, "t_hours": 0.0, "sizes": {str(s): {"monthly_figure_pct": 0.0, "max_dd_pct": 0.0} for s in lag.SIZES}}
                continue
            acc, _ = lag.book(m, "ret", 1)
            hourly = acc.groupby(acc["t_sig"] // 3_600_000)["ret"].sum()
            t = float(hourly.mean() / hourly.std() * math.sqrt(len(hourly))) if len(hourly) > 2 and hourly.std() > 0 else 0.0
            r[mult] = {"trades": int(len(acc)), "mean_bp": round(float(acc["ret"].mean() * 1e4), 3),
                       "median_bp": round(float(acc["ret"].median() * 1e4), 3), "t_hours": round(t, 2),
                       "win_pct": round(float((acc["ret"] > 0).mean() * 100), 1), "taker_exit_pct": round(float(acc["taker"].mean() * 100), 1),
                       "mean_bp_at_0_2": round(float((acc["ret"] - 2e-4 * acc["taker"]).mean() * 1e4), 3),
                       "coins": int(acc["coin"].nunique()), "sizes": {}}
            for s in lag.SIZES:
                _, daily = lag.book(m, "ret", s)
                daily = daily.reindex(days, fill_value=0.0)
                r[mult]["sizes"][str(s)] = {"monthly_figure_pct": round(float(daily.mean() * 30 * 100), 2),
                                            "max_dd_pct": round(lag.drawdown(daily) * 100, 2)}
                if mult == "1x" and s == 1:
                    pnl[f"quote|{key}"] = daily
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def main():
    mode = sys.argv[1]
    path = f"{HERE}/results_quote_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_quote_dev.json"))
        if not any(v["passes"] for v in dev.values()):
            raise SystemExit("no trial passed development: the holdout stays closed")
    trades = run(mode)
    if dev is not None:
        trades = trades[trades["trial"].isin([k for k, v in dev.items() if v["passes"]])]
    out, pnl = evaluate(trades, mode)
    for key in out:
        out[key]["passes"], out[key]["size"] = lag.passes(out[key], None if dev is None else dev[key]["size"])
    if mode == "dev":
        pnl.to_parquet("/w/pnl_round31.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    if mode != "rehearsal":
        print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
