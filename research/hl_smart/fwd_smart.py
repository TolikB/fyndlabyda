"""Forward paper test: follow Hyperliquid's best perp traders (round 13 follow-up, research only)

Pre-registered on 2026-11-01, after the round 13 gate passed (results_persist.json: top decile
+4.86 pp a month over the median, CI 1.02..9.88) and before any position or price of the forward
period was looked at. Public read-only data; nothing here signs, sends or places an order.

Why forward: the round 13 sample holds accounts that are worth >= $1M TODAY. diag_persist.txt shows
the edge at +5.6 pp before 2026-05 but +1.4 pp over the last 6 months, where that survivorship
selection is weakest. Every set below is frozen when it is ranked, so nothing here can survive by
hindsight.

Ranking (rank/<date>/), every Wednesday from 01:00 UTC; the first one, 2026-11-01, reuses the round
13 download (2026-10-31 20:40 .. 11-01 00:40 UTC)
  candidates  the live leaderboard's accounts >= $1M, plus every member of the previous 5 rankings'
              sets (so that accounts which shrink or blow up are still measured); one `portfolio`
              call each, 4 s apart
  eligible    >= $1M in that leaderboard, not among its 10 largest, month volume <= 200x account
              value, perpAllTime PnL not zero throughout, and a finite past ROI
  past ROI    the round 13 formula on the finer 30-day series: perpMonth PnL change from its first to
              its last point over the mean of the month account value at those points (both >= $100k),
              scaled to 30 days by the span (>= 10 days), clipped to [-1, +1]
  smart set   the top decile of eligible accounts by past ROI (at least 50 eligible)
  control set the same number drawn at random from the other eligible accounts (seed = YYYYMMDD)
  A ranking is polled from the moment its download finishes (with the previous sets for 2 hours) and
  rules the books from fills at least 30 minutes later; until then the previous sets stay in force.
  A Wednesday that ends before its download finishes leaves the previous sets in force for that week.

A. Persistence, measured forward. Each later ranking within 29.5 days re-reads every set member's
   perp PnL since its ranking time (perpMonth, the observation nearest within 7 days), over the mean
   account value, scaled to 30 days, clipped, no $100k floor. The report uses the latest re-read and
   counts a ranking as complete at >= 21 days. Statistic: mean smart minus mean control outcome.

B. Copy ledgers, one per set (ledger_smart.jsonl, ledger_control.jsonl)
  positions   clearinghouseState of every member of the sets in force, every 15 minutes on the clock
              (pos/<date>.jsonl)
  decision    daily, from each member's latest poll at or before 12:05 UTC and no older than 1 hour:
                score(coin) = mean over those members of signed position notional / account value
                (0 for a member without the coin; members with account value <= 0 are left out)
              coins: Hyperliquid perps whose Binance USDT-M perpetual is TRADING (kPEPE -> 1000PEPEUSDT,
              BTC -> BTCUSDT, else 1000<coin>USDT) and that at least 3 of those members hold
              book: the 5 coins with the largest |score|, side = sign(score), 20% of equity each
              (gross 1x); fewer qualifying coins leave the rest in cash. Fewer than half the members
              fresh: no rebalance that day, the book is held and marked.
  fill        Binance 1-minute open at 12:05 UTC. A held symbol without that bar is closed at its last
              1-minute close with a 2% penalty (bt.DELIST_PENALTY); a symbol without that bar cannot be
              chosen that day, and the next coin by |score| takes its slot.
  P&L         from fill to fill: sum of weight x price change, minus weight x the Binance funding rates
              settled in between, minus |new - old weight| x (0.05% taker + bt.slippage tier of the
              20-day median daily quote volume: 2/5/10/25 bp). Equity compounds from 1.0.
  A missed day is not replayed: the book is held and marked at the next fill.

Verdict, on or after 2026-12-27 (8 weeks of ledger)
  - the smart ledger at 1x: return >= 0 and max drawdown <= 35%;
  - the smart ledger beats the control ledger;
  - reported, not judged: the monthly return, the gross leverage that would have made 5% a month and
    the drawdown at that leverage, and statistic A with its count of complete rankings.
There is no development backtest of the copy rule (no position history exists), so the plan's
"nothing outside the development range" check cannot apply. Eight weeks is a gate for more forward
time, not proof. Real money is the user's decision.

  python fwd_smart.py seed-rank      the 2026-11-01 ranking from the round 13 download
  python fwd_smart.py loop           polls, rankings and fills (container hl-smart-fwd)
  python fwd_smart.py dry-trade      today's books from the latest polls, nothing written
  python fwd_smart.py report         report.json
"""

from __future__ import annotations

import glob
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.request

import numpy as np

import test_persist as tp

HERE = os.path.dirname(os.path.abspath(__file__))
FWD = os.environ.get("FWD", f"{HERE}/fwd")
SEED_SRC = "/w/data/hl_portfolio"
SEED_DATE = "2026-11-01"
INFO = "https://api.hyperliquid.xyz/info"
BOARD = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
FAPI = "https://fapi.binance.com"
DAY, HOUR, MINUTE = 86_400_000, 3_600_000, 60_000
RANK_WEEKDAY, RANK_HOUR = 2, 1            # Wednesday, 01:00 UTC
POLL_MS = 15 * MINUTE
FILL_OFFSET = 12 * HOUR + 5 * MINUTE      # 12:05 UTC
FRESH_MS = HOUR
MIN_FRESH_SHARE = 0.5
MIN_HOLDERS, SLOTS, SLOT_W = 3, 5, 0.2
PRIOR_SETS = 5
EFFECT_DELAY, OVERLAP_MS = 30 * MINUTE, 2 * HOUR
OUTCOME_MAX_DAYS, OUTCOME_DONE_DAYS = 29.5, 21
PAUSE = 4.0
FEE, DELIST_PENALTY = 0.0005, 0.02
SLIP_TIERS = [(1e9, 0.0002), (200e6, 0.0005), (50e6, 0.0010)]
SLIP_DEFAULT = 0.0025
SETS = ("smart", "control")


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), msg, flush=True)


def utc_date(ms):
    return time.strftime("%Y-%m-%d", time.gmtime(ms / 1000))


def day_start(ms):
    return ms // DAY * DAY


# ---------------------------------------------------------------- pure rules

def binance_symbol(coin, listed):
    """Hyperliquid coin -> Binance USDT-M perpetual name, or None when Binance has no such perp."""
    if coin.startswith("k") and coin[1:].isupper():
        name = f"1000{coin[1:]}USDT"
        return name if name in listed else None
    for name in (f"{coin}USDT", f"1000{coin}USDT"):
        if name in listed:
            return name
    return None


def slip(med_qvol):
    return next((s for th, s in SLIP_TIERS if med_qvol > th), SLIP_DEFAULT)


def month_roi(reply):
    """Past 30-day perp ROI from one `portfolio` reply (round 13 formula on perpMonth), or nan."""
    P = tp.series(reply["perpMonth"]["pnlHistory"])
    V = tp.series(reply["month"]["accountValueHistory"])
    if len(P[0]) < 2:
        return np.nan
    t0, t1 = P[0][0], P[0][-1]
    span = t1 - t0
    v0, v1 = tp.at(*V, t0), tp.at(*V, t1)
    if span < tp.MIN_SPAN or not np.all(np.isfinite([v0, v1])) or v0 < tp.MIN_V or v1 < tp.MIN_V:
        return np.nan
    return float(np.clip((P[1][-1] - P[1][0]) / ((v0 + v1) / 2) * tp.WINDOW / span, -1, 1))


def outcome_roi(reply, t_rank):
    """Perp ROI since t_rank from a later `portfolio` reply, scaled to 30 days, or nan."""
    P = tp.series(reply["perpMonth"]["pnlHistory"])
    V = tp.series(reply["month"]["accountValueHistory"])
    i = tp.snap(P[0], t_rank)
    if i is None or len(P[0]) < 2:
        return np.nan
    span = P[0][-1] - P[0][i]
    v0, v1 = tp.at(*V, P[0][i]), tp.at(*V, P[0][-1])
    if span < tp.MIN_SPAN or not np.all(np.isfinite([v0, v1])) or (v0 + v1) <= 0:
        return np.nan
    return float(np.clip((P[1][-1] - P[1][i]) / ((v0 + v1) / 2) * tp.WINDOW / span, -1, 1))


def eligible_rois(board_rows, replies):
    """{addr: past ROI} over the eligible accounts, and a filter log."""
    rows = sorted(board_rows, key=lambda r: -float(r["accountValue"]))
    big = [r for r in rows if float(r["accountValue"]) >= 1e6]
    log_ = {"board_1m": len(big), "replies": 0, "top_dropped": 0, "market_makers": 0, "zero_perp": 0, "no_roi": 0}
    out = {}
    for n, r in enumerate(big):
        a = r["ethAddress"]
        if a not in replies:
            continue
        log_["replies"] += 1
        if n < tp.TOP_DROP:
            log_["top_dropped"] += 1
            continue
        if float(dict(r["windowPerformances"])["month"]["vlm"]) > tp.MM_TURNOVER * float(r["accountValue"]):
            log_["market_makers"] += 1
            continue
        d = replies[a]
        p = tp.series(d["perpAllTime"]["pnlHistory"])[1]
        if len(p) == 0 or not np.any(p != 0):
            log_["zero_perp"] += 1
            continue
        x = month_roi(d)
        if not np.isfinite(x):
            log_["no_roi"] += 1
            continue
        out[a] = x
    return out, log_


def pick_sets(rois, date):
    """Top decile by past ROI, and an equal-size random control from the rest (seed = YYYYMMDD)."""
    if len(rois) < tp.MIN_PER_DATE:
        return None
    addrs = sorted(rois)
    x = np.array([rois[a] for a in addrs])
    smart = [a for a, v in zip(addrs, x) if v >= np.quantile(x, 0.9)]
    rest = [a for a in addrs if a not in set(smart)]
    rng = np.random.default_rng(int(date.replace("-", "")))
    control = sorted(rng.choice(rest, size=min(len(smart), len(rest)), replace=False).tolist())
    return {"smart": sorted(smart), "control": control}


def latest_obs(rows, addrs, t):
    """Each address's latest poll at or before t and no older than FRESH_MS."""
    want, best = set(addrs), {}
    for r in rows:
        a = r["a"]
        if a in want and t - FRESH_MS <= r["t"] <= t and (a not in best or r["t"] > best[a]["t"]):
            best[a] = r
    return best


def scores(obs, listed):
    """{symbol: (score, holders)} from fresh observations; coins without a Binance perp are dropped."""
    usable = [r for r in obs.values() if r["av"] and r["av"] > 0]
    tot, holders = {}, {}
    for r in usable:
        for coin, szi, value in r["p"]:
            sym = binance_symbol(coin, listed)
            if sym is None or not szi:
                continue
            tot[sym] = tot.get(sym, 0.0) + np.sign(szi) * abs(value) / r["av"]
            holders[sym] = holders.get(sym, 0) + 1
    n = len(usable)
    return {s: (tot[s] / n, holders[s]) for s in tot} if n else {}


def target_book(sc, banned=()):
    """The SLOTS coins with the largest |score| among those with >= MIN_HOLDERS holders, 20% each."""
    ok = [(s, v) for s, (v, h) in sc.items() if h >= MIN_HOLDERS and v != 0 and s not in banned]
    ok.sort(key=lambda kv: (-abs(kv[1]), kv[0]))
    return {s: float(np.sign(v)) * SLOT_W for s, v in ok[:SLOTS]}


def step(old, px_prev, px_now, funding, new, slips, dead=()):
    """P&L of one fill-to-fill period. old/new: {sym: weight}; funding: {sym: sum of rates}."""
    gross = sum(w * (px_now[s] / px_prev[s] - 1) for s, w in old.items())
    gross -= sum(abs(w) * DELIST_PENALTY for s, w in old.items() if s in dead)
    fund = -sum(w * funding.get(s, 0.0) for s, w in old.items())
    syms = set(old) | set(new)
    costs = sum(abs(new.get(s, 0.0) - old.get(s, 0.0)) * (FEE + slips.get(s, SLIP_DEFAULT)) for s in syms)
    return gross, fund, costs, gross + fund - costs


# ---------------------------------------------------------------- I/O

def http_json(url, payload=None, timeout=30):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def hl(payload, tries=6):
    for attempt in range(tries):
        try:
            return http_json(INFO, payload, 60)
        except urllib.error.HTTPError as exc:
            time.sleep(60 if exc.code == 429 else 5 + 5 * attempt)
        except Exception:  # noqa: BLE001
            time.sleep(5 + 5 * attempt)
    raise RuntimeError(f"gave up on {payload}")


def fapi(path):
    for attempt in range(5):
        try:
            return http_json(FAPI + path, None, 30)
        except Exception:  # noqa: BLE001
            time.sleep(5 + 5 * attempt)
    raise RuntimeError(f"gave up on {path}")


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as fh:
        json.dump(obj, fh, indent=1, sort_keys=True)
    os.replace(path + ".tmp", path)


def read_gz(path):
    return dict(json.load(gzip.open(path)))


def ranks():
    return sorted(os.path.basename(os.path.dirname(p)) for p in glob.glob(f"{FWD}/rank/*/sets.json"))


def load_sets(date):
    return json.load(open(f"{FWD}/rank/{date}/sets.json"))


def finite_or_none(v):
    return float(v) if v is not None and np.isfinite(v) else None


def finish_rank(date, board_rows, replies, t_rank):
    rois, flog = eligible_rois(board_rows, replies)
    sets = pick_sets(rois, date)
    doc = {"date": date, "t": t_rank, "effective_t": int(time.time() * 1000), "filters": flog,
           "eligible": len(rois), "past_roi": rois,
           "sets": sets, "t_member": {a: float(replies[a]["perpMonth"]["pnlHistory"][-1][0])
                                       for s in (sets or {}).values() for a in s}}
    outs = {}
    for prev in ranks():
        p = load_sets(prev)
        days = (t_rank - p["t"]) / DAY
        if p["sets"] and 0 < days < OUTCOME_MAX_DAYS:
            outs[prev] = {"days": round(days, 2)}
            for k in SETS:
                outs[prev][k] = {a: (finite_or_none(outcome_roi(replies[a], p["t_member"][a])) if a in replies
                                     else None) for a in p["sets"][k]}
    doc["outcomes"] = outs
    write_json(f"{FWD}/rank/{date}/sets.json", doc)
    log(f"ranking {date}: eligible {len(rois)}, smart {len((sets or {}).get('smart', []))}, filters {flog}")
    return doc


def seed_rank():
    board = json.load(gzip.open(f"{SEED_SRC}/leaderboard.json.gz"))["leaderboardRows"]
    replies = {os.path.basename(f)[:-8]: read_gz(f) for f in glob.glob(f"{SEED_SRC}/portfolio/*.json.gz")}
    t_rank = float(np.median([d["perpMonth"]["pnlHistory"][-1][0] for d in replies.values()
                              if d["perpMonth"]["pnlHistory"]]))
    return finish_rank(SEED_DATE, board, replies, t_rank)


class Ranker:
    """One Wednesday ranking, done one `portfolio` call at a time so that polls keep their schedule."""

    def __init__(self, date):
        self.date, self.dir = date, f"{FWD}/rank/{date}"
        os.makedirs(f"{self.dir}/portfolio", exist_ok=True)
        cand = f"{self.dir}/candidates.json"
        if not os.path.exists(cand):
            board = http_json(BOARD, None, 120)
            with gzip.open(f"{self.dir}/leaderboard.json.gz", "wt") as fh:
                json.dump(board, fh)
            big = [r["ethAddress"] for r in board["leaderboardRows"] if float(r["accountValue"]) >= 1e6]
            prior = [a for d in ranks()[-PRIOR_SETS:] for k in SETS for a in (load_sets(d)["sets"] or {}).get(k, [])]
            write_json(cand, sorted(set(big) | set(prior)))
        self.todo = [a for a in json.load(open(cand)) if not os.path.exists(f"{self.dir}/portfolio/{a}.json.gz")]
        self.next_at = 0.0

    def work(self, now_s):
        """Fetch one reply when the pacing allows; finish the ranking when none is left. True when done."""
        if self.todo and now_s >= self.next_at:
            a = self.todo.pop(0)
            try:
                raw = hl({"type": "portfolio", "user": a})
                with gzip.open(f"{self.dir}/portfolio/{a}.json.gz.tmp", "wt") as fh:
                    json.dump(raw, fh)
                os.replace(f"{self.dir}/portfolio/{a}.json.gz.tmp", f"{self.dir}/portfolio/{a}.json.gz")
            except RuntimeError as exc:
                log(f"ranking {self.date}: {exc}")
            self.next_at = now_s + PAUSE
        if self.todo:
            return False
        board = json.load(gzip.open(f"{self.dir}/leaderboard.json.gz"))["leaderboardRows"]
        replies = {os.path.basename(f)[:-8]: read_gz(f) for f in glob.glob(f"{self.dir}/portfolio/*.json.gz")}
        t_rank = float(np.median([d["perpMonth"]["pnlHistory"][-1][0] for d in replies.values()
                                  if d["perpMonth"]["pnlHistory"]]))
        finish_rank(self.date, board, replies, t_rank)
        return True


def members(now_ms):
    """Addresses to poll: the newest sets, plus the previous ones for OVERLAP_MS after a switch."""
    r = ranks()
    keep = r[-1:] + (r[-2:-1] if r and now_ms - load_sets(r[-1])["effective_t"] < OVERLAP_MS else [])
    return sorted({a for d in keep for k in SETS for a in (load_sets(d)["sets"] or {}).get(k, [])})


def sets_in_force(fill_t):
    r = [d for d in ranks() if load_sets(d)["effective_t"] <= fill_t - EFFECT_DELAY]
    return (r[-1], load_sets(r[-1])["sets"] or {}) if r else (None, {})


def poll_once(addrs):
    rows = []
    for a in addrs:
        try:
            st = hl({"type": "clearinghouseState", "user": a}, tries=3)
        except RuntimeError:
            continue
        pos = [[p["position"]["coin"], float(p["position"]["szi"]), float(p["position"]["positionValue"])]
               for p in st.get("assetPositions") or [] if float(p["position"]["szi"]) != 0]
        rows.append({"t": int(st.get("time") or time.time() * 1000), "a": a,
                     "av": float((st.get("marginSummary") or {}).get("accountValue") or 0.0), "p": pos})
        time.sleep(0.25)
    if rows:
        path = f"{FWD}/pos/{utc_date(rows[0]['t'])}.jsonl"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as fh:
            fh.write("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows))
    return len(rows)


def read_polls(day_ms):
    out = []
    for d in (utc_date(day_ms - DAY), utc_date(day_ms)):
        p = f"{FWD}/pos/{d}.jsonl"
        if os.path.exists(p):
            out += [json.loads(line) for line in open(p) if line.strip()]
    return out


def listed_perps():
    info = fapi("/fapi/v1/exchangeInfo")
    return {s["symbol"] for s in info["symbols"] if s["contractType"] == "PERPETUAL"
            and s["quoteAsset"] == "USDT" and s["status"] == "TRADING"}


def minute_open(sym, t):
    k = fapi(f"/fapi/v1/klines?symbol={sym}&interval=1m&startTime={t}&limit=1")
    return float(k[0][1]) if k and int(k[0][0]) == t else None


def last_close(sym, t):
    k = fapi(f"/fapi/v1/klines?symbol={sym}&interval=1m&endTime={t}&limit=1")
    return float(k[-1][4]) if k else None


def funding_sum(sym, t0, t1):
    rows = fapi(f"/fapi/v1/fundingRate?symbol={sym}&startTime={t0 + 1}&endTime={t1}&limit=1000")
    return sum(float(r["fundingRate"]) for r in rows if t0 < int(r["fundingTime"]) <= t1)


def med_qvol(sym):
    k = fapi(f"/fapi/v1/klines?symbol={sym}&interval=1d&limit=21")[:-1]
    return float(np.median([float(b[7]) for b in k])) if k else 0.0


def ledger_path(name):
    return f"{FWD}/ledger_{name}.jsonl"


def last_entry(name):
    p = ledger_path(name)
    if not os.path.exists(p):
        return None
    lines = [line for line in open(p) if line.strip()]
    return json.loads(lines[-1]) if lines else None


def priced_book(sc, fill_t, px, dead):
    """target_book, skipping coins without a 1-minute bar at the fill (they join dead)."""
    while True:
        new = target_book(sc, banned=dead)
        for s in new:
            if s not in px:
                px[s] = minute_open(s, fill_t)
        missing = [s for s in new if px[s] is None]
        if not missing:
            return new
        dead.update(missing)


def trade(fill_t, write=True, rank=None):
    rank, sets = (rank, load_sets(rank)["sets"] or {}) if rank else sets_in_force(fill_t)
    if rank is None:
        return []
    polls, listed = read_polls(day_start(fill_t)), listed_perps()
    lines = []
    for name in SETS:
        prev = last_entry(name)
        if prev and prev["fill_t"] >= fill_t:
            continue
        old = prev["book"] if prev else {}
        obs = latest_obs(polls, sets.get(name, []), fill_t)
        sc = scores(obs, listed)
        fresh = len(obs) >= MIN_FRESH_SHARE * max(len(sets.get(name, [])), 1)
        px, dead = {}, set()
        for s in old:
            px[s] = minute_open(s, fill_t)
            if px[s] is None:
                dead.add(s)
                px[s] = last_close(s, fill_t) or prev["px"][s]
        if fresh:
            new = priced_book(sc, fill_t, px, dead)
        else:
            new = {s: w for s, w in old.items() if s not in dead}
        funding = {s: funding_sum(s, prev["fill_t"], fill_t) for s in old} if prev else {}
        slips = {s: slip(med_qvol(s)) for s in set(old) | set(new)}
        gross, fund, costs, pnl = step(old, prev["px"] if prev else {}, px, funding, new, slips, dead & set(old))
        equity = (prev["equity"] if prev else 1.0) * (1 + pnl)
        line = {"date": utc_date(fill_t), "fill_t": fill_t, "set": name, "rank": rank,
                "members": len(sets.get(name, [])), "fresh": len(obs), "rebalanced": bool(fresh),
                "book": new, "px": {s: px[s] for s in new},
                "scores": {s: [round(v, 5), h] for s, (v, h) in sorted(sc.items(), key=lambda kv: -abs(kv[1][0]))[:12]},
                "gross": gross, "funding": fund, "costs": costs, "pnl": pnl, "equity": equity, "dead": sorted(dead)}
        lines.append(line)
        if write:
            with open(ledger_path(name), "a") as fh:
                fh.write(json.dumps(line, separators=(",", ":")) + "\n")
            log(f"{name} {line['date']}: pnl {pnl * 100:+.3f}% equity {equity:.4f} book {new}")
    return lines


def report():
    out = {"generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "ledgers": {}, "persistence": {}}
    for name in SETS:
        p = ledger_path(name)
        rows = [json.loads(line) for line in open(p)] if os.path.exists(p) else []
        if not rows:
            continue
        eq = np.array([r["equity"] for r in rows])
        peak = np.maximum.accumulate(np.r_[1.0, eq])[1:]
        days = (rows[-1]["fill_t"] - rows[0]["fill_t"]) / DAY
        pnl = np.array([r["pnl"] for r in rows[1:]])
        out["ledgers"][name] = {"first": rows[0]["date"], "last": rows[-1]["date"], "fills": len(rows),
                                "return_pct": round((eq[-1] - 1) * 100, 3),
                                "monthly_pct": round(((eq[-1]) ** (30.4 / days) - 1) * 100, 3) if days > 0 else None,
                                "max_dd_pct": round(float((1 - eq / peak).max()) * 100, 3),
                                "daily_sd_pct": round(float(pnl.std()) * 100, 3) if len(pnl) > 1 else None}
    latest = {}
    for d in ranks():
        for prev, o in (load_sets(d).get("outcomes") or {}).items():
            latest[prev] = o
    for prev, o in latest.items():
        sm = [v for v in o["smart"].values() if v is not None]
        co = [v for v in o["control"].values() if v is not None]
        out["persistence"][prev] = {"days": o["days"], "complete": o["days"] >= OUTCOME_DONE_DAYS,
                                    "smart_mean_pp": round(float(np.mean(sm)) * 100, 2) if sm else None,
                                    "control_mean_pp": round(float(np.mean(co)) * 100, 2) if co else None,
                                    "smart_n": len(sm), "control_n": len(co),
                                    "missing": len(o["smart"]) + len(o["control"]) - len(sm) - len(co)}
    write_json(f"{FWD}/report.json", out)
    print(json.dumps(out, indent=1))


def loop():
    last_slot, ranker, tried = None, None, 0
    while True:
        now_ms = int(time.time() * 1000)
        try:
            slot = now_ms // POLL_MS
            today = day_start(now_ms)
            fill_t = today + FILL_OFFSET
            if slot != last_slot:
                last_slot = slot
                log(f"poll: {poll_once(members(now_ms))} accounts")
                continue
            if now_ms >= fill_t + 2 * MINUTE and now_ms - tried > 10 * MINUTE and any(
                    (last_entry(k) or {"fill_t": 0})["fill_t"] < fill_t for k in SETS):
                tried = now_ms
                trade(fill_t)
                report()
                continue
            date = utc_date(now_ms)
            wd = time.gmtime(now_ms / 1000).tm_wday
            if wd == RANK_WEEKDAY and now_ms >= today + RANK_HOUR * HOUR and date not in ranks():
                ranker = ranker if ranker and ranker.date == date else Ranker(date)
                if ranker.work(time.time()):
                    ranker = None
                continue
        except Exception as exc:  # noqa: BLE001
            log(f"error: {exc!r}")
            time.sleep(30)
        time.sleep(1)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "loop"
    if cmd == "seed-rank":
        seed_rank()
    elif cmd == "dry-trade":
        for ln in trade(int(time.time() * 1000) // MINUTE * MINUTE - 2 * MINUTE, write=False, rank=ranks()[-1]):
            print(json.dumps({k: ln[k] for k in ("set", "members", "fresh", "rebalanced", "book", "scores")}))
    elif cmd == "report":
        report()
    else:
        loop()
