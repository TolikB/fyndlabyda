"""Round 14: do Hyperliquid liquidation clusters pull the price? (research only)

Implements the test pre-registered in README.md on 2026-10-31, before any data existed. This file was
written on 2026-11-01 and checked only on synthetic data (selftest_liqmap.py); the collected data is
first read when this script runs after the sample ends. No orders are placed anywhere.

Sample: from the collector start + 24 h to start + 28 days (START below, from status.json up_s).
Snapshots: every 15 minutes on the clock. Coins: the 40 live perps with the highest day_ntl_vlm in the
latest ctx row at or before the snapshot.
Map at t: each address's latest acct observation at or before t, kept when its age is <= 1 h (total
notional >= $1M at that observation), <= 6 h (>= $100k) or <= 24 h (otherwise); that poll's positions with
a liquidation price add |value| to the 0.25% bucket of |liq_px / mid - 1|, longs below the mid and shorts
above it, out to 10% (a liquidation price on the wrong side of the mid is left out).
Event: per side, the bucket with the most notional among distances 1-5%; it counts when that notional is
>= 1% of the coin's day_ntl_vlm; at most one event per coin and side in 4 h, the first kept.
Outcome: the 1-minute mid touches the bucket's near edge (k x 0.25% from the mid at t) within 4 h.
Control: snapshots of the same coin and side with no qualifying cluster, in the same tercile (per coin) of
the realised volatility (std of 1-minute mid log returns over the previous 4 h); their touch rate is
measured at the event's own distance (a stricter match than the README's 0.5% distance bins).
H1 (pass/fail): mean(event touch - matched control touch rate) >= +5 pp, and its 95% day-block bootstrap
interval (2,000 resamples of UTC days) excludes zero.
H2 (reported): after a touch, the largest excursion beyond the level within 30 minutes, and the return
from the level 60 minutes after the touch.
Trading versions (magnet, trap) run only if H1 passes; they are the README's rules and are added then.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research/hl_liqmap mounted at /w/hl_liqmap)
  python test_liqmap.py          results.json, events.parquet
"""

from __future__ import annotations

import glob
import json
import os

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("DATA", f"{HERE}/data")
MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000
START = int(pd.Timestamp("2026-10-31 18:23:00", tz="UTC").value // 1_000_000)
STEP = 15 * MIN
TOP = 40
BUCKET = 0.0025
MAX_DIST = 0.10
LO_K, HI_K = 4, 20                 # buckets covering distances [1%, 5%)
MIN_SHARE = 0.01
GAP = 4 * HOUR
HORIZON = 4 * HOUR
VOL_LOOKBACK = 4 * HOUR
BOOT, SEED = 2000, 20261031
H1_BAR = 0.05


def age_limit(total_ntl):
    return HOUR if total_ntl >= 1e6 else 6 * HOUR if total_ntl >= 1e5 else DAY


def read(table, lo, hi):
    days = pd.date_range(pd.Timestamp(lo, unit="ms").normalize(), pd.Timestamp(hi - 1, unit="ms").normalize())
    parts = [p for d in days for p in sorted(glob.glob(f"{DATA}/{table}/{d:%Y-%m-%d}/*.parquet"))]
    if not parts:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    return df[(df["ts"] >= lo) & (df["ts"] < hi)]


def live_observations(acct, t):
    """acct rows with ts <= t -> (addr, ts) of each address's latest poll, if young enough."""
    a = acct[acct["ts"] <= t].sort_values("ts").groupby("addr", sort=False).tail(1)
    keep = (t - a["ts"]) <= a["total_ntl"].map(age_limit)
    return a.loc[keep, ["addr", "ts"]]


def buckets(pos, mids):
    """pos rows of the live polls for one coin, mid at t -> {(side, k): notional}; side -1 below, +1 above."""
    out = {}
    for szi, value, liq in zip(pos["szi"].to_numpy(), pos["value"].to_numpy(), pos["liq_px"].to_numpy()):
        if not np.isfinite(liq) or liq <= 0 or szi == 0:
            continue
        d = liq / mids - 1
        side = -1 if szi > 0 else 1
        if np.sign(d) != side or abs(d) > MAX_DIST:
            continue
        k = int(abs(d) // BUCKET)
        out[(side, k)] = out.get((side, k), 0.0) + abs(value)
    return out


def cluster(bk, side, day_vlm):
    """-> bucket index k of a qualifying cluster on this side, else None."""
    cand = {k: v for (s, k), v in bk.items() if s == side and LO_K <= k < HI_K}
    if not cand:
        return None
    k = max(cand, key=cand.get)
    return k if day_vlm > 0 and cand[k] >= MIN_SHARE * day_vlm else None


def touched(m_t, m_px, t, mid, side, dist, horizon=HORIZON):
    """Did the mid reach mid*(1 + side*dist) within (t, t + horizon]?"""
    lo, hi = np.searchsorted(m_t, t, "right"), np.searchsorted(m_t, t + horizon, "right")
    if hi <= lo or m_t[hi - 1] < t + horizon - 5 * MIN:
        return None
    seg = m_px[lo:hi]
    level = mid * (1 + side * dist)
    return bool(seg.min() <= level) if side < 0 else bool(seg.max() >= level)


def realised_vol(m_t, m_px, t):
    lo, hi = np.searchsorted(m_t, t - VOL_LOOKBACK, "right"), np.searchsorted(m_t, t, "right")
    seg = m_px[lo:hi]
    return float(np.std(np.diff(np.log(seg)))) if len(seg) > 30 else np.nan


def mid_series(mids):
    return {c: (g["ts"].to_numpy(np.int64), g["mid"].to_numpy(float)) for c, g in mids.sort_values("ts").groupby("coin")}


def snapshots(acct, pos, mid_by, ctx, times):
    """-> DataFrame of (t, coin, side, k_cluster or -1, mid, vol) for every snapshot, coin and side."""
    rows = []
    for t in times:
        c = ctx[ctx["ts"] <= t]
        if c.empty:
            continue
        latest = c.sort_values("ts").groupby("coin").tail(1)
        top = latest.nlargest(TOP, "day_ntl_vlm")
        obs = live_observations(acct, t)
        live = pos.merge(obs, on=["addr", "ts"])
        for coin, vlm in zip(top["coin"], top["day_ntl_vlm"]):
            if coin not in mid_by:
                continue
            m_t, m_px = mid_by[coin]
            j = np.searchsorted(m_t, t, "right") - 1
            if j < 0 or t - m_t[j] > 5 * MIN:
                continue
            mid = m_px[j]
            bk = buckets(live[live["coin"] == coin], mid)
            vol = realised_vol(m_t, m_px, t)
            for side in (-1, 1):
                k = cluster(bk, side, vlm)
                rows.append({"t": t, "coin": coin, "side": side, "k": -1 if k is None else k, "mid": mid, "vol": vol})
    return pd.DataFrame(rows)


def thin_events(snap):
    """Qualifying clusters, at most one per coin and side in 4 h (the first kept)."""
    ev = snap[snap["k"] >= 0].sort_values("t", kind="stable")
    keep, last = [], {}
    for i, coin, side, t in zip(ev.index, ev["coin"], ev["side"], ev["t"]):
        if t - last.get((coin, side), -10 ** 18) >= GAP:
            keep.append(i)
            last[(coin, side)] = t
    return ev.loc[keep]


def evaluate(snap, mid_by):
    snap = snap.copy()
    snap["tercile"] = snap.groupby("coin")["vol"].transform(
        lambda v: pd.qcut(v.rank(method="first"), 3, labels=False) if v.notna().sum() >= 3 else np.nan)
    ev = thin_events(snap)
    ctrl = snap[snap["k"] < 0]
    rows = []
    for _, e in ev.iterrows():
        m_t, m_px = mid_by[e["coin"]]
        dist = e["k"] * BUCKET
        hit = touched(m_t, m_px, e["t"], e["mid"], e["side"], dist)
        if hit is None or not np.isfinite(e["tercile"]):
            continue
        cs = ctrl[(ctrl["coin"] == e["coin"]) & (ctrl["side"] == e["side"]) & (ctrl["tercile"] == e["tercile"])]
        ch = [touched(m_t, m_px, t, m, e["side"], dist) for t, m in zip(cs["t"], cs["mid"])]
        ch = [x for x in ch if x is not None]
        if not ch:
            continue
        rows.append({"t": int(e["t"]), "coin": e["coin"], "side": int(e["side"]), "dist": dist, "touch": hit,
                     "control_rate": float(np.mean(ch)), "controls": len(ch), **h2(m_t, m_px, e, dist, hit)})
    return pd.DataFrame(rows)


def h2(m_t, m_px, e, dist, hit):
    if not hit:
        return {"excursion_30m": np.nan, "ret_60m": np.nan}
    level = e["mid"] * (1 + e["side"] * dist)
    lo, hi = np.searchsorted(m_t, e["t"], "right"), np.searchsorted(m_t, e["t"] + HORIZON, "right")
    seg_t, seg = m_t[lo:hi], m_px[lo:hi]
    i = int(np.flatnonzero(seg <= level)[0] if e["side"] < 0 else np.flatnonzero(seg >= level)[0])
    t_hit = seg_t[i]
    w = (m_t > t_hit) & (m_t <= t_hit + 30 * MIN)
    beyond = (level / m_px[w].min() - 1) if e["side"] < 0 else (m_px[w].max() / level - 1)
    j = np.searchsorted(m_t, t_hit + 60 * MIN)
    back = (m_px[j] / level - 1) if j < len(m_t) else np.nan
    return {"excursion_30m": float(beyond) if w.any() else np.nan, "ret_60m": float(back)}


def h1(res, boot=BOOT, seed=SEED):
    diff = (res["touch"].astype(float) - res["control_rate"]).to_numpy()
    days = (res["t"] // DAY).to_numpy()
    uniq = np.unique(days)
    rng = np.random.default_rng(seed)
    by_day = {d: diff[days == d] for d in uniq}
    means = []
    for _ in range(boot):
        pick = rng.choice(uniq, len(uniq), replace=True)
        means.append(np.concatenate([by_day[d] for d in pick]).mean())
    lo, hi = np.percentile(means, [2.5, 97.5])
    m = float(diff.mean())
    return {"events": int(len(res)), "days": int(len(uniq)), "mean_pp": round(m * 100, 2),
            "ci_pp": [round(lo * 100, 2), round(hi * 100, 2)], "event_touch_pct": round(res["touch"].mean() * 100, 1),
            "control_touch_pct": round(res["control_rate"].mean() * 100, 1),
            "passes": bool(m >= H1_BAR and lo > 0)}


def main():
    lo, hi = START + DAY, START + 28 * DAY
    mid_by = mid_series(read("mids", lo - VOL_LOOKBACK, hi + HORIZON))
    ctx = read("ctx", lo - DAY, hi)
    frames = []
    for d0 in range(lo // DAY * DAY, hi, DAY):          # a day at a time: positions are tens of millions of rows
        times = list(range(-(-max(lo, d0) // STEP) * STEP, min(hi, d0 + DAY), STEP))
        if times:
            acct = read("acct", times[0] - DAY, times[-1] + 1)
            pos = read("pos", times[0] - DAY, times[-1] + 1)
            frames.append(snapshots(acct, pos, mid_by, ctx, times))
            print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d} snapshots {len(times)}", flush=True)
    res = evaluate(pd.concat(frames, ignore_index=True), mid_by)
    out = {"h1": h1(res), "h2": {"touches": int(res["touch"].sum()),
                                 "excursion_30m_median_pct": round(float(res["excursion_30m"].median() * 100), 3),
                                 "ret_60m_median_pct": round(float(res["ret_60m"].median() * 100), 3)}}
    res.to_parquet(f"{HERE}/events.parquet")
    with open(f"{HERE}/results.json", "w") as fh:
        json.dump(out, fh, indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
