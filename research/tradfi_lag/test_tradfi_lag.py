"""Do Hyperliquid's xyz stock perps lag Binance's by more than an order takes to arrive? (round 37, research only)

Pre-registered on 2026-11-11, before the development window opened. Of the collected rows only counts, column names and
clock differences were read (no price), and this file was checked on synthetic data only (selftest_tradfi_lag.py). No
orders are placed anywhere, and no keys are used.

Why: rounds 20, 25 and 30 ask whether a slower venue follows Binance late enough to trade. Round 20 has to clear
Hyperliquid's 0.045% a side. The xyz dex lists stocks, funds and metals that Binance also lists as TradFi perps, and in
growth mode its taker pays 0.009% a side (Hyperliquid's fee page, read 2026-11-10), so a far smaller move could pay.

Data (collector.py, all on the host's own clock, 100 ms buckets)
  bd     Binance depth5@100ms, the last book of a bucket; the signal price is its mid
  hl     xyz best bid and ask, with the block time h_last and the local receive time r_last
  rtt    a REST round trip to Hyperliquid each minute
  pairs  pairs/<date>.json, built at the start of each UTC day
Pairs: those of the day's file with "depth" (the 30 with the largest 24 h Binance quote volume when the day began),
less type PREMARKET, where the two contracts settle on different things.

Signal, per pair, on the local grid: that of ../xvenue/test_lag.py with the Binance mid in place of the last trade.
  r_bn = Binance mid now / 1 s ago - 1; r_hl the same for the xyz mid; gap = r_bn - r_hl
  fire when |r_bn| >= THETA, gap has the sign of r_bn with |gap| >= THETA / 2, the xyz quote changed in the last 10 s
  and its touch on the side taken holds 2,500 USDT; then the pair rests 5 s
A Binance mid counts only while its book is no wider than THETA: through a wider one, a move of THETA cannot be told
from one side stepping back.
Both feeds push on change, so a carried-forward price is exact unless the connection was down. A feed that sent no row
for any name for over 2 s was down, and so was one whose rows of the day begin or end over 2 s from the day's bounds: a
price is not used from then until that name's next row. Signals in the first or the last minute of a UTC day are
dropped: the collector changes its connections five seconds after midnight.

Two hosts, the same signals
  vm    this host: an order sent at the end of the signal's bucket reaches the venue ONE_WAY later, ONE_WAY = the day's
        median Hyperliquid round trip / 2
  colo  a host next to both venues, which sees every message NET earlier (NET = the day's 5th percentile of local
        receive time less Binance event time, less 5 ms) and whose order takes 10 ms: it arrives at t_sig - NET + 10 ms
Fill, on the venue's clock: the quote standing when the order arrives, i.e. the last row with block time < arrival;
when the next row's block time is within 200 ms of the arrival, the worse of the two for the taker, because
Hyperliquid runs a block's cancels before its takers. If the feed was down between that row and the time a change at
the arrival would have been heard, the first row with block time >= arrival is used instead, as in round 20. Entry
takes the ask (long) or bid (short); the exit the same at t_sig + H, on the other side.
Costs: fee a side = 0.045% x (scale + 1 if scale < 1 else 2 x scale) x (0.1 in growth mode), from that day's pairs
file; both fills cross the spread. The 2x run doubles the fee and takes a further half spread a side. Funding is
ignored over at most 30 s. The report also gives the mean with growth mode off, which a deployer may switch off.
Book: as round 25, at most 5 open trades, each 0.2 x s of equity, s in {1, 2, 3}.

Trials: 18 = host {vm, colo} x THETA {0.05%, 0.1%, 0.2%} x H {2 s, 5 s, 30 s}.

Bar on development (2026-11-12 .. 2026-11-26 00:00 UTC), as round 25
  - mean net return a trade > 0 with t >= 2.5, t over the hourly sums of trade returns
  - monthly figure = mean daily book return x 30 >= 5% at the smallest such s*, max drawdown <= 35% at s*
  - monthly figure at s* > 0 at 2x costs
Holdout: 2026-11-26 .. 2026-12-10 00:00 UTC, one look, only for trials that pass. A pass earns only a forward paper
series; for a colo trial that series needs a host next to the venues. Rows of 2026-11-10 and 2026-11-11 are a rehearsal
of this code.

Known limits: the colo trials assume this host's path to both venues takes the same time and that a faster order would
have met the book that was recorded. ONE_WAY is half a round trip that opens a new connection each time: it overstates
the network leg of an order on a kept connection and leaves out the wait for the next block, which the 200 ms rule is
there for. Block times are set against this host's clock, which chrony held within a millisecond of NTP time on
2026-11-10. A quote that stood for less than a bucket may be missing from the rows, and no backtest shows queue
position inside a block.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_tradfi_lag.py rehearsal   results_tradfi_lag_rehearsal.json: code check only, no reading of the numbers
  python test_tradfi_lag.py dev         results_tradfi_lag_dev.json, /w/pnl_round37.parquet
  python test_tradfi_lag.py holdout     results_tradfi_lag_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "xvenue"))
import test_lag as lag  # noqa: E402

DATA = os.environ.get("OUT", f"{HERE}/data")
PNL = "/w/pnl_round37.parquet"
DAY, B = lag.DAY, lag.B
BASE_FEE = 0.00045
GAP_MS = 2_000
WINDOW = 200
EDGE_MS = 60_000
COLO_MS = 10
HOSTS = ("vm", "colo")
TRIALS = {f"{h}|{k}": (h, *v) for h in HOSTS for k, v in lag.trial_map().items()}
PERIODS = {"rehearsal": ("2026-11-10", "2026-11-12"), "dev": ("2026-11-12", "2026-11-26"),
           "holdout": ("2026-11-26", "2026-12-10")}
lag.PERIODS = PERIODS


def fee_side(growth, scale):
    """The xyz taker fee a side, by Hyperliquid's formula for a market of a builder-deployed dex."""
    return BASE_FEE * (scale + 1 if scale < 1 else scale * 2) * (0.1 if growth else 1.0)


def silences(buckets, lo=None, hi=None):
    """Bucket starts of a whole feed -> (starts, ends) in local ms of the stretches over GAP_MS without a row. With the
    day's bounds lo and hi, the stretches before the first row and after the last count as well."""
    b = np.unique(np.asarray(buckets, dtype=np.int64))
    if lo is not None:
        b = np.r_[lo - B, b, hi]
    k = np.flatnonzero(np.diff(b) > GAP_MS)
    return b[k] + B, b[k + 1]


def blind(n, day0, pos, starts, ends):
    """Grid mask of the buckets where a name's carried-forward value is not known to stand: the feed was silent, or has
    been since the name's last row. pos is the grid index of that row (-1 before the first)."""
    bad = np.zeros(n, dtype=bool)
    for s, e in zip(starts, ends):
        i0, i1 = max(0, int((s - day0) // B)), min(n, int((e - day0) // B))
        bad[i0:int(np.searchsorted(pos, i1))] = True
    return bad


def fill(h, r, bid, ask, arrival, buy, heard, starts, ends):
    """(bid, ask) met by a taker order that reaches the venue at `arrival`; rows sorted by block time h, r their local
    receive times. heard: how long after its block time a change is received here."""
    j = int(np.searchsorted(h, arrival))                       # the first row with block time >= arrival
    if j > 0:
        k = int(np.searchsorted(ends, r[j - 1], side="right"))  # the first silence that ends after the standing row
        if k == len(starts) or starts[k] >= arrival + heard:
            c = j - 1
            if j < len(h) and h[j] - arrival <= WINDOW and (ask[j] > ask[c] if buy else bid[j] < bid[c]):
                c = j
            return float(bid[c]), float(ask[c])
    return (float(bid[j]), float(ask[j])) if j < len(h) else None


def pair_day(bd, hl, day0, fee, hosts, heard, trials, bn_gaps, hl_gaps):
    """bd, hl: one pair's rows within one UTC day. hosts: {name: ms from t_sig to the order's arrival}.
    trials: {key: (host, theta, H)}. -> trade rows."""
    n = DAY // B
    bi = ((bd["b"].to_numpy(np.int64) - day0) // B).clip(0, n - 1)
    li = ((hl["b"].to_numpy(np.int64) - day0) // B).clip(0, n - 1)
    mid, bpos = lag.grid_ffill(bi, ((bd["bid"] + bd["ask"]) / 2).to_numpy(float), n)
    wide, _ = lag.grid_ffill(bi, ((bd["ask"] - bd["bid"]) / (bd["bid"] + bd["ask"]) * 2).to_numpy(float), n)
    bid, pos = lag.grid_ffill(li, hl["bid"].to_numpy(float), n)
    ask, _ = lag.grid_ffill(li, hl["ask"].to_numpy(float), n)
    bid_sz, _ = lag.grid_ffill(li, hl["bid_sz"].to_numpy(float), n)
    ask_sz, _ = lag.grid_ffill(li, hl["ask_sz"].to_numpy(float), n)
    mid[blind(n, day0, bpos, *bn_gaps)] = np.nan
    dark = blind(n, day0, pos, *hl_gaps)
    bid[dark], ask[dark] = np.nan, np.nan
    x = hl.sort_values(["h_last", "r_last"])
    hs, rs = x["h_last"].to_numpy(np.int64), x["r_last"].to_numpy(np.int64)
    hb, ha = x["bid"].to_numpy(float), x["ask"].to_numpy(float)
    by_theta, rows = {}, []
    for key, (host, theta, H) in trials.items():
        by_theta.setdefault(theta, {}).setdefault(host, []).append((key, H))
    for theta, by_host in by_theta.items():
        for i, side in lag.signals(np.where(wide <= theta, mid, np.nan), bid, ask, bid_sz, ask_sz, pos, theta):
            t_sig = day0 + i * B + B                            # the bucket's end on the local clock
            if not day0 + EDGE_MS <= t_sig <= day0 + DAY - EDGE_MS:
                continue
            for host, keys in by_host.items():
                q_in = fill(hs, rs, hb, ha, t_sig + hosts[host], side > 0, heard, *hl_gaps)
                if q_in is None:
                    continue
                for key, H in keys:
                    q_out = fill(hs, rs, hb, ha, t_sig + H + hosts[host], side < 0, heard, *hl_gaps)
                    if q_out is None:
                        continue
                    gross, less = lag.trade_returns(side, q_in, q_out)
                    rows.append({"trial": key, "t_sig": t_sig, "exit_t": t_sig + H, "side": side, "gross": gross,
                                 "ret1": gross - 2 * fee, "ret2": less - 4 * fee})
    return rows


def read_day(table, day0, key, names, cols):
    """One UTC day of a table -> ({name: frame of cols}, the bucket starts of the whole feed). A part is filed under
    the date it was written on, so the day's last rows are in the first part of the next date."""
    day, after = (f"{pd.Timestamp(d, unit='ms'):%Y-%m-%d}" for d in (day0, day0 + DAY))
    parts = sorted(glob.glob(f"{DATA}/{table}/{day}/*.parquet")) + sorted(glob.glob(f"{DATA}/{table}/{after}/000*.parquet"))
    by_name, buckets, out = {}, [], {}
    for p in parts:
        df = pd.read_parquet(p, columns=[key, *cols])
        df = df[(df["b"] >= day0) & (df["b"] < day0 + DAY)]
        buckets.append(df["b"].unique())
        for name, g in df[df[key].isin(names)].groupby(key):
            by_name.setdefault(name, []).append(g[cols])
    for name in list(by_name):
        out[name] = pd.concat(by_name.pop(name), ignore_index=True)
    return out, np.concatenate(buckets) if buckets else np.array([], dtype=np.int64)


def day_pairs(day0):
    path = f"{DATA}/pairs/{pd.Timestamp(day0, unit='ms'):%Y-%m-%d}.json"
    if not os.path.exists(path):
        return {}
    return {c: p for c, p in json.load(open(path)).items() if p["depth"] and p["type"] != "PREMARKET"}


def run(period, trials):
    lo, hi = (lag.ms(x) for x in PERIODS[period])
    rtt = lag.read(DATA, "rtt", lo, hi)
    rows, days = [], []
    for d0 in range(lo, hi, DAY):
        pairs = day_pairs(d0)
        bd, bd_b = read_day("bd", d0, "sym", {p["binance"] for p in pairs.values()}, ["b", "bid", "ask", "e_last", "r_last"])
        hl, hl_b = read_day("hl", d0, "coin", set(pairs), ["b", "bid", "ask", "bid_sz", "ask_sz", "h_last", "r_last"])
        if not bd or not hl:
            continue
        rt = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "hl")]["ms"] if len(rtt) else []
        one_way = int(round(rt.median() / 2)) if len(rt) else 150
        net = max(0, int(round(np.percentile(np.concatenate([(g["r_last"] - g["e_last"]).to_numpy() for g in bd.values()]), 5))) - 5)
        heard = int(round(np.median(np.concatenate([(g["r_last"] - g["h_last"]).to_numpy() for g in hl.values()]))))
        hosts = {"vm": one_way, "colo": COLO_MS - net}
        bn_gaps, hl_gaps = silences(bd_b, d0, d0 + DAY), silences(hl_b, d0, d0 + DAY)
        used = 0
        for coin, p in pairs.items():
            if p["binance"] not in bd or coin not in hl:
                continue
            used += 1
            fee, fee_off = fee_side(p["growth"], p["fee_scale"]), fee_side(False, p["fee_scale"])
            for t in pair_day(bd[p["binance"]], hl[coin], d0, fee, hosts, heard, trials, bn_gaps, hl_gaps):
                rows.append({"coin": coin, "ret_off": t["gross"] - 2 * fee_off, **t})
        days.append({"day": f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d}", "pairs": used, "one_way_ms": one_way, "net_ms": net,
                     "heard_ms": heard, "binance_silences": int(len(bn_gaps[0])), "hl_silences": int(len(hl_gaps[0])),
                     "trades_so_far": len(rows)})
        print(json.dumps(days[-1]), flush=True)
    return pd.DataFrame(rows), days


def main():
    mode = sys.argv[1]
    path = f"{HERE}/results_tradfi_lag_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_tradfi_lag_dev.json"))
        ok = [k for k in TRIALS if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trials = {k: TRIALS[k] for k in ok}
    else:
        trials = TRIALS
    trades, days = run(mode, trials)
    if trades.empty:
        raise SystemExit("no trades at all: a data bug until shown otherwise")
    out, pnl = lag.evaluate(trades, mode)
    for key in trials:
        if key not in out:
            out[key] = {"trades": 0, "passes": False, "size": None}
            continue
        out[key]["passes"], out[key]["size"] = lag.passes(out[key], None if dev is None else dev[key]["size"])
        acc, _ = lag.book(trades[trades["trial"] == key], "ret_off", 1)
        out[key]["mean_bp_growth_off"] = round(float(acc["ret_off"].mean() * 1e4), 3)
    out["_days"] = days
    if mode == "dev":
        pnl = pnl.reindex(columns=[f"lag|{k}" for k in trials], fill_value=0.0)
        pnl.rename(columns=lambda c: c.replace("lag|", "tradfilag|")).to_parquet(PNL)
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    if mode != "rehearsal":
        print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
