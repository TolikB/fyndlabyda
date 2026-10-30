"""Round 5: intraday reversal after extreme hourly moves (research only).

Event: in hour h a top-60 symbol (universe from the previous day) moved more than
`move` open-to-open. Fade it: buy after a drop / sell after a spike at the open of
h+1 (the move is complete and known), exit at the open of h+1+k. Variants: raw move,
or the move in excess of BTC's move that hour (idiosyncratic shocks only).
Each event takes 5% of equity, one hour's events are capped at 100% gross; costs are
the taker fee plus liquidity-tiered slippage on both sides, plus 1 bp per hour held
as a funding allowance.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

import bt
import evalx

ROOT = "/w"


def main():
    h = pd.read_parquet(f"{bt.DATA}/klines_1h.parquet", columns=["symbol", "open_time", "open", "quote_volume"])
    h = h[(h["quote_volume"] > 0) & ~h["symbol"].isin(bt.EXCLUDE)]
    h["t"] = pd.to_datetime(h["open_time"], unit="ms")
    opn = h.pivot_table(index="t", columns="symbol", values="open", aggfunc="first").astype("float32")
    del h
    _, close, qvol, _ = bt.load()
    live_d, med = bt.universe(close, qvol, 60)
    slip_d = bt.slippage(med).shift(1)
    days = opn.index.normalize() - pd.Timedelta(days=1)
    live = live_d.reindex(columns=opn.columns).reindex(days).fillna(False).to_numpy()
    slip = slip_d.reindex(columns=opn.columns).reindex(days).fillna(0.0025).to_numpy()
    o = opn.to_numpy()
    ret = o[1:] / o[:-1] - 1.0                       # hour h: open h -> open h+1
    btc = ret[:, list(opn.columns).index("BTCUSDT")][:, None]
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    results, series = {}, {}
    for basis in ("raw", "excess"):
        signal = ret - btc if basis == "excess" else ret
        for move in (0.05, 0.08, 0.12):
            for side_name in ("buy_drop", "sell_spike"):
                hit = (signal < -move) if side_name == "buy_drop" else (signal > move)
                hit &= live[:-1]
                rows, cols = np.nonzero(hit)
                direction = 1.0 if side_name == "buy_drop" else -1.0
                for k in (1, 3, 6, 12):
                    entry = rows + 1                 # open of hour h+1
                    exit_ = entry + k
                    ok = exit_ < len(o)
                    r, c, en, ex = rows[ok], cols[ok], entry[ok], exit_[ok]
                    move_after = o[ex, c] / o[en, c] - 1.0
                    good = np.isfinite(move_after)
                    r, c, en, move_after = r[good], c[good], en[good], move_after[good]
                    cost = 2 * (bt.FEE + slip[en, c]) + 0.0001 * k
                    net = direction * move_after - cost
                    times = opn.index[en]
                    frame = pd.DataFrame({"t": times, "net": net, "gross": direction * move_after})
                    per_hour = frame.groupby("t")["net"].transform("size")
                    frame["pnl"] = np.minimum(0.05, 1.0 / per_hour) * frame["net"]
                    daily = frame.groupby(frame["t"].dt.normalize())["pnl"].sum()
                    daily = daily.reindex(pd.date_range("2020-01-01", end.normalize(), freq="D"), fill_value=0.0)
                    key = f"rev|{basis}|{side_name}|m{move}|k{k}"
                    series[key] = daily
                    dev = daily[daily.index < holdout_start]
                    dev_f = frame[frame["t"] < holdout_start]
                    s = bt.stats(dev)
                    results[key] = {"dev": s, "events": len(dev_f)}
                    print(f"{key:<36} n {len(dev_f):>6} gross {dev_f['gross'].mean()*1e4:7.1f}bp "
                          f"med {dev_f['gross'].median()*1e4:6.1f}bp net {dev_f['net'].mean()*1e4:7.1f}bp "
                          f"sh {s['sharpe']:>5} cagr {s['cagr_pct']:>6} dd {s['max_dd_pct']:>5} "
                          f"q+ {evalx.quarters_positive(dev):4.0f} yrs {s['by_year_pct']}", flush=True)
    with open(f"{ROOT}/results_round5.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{ROOT}/pnl_round5.parquet")


if __name__ == "__main__":
    main()
