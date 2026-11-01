"""Known-answer checks for fwd_sniper.py (no network). Prints "selftest OK"."""

import json
import os
import tempfile

import numpy as np

import fwd_sniper as fs

DAY = fs.DAY
T = 1_793_600_000_000

assert fs.coin_of("KRW-NIL") == "NIL" and fs.coin_of("USDT-1INCH") == "1INCH" and fs.coin_of("X") is None
assert fs.slip(2e9) == 0.0002 and fs.slip(1e8) == 0.0010 and fs.slip(1e6) == 0.0025
info = [{"symbol": "NILUSDT", "status": "TRADING", "contractType": "PERPETUAL", "onboardDate": T - 10 * DAY},
        {"symbol": "1000BONKUSDT", "status": "TRADING", "contractType": "PERPETUAL", "onboardDate": T - 10 * DAY},
        {"symbol": "NEWUSDT", "status": "TRADING", "contractType": "PERPETUAL", "onboardDate": T - DAY},
        {"symbol": "OLDUSDT", "status": "SETTLING", "contractType": "PERPETUAL", "onboardDate": T - 99 * DAY}]
assert fs.perp_for("NIL", info, T) == "NILUSDT" and fs.perp_for("BONK", info, T) == "1000BONKUSDT"
assert fs.perp_for("NEW", info, T) is None and fs.perp_for("OLD", info, T) is None and fs.perp_for("ZZZ", info, T) is None

# paper fill: entry floor at t_seen + one way, exit at +60 s, pre-move over the 10 s before t_seen
t = np.array([T - 20_000, T - 5_000, T + 150, T + 400, T + 900, T + 60_200], np.int64)
p = np.array([10.0, 10.5, 11.0, 11.4, 11.6, 11.8])
q = np.array([1.0, 1.0, 1.0, 1.0, 2.0, 1.0])
buy = np.array([True, True, True, True, False, False])
r = fs.paper(t, p, q, buy, T, 130)
assert r["entry_t"] == T + 150 and abs(r["entry"] - 11.2) < 1e-12        # buyers in [T+130, T+1130): (11 + 11.4)/2
assert r["exit"] == 11.8 and r["exit_t"] == T + 60_200 and r["pre_move_pct"] == 5.0
assert fs.paper(t[t < T + 60_000], p[t < T + 60_000], q[t < T + 60_000], buy[t < T + 60_000], T, 130) is None
assert abs(fs.net(10.0, 11.0, 0.001) - (0.1 - 0.001 - 0.003)) < 1e-12

# watcher lines -> triggers: the baseline is ignored, one coin counts once within 72 h
fs.WATCH = tempfile.mkdtemp()
fs.FWD = tempfile.mkdtemp()
with open(f"{fs.WATCH}/markets.jsonl", "w") as fh:
    fh.write(json.dumps({"t": T - 100, "baseline": True, "markets": ["KRW-BTC", "BTC-ETH"]}) + "\n")
    fh.write(json.dumps({"t": T, "added": ["KRW-NIL", "BTC-NIL"], "removed": [], "flags": []}) + "\n")
    fh.write(json.dumps({"t": T + 5, "added": ["USDT-NIL"], "removed": [], "flags": []}) + "\n")
st = fs.State()
fs.read_new(st)
assert [e["market"] for e in st.pending] == ["KRW-NIL"] and st.pending[0]["t_seen"] == T
with open(f"{fs.WATCH}/markets.jsonl", "a") as fh:
    fh.write(json.dumps({"t": T + 73 * 3_600_000, "added": ["KRW-NIL2", "BTC-BONK"], "removed": [], "flags": []}) + "\n")
    fh.write('{"t": 1, "added": ["KRW-PARTIAL"')                           # an unfinished line is left for later
fs.read_new(st)
assert [e["coin"] for e in st.pending] == ["NIL", "NIL2", "BONK"]
st.save()
st2 = fs.State()
assert st2.offset == st.offset and len(st2.pending) == 3

# report on an empty ledger
assert fs.report()["trades"] == 0

print("selftest OK")
