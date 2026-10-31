"""Known-answer checks for collector.py (no network). Prints "selftest OK"."""

import glob
import os
import tempfile

import collector as c

# parsing helpers
assert c.num("1.5") == 1.5 and c.num(None) is None and c.num("nan") is None and c.num("x") is None

# poll intervals by tier
assert c.interval(None, False) == 0.0
assert c.interval(0.0, False) == c.EMPTY_S
assert c.interval(5e3, False) == 86400
assert c.interval(2e4, False) == 28800
assert c.interval(2e5, False) == 7200
assert c.interval(2e6, False) == 1800
assert c.interval(6e6, False) == 900
assert c.interval(6e6, True) == 600.0      # a quarter of 900, floored at 10 minutes
assert c.interval(2e5, True) == 1800.0

# clearinghouseState shape as seen live on 2026-10-31
state = {"marginSummary": {"accountValue": "42455.7", "totalNtlPos": "1159847.9"},
         "assetPositions": [
             {"type": "oneWay", "position": {"coin": "BTC", "szi": "13.55", "leverage": {"type": "cross", "value": 40},
                                             "entryPx": "77656.2", "positionValue": "1053096.5",
                                             "unrealizedPnl": "701.6", "liquidationPx": "75905.2",
                                             "marginUsed": "26327.4"}},
             {"type": "oneWay", "position": {"coin": "ETH", "szi": "-2.0", "leverage": {"type": "isolated", "value": 3},
                                             "entryPx": "3000", "positionValue": "6000", "unrealizedPnl": "0",
                                             "liquidationPx": None, "marginUsed": "2000"}},
             {"type": "oneWay", "position": {"coin": "SOL", "szi": "0.0", "positionValue": "0"}}]}
acct, pos, ntl = c.parse_state("0xabc", 1000, state)
assert [p["coin"] for p in pos] == ["BTC", "ETH"], pos
assert pos[0]["liq_px"] == 75905.2 and pos[0]["lev_type"] == "cross" and pos[0]["lev"] == 40.0
assert pos[1]["szi"] == -2.0 and pos[1]["liq_px"] is None
assert abs(ntl - 1059096.5) < 1e-6 and acct["n_pos"] == 2 and acct["account_value"] == 42455.7
acct0, pos0, ntl0 = c.parse_state("0xdef", 1000, {"marginSummary": {"accountValue": "5"}, "assetPositions": []})
assert pos0 == [] and ntl0 == 0.0 and acct0["n_pos"] == 0

# asset contexts: delisted coins are recorded but not live
ctx_doc = [{"universe": [{"name": "BTC"}, {"name": "OLD", "isDelisted": True}]},
           [{"markPx": "70000", "funding": "0.0000125", "openInterest": "100", "dayNtlVlm": "1e9"}, {"markPx": "1"}]]
rows, live = c.parse_ctx(5, ctx_doc)
assert live == ["BTC"] and len(rows) == 2 and rows[0]["mark"] == 70000.0 and rows[0]["funding"] == 0.0000125

# schedule: an overdue whale, then a never-polled big trader; a fresh small account is not due
now = 1_793_000_000.0                       # a real epoch: "never traded" (0.0) must read as long ago
r = c.Registry()
r.polled("whale", now - 2700, 6e6)          # 45 min after a 15-minute interval -> score 3
r.seen_trade("newbie", now - 10, 1e6)       # never polled -> 2 + log10(1e6 + 1) / 10 = 2.6
r.polled("small", now - 3600, 5e3)          # 1 h of 24 h -> not due
order, n_due = r.due(now, 10)
assert order == ["whale", "newbie"] and n_due == 2, (order, n_due)
assert abs(r.score(r.a["newbie"], now) - 2.6) < 1e-3
r.seen_trade("whale", now - 100, 50.0)      # traded since its poll -> interval 600 s -> score 4.5
assert abs(r.score(r.a["whale"], now) - 2700 / 600) < 1e-9
assert r.due(now, 1)[0] == ["whale"]
r.failed("newbie", now)                     # an error backs off instead of retrying forever
assert r.a["newbie"]["ntl"] == 0.0 and r.score(r.a["newbie"], now) == 0.0

# pruning keeps leaderboard whales and anything with a position
r.seed("lbwhale", 2e6, now - 30 * 86400)
r.a["lbwhale"]["first"] = now - 30 * 86400
r.polled("gone", now - 20 * 86400, 0.0)
r.a["gone"]["first"] = now - 20 * 86400
assert r.prune(now) == 1 and "gone" not in r.a and "lbwhale" in r.a and "whale" in r.a

# limiter halves its rate after a 429
lim = c.Limiter(200)
assert lim.rate() == 200
lim.penalize()
assert lim.rate() == 100

with tempfile.TemporaryDirectory() as d:
    # registry round trip
    p = os.path.join(d, "registry.json.gz")
    r.save(p)
    r2 = c.Registry()
    r2.load(p)
    assert r2.a == r.a
    # parquet round trip keeps nulls and types
    s = c.Store(d)
    s.add("acct", [acct, acct0])
    s.add("pos", pos)
    s.add("ctx", rows)
    s.add("mids", [{"ts": 1, "coin": "BTC", "mid": 70000.0}])
    s.flush()
    import pyarrow.parquet as pq
    got = pq.read_table(glob.glob(os.path.join(d, "pos", "*", "*.parquet"))[0]).to_pylist()
    assert got == pos, got
    assert s.written == {"acct": 2, "pos": 2, "mids": 1, "ctx": 2}
    assert not glob.glob(os.path.join(d, "*", "*", "*.tmp"))

print("selftest OK")
