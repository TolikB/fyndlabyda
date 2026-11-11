"""Known-answer checks for collector.py on canned frames (no network). Prints "selftest OK"."""

import collector as c

# pairs: same ticker, an alias, a delisted thin market, a delisted twin, a ticker REF does not list, a dead dex
metas = {
    "xyz": {"universe": [{"name": "xyz:NVDA", "maxLeverage": 20, "growthMode": "enabled", "deployerFeeScale": "1.0"},
                         {"name": "xyz:ANT", "maxLeverage": 5, "onlyIsolated": True},
                         {"name": "xyz:TTWO", "maxLeverage": 10, "growthMode": "enabled", "deployerFeeScale": "0.5"},
                         {"name": "xyz:COIN", "maxLeverage": 10, "isDelisted": True},
                         {"name": "xyz:GOLD", "maxLeverage": 25}]},
    "io": {"universe": [{"name": "io:NVDA", "maxLeverage": 10, "growthMode": "enabled", "deployerFeeScale": "1.0",
                         "onlyIsolated": True},
                        {"name": "io:ANTH", "maxLeverage": 6, "growthMode": "enabled"},
                        {"name": "io:OAI", "maxLeverage": 6},
                        {"name": "io:COIN", "maxLeverage": 10},
                        {"name": "io:GOLD", "maxLeverage": 10, "isDelisted": True}]},
    "para": {"universe": [{"name": "para:TTWO", "maxLeverage": 5, "deployerFeeScale": "2.0"},
                          {"name": "para:NVDA", "maxLeverage": 5}]},
    "flx": {"universe": [{"name": "flx:NVDA", "isDelisted": True}, {"name": "flx:GOLD", "isDelisted": True}]},
}
p = c.pairs_from(metas)
assert sorted(p) == ["io:ANTH", "io:NVDA", "para:NVDA", "para:TTWO"], sorted(p)
assert p["io:NVDA"] == {"twin": "xyz:NVDA", "dex": "io", "growth": True, "fee_scale": 1.0, "max_lev": 10, "isolated": True,
                        "twin_growth": True, "twin_fee_scale": 1.0, "twin_max_lev": 20, "twin_isolated": False}
assert p["io:ANTH"]["twin"] == "xyz:ANT" and p["io:ANTH"]["twin_isolated"] and not p["io:ANTH"]["twin_growth"]
assert p["io:ANTH"]["fee_scale"] == 1.0 and p["io:ANTH"]["twin_fee_scale"] == 1.0      # no scale given: 1.0
assert p["para:TTWO"] == {"twin": "xyz:TTWO", "dex": "para", "growth": False, "fee_scale": 2.0, "max_lev": 5,
                          "isolated": False, "twin_growth": True, "twin_fee_scale": 0.5, "twin_max_lev": 10,
                          "twin_isolated": False}
assert c.REF == "xyz" and c.ALIASES == {"ANTH": "ANT"} and (c.BUCKET, c.FRESH_MS) == (100, 10_000)

# subscriptions: bbo and trades for a thin market, one bbo for each distinct twin (two thin NVDA books share one)
subs = [(s["subscription"]["type"], s["subscription"]["coin"]) for s in c.subscriptions(p)]
assert all(s["method"] == "subscribe" for s in c.subscriptions(p))
assert subs == [("bbo", "io:ANTH"), ("trades", "io:ANTH"), ("bbo", "io:NVDA"), ("trades", "io:NVDA"),
                ("bbo", "para:NVDA"), ("trades", "para:NVDA"), ("bbo", "para:TTWO"), ("trades", "para:TTWO"),
                ("bbo", "xyz:ANT"), ("bbo", "xyz:NVDA"), ("bbo", "xyz:TTWO")], subs

# HL trades: a coin's first frame on a connection is the batch of past trades; later, a trade over 10 s old is dropped
seen = set()
old = [{"coin": "io:NVDA", "side": "B", "px": "100", "sz": "1", "time": 1}]
assert c.fresh_trades(old, seen, 500) == [] and c.fresh_trades(old, seen, 500) == old and c.fresh_trades([], seen, 500) == []
assert c.fresh_trades(old + [{"coin": "io:NVDA", "time": 5000}], seen, 10_002) == [{"coin": "io:NVDA", "time": 5000}]
assert c.fresh_trades([{"coin": "io:NVDA", "time": 2}], seen, 10_002) == [{"coin": "io:NVDA", "time": 2}]
assert c.fresh_trades([{"coin": "io:NVDA", "time": 1}], seen, 10_002) == []

# handlers: the last book of a bucket, trades by side, bad frames counted without a reconnect
k = c.Collector()
seen = set()
bbo = {"channel": "bbo", "data": {"coin": "io:NVDA", "time": 7, "bbo": [{"px": "100", "sz": "5", "n": 1}, {"px": "100.1", "sz": "6", "n": 2}]}}
k.on_hl(bbo, 1234, seen)
assert k.hl.rows[("io:NVDA", 1200)] == {"b": 1200, "coin": "io:NVDA", "bid": 100.0, "ask": 100.1, "bid_sz": 5.0, "ask_sz": 6.0,
                                        "h_last": 7, "r_last": 1234}
k.on_hl({"channel": "bbo", "data": {"coin": "io:NVDA", "time": 8, "bbo": [{"px": "100.05", "sz": "1"}, {"px": "100.1", "sz": "2"}]}}, 1290, seen)
assert k.hl.rows[("io:NVDA", 1200)]["bid"] == 100.05 and k.hl.rows[("io:NVDA", 1200)]["r_last"] == 1290
k.on_hl({"channel": "bbo", "data": {"coin": "io:NVDA", "time": 9, "bbo": [None, {"px": "1", "sz": "1"}]}}, 1300, seen)
assert ("io:NVDA", 1300) not in k.hl.rows
live = {"channel": "trades", "data": [{"coin": "io:NVDA", "side": "A", "px": "100", "sz": "2", "time": 9},
                                      {"coin": "io:NVDA", "side": "B", "px": "101", "sz": "1", "time": 9}]}
k.on_hl(live, 1400, seen)
assert k.hlt.rows == {} and k.n["hlt_dropped"] == 2
k.on_hl(live, 1450, seen)
y = k.hlt.rows[("io:NVDA", 1400)]
assert (y["buy_q"], y["sell_q"], y["last"], y["hi"], y["lo"], y["n"], y["h_last"], y["r_last"]) == \
    (101.0, 200.0, 101.0, 101.0, 100.0, 2, 9, 1450)
assert (y["buy_px"], y["sell_px"]) == (101.0, 100.0)
k.on_hl({"channel": "trades", "data": [{"coin": "io:NVDA", "side": "A", "px": "99", "sz": "1", "time": 11}]}, 1460, seen)
y = k.hlt.rows[("io:NVDA", 1400)]
assert (y["last"], y["hi"], y["lo"], y["sell_q"], y["sell_px"], y["n"], y["h_last"], y["r_last"]) ==     (99.0, 101.0, 99.0, 299.0, 99.0, 3, 11, 1460)
k.on_hl({"channel": "trades", "data": [{"coin": "io:NVDA", "side": "B", "px": "102", "sz": "1", "time": 10}]}, 1520, seen)
z = k.hlt.rows[("io:NVDA", 1500)]
assert z["buy_px"] == 102.0 and z["sell_px"] != z["sell_px"] and z["sell_q"] == 0.0
k.on_hl(live, 20_000, seen)
assert k.n["hlt_dropped"] == 4 and ("io:NVDA", 20_000) not in k.hlt.rows
k.on_hl({"channel": "subscriptionResponse", "data": {"method": "subscribe"}}, 1500, seen)
assert k.n == {"hl_msgs": 2, "hlt_msgs": 4, "hlt_dropped": 4, "hl_reconnects": 0, "errors": 0}, k.n
assert len(k.hlt.take()) == 2 and k.hlt.rows == {} and len(k.hl.take()) == 1
print("selftest OK")
