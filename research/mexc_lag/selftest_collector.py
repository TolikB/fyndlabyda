"""Known-answer checks for collector.py on canned frames (no network). Prints "selftest OK"."""

import collector as c

# names: direct, 1000 prefix, aliases, none
listed = {"DOGEUSDT", "1000SHIBUSDT", "TRUMPUSDT", "XAGUSDT"}
assert c.binance_name("DOGE_USDT", listed) == "DOGEUSDT"
assert c.binance_name("SHIB_USDT", listed) == "1000SHIBUSDT"
assert c.binance_name("TRUMPOFFICIAL_USDT", listed) == "TRUMPUSDT"
assert c.binance_name("SILVER_USDT", listed) == "XAGUSDT"
assert c.binance_name("AKE_USDT", listed) is None

# universe: zero fee only, spread cap, largest first, the top kept
costs = {"A_USDT": {"maker_bp": 0, "taker_bp": 0, "qv": 5, "spread_bp": 1.0},
         "B_USDT": {"maker_bp": 0, "taker_bp": 2, "qv": 9, "spread_bp": 1.0},
         "C_USDT": {"maker_bp": 0, "taker_bp": 0, "qv": 9, "spread_bp": 4.0},
         "D_USDT": {"maker_bp": 0, "taker_bp": 0, "qv": 7, "spread_bp": 3.0}}
u = c.pick(costs, {"AUSDT"}, {"A_USDT": 0.1, "D_USDT": 1.0})
assert list(u) == ["D_USDT", "A_USDT"] and u["A_USDT"] == {"binance": "AUSDT", "size": 0.1} and u["D_USDT"]["binance"] is None

# depth: the touch, five levels, the bucket
frame = {"symbol": "ZEC_USDT", "channel": "push.depth.full", "ts": 7,
         "data": {"cts": 5, "asks": [[11.0, 3, 1], [12.0, 4, 1]], "bids": [[10.0, 1, 1], [9.0, 2, 1]], "version": 1}}
r = c.depth_row(frame, 1234)
assert r == {"b": 1200, "sym": "ZEC_USDT", "bid": 10.0, "ask": 11.0, "bid_v": 1.0, "ask_v": 3.0, "bids5_v": 3.0, "asks5_v": 7.0,
             "e_last": 5, "r_last": 1234}
assert c.depth_row({"symbol": "Z", "data": {"asks": [], "bids": [[1, 1, 1]]}}, 1) is None

# deals: a list or a single trade
one = {"p": 1263.8, "v": 13, "T": 1, "O": 1, "M": 2, "t": 9, "i": "1"}
assert c.deal_rows({"symbol": "Z", "data": [one, one]}, 3)[1] == {"sym": "Z", "t": 9, "r": 3, "p": 1263.8, "v": 13.0, "side": 1}
assert len(c.deal_rows({"symbol": "Z", "data": one}, 3)) == 1

# Binance buckets: quote by side, range, last
k = c.Collector()
k.add_bn("A", 10.0, 2.0, False, 1, 1001)
k.add_bn("A", 12.0, 1.0, True, 2, 1050)
x = k.bn[("A", 1000)]
assert (x["buy_q"], x["sell_q"], x["hi"], x["lo"], x["last"], x["n"]) == (20.0, 12.0, 12.0, 10.0, 12.0, 2)
print("selftest OK")
