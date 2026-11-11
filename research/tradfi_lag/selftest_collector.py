"""Known-answer checks for collector.py on canned frames (no network). Prints "selftest OK"."""

import collector as c

# pairs: same ticker, an alias, a delisted market, a crypto perp of the same name, a halted symbol; depth for the busiest
c.DEPTH_TOP = 2
symbols = [{"symbol": "NVDAUSDT", "baseAsset": "NVDA", "quoteAsset": "USDT", "contractType": "TRADIFI_PERPETUAL",
            "status": "TRADING", "underlyingType": "EQUITY"},
           {"symbol": "XAUUSDT", "baseAsset": "XAU", "quoteAsset": "USDT", "contractType": "TRADIFI_PERPETUAL",
            "status": "TRADING", "underlyingType": "COMMODITY"},
           {"symbol": "TSLAUSDT", "baseAsset": "TSLA", "quoteAsset": "USDT", "contractType": "TRADIFI_PERPETUAL",
            "status": "TRADING", "underlyingType": "EQUITY"},
           {"symbol": "BBUSDT", "baseAsset": "BB", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
           {"symbol": "MUUSDT", "baseAsset": "MU", "quoteAsset": "USDT", "contractType": "TRADIFI_PERPETUAL",
            "status": "SETTLING", "underlyingType": "EQUITY"},
           {"symbol": "COINUSDT", "baseAsset": "COIN", "quoteAsset": "USDT", "contractType": "TRADIFI_PERPETUAL",
            "status": "TRADING", "underlyingType": "EQUITY"}]
universe = [{"name": "xyz:NVDA", "maxLeverage": 20, "growthMode": "enabled", "deployerFeeScale": "0.5"},
            {"name": "xyz:GOLD", "maxLeverage": 25},
            {"name": "xyz:TSLA", "maxLeverage": 20, "growthMode": "enabled"}, {"name": "xyz:BB", "maxLeverage": 10},
            {"name": "xyz:MU", "maxLeverage": 10}, {"name": "xyz:COIN", "maxLeverage": 10, "isDelisted": True},
            {"name": "xyz:JP225", "maxLeverage": 20}]
p = c.pairs_from(symbols, universe, {"NVDAUSDT": 9.0, "XAUUSDT": 7.0, "TSLAUSDT": 1.0})
assert sorted(p) == ["xyz:GOLD", "xyz:NVDA", "xyz:TSLA"], sorted(p)
assert p["xyz:GOLD"] == {"binance": "XAUUSDT", "type": "COMMODITY", "growth": False, "fee_scale": 1.0, "max_lev": 25,
                         "volume": 7.0, "depth": True}
assert p["xyz:NVDA"]["growth"] and p["xyz:NVDA"]["depth"] and not p["xyz:TSLA"]["depth"] and p["xyz:NVDA"]["fee_scale"] == 0.5

# depth: the touch whatever the order of levels, quote on five levels, an empty side
frame = {"e": "depthUpdate", "E": 5, "s": "NVDAUSDT", "b": [["99.0", "2"], ["100.0", "1"]], "a": [["101.0", "3"], ["102.0", "1"]]}
assert c.depth_cols(frame) == {"bid": 100.0, "ask": 101.0, "bid_sz": 1.0, "ask_sz": 3.0, "bids5_q": 298.0, "asks5_q": 405.0}
assert c.depth_cols({"b": [], "a": [["1", "1"]]}) is None

# HL trades: a coin's first frame on a connection is the batch of past trades; later, a trade over 10 s old is dropped
seen = set()
old = [{"coin": "xyz:NVDA", "side": "B", "px": "100", "sz": "1", "time": 1}]
assert c.fresh_trades(old, seen, 500) == [] and c.fresh_trades(old, seen, 500) == old and c.fresh_trades([], seen, 500) == []
assert c.fresh_trades(old + [{"coin": "xyz:NVDA", "time": 5000}], seen, 10_002) == [{"coin": "xyz:NVDA", "time": 5000}]

# handlers: trades by side, the last book of a bucket, bad frames counted without a reconnect
k = c.Collector()
k.on_bn_trade({"data": {"e": "aggTrade", "s": "A", "p": "10", "q": "2", "m": False, "E": 1}}, 1001, None)
k.on_bn_trade({"data": {"e": "aggTrade", "s": "A", "p": "12", "q": "1", "m": True, "E": 2}}, 1050, None)
x = k.bn.rows[("A", 1000)]
assert (x["buy_q"], x["sell_q"], x["hi"], x["lo"], x["last"], x["n"], x["e_last"], x["r_last"]) == (20.0, 12.0, 12.0, 10.0, 12.0, 2, 2, 1050)
assert (x["buy_px"], x["sell_px"]) == (10.0, 12.0)
k.on_bn_trade({"data": {"e": "aggTrade", "s": "A", "p": "11", "q": "1", "m": False, "E": 3}}, 1100, None)
assert k.bn.rows[("A", 1100)]["buy_px"] == 11.0 and k.bn.rows[("A", 1100)]["sell_px"] != k.bn.rows[("A", 1100)]["sell_px"]
k.on_bn_depth({"data": frame}, 1010, None)
k.on_bn_depth({"data": {**frame, "E": 6, "b": [["100.5", "4"]]}}, 1090, None)
assert k.bd.rows[("NVDAUSDT", 1000)] == {"b": 1000, "sym": "NVDAUSDT", "bid": 100.5, "ask": 101.0, "bid_sz": 4.0, "ask_sz": 3.0,
                                         "bids5_q": 402.0, "asks5_q": 405.0, "e_last": 6, "r_last": 1090}
seen = set()
bbo = {"channel": "bbo", "data": {"coin": "xyz:NVDA", "time": 7, "bbo": [{"px": "100", "sz": "5", "n": 1}, {"px": "100.1", "sz": "6", "n": 2}]}}
k.on_hl(bbo, 1234, seen)
assert k.hl.rows[("xyz:NVDA", 1200)] == {"b": 1200, "coin": "xyz:NVDA", "bid": 100.0, "ask": 100.1, "bid_sz": 5.0, "ask_sz": 6.0,
                                         "h_last": 7, "r_last": 1234}
k.on_hl({"channel": "bbo", "data": {"coin": "xyz:NVDA", "time": 8, "bbo": [None, {"px": "1", "sz": "1"}]}}, 1300, seen)
assert ("xyz:NVDA", 1300) not in k.hl.rows
live = {"channel": "trades", "data": [{"coin": "xyz:NVDA", "side": "A", "px": "100", "sz": "2", "time": 9},
                                      {"coin": "xyz:NVDA", "side": "B", "px": "101", "sz": "1", "time": 9}]}
k.on_hl(live, 1400, seen)
assert k.hlt.rows == {} and k.n["hlt_dropped"] == 2
k.on_hl(live, 1450, seen)
y = k.hlt.rows[("xyz:NVDA", 1400)]
assert (y["buy_q"], y["sell_q"], y["last"], y["n"], y["h_last"]) == (101.0, 200.0, 101.0, 2, 9)
assert (y["buy_px"], y["sell_px"]) == (101.0, 100.0)
k.on_hl(live, 20_000, seen)
assert k.n["hlt_dropped"] == 4 and ("xyz:NVDA", 20_000) not in k.hlt.rows
k.on_hl({"channel": "subscriptionResponse", "data": {"method": "subscribe"}}, 1500, seen)
assert k.n == {"bn_msgs": 3, "bd_msgs": 2, "hl_msgs": 1, "hlt_msgs": 2, "hlt_dropped": 4, "bn_reconnects": 0, "bd_reconnects": 0,
               "hl_reconnects": 0, "errors": 0}
assert len(k.hlt.take()) == 1 and k.hlt.rows == {}
print("selftest OK")
