"""Known-answer checks for collector.py on canned venue payloads (no network). Prints "selftest OK"."""

import math

import collector as c

# one parser per venue: symbol, bid, ask, sizes, venue time; quarterly and USDC contracts stay out
r = c.p_binance([{"symbol": "BTCUSDT", "bidPrice": "10.5", "askPrice": "10.6", "bidQty": "2", "askQty": "3", "time": 5},
                 {"symbol": "BTCUSDC", "bidPrice": "1", "askPrice": "2", "bidQty": "1", "askQty": "1", "time": 5}])
assert r == [("BTCUSDT", 10.5, 10.6, 2.0, 3.0, 5.0)]

r = c.p_bybit({"time": 7, "result": {"list": [{"symbol": "ETHUSDT", "bid1Price": "1", "ask1Price": "2", "bid1Size": "3",
                                               "ask1Size": "4"}, {"symbol": "BTC-26DEC25"}]}})
assert r == [("ETHUSDT", 1.0, 2.0, 3.0, 4.0, 7.0)]

r = c.p_okx({"data": [{"instId": "BTC-USDT-SWAP", "bidPx": "1", "askPx": "2", "bidSz": "3", "askSz": "4", "ts": "9"},
                      {"instId": "BTC-USD-SWAP", "bidPx": "1", "askPx": "2"}]})
assert r == [("BTC-USDT-SWAP", 1.0, 2.0, 3.0, 4.0, 9.0)]

r = c.p_gate([{"contract": "BTC_USDT", "highest_bid": "1", "lowest_ask": "2"}])
assert r[0][:3] == ("BTC_USDT", 1.0, 2.0) and math.isnan(r[0][3]) and math.isnan(r[0][5])

r = c.p_bitget({"data": [{"symbol": "BTCUSDT", "bidPr": "1", "askPr": "2", "bidSz": "3", "askSz": "4", "ts": "9"}]})
assert r == [("BTCUSDT", 1.0, 2.0, 3.0, 4.0, 9.0)]

r = c.p_mexc({"data": [{"symbol": "BTC_USDT", "bid1": 1, "ask1": 2, "timestamp": 9}, {"symbol": "BTC_USDC", "bid1": 1, "ask1": 2},
                       {"symbol": "BTC_EUR", "bid1": 1, "ask1": 2}]})
assert len(r) == 2 and r[0][:3] == ("BTC_USDT", 1.0, 2.0) and r[0][5] == 9.0

r = c.p_kucoin({"data": [{"symbol": "XBTUSDTM", "bestBidPrice": "1", "bestAskPrice": "2", "bestBidSize": 3,
                          "bestAskSize": 4, "ts": 9_000_000}, {"symbol": "XBTUSDM"}]})
assert r == [("XBTUSDTM", 1.0, 2.0, 3.0, 4.0, 9.0)]

# slow tables: 24 h quote volume and funding
assert c.s_okx({"data": [{"instId": "A", "volCcy24h": "10", "last": "2"}]})[0][:2] == ("A", 20.0)
assert c.s_gate([{"contract": "A", "volume_24h_quote": "5", "funding_rate": "0.1"}]) == [("A", 5.0, 0.1)]
assert c.s_mexc({"data": [{"symbol": "A", "amount24": 5, "fundingRate": 0.1}]}) == [("A", 5.0, 0.1)]

# only moved, valid quotes pass; a crossed or empty book is dropped and leaves the state alone
last = {}
row = ("A", 1.0, 2.0, 1.0, 1.0, 0.0)
assert c.changed([row], last) == [row]
assert c.changed([row], last) == []
assert c.changed([("A", 1.0, 2.0, 9.0, 9.0, 1.0)], last) == []     # a new size alone is not a new price
assert c.changed([("A", 3.0, 2.0, 1.0, 1.0, 0.0), ("B", c.NAN, 2.0, 1.0, 1.0, 0.0), ("C", 0.0, 2.0, 1.0, 1.0, 0.0)], last) == []
assert last == {"A": (1.0, 2.0)}
assert len(c.changed([("A", 1.5, 2.0, 1.0, 1.0, 0.0)], last)) == 1

# Lighter ticker: market from the channel, microseconds to milliseconds, an empty side skipped
m = {"channel": "ticker:145", "ticker": {"s": "WTI", "a": {"price": "88.399", "size": "7"}, "b": {"price": "88.392", "size": "3"},
                                         "last_updated_at": 1793871895216051}}
assert c.lt_ticker(m) == (145, 88.392, 88.399, 3.0, 7.0, 1793871895216)
assert c.lt_ticker({"channel": "ticker:1", "ticker": {"a": {}, "b": {"price": "1", "size": "1"}}}) is None

# 100 ms buckets keep the last state per market and bucket
s = c.Store()
s.bucket("BTC", 1.0, 2.0, 1.0, 1.0, 5, 1000)
s.bucket("BTC", 3.0, 4.0, 1.0, 1.0, 6, 1099)
s.bucket("BTC", 5.0, 6.0, 1.0, 1.0, 7, 1100)
s.add("polls", [(1, 2, "binance", 3, 1, True)])
tables, lt = s.take()
assert [x["bid"] for x in lt] == [3.0, 5.0] and [x["b"] for x in lt] == [1000, 1100]
assert len(tables["polls"]) == 1 and s.take() == ({"bbo": [], "polls": [], "slow": [], "rtt": []}, [])
print("selftest OK")
