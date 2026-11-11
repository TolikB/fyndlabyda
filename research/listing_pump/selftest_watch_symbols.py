"""Known-answer checks for watch_symbols.py (no network). Prints "selftest OK"."""

import json
import os
import tempfile

import watch_symbols as w

spot = json.dumps({"symbols": [{"symbol": "AUSDT", "status": "BREAK", "baseAsset": "A", "quoteAsset": "USDT"},
                               {"symbol": "BUSDT", "status": "BREAK", "baseAsset": "B", "quoteAsset": "USDT"}]})
s0 = w.parse("spot_break", spot)
assert s0 == {"AUSDT": {"status": "BREAK", "base": "A", "quote": "USDT"},
              "BUSDT": {"status": "BREAK", "base": "B", "quote": "USDT"}}
fut = json.dumps({"symbols": [{"symbol": "XUSDT", "status": "PENDING_TRADING", "baseAsset": "X", "quoteAsset": "USDT",
                               "contractType": "PERPETUAL", "onboardDate": 1794000000000, "underlyingType": "COIN"}]})
assert w.parse("futures", fut)["XUSDT"]["onboard"] == 1794000000000
cb = json.dumps([{"id": "X-USD", "status": "online", "trading_disabled": True, "auction_mode": False,
                  "cancel_only": False, "post_only": False, "limit_only": False}])
assert w.parse("coinbase", cb)["X-USD"]["trading_disabled"] is True

s1 = dict(s0)
del s1["BUSDT"]
s1["CUSDT"] = {"status": "BREAK", "base": "C", "quote": "USDT"}
s1["AUSDT"] = {"status": "TRADING", "base": "A", "quote": "USDT"}
added, removed, changed = w.diff(s0, s1)
assert added == [{"symbol": "CUSDT", "status": "BREAK", "base": "C", "quote": "USDT"}]
assert removed == ["BUSDT"]
assert changed == [["AUSDT", s0["AUSDT"], s1["AUSDT"]]]
assert w.diff(s0, s0) == ([], [], [])
assert w.counts(s1) == {"TRADING": 1, "BREAK": 1}

with tempfile.TemporaryDirectory() as d:
    w.WATCH = d
    ep = w.Endpoint("futures", "fapi.binance.com", "/fapi/v1/exchangeInfo", 5.0)
    assert ep.next_wait(200, b"", {}) == 5.0
    assert ep.next_wait(500, b"", {}) == 10.0 and ep.next_wait(None, b"", {}) == 20.0
    assert ep.next_wait(200, b"", {}) == 5.0 and ep.fails == 0
    assert ep.next_wait(429, b"", {"retry-after": "7200"}) == 7200.0
    assert ep.next_wait(418, b"", {}) == 3600.0
    assert ep.next_wait(503, b"<html>cloudflare</html>", {}) == 3600.0
    rows = [json.loads(x) for x in open(os.path.join(d, "blocked.jsonl"))]
    assert [r["status"] for r in rows] == [429, 418, 503] and rows[0]["pause_s"] == 7200.0

print("selftest OK")
