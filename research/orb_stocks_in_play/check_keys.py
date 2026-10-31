"""Check that ~/.alpaca/keys.env works for historical SIP data, without printing the keys."""

import alpaca_data

c = alpaca_data.Client()
b = c.bars(["AAPL"], "5Min", "2016-01-04T14:30:00Z", "2016-01-04T14:34:00Z")
rows = b.get("AAPL") or []
print("keys OK: 2016-01-04 AAPL first 5-minute bar", rows[0] if rows else "missing")
acct_assets = c.assets("active")
print("asset list OK:", len(acct_assets), "active US equities")
