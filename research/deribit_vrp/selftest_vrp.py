"""Known-answer checks for bt_vrp.py on synthetic trades (no data needed). Prints "selftest OK"."""

import math

import pandas as pd

import bt_vrp as v


def close(x, y):
    return abs(x - y) < 1e-9


assert v.parse_name("BTC-6JAN23-16750-P") == (pd.Timestamp("2023-01-06"), 16750.0, "P")
assert v.parse_name("ETH-27DEC24-3500-C") == (pd.Timestamp("2024-12-27"), 3500.0, "C")

F = pd.Timestamp("2023-01-06")
IV = 50.0
SW = IV / 100 * math.sqrt(7 / 365)


def trade(name, price, mark, direction, iv=IV, index=20000.0):
    return {"instrument_name": name, "price": price, "mark_price": mark, "iv": iv, "index_price": index,
            "direction": direction, "amount": 1.0}


up, dn = round(20000 * math.exp(SW) / 500) * 500, round(20000 * math.exp(-SW) / 500) * 500
up2, dn2 = round(20000 * math.exp(2 * SW) / 500) * 500, round(20000 * math.exp(-2 * SW) / 500) * 500
rows = [
    trade("BTC-13JAN23-20000-C", 0.030, 0.031, "sell"), trade("BTC-13JAN23-20000-C", 0.032, 0.031, "buy"),
    trade("BTC-13JAN23-20000-P", 0.029, 0.030, "sell"),
    trade(f"BTC-13JAN23-{up}-C", 0.010, 0.011, "buy"),           # no taker sell: fallback mark -10%
    trade(f"BTC-13JAN23-{dn}-P", 0.009, 0.0095, "sell"),
    trade(f"BTC-13JAN23-{up2}-C", 0.003, 0.0028, "buy"), trade(f"BTC-13JAN23-{dn2}-P", 0.0025, 0.0024, "buy"),
    trade("BTC-20JAN23-20000-C", 0.5, 0.5, "sell", iv=999),       # another expiry: ignored
    trade("BTC_USDC-13JAN23-20000-C", 600.0, 600.0, "sell", iv=999),  # USDC-margined: ignored
]
tr, s0, sw = v.week_setup(pd.DataFrame(rows), F)
assert close(s0, 20000.0) and close(sw, SW) and (tr["expiry"] == pd.Timestamp("2023-01-13")).all()
assert not tr["instrument_name"].str.startswith("BTC_USDC").any()

legs = v.legs_for(tr, s0, sw, "straddle")
assert [(l[0], l[1], l[4]) for l in legs] == [("BTC-13JAN23-20000-C", -1, 0.030), ("BTC-13JAN23-20000-P", -1, 0.029)]
legs = v.legs_for(tr, s0, sw, "strangle")
assert legs[0][2] == up and close(legs[0][4], 0.011 * 0.9) and legs[1][2] == dn and close(legs[1][4], 0.009)
legs = v.legs_for(tr, s0, sw, "condor")
assert [l[2] for l in legs] == [up, dn, up2, dn2] and close(legs[2][4], 0.003) and close(legs[3][4], 0.0025)

# straddle settled at 21,000: premium 0.059 BTC * 20,000 = 1,180; pays 1,000 on the call
straddle = [("c", -1, 20000.0, "C", 0.030, 0.031), ("p", -1, 20000.0, "P", 0.029, 0.030)]
fees = 2 * min(0.0003 * 20000, 0.125 * 0.03 * 20000) + min(0.00015 * 21000, 0.125 * 1000)
assert close(v.week_return(straddle, 20000, 21000, 1.0), (1180 - 1000 - fees) / 20000)
# 2x: each sell moves once more by its distance from the mark, fees double
prem2 = (0.029 + 0.028) * 20000
assert close(v.week_return(straddle, 20000, 21000, 2.0), (prem2 - 1000 - 2 * fees) / 20000)
# a long wing pays: bought at 0.003, settles 1,000 in the money
wing = [("w", 1, 25000.0, "C", 0.003, 0.0028)]
fee_w = min(0.0003 * 20000, 0.125 * 60) + min(0.00015 * 26000, 0.125 * 1000)
assert close(v.week_return(wing, 20000, 26000, 1.0), (-60 + 1000 - fee_w) / 20000)
# the fee cap binds on a cheap leg: 12.5% of a 2 USD premium
cheap = [("x", -1, 30000.0, "C", 0.0001, 0.0001)]
assert close(v.week_return(cheap, 20000, 20000, 1.0), (2 - 0.25) / 20000)

# book: leverage scales returns; a loss beyond equity is capped at -100%
w = pd.Series([0.01, -0.02, -0.8], index=pd.to_datetime(["2023-01-13", "2023-01-20", "2023-01-27"]))
d = v.book(w, 2.0)
assert close(d.sum(), 0.02 - 0.04 - 1.0) and close(d.loc["2023-01-20"], -0.04) and len(d) == 22

print("selftest OK")
