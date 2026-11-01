"""Known-answer checks for bt_badnews.py on real titles and synthetic trades (no network). Prints "selftest OK"."""

import pandas as pd

import bt_badnews as bb

U = {
    "디스트릭트0x(DNT) 거래 유의 종목 지정 안내": ["DNT"],
    "이니시아(INIT) 유의 촉구 안내": ["INIT"],
    "블러(BLUR) 거래지원 종료 안내 (11/30 15:00)": ["BLUR"],
    "소폰(SOPH) 거래지원 종료 안내 (11/16 15:00)": ["SOPH"],
    "리졸브(RESOLV) 거래 유의 종목 지정 안내": ["RESOLV"],
    "엔진코인(ENJ) KRW 마켓 거래지원 종료(10/10 18:00) 및 BTC 마켓 유의 종목 지정 해제 안내": [],
    "이니시아(INIT) 거래 유의 종목 지정 해제 안내": [],
    "에이(AAA) BTC 마켓 거래지원 종료 안내": [],
    "닐리온(NIL) KRW 마켓 디지털 자산 추가": [],
    "ABC(에이비씨), DEF(디이에프) 거래지원 종료 안내 (11/16 15:00)": ["ABC", "DEF"],
}
for title, want in U.items():
    assert bb.parse_upbit_bad(title) == want, (title, bb.parse_upbit_bad(title))

B = {
    "Binance Will Delist ALPACA, PDA, VGX and WING on 2025-05-02": ["ALPACA", "PDA", "VGX", "WING"],
    "Binance Futures Will Delist USDⓈ-M AERGOUSDT Perpetual Contract (2026-07-24)": ["AERGO"],
    "Binance Futures Will Delist USDⓈ-M HIFIUSDT and BAKEUSDT Perpetual Contracts (2025-10-03)": ["HIFI", "BAKE"],
    "Binance Futures Will Delist Multiple USDⓈ-M Perpetual Contracts (2026-01-21)": [],
    "Binance Futures Will Delist and Update the Leverage & Margin Tiers of USDⓈ-M ANTUSDT, DGBUSDT Perpetual "
    "Contracts": [],
    "Binance Futures Will Delist and Update the Leverage & Margin Tiers of USDⓈ-M PHBBUSD, SANDBUSD and UNIBUSD "
    "Perpetual Contracts": [],
    "Binance Will Extend the Monitoring Tag to Include ACT, BLUR, PIVX & QKC on 2026-06-18": ["ACT", "BLUR", "PIVX", "QKC"],
    "Binance Will Delist Margin Pairs on 2025-01-01": [],
    "Binance Will List Genius Terminal (GENIUS)": [],
}
for title, want in B.items():
    assert bb.parse_binance_bad(title) == want, (title, bb.parse_binance_bad(title))

# short entry: the lower of the first trade and the seller VWAP in the queue second; exit at T + 60 s
T = 1_750_000_000_000
rows = [(T - 3000, 100.0, 1.0, False), (T + 2100, 95.0, 1.0, False), (T + 2500, 91.0, 3.0, False),
        (T + 2700, 99.0, 5.0, True), (T + 59_000, 90.0, 1.0, True), (T + 60_400, 88.0, 1.0, True)]
w = pd.DataFrame(rows, columns=["t", "p", "q", "buy"])
assert bb.trade(w, T, 2.0) == (T + 2100, 92.0, T + 60_400, 88.0)          # (95 + 3*91)/4 = 92
w2 = w.copy()
w2.loc[1, "p"] = 80.0
assert bb.trade(w2, T, 2.0)[1] == 80.0
assert bb.trade(w[(w["t"] < T) | (w["t"] > T + 20_000)], T, 5.0) is None
assert bb.trade(w[w["t"] < T + 60_000], T, 2.0) is None
# costs: the short earns the fall and positive funding, pays taker twice and the tier three times
assert abs(bb.net(100, 90, 0.001, 0.001, 1.0) - (0.10 + 0.001 - (0.001 + 0.003))) < 1e-12
assert abs(bb.net(100, 90, 0.0, 0.001, 2.0) - (0.10 - 2 * 0.004)) < 1e-12

print("selftest OK")
