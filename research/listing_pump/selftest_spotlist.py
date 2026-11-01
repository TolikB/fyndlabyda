"""Known-answer checks for bt_spotlist.py on real titles and synthetic bars (no network). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_spotlist as b

MIN, HOUR, DAY = b.MIN, b.HOUR, b.DAY

# Upbit titles, copied from the notice list
U = {
    "KRW, BTC 마켓 디지털 자산 추가 (SOL, MATIC, NU)": ["SOL", "MATIC", "NU"],
    "KRW, BTC 마켓 디지털 자산 추가 (T) 및 이에 따른 NU 유의 종목 지정 안내 (NU <> T 스왑 사이트 추가 안내)": ["T"],
    "닐리온(NIL) KRW 마켓 디지털 자산 추가": ["NIL"],
    "펄(PRL) 신규 거래지원 안내 (KRW, BTC, USDT 마켓) (유통량 이슈에 대한 소명 및 거래지원)": ["PRL"],
    "셀레스티아(TIA)(KRW, BTC, USDT 마켓), 아이오넷(IO)(BTC, USDT 마켓) 신규 거래지원 안내 (주문 타입 제한 관련 안내)": ["TIA"],
    "라이브피어(LPT)(KRW, USDT 마켓), 포켓네트워크(POKT)(KRW 마켓) 디지털 자산 추가": ["LPT", "POKT"],
    "빅타임(BIGTIME), 아카시네트워크(AKT) 신규 거래지원 안내 (KRW, BTC, USDT 마켓) (AKT 거래지원 개시 시점 연기 안내)":
        ["BIGTIME", "AKT"],
    "KRW, BTC, USDT 마켓 디지털 자산 추가 (MNT, POKT) (POKT 거래지원 개시 시점 연기 안내)": ["MNT", "POKT"],
    "인젝티브(INJ) KRW, USDT 마켓 디지털 자산 추가": ["INJ"],
    "엔진코인(ENJ) KRW 마켓 거래지원 종료(10/10 18:00) 및 BTC 마켓 유의 종목 지정 해제 안내": [],
    "테더(USDT) KRW, BTC 마켓 추가 안내 (출금 개시 안내)": [],
    "이더리움(ETH/KRW) 상장": [],
    "PUFFER(퍼퍼), SWELL(스웰네트워크) 신규 거래지원 안내 (BTC, USDT 마켓) (SWELL 국문명 변경 안내)": [],
    "BTC 마켓 디지털 자산 추가 (피카 PICA)": [],
    "블러(BLUR) 거래지원 종료 안내 (11/30 15:00)": [],
}
for title, want in U.items():
    assert b.parse_upbit(title) == want, (title, b.parse_upbit(title))
assert b.parse_upbit("ABC(에이비씨), DEF(디이에프) 신규 거래지원 안내 (KRW 마켓)") == ["ABC", "DEF"]

B = {
    "Binance Will List Genius Terminal (GENIUS) and OpenGradient (OPG) with Seed Tag Applied": ["GENIUS", "OPG"],
    "Binance Will List First Neiro on Ethereum (NEIRO), Turbo (TURBO), and Baby Doge Coin (1MBABYDOGE) with Seed Tag "
    "Applied": ["NEIRO", "TURBO", "1MBABYDOGE"],
    "Binance Will List Osmosis (OSMO) in the Innovation Zone": ["OSMO"],
    "Binance Will List BNBUP, BNBDOWN, XTZUP and XTZDOWN Binance Leveraged Tokens": [],
    "Binance Will List PAX/BNB Trading Pair": [],
    "Binance Will List AAVEUP & AAVEDOWN Binance Leveraged Tokens": [],
    "Binance Futures Will Launch USDⓈ-M ABC Perpetual Contract": [],
    "Binance Will Add 5 bStocks Tokenized Securities as Collateral Asset - 2026-10-28": [],
}
for title, want in B.items():
    assert b.parse_binance(title) == want, (title, b.parse_binance(title))

# KST -> UTC, the entry minute
assert b.kst_ms("2026-10-30T16:00:00+09:00") == int(pd.Timestamp("2026-10-30 07:00:00").value // 1_000_000)
T = int(pd.Timestamp("2025-01-31 08:17:39").value // 1_000_000)
assert b.minute_at_or_after(T + b.DELAY_LONG) == int(pd.Timestamp("2025-01-31 08:19:00").value // 1_000_000)
assert b.minute_at_or_after(T // MIN * MIN) == T // MIN * MIN

# perps: plain, 1000-prefix, too young, excluded, dedupe within 72h
first = {"AUSDT": 0, "1000BUSDT": 0, "CUSDT": T - DAY, "USDCUSDT": 0}
assert b.perp_for("A", first, T) == ("AUSDT", None) and b.perp_for("B", first, T) == ("1000BUSDT", None)
assert b.perp_for("C", first, T) == (None, "young") and b.perp_for("USDC", first, T) == (None, "no perp")
anns = [{"src": "upbit", "ann": "u1", "title": "x", "T": T, "tickers": ["A", "C"]},
        {"src": "binance", "ann": "b1", "title": "y", "T": T + 10 * HOUR, "tickers": ["A"]},
        {"src": "binance", "ann": "b2", "title": "z", "T": T + 80 * HOUR, "tickers": ["A", "Q"]}]
ev = b.events(anns, first)
assert list(ev["sym"].fillna("-")) == ["AUSDT", "-", "-", "AUSDT", "-"]
assert list(ev["reason"].fillna("-")) == ["-", "young", "dedupe", "-", "no perp"]

# synthetic minute bars
t0 = T // MIN * MIN - 2 * HOUR
n = 3 * 24 * 60
t = t0 + np.arange(n) * MIN
o = np.full(n, 100.0)
bars = {"t": t.astype(np.int64), "o": o.copy(), "h": o + 1, "l": o - 1, "c": o.copy(), "qv": np.full(n, 1e5)}
e1 = b.minute_at_or_after(T + b.DELAY_LONG)
i1 = int((e1 - t0) // MIN)
bars["o"][i1], bars["o"][i1 + 15], bars["o"][i1 + 60] = 100.0, 103.0, 98.0
assert b.trade(bars, T, "long", b.DELAY_LONG, 15 * MIN) == (e1, 100.0, e1 + 15 * MIN, 103.0, False)
assert b.trade(bars, T, "long", b.DELAY_LONG, 60 * MIN)[3] == 98.0
# a missing exit bar uses the next bar within an hour; beyond that the trade is skipped
keep = np.ones(n, bool)
keep[i1 + 15:i1 + 20] = False
gap = {k: v[keep] for k, v in bars.items()}
assert b.trade(gap, T, "long", b.DELAY_LONG, 15 * MIN)[2] == e1 + 20 * MIN
keep[i1 + 15:i1 + 80] = False
assert b.trade({k: v[keep] for k, v in bars.items()}, T, "long", b.DELAY_LONG, 15 * MIN) is None
# no volume at entry: skipped
zero = {k: v.copy() for k, v in bars.items()}
zero["qv"][i1] = 0
assert b.trade(zero, T, "long", b.DELAY_LONG, 15 * MIN) is None

# fade: stop at 1.30x, a gap above the stop exits at the open
ef = b.minute_at_or_after(T + b.DELAY_FADE)
jf = int((ef - t0) // MIN)
fb = {k: v.copy() for k, v in bars.items()}
assert b.trade(fb, T, "fade", b.DELAY_FADE, b.FADE_H) == (ef, 100.0, ef + b.FADE_H, 100.0, False)
fb["h"][jf + 100] = 131.0
assert b.trade(fb, T, "fade", b.DELAY_FADE, b.FADE_H) == (ef, 100.0, int(t[jf + 100]), 130.0, True)
fb["o"][jf + 100], fb["h"][jf + 100] = 150.0, 155.0
assert b.trade(fb, T, "fade", b.DELAY_FADE, b.FADE_H)[3] == 150.0

# costs: longs pay the tier twice at entry, shorts once a side; funding sign
assert abs(b.net("long", 100, 103, 0.001, 0.0010, 1.0) - (0.03 - 0.001 - (0.001 + 0.003))) < 1e-12
assert abs(b.net("long", 100, 103, 0.0, 0.0010, 2.0) - (0.03 - 2 * 0.004)) < 1e-12
assert abs(b.net("fade", 100, 90, 0.001, 0.0010, 1.0) - (0.10 + 0.001 - 0.003)) < 1e-12

# notices average their tickers; the book caps open announcements at 3; the rate counts 2024-07..2025-06
d0 = int(pd.Timestamp("2024-08-01").value // 1_000_000)
tr = pd.DataFrame({"ann": ["a", "a", "b", "c", "d", "e"], "T": [d0, d0, d0, d0, d0, d0 + 5 * DAY],
                   "entry_t": [d0, d0, d0 + 1, d0 + 2, d0 + 3, d0 + 5 * DAY],
                   "exit_t": [d0 + HOUR] * 5 + [d0 + 5 * DAY + HOUR],
                   "ret1": [0.02, 0.04, 0.01, 0.01, 0.05, -0.01]})
nt = b.notices(tr, "ret1")
assert abs(nt.set_index("ann").loc["a", "ret"] - 0.03) < 1e-12
acc, daily = b.book(nt, 2)
assert list(acc["ann"]) == ["a", "b", "c", "e"]                      # d arrives while 3 are open
assert abs(daily.iloc[0] - 2 * (0.03 + 0.01 + 0.01)) < 1e-12 and abs(daily.iloc[1] + 0.02) < 1e-12
assert b.rate(acc) == 4 / 12

# verdict: s* is the smallest size reaching 5%; drawdown and 2x checked at s*
mk = lambda figs, dd: {str(s): {"monthly_figure_pct": f, "book": {"max_dd_pct": dd, "monthly_mean_pct": f}}
                       for s, f in zip(b.SIZES, figs)}
r = {"1x": {"mean_pct": 1.0, "t_ann": 3.0, "sizes": mk([3, 6, 9], 20)}, "2x": {"sizes": mk([1, 2, 3], 30)}}
assert b.passes(r) == (True, 2)
r["1x"]["t_ann"] = 2.4
assert b.passes(r) == (False, None)
r["1x"]["t_ann"], r["1x"]["sizes"] = 3.0, mk([3, 6, 9], 40)
assert b.passes(r) == (False, 2)
r["1x"]["sizes"], r["2x"]["sizes"] = mk([1, 2, 4], 10), mk([0, 0, 0], 10)
assert b.passes(r) == (False, None)

print("selftest OK")
