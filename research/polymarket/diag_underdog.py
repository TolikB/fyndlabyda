"""Is round 29's holdout pass real? Splits of the passing trial's signals (descriptive, no trial).

A gain of this size is more likely a flaw than an edge, so every split asks where it comes from:
  month        one market regime
  kind         crypto ladders, sports, politics
  question     "by a date" markets can end early, and a moved end date would look ahead
  close - end  whether the market closed on schedule
  side         whether the underdog is the first or the second outcome
  volume, life thin or short-lived markets
Each line: signals, the underdog's mean price, its win rate, and the gap in points.

  python diag_underdog.py
"""

from __future__ import annotations

import pandas as pd

import bt_bonds as b
import bt_underdog as u

DAY = b.DAY
LOW, HIGH = u.BANDS["f70"]
KINDS = {"crypto": "bitcoin|ethereum|solana|xrp|btc|eth-|crypto|-up-or-down",
         "sport": "-vs-|nba|nfl|nhl|mlb|ncaa|premier-league|champions-league|ufc|tennis|epl|-win-on-|spread|o-u-",
         "politics": "election|president|trump|senate|congress|governor|parliament|minister"}


def kind(slug):
    for name, words in KINDS.items():
        if pd.Series([slug]).str.contains(words, regex=True).iloc[0]:
            return name
    return "other"


def question(slug):
    s = "-" + slug + "-"
    if "-by-" in s or "-before-" in s:
        return "by a date"
    return "other"


def rows(markets, px, K):
    out = []
    for mk in markets:
        s = u.signal(mk, *px[mk["id"]], K, LOW, HIGH)
        if s and mk["outcome_yes"] != 0.5:
            out.append({"te": s["te"], "p": s["p"], "won": s["payout"], "yes": s["yes"], "volume": mk["volume"], "slug": mk["slug"],
                        "late_d": (mk["closed"] - mk["end"]) / DAY, "life_d": (mk["end"] - mk["start"]) / DAY, "neg": mk["neg_risk"],
                        "event": mk["event"]})
    d = pd.DataFrame(out)
    d["kind"] = [kind(x) for x in d["slug"]]
    d["question"] = [question(x) for x in d["slug"]]
    d["month"] = pd.to_datetime(d["te"], unit="s").dt.strftime("%Y-%m")
    return d


def show(d, by):
    g = d.groupby(by, observed=True).agg(n=("p", "size"), events=("event", "nunique"), price=("p", "mean"), won=("won", "mean"))
    g["gap_pp"] = (g["won"] - g["price"]) * 100
    print(g.round(3).to_string(), "\n")


def main():
    markets, px = b.load()
    d = rows(markets, px, 3)
    d["period"] = d["te"].map(lambda t: "dev" if t < b.sec(b.PERIODS["dev"][1]) else "holdout")
    d = d[d["te"] < b.sec(b.PERIODS["holdout"][1])]
    d["close_vs_end"] = pd.cut(d["late_d"], [-1e9, -1, -0.1, 0.1, 1, 1e9], labels=["over 1 d early", "up to 1 d early", "on time", "up to 1 d late", "over 1 d late"])
    d["vol"] = pd.cut(d["volume"], [0, 1e5, 3e5, 1e6, 1e12], labels=["<100k", "100-300k", "300k-1M", ">1M"])
    d["life"] = pd.cut(d["life_d"], [0, 7, 30, 90, 1e6], labels=["<7 d", "7-30 d", "30-90 d", ">90 d"])
    for by in ("period", "month", ["period", "kind"], ["period", "question"], ["period", "close_vs_end"], ["period", "yes"],
               ["period", "neg"], ["period", "vol"], ["period", "life"]):
        show(d, by)
    y = d[(d["period"] == "holdout")]
    show(y, ["kind", "question", "yes"])


if __name__ == "__main__":
    main()
