"""Are round 27's overpriced favourites real, or a product of the volume floor? (descriptive, development entries only, no trial)

The universe keeps markets by their FINAL volume, which is not known at entry. A surprise draws trading, so near the floor
a market may be in the sample because the favourite lost. If so, the gap between price and win rate shrinks as the floor
rises. The table also splits by the kind of question, read from the slug.

  python diag_bonds.py
"""

from __future__ import annotations

import pandas as pd

import bt_bonds as b

FLOORS = (5e4, 2e5, 1e6, 5e6)
KINDS = {"crypto": "bitcoin|ethereum|solana|xrp|btc|eth-|crypto|-up-or-down",
         "sport": "-vs-|nba|nfl|nhl|mlb|ncaa|premier-league|champions-league|ufc|tennis|epl|-win-on-",
         "politics": "election|president|trump|senate|congress|governor|parliament|minister"}


def rows(markets, px, k, lo, hi):
    out = []
    for mk in markets:
        s = b.signal(mk, *px[mk["id"]], k, 0.6, 0.985)
        if s and lo <= s["te"] < hi and mk["outcome_yes"] != 0.5:
            out.append({"p": s["p"], "won": s["payout"], "volume": mk["volume"], "slug": mk["slug"]})
    return pd.DataFrame(out)


def gap(d):
    return {"n": len(d), "price": round(d["p"].mean(), 4), "won": round(d["won"].mean(), 4),
            "gap_pp": round((d["won"].mean() - d["p"].mean()) * 100, 2)}


def main():
    markets, px = b.load()
    lo, hi = (b.sec(x) for x in b.PERIODS["dev"])
    for k in b.KS.values():
        d = rows(markets, px, k, lo, hi)
        print(f"K = {k} days, favourites priced 0.60-0.985")
        for floor in FLOORS:
            print(f"  volume >= {floor:>12,.0f}", gap(d[d["volume"] >= floor]))
        for name, words in KINDS.items():
            print(f"  {name:<20}", gap(d[d["slug"].str.contains(words, regex=True)]))
        rest = d[~d["slug"].str.contains("|".join(KINDS.values()), regex=True)]
        print(f"  {'other':<20}", gap(rest))


if __name__ == "__main__":
    main()
