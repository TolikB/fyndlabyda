"""Quoted spreads of MEXC's low-fee USDT contracts: the cost table of rounds 30 to 33 (descriptive, no trial).

The test rounds read this table and never recompute it, so their costs are fixed before they see a price. For every USDT
contract with a taker fee of at most 2 bp and at least MIN_QV of 24 h volume: the median quoted spread on a 3 s grid, each
quote carried at most 5 minutes, so that a quiet spell counts for its length.

  python mexc_costs.py [hours]     writes mexc_costs.json, prints the zero-fee list
"""

from __future__ import annotations

import glob
import json
import sys

import numpy as np
import pandas as pd

import pilot_mexc as p

MIN_QV = 1e6
CARRY = 100


def spread_bp(g):
    """g: bid and ask by 3 s bin. -> median spread in bp over the bins from the first quote on."""
    x = g.reindex(pd.RangeIndex(g.index.min(), g.index.max() + 1)).ffill(limit=CARRY).dropna()
    return float(((x["ask"] / x["bid"] - 1) * 1e4).median())


def main():
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 24
    d = p.fees()
    slow = pd.concat(pd.read_parquet(f) for f in sorted(glob.glob(f"{p.DATA}/slow/*/*.parquet"))[-12:]).astype({"v": "object", "s": "object"})
    d["qv"] = d["symbol"].map(slow[slow["v"] == "mexc"].groupby("s")["qv"].last()).fillna(0.0)
    d["maker_bp"] = d["makerFeeRate"].astype(float) * 1e4
    d["taker_bp"] = d["takerFeeRate"].astype(float) * 1e4
    d = d[d["symbol"].str.endswith("_USDT") & (d["taker_bp"] <= 2) & (d["qv"] >= MIN_QV) & d["apiAllowed"]]
    q = p.quotes(hours, d["symbol"].tolist())
    d["spread_bp"] = d["symbol"].map(lambda s: spread_bp(q[s]) if s in q else np.nan)
    d = d.dropna(subset=["spread_bp"]).sort_values("qv", ascending=False)
    table = {r["symbol"]: {"maker_bp": r["maker_bp"], "taker_bp": r["taker_bp"], "qv": round(r["qv"]), "spread_bp": round(r["spread_bp"], 3)}
             for _, r in d.iterrows()}
    with open(f"{p.HERE}/mexc_costs.json", "w") as fh:
        json.dump({"hours": hours, "day": str(pd.Timestamp.now(tz="UTC").date()), "contracts": table}, fh, indent=1)
    free = d[d["taker_bp"] == 0]
    print(len(d), "contracts,", len(free), "at 0/0")
    print("zero-fee, spread bp quantiles", free["spread_bp"].quantile([.1, .25, .5, .75, .9]).round(2).to_dict())
    print("zero-fee with spread <= 3 bp:", int((free["spread_bp"] <= 3).sum()), "| <= 1.5 bp:", int((free["spread_bp"] <= 1.5).sum()))
    print(free[["symbol", "qv", "spread_bp"]].head(45).round(2).to_string(index=False))


if __name__ == "__main__":
    main()
