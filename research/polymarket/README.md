# Polymarket favourites and underdogs (rounds 27 and 29, research only)

No orders are placed anywhere, and no keys are used.

**Access.** Polymarket's [own list](https://docs.polymarket.com/polymarket-learn/FAQ/geoblocking) names only some regions of
Ukraine, but the network of the workstation behind this project sends every Polymarket host to a third-party address, which
looks like a provider's block. These files therefore say nothing about whether the account holder may trade there, and
nothing here suggests working around a block.

| File | Purpose |
|---|---|
| `download_markets.py` | Market metadata from Gamma, in windows because its offsets stop at about 2,000 |
| `download_prices.py` | Hourly prices from the CLOB, one range a market |
| `bt_bonds.py`, `selftest_bonds.py` | Round 27: buy favourites priced 0.80..0.985 before the scheduled end |
| `diag_bonds.py` | Round 27's gap by volume floor and kind of market (descriptive, no trial) |
| `results_bonds_dev.json`, `dev.log` | Round 27's development results |
| `bt_underdog.py`, `selftest_underdog.py` | Round 29: buy the underdog 1 or 3 days before the end |
| `results_underdog_*.json`, `*_underdog.log` | Round 29's results |
| `diag_underdog.py` | Round 29's signals by month, kind and market (descriptive, no trial) |
| `sample_all.py`, `sample_all.txt` | All markets of sample days with no volume floor (descriptive, no trial) |

## Round 27, 2026-11-05: all 9 trials fail development, with the wrong sign

The rules are in the docstring of `bt_bonds.py`, committed before any price (19af3d6). 105,505 markets have history. Every
trial loses, between −0.9% and −4.0% a month at f = 5%, and the holdout was not opened.

Favourites win less often than their price says, on development entries, 2024–2025:

| Favourite's price | 1 day before | 3 days before | 7 days before |
|---|---:|---:|---:|
| 0.60–0.70 | −3.0 pp | −2.4 pp | −2.6 pp |
| 0.70–0.80 | −3.2 pp | −3.8 pp | −1.1 pp |
| 0.80–0.90 | −3.1 pp | −3.1 pp | −1.3 pp |
| 0.90–0.95 | −1.1 pp | −1.7 pp | −0.5 pp |
| 0.95–0.985 | −0.2 pp | −1.0 pp | −0.4 pp |
| 0.985 and over | +0.1 pp | 0.0 pp | +0.1 pp |

Cells are the win rate less the price. So the studies that find favourites underpriced do not hold here.

## The gap is a selection on final volume, so neither round says anything

The universe keeps markets by final volume, which is unknown at entry, and a surprise draws trading. A first check raised the
floor, saw the gap stay and concluded the floor was not the cause. That check cannot work: volumes are heavy-tailed, so a surprise
that multiplies volume changes the odds of inclusion alike at any floor.

`sample_all.py` (`sample_all.txt`) takes every market of 14 sample days with no floor, 43,967 with a price 3 days before the end.
Favourites priced 0.70–0.80, win rate less price:

| Final volume | Markets | Gap |
|---|---:|---:|
| all | 3,928 | +6.2 pp |
| under $1,000 | 2,432 | +10.5 pp |
| $1,000–10,000 | 780 | +2.8 pp |
| $10,000–50,000 | 439 | −2.5 pp |
| $50,000 and over | 277 | −7.3 pp |

The gap falls steadily with final volume and changes sign. The markets over the floor are there partly because the favourite lost.

- **Round 29 fails, despite its formal pass.** The rule fixed before this table said a gap under 2 pp or of the other sign makes the
  pass an artefact. Its +84% a month at t 5.52 (holdout, `underdog|k3|f70`) came from choosing markets by a number known only afterwards.
  A live book cannot do that.
- **Round 27's failure means little too.** The same selection worked against the favourites.
- **The +5 pp over all markets is no edge.** Half these markets end with $10 of volume or less, so their prices cannot be traded.

A fair test would need each market's volume up to the entry, and access to the venue. Neither is in hand, so this line stops here.

**Lesson:** a universe filter must use what is known at entry. A pass this large was a flaw, and only an unfiltered sample could show it.

## Notes

- `prices-history` with `interval=max` returns only the last 31 days at hourly fidelity, empty on older markets. Ask with
  `startTs` and `endTs`: 15 days works on every quarter since 2024, 21 days is a 400. The first run had no development prices
  and gave 0 signals; its table was deleted unread, because all its prices lay in the holdout.
- Hourly prices are not quotes, and the 1x cost assumes a half spread of 0.5 cent.
