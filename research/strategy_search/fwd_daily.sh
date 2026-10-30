#!/bin/sh
# Forward paper test of the frozen four-sleeve combination (research only, no orders).
# Fetches the newly closed Binance daily bars and funding, then appends the newly
# final days to ledger.jsonl and rewrites report.json.
set -e
date -u +%FT%TZ
cd /opt/funding_arbitrage_research/fwd
docker run --rm --cpus 2 --memory 4g --name funding_research_fwd \
  -v /opt/funding_arbitrage_research:/w -w /w/code funding-research:py312 \
  sh -c "python fwd_update.py /w/fwd && python fwd_run.py /w/fwd/data"
