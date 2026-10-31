#!/bin/sh
# Build, self-test and (re)start the collector on the VM. Touches only the hl-liqmap container.
set -eu
cd /opt/funding_arbitrage_research/hl_liqmap
docker build -q -t hl-liqmap:py312 . >/dev/null
docker run --rm hl-liqmap:py312 python selftest_collector.py
mkdir -p data
docker rm -f hl-liqmap >/dev/null 2>&1 || true
docker run -d --name hl-liqmap --restart unless-stopped \
  --cpus 0.5 --cpu-shares 256 --memory 1g \
  --log-opt max-size=20m --log-opt max-file=3 \
  -e RATE_PER_MIN="${RATE_PER_MIN:-200}" \
  -v /opt/funding_arbitrage_research/hl_liqmap/data:/data \
  hl-liqmap:py312
