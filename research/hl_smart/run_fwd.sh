#!/bin/sh
# Self-test and (re)start the forward "follow the best traders" series on the VM. Touches only hl-smart-fwd.
set -eu
cd /opt/funding_arbitrage_research/hl_smart
docker run --rm -v /opt/funding_arbitrage_research/hl_smart:/w/hl_smart -w /w/hl_smart funding-research:py312 python selftest_fwd.py >/dev/null
docker rm -f hl-smart-fwd >/dev/null 2>&1 || true
docker run -d --name hl-smart-fwd --restart unless-stopped \
  --cpus 0.25 --cpu-shares 128 --memory 768m \
  --log-opt max-size=20m --log-opt max-file=3 \
  -v /opt/funding_arbitrage_research/hl_smart:/w/hl_smart -w /w/hl_smart \
  funding-research:py312 python -u fwd_smart.py loop
