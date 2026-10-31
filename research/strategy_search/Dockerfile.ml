# Research image with LightGBM for round 10; the forward cron keeps using funding-research:py312.
FROM funding-research:py312
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/* \
    && pip install -q --root-user-action=ignore --disable-pip-version-check lightgbm==4.5.0
