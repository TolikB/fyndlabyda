FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# Dependencies are pinned so a rebuild on the VM installs the tested versions.
COPY pyproject.toml README.md constraints.txt ./
COPY src ./src
RUN pip install --no-cache-dir -c constraints.txt . \
    && useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin funding

COPY alembic.ini ./
COPY migrations ./migrations
COPY config ./config
COPY dashboard ./dashboard
COPY ops/release-manifest.json ./ops/release-manifest.json

USER funding
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=10s --start-period=180s --retries=4 \
  CMD ["python", "-c", "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=5).status == 200 else 1)"]
CMD ["sh", "-c", "alembic upgrade head && exec uvicorn funding_arbitrage.main:app --host 0.0.0.0 --port 8000 --no-access-log"]
