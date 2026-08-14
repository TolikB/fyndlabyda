FROM python:3.12-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md requirements.lock ./
RUN pip install --no-cache-dir --requirement requirements.lock

COPY src ./src
COPY config ./config
COPY dashboard ./dashboard
COPY alembic.ini ./
COPY migrations ./migrations

RUN pip install --no-cache-dir --no-deps --no-build-isolation .
COPY scripts ./scripts

# The API, paper runner, and Alembic migrations require no root privileges at
# runtime. A fixed numeric identity also lets Compose enforce the same boundary
# even if a future base-image default changes.
RUN groupadd --system --gid 10001 funding \
    && useradd --system --uid 10001 --gid funding --home-dir /app \
        --shell /usr/sbin/nologin funding
USER 10001:10001

EXPOSE 8000
CMD ["uvicorn", "funding_arbitrage.main:app", "--host", "0.0.0.0", "--port", "8000"]
