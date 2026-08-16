FROM python:3.12-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 AS native-builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /native
COPY native/low_latency/native_low_latency.c ./
RUN gcc -O3 -std=c11 -Wall -Wextra -Werror native_low_latency.c \
    -o funding-native-low-latency \
    && ./funding-native-low-latency --self-test 200000

FROM python:3.12-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app

COPY requirements-linux.lock ./
RUN pip install --no-cache-dir --require-hashes --requirement requirements-linux.lock

COPY src ./src
COPY config ./config
COPY dashboard ./dashboard
COPY alembic.ini ./
COPY migrations ./migrations

COPY scripts ./scripts
COPY --from=native-builder /native/funding-native-low-latency /usr/local/bin/

# The API, paper runner, and Alembic migrations require no root privileges at
# runtime. A fixed numeric identity also lets Compose enforce the same boundary
# even if a future base-image default changes.
RUN groupadd --system --gid 10001 funding \
    && useradd --system --uid 10001 --gid funding --home-dir /app \
        --shell /usr/sbin/nologin funding \
    && mkdir -p /app/.runtime \
    && chown funding:funding /app/.runtime
USER 10001:10001

EXPOSE 8000
CMD ["uvicorn", "funding_arbitrage.main:app", "--host", "0.0.0.0", "--port", "8000"]
