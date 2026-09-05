FROM python:3.12-alpine@sha256:d09d15e60962ca365d1cd544a48773bac9d33f2fb1b00f2aa0deec78ade7dc31 AS native-builder

RUN apk add --no-cache \
    gcc \
    libcrypto3=3.5.8-r0 \
    libssl3=3.5.8-r0 \
    musl-dev
WORKDIR /native
COPY native/low_latency/native_low_latency.c ./
RUN gcc -O3 -std=c11 -Wall -Wextra -Werror native_low_latency.c \
    -o funding-native-low-latency \
    && ./funding-native-low-latency --self-test 200000

FROM python:3.12-alpine@sha256:d09d15e60962ca365d1cd544a48773bac9d33f2fb1b00f2aa0deec78ade7dc31 AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app

COPY requirements-linux.lock ./
RUN apk add --no-cache \
      libcrypto3=3.5.8-r0 \
      libssl3=3.5.8-r0 \
      libuuid=2.42.3-r0 \
    && pip install --no-cache-dir --require-hashes --requirement requirements-linux.lock

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
RUN addgroup -S -g 10001 funding \
    && adduser -S -D -H -u 10001 -G funding -s /sbin/nologin funding \
    && mkdir -p /app/.runtime \
    && chown funding:funding /app/.runtime
USER 10001:10001

EXPOSE 8000
CMD ["uvicorn", "funding_arbitrage.main:app", "--host", "0.0.0.0", "--port", "8000"]
