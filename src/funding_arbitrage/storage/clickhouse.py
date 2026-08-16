"""Authenticated ClickHouse JSONEachRow writer for V1 analytical domains."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from funding_arbitrage.domain.decisions import SignalIntent
from funding_arbitrage.domain.events import EventEnvelope

ALLOWED_TABLES = frozenset(
    {
        "raw_market_events",
        "feature_events",
        "signal_events",
        "telemetry_events",
    }
)


class ClickHouseStoragePolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str = "http://clickhouse:8123"
    database: str = "funding_analytics"
    username: str = Field(min_length=1)
    password: SecretStr
    verify_tls: bool = True
    request_timeout_seconds: float = Field(default=10, gt=0)
    maximum_batch_rows: int = Field(default=5000, gt=0)
    maximum_batch_bytes: int = Field(default=8_000_000, gt=0)


class FeatureAnalyticsEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_id: str | None = None
    feature_set_version: str = Field(min_length=1)
    feature_name: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    event_time: datetime
    source_event_id: str = Field(min_length=1)
    value: float
    quality: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class TelemetryAnalyticsEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_id: str | None = None
    service: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    venue: str = "GLOBAL"
    strategy_id: str = "GLOBAL"
    event_time: datetime
    value: float
    unit: str = Field(min_length=1)
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("event_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class ClickHouseHttpWriter:
    def __init__(
        self,
        policy: ClickHouseStoragePolicy,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.policy = policy
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            base_url=policy.url,
            auth=(policy.username, policy.password.get_secret_value()),
            verify=policy.verify_tls,
            timeout=policy.request_timeout_seconds,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def write_market_events(self, events: tuple[EventEnvelope[Any], ...]) -> int:
        rows = tuple(_market_row(event) for event in events)
        return await self.insert_rows("raw_market_events", rows)

    async def write_features(self, events: tuple[FeatureAnalyticsEvent, ...]) -> int:
        rows = tuple(_feature_row(event) for event in events)
        return await self.insert_rows("feature_events", rows)

    async def write_signals(self, signals: tuple[SignalIntent, ...]) -> int:
        rows = tuple(_signal_row(signal) for signal in signals)
        return await self.insert_rows("signal_events", rows)

    async def write_telemetry(self, events: tuple[TelemetryAnalyticsEvent, ...]) -> int:
        rows = tuple(_telemetry_row(event) for event in events)
        return await self.insert_rows("telemetry_events", rows)

    async def insert_rows(
        self,
        table: str,
        rows: tuple[dict[str, Any], ...],
    ) -> int:
        if table not in ALLOWED_TABLES:
            raise ValueError("ClickHouse table is not allowlisted")
        if not rows:
            return 0
        if len(rows) > self.policy.maximum_batch_rows:
            raise ValueError("ClickHouse batch row limit exceeded")
        body = "\n".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
            for row in rows
        ).encode()
        if len(body) > self.policy.maximum_batch_bytes:
            raise ValueError("ClickHouse batch byte limit exceeded")
        response = await self.client.post(
            "/",
            params={
                "database": self.policy.database,
                "query": f"INSERT INTO {table} FORMAT JSONEachRow",
            },
            content=body,
            headers={"Content-Type": "application/x-ndjson"},
        )
        if response.status_code >= 300:
            raise RuntimeError(
                f"ClickHouse insert failed with HTTP {response.status_code}"
            )
        return len(rows)


def _market_row(event: EventEnvelope[Any]) -> dict[str, Any]:
    payload = event.payload.model_dump(mode="json")
    instrument = getattr(event.payload, "instrument", None)
    instrument_id = instrument.canonical_id if instrument is not None else "GLOBAL"
    return {
        "row_id": event.metadata.event_id,
        "event_kind": event.kind.value,
        "source": event.metadata.source,
        "instrument_id": instrument_id,
        "sequence_id": event.metadata.sequence_id,
        "correlation_id": event.metadata.correlation_id,
        "quality": event.metadata.quality.value,
        "event_time": _clickhouse_time(event.metadata.exchange_timestamp),
        "receive_time": _clickhouse_time(event.metadata.receive_timestamp),
        "monotonic_ns": event.metadata.monotonic_ns,
        "payload_version": event.metadata.payload_version,
        "payload_hash": _payload_hash(payload),
        "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


def _feature_row(event: FeatureAnalyticsEvent) -> dict[str, Any]:
    payload = event.model_dump(mode="json")
    row_id = event.row_id or _row_id("feature", payload)
    return {
        **payload,
        "row_id": row_id,
        "event_time": _clickhouse_time(event.event_time),
        "payload": json.dumps(event.payload, sort_keys=True, separators=(",", ":")),
    }


def _signal_row(signal: SignalIntent) -> dict[str, Any]:
    payload = signal.model_dump(mode="json")
    return {
        "row_id": _row_id("signal", payload),
        "signal_id": signal.signal_id,
        "strategy_id": signal.strategy_id,
        "signal_type": signal.signal_type.value,
        "mode": signal.mode.value,
        "regime": signal.regime.value,
        "instrument_id": signal.primary_instrument.canonical_id,
        "event_time": _clickhouse_time(signal.created_at),
        "expires_at": _clickhouse_time(signal.expires_at),
        "quality_score": float(signal.quality_score),
        "confidence": float(signal.confidence),
        "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


def _telemetry_row(event: TelemetryAnalyticsEvent) -> dict[str, Any]:
    payload = event.model_dump(mode="json")
    return {
        **payload,
        "row_id": event.row_id or _row_id("telemetry", payload),
        "event_time": _clickhouse_time(event.event_time),
        "labels": json.dumps(event.labels, sort_keys=True, separators=(",", ":")),
    }


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _row_id(prefix: str, payload: dict[str, Any]) -> str:
    return f"{prefix}_" + _payload_hash(payload)[:32]


def _clickhouse_time(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
