"""Authenticated, idempotent ClickHouse writer for V1 analytical domains."""

from __future__ import annotations

import hashlib
import json
import ssl
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from funding_arbitrage.domain.decisions import SignalIntent
from funding_arbitrage.domain.events import (
    BookDelta,
    BookSnapshot,
    EventEnvelope,
    InstrumentKey,
    TradeTick,
)

ALLOWED_TABLES = frozenset(
    {
        "raw_market_events",
        "normalized_trades",
        "orderbook_deltas",
        "orderbook_snapshots",
        "feature_snapshots",
        "regime_snapshots",
        "strategy_decisions",
        "execution_telemetry",
    }
)
FEATURE_SET_VERSION = "multi-regime-v1"


class ClickHouseStoragePolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str = "https://clickhouse:8443"
    database: str = "funding_analytics"
    username: str = Field(min_length=1)
    password: SecretStr
    verify_tls: bool = True
    request_timeout_seconds: float = Field(default=10, gt=0)
    maximum_batch_rows: int = Field(default=5000, gt=0)
    maximum_batch_bytes: int = Field(default=8_000_000, gt=0)

    @model_validator(mode="after")
    def require_encrypted_transport(self) -> ClickHouseStoragePolicy:
        if self.verify_tls and not self.url.lower().startswith("https://"):
            raise ValueError("verified ClickHouse transport requires HTTPS")
        return self


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
    source_event_id: str = ""
    instrument_id: str = "GLOBAL"
    service: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    venue: str = "GLOBAL"
    strategy_id: str = "GLOBAL"
    event_time: datetime
    value: float
    unit: str = Field(min_length=1)
    labels: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class DecisionAnalyticsBatch(BaseModel):
    """Immutable projection input from a durable PostgreSQL decision row."""

    model_config = ConfigDict(frozen=True)

    row_id: int = Field(gt=0)
    batch_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    mode: str = Field(min_length=1)
    regime: str = Field(min_length=1)
    event_time: datetime
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: dict[str, Any]

    @field_validator("event_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_payload_integrity(self) -> DecisionAnalyticsBatch:
        if _payload_hash(self.payload) != self.payload_hash:
            raise ValueError("decision analytics payload checksum mismatch")
        payload = self.payload
        if payload.get("batch_id") != self.batch_id:
            raise ValueError("decision analytics batch identity mismatch")
        if payload.get("source_event_id") != self.source_event_id:
            raise ValueError("decision analytics source identity mismatch")
        if payload.get("mode") != self.mode:
            raise ValueError("decision analytics mode mismatch")
        instrument_payload = payload.get("instrument")
        if not isinstance(instrument_payload, dict):
            raise ValueError("decision analytics instrument payload is missing")
        instrument = InstrumentKey.model_validate(instrument_payload)
        if instrument.canonical_id != self.instrument_id:
            raise ValueError("decision analytics instrument identity mismatch")
        regime_payload = payload.get("regime")
        if (
            not isinstance(regime_payload, dict)
            or regime_payload.get("regime") != self.regime
        ):
            raise ValueError("decision analytics regime mismatch")
        raw_timestamp = payload.get("timestamp")
        if not isinstance(raw_timestamp, str):
            raise ValueError("decision analytics timestamp is missing")
        try:
            payload_timestamp = _utc(
                datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
            )
        except ValueError as exc:
            raise ValueError("decision analytics timestamp is invalid") from exc
        if payload_timestamp != self.event_time:
            raise ValueError("decision analytics timestamp mismatch")
        for key in ("technical", "orderflow", "structure", "orchestration"):
            if not isinstance(payload.get(key), dict):
                raise ValueError(f"decision analytics {key} payload is missing")
        for key in (
            "evaluations",
            "risk_authorizations",
            "execution_plans",
            "risk_context_missing_signal_ids",
        ):
            if not isinstance(payload.get(key), list):
                raise ValueError(f"decision analytics {key} payload is invalid")
        return self


class ClickHouseHttpWriter:
    """Write bounded blocks with stable retry-deduplication tokens."""

    def __init__(
        self,
        policy: ClickHouseStoragePolicy,
        client: httpx.AsyncClient | None = None,
        *,
        tls_context: ssl.SSLContext | None = None,
    ) -> None:
        self.policy = policy
        self._owns_client = client is None
        verification: bool | ssl.SSLContext = (
            tls_context if policy.verify_tls and tls_context is not None else policy.verify_tls
        )
        self.client = client or httpx.AsyncClient(
            base_url=policy.url,
            auth=(policy.username, policy.password.get_secret_value()),
            verify=verification,
            timeout=policy.request_timeout_seconds,
        )

    async def ping(self) -> None:
        response = await self.client.post(
            "/",
            params={"database": self.policy.database, "query": "SELECT 1"},
        )
        if response.status_code >= 300:
            raise RuntimeError(
                f"ClickHouse health query failed with HTTP {response.status_code}"
            )

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def write_market_events(self, events: tuple[EventEnvelope[Any], ...]) -> int:
        """Persist raw events and all typed market projections before acknowledging."""

        await self._insert_all(
            "raw_market_events",
            tuple(_market_row(event) for event in events),
        )
        await self._insert_all(
            "normalized_trades",
            tuple(
                _trade_row(event)
                for event in events
                if isinstance(event.payload, TradeTick)
            ),
        )
        await self._insert_all(
            "orderbook_snapshots",
            tuple(
                _book_snapshot_row(event)
                for event in events
                if isinstance(event.payload, BookSnapshot)
            ),
        )
        await self._insert_all(
            "orderbook_deltas",
            tuple(
                _book_delta_row(event)
                for event in events
                if isinstance(event.payload, BookDelta)
            ),
        )
        return len(events)

    async def write_decision_batches(
        self,
        batches: tuple[DecisionAnalyticsBatch, ...],
    ) -> int:
        """Persist every decision projection before cursor acknowledgement."""

        await self._insert_all(
            "feature_snapshots",
            tuple(
                row
                for batch in batches
                for row in _decision_feature_rows(batch)
            ),
        )
        await self._insert_all(
            "regime_snapshots",
            tuple(_decision_regime_row(batch) for batch in batches),
        )
        await self._insert_all(
            "strategy_decisions",
            tuple(_decision_strategy_row(batch) for batch in batches),
        )
        await self._insert_all(
            "execution_telemetry",
            tuple(_decision_execution_row(batch) for batch in batches),
        )
        return len(batches)

    async def write_features(self, events: tuple[FeatureAnalyticsEvent, ...]) -> int:
        rows = tuple(_feature_row(event) for event in events)
        return await self._insert_all("feature_snapshots", rows)

    async def write_signals(self, signals: tuple[SignalIntent, ...]) -> int:
        rows = tuple(_signal_row(signal) for signal in signals)
        return await self._insert_all("strategy_decisions", rows)

    async def write_telemetry(self, events: tuple[TelemetryAnalyticsEvent, ...]) -> int:
        rows = tuple(_telemetry_row(event) for event in events)
        return await self._insert_all("execution_telemetry", rows)

    async def _insert_all(
        self,
        table: str,
        rows: tuple[dict[str, Any], ...],
    ) -> int:
        """Split complete projections without exceeding configured HTTP bounds."""

        inserted = 0
        batch: list[dict[str, Any]] = []
        batch_bytes = 0
        for row in rows:
            encoded_size = len(_encoded_row(row))
            separator_size = 1 if batch else 0
            if batch and (
                len(batch) >= self.policy.maximum_batch_rows
                or batch_bytes + separator_size + encoded_size
                > self.policy.maximum_batch_bytes
            ):
                inserted += await self.insert_rows(table, tuple(batch))
                batch = []
                batch_bytes = 0
                separator_size = 0
            batch.append(row)
            batch_bytes += separator_size + encoded_size
        if batch:
            inserted += await self.insert_rows(table, tuple(batch))
        return inserted

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
        body = b"\n".join(_encoded_row(row) for row in rows)
        if len(body) > self.policy.maximum_batch_bytes:
            raise ValueError("ClickHouse batch byte limit exceeded")
        deduplication_token = hashlib.sha256(
            table.encode() + b"\0" + body
        ).hexdigest()
        response = await self.client.post(
            "/",
            params={
                "database": self.policy.database,
                "query": f"INSERT INTO {table} FORMAT JSONEachRow",
                "insert_deduplicate": "1",
                "insert_deduplication_token": deduplication_token,
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
    return {
        **_market_identity(event),
        "event_kind": event.kind.value,
        "sequence_id": event.metadata.sequence_id,
        "native_sequence": event.metadata.native_sequence,
        "correlation_id": event.metadata.correlation_id,
        "monotonic_ns": event.metadata.monotonic_ns,
        "payload_version": event.metadata.payload_version,
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _trade_row(event: EventEnvelope[Any]) -> dict[str, Any]:
    trade = event.payload
    if not isinstance(trade, TradeTick):
        raise TypeError("normalized trade projection requires TradeTick")
    payload = trade.model_dump(mode="json")
    return {
        **_market_identity(event),
        "trade_id": trade.trade_id,
        "price": str(trade.price),
        "quantity": str(trade.quantity),
        "aggressor_side": trade.aggressor_side.value if trade.aggressor_side else "",
        "sequence_id": event.metadata.sequence_id,
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _book_snapshot_row(event: EventEnvelope[Any]) -> dict[str, Any]:
    snapshot = event.payload
    if not isinstance(snapshot, BookSnapshot):
        raise TypeError("book snapshot projection requires BookSnapshot")
    payload = snapshot.model_dump(mode="json")
    return {
        **_market_identity(event),
        "sequence": snapshot.sequence,
        "checksum": snapshot.checksum,
        "bid_count": len(snapshot.bids),
        "ask_count": len(snapshot.asks),
        "best_bid": str(snapshot.bids[0].price) if snapshot.bids else None,
        "best_ask": str(snapshot.asks[0].price) if snapshot.asks else None,
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _book_delta_row(event: EventEnvelope[Any]) -> dict[str, Any]:
    delta = event.payload
    if not isinstance(delta, BookDelta):
        raise TypeError("book delta projection requires BookDelta")
    payload = delta.model_dump(mode="json")
    return {
        **_market_identity(event),
        "first_sequence": delta.first_sequence,
        "last_sequence": delta.last_sequence,
        "previous_sequence": delta.previous_sequence,
        "checksum": delta.checksum,
        "update_count": len(delta.updates),
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _market_identity(event: EventEnvelope[Any]) -> dict[str, Any]:
    instrument = getattr(event.payload, "instrument", None)
    instrument_id = instrument.canonical_id if instrument is not None else "GLOBAL"
    return {
        "row_id": event.metadata.event_id,
        "source": event.metadata.source,
        "instrument_id": instrument_id,
        "quality": event.metadata.quality.value,
        "event_time": _clickhouse_time(event.metadata.exchange_timestamp),
        "receive_time": _clickhouse_time(event.metadata.receive_timestamp),
    }


def _decision_feature_rows(
    batch: DecisionAnalyticsBatch,
) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for name in ("technical", "orderflow", "structure", "derivatives"):
        snapshot = batch.payload.get(name)
        if not isinstance(snapshot, dict):
            continue
        feature_payload = dict(snapshot)
        quality = str(feature_payload.get("data_quality", "UNKNOWN"))
        row_identity = {"batch_id": batch.batch_id, "feature_name": name}
        rows.append(
            {
                "row_id": _row_id("feature", row_identity),
                "batch_id": batch.batch_id,
                "feature_set_version": FEATURE_SET_VERSION,
                "feature_name": name,
                "instrument_id": batch.instrument_id,
                "event_time": _clickhouse_time(batch.event_time),
                "source_event_id": batch.source_event_id,
                "value": 0.0,
                "quality": quality,
                "payload_hash": _payload_hash(feature_payload),
                "payload": _json(feature_payload),
            }
        )
    return tuple(rows)


def _decision_regime_row(batch: DecisionAnalyticsBatch) -> dict[str, Any]:
    raw = batch.payload.get("regime")
    payload = dict(raw) if isinstance(raw, dict) else {"regime": batch.regime}
    return {
        "row_id": _row_id("regime", {"batch_id": batch.batch_id}),
        "batch_id": batch.batch_id,
        "source_event_id": batch.source_event_id,
        "instrument_id": batch.instrument_id,
        "event_time": _clickhouse_time(batch.event_time),
        "regime": str(payload.get("regime", batch.regime)),
        "candidate": str(payload.get("candidate", batch.regime)),
        "confidence": float(payload.get("confidence", 0)),
        "quality": str(payload.get("data_quality", "UNKNOWN")),
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _decision_strategy_row(batch: DecisionAnalyticsBatch) -> dict[str, Any]:
    payload = batch.payload
    orchestration = _mapping(payload.get("orchestration"))
    decisions = _sequence(orchestration.get("decisions"))
    active = _sequence(orchestration.get("active"))
    risk = _sequence(payload.get("risk_authorizations"))
    plans = _sequence(payload.get("execution_plans"))
    return {
        "row_id": batch.batch_id,
        "batch_id": batch.batch_id,
        "source_event_id": batch.source_event_id,
        "signal_id": "",
        "strategy_id": "multi-regime-v1",
        "mode": batch.mode,
        "regime": batch.regime,
        "instrument_id": batch.instrument_id,
        "event_time": _clickhouse_time(batch.event_time),
        "expires_at": None,
        "decision_count": len(decisions),
        "active_signal_count": len(active),
        "approved_risk_count": _approved_risk_count(risk),
        "execution_plan_count": len(plans),
        "quality_score": 0.0,
        "confidence": float(_mapping(payload.get("regime")).get("confidence", 0)),
        "payload_hash": batch.payload_hash,
        "payload": _json(payload),
    }


def _decision_execution_row(batch: DecisionAnalyticsBatch) -> dict[str, Any]:
    payload = batch.payload
    risk = _sequence(payload.get("risk_authorizations"))
    plans = _sequence(payload.get("execution_plans"))
    instrument = _mapping(payload.get("instrument"))
    telemetry_payload = {
        "batch_id": batch.batch_id,
        "risk_authorization_count": len(risk),
        "approved_risk_count": _approved_risk_count(risk),
        "execution_plan_count": len(plans),
        "risk_context_missing_signal_ids": _sequence(
            payload.get("risk_context_missing_signal_ids")
        ),
    }
    return {
        "row_id": _row_id("execution", {"batch_id": batch.batch_id}),
        "source_event_id": batch.source_event_id,
        "instrument_id": batch.instrument_id,
        "service": "multi_regime_runtime",
        "metric": "decision_batch_execution_plans",
        "venue": str(instrument.get("venue", "GLOBAL")).upper(),
        "strategy_id": "multi-regime-v1",
        "event_time": _clickhouse_time(batch.event_time),
        "value": float(len(plans)),
        "unit": "plans",
        "labels": _json(
            {
                "mode": batch.mode,
                "regime": batch.regime,
            }
        ),
        "payload_hash": _payload_hash(telemetry_payload),
        "payload": _json(telemetry_payload),
    }


def _feature_row(event: FeatureAnalyticsEvent) -> dict[str, Any]:
    payload = event.model_dump(mode="json")
    row_id = event.row_id or _row_id("feature", payload)
    feature_payload = event.payload
    return {
        "row_id": row_id,
        "batch_id": "",
        "feature_set_version": event.feature_set_version,
        "feature_name": event.feature_name,
        "instrument_id": event.instrument_id,
        "event_time": _clickhouse_time(event.event_time),
        "source_event_id": event.source_event_id,
        "value": event.value,
        "quality": event.quality,
        "payload_hash": _payload_hash(feature_payload),
        "payload": _json(feature_payload),
    }


def _signal_row(signal: SignalIntent) -> dict[str, Any]:
    payload = signal.model_dump(mode="json")
    return {
        "row_id": _row_id("signal", payload),
        "batch_id": "",
        "source_event_id": "",
        "signal_id": signal.signal_id,
        "strategy_id": signal.strategy_id,
        "mode": signal.mode.value,
        "regime": signal.regime.value,
        "instrument_id": signal.primary_instrument.canonical_id,
        "event_time": _clickhouse_time(signal.created_at),
        "expires_at": _clickhouse_time(signal.expires_at),
        "decision_count": 1,
        "active_signal_count": 1,
        "approved_risk_count": 0,
        "execution_plan_count": 0,
        "quality_score": float(signal.quality_score),
        "confidence": float(signal.confidence),
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _telemetry_row(event: TelemetryAnalyticsEvent) -> dict[str, Any]:
    payload = event.payload
    identity = event.model_dump(mode="json")
    return {
        "row_id": event.row_id or _row_id("telemetry", identity),
        "source_event_id": event.source_event_id,
        "instrument_id": event.instrument_id,
        "service": event.service,
        "metric": event.metric,
        "venue": event.venue,
        "strategy_id": event.strategy_id,
        "event_time": _clickhouse_time(event.event_time),
        "value": event.value,
        "unit": event.unit,
        "labels": _json(event.labels),
        "payload_hash": _payload_hash(payload),
        "payload": _json(payload),
    }


def _approved_risk_count(risk: list[Any]) -> int:
    return sum(
        _mapping(_mapping(item).get("decision")).get("approved") is True
        for item in risk
    )


def _encoded_row(row: dict[str, Any]) -> bytes:
    return _json(row).encode()


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _sequence(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json(payload).encode()).hexdigest()


def _row_id(prefix: str, payload: dict[str, Any]) -> str:
    return f"{prefix}_" + _payload_hash(payload)[:32]


def _json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _clickhouse_time(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)