from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import (
    Base,
    MultiRegimeDecisionRecord,
    RiskDecisionRecord,
)
from funding_arbitrage.database.repositories.multi_regime import (
    MultiRegimeDecisionIntegrityError,
    save_multi_regime_batch,
)
from funding_arbitrage.domain.decisions import MarketRegime, RiskDecision
from funding_arbitrage.domain.events import InstrumentKey, InstrumentType, TradingMode
from funding_arbitrage.regime import RegimeSnapshot
from funding_arbitrage.risk.portfolio import (
    PortfolioRiskAuthorization,
    RiskHierarchyCaps,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


class BatchStub(BaseModel):
    model_config = ConfigDict(frozen=True)

    batch_id: str
    source_event_id: str
    mode: TradingMode
    timestamp: datetime
    instrument: InstrumentKey
    regime: RegimeSnapshot
    evaluations: tuple[dict[str, str], ...] = ()
    execution_plans: tuple[dict[str, str], ...] = ()
    risk_authorizations: tuple[PortfolioRiskAuthorization, ...] = ()
    marker: str = "original"


def _batch(*, marker: str = "original") -> BatchStub:
    decision = RiskDecision(
        signal_id="signal-1",
        decision_id="risk-1",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("10"),
        approved_quantity=Decimal("1"),
        approved_notional=Decimal("100"),
        max_slippage_bps=Decimal("5"),
        max_execution_seconds=5,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    authorization = PortfolioRiskAuthorization(
        decision=decision,
        hierarchy=RiskHierarchyCaps(
            caps_usd={"requested": Decimal("100")},
            pre_multiplier_notional_usd=Decimal("100"),
            combined_multiplier=Decimal("1"),
            sized_notional_usd=Decimal("100"),
            binding_constraints=("requested",),
        ),
    )
    regime = RegimeSnapshot(
        instrument=INSTRUMENT,
        timestamp=NOW,
        regime=MarketRegime.RANGE,
        candidate=MarketRegime.RANGE,
        confidence=Decimal("0.8"),
        regime_since=NOW,
        dwell_seconds=Decimal("0"),
        pending_confirmations=0,
        data_quality="VALID",
    )
    return BatchStub(
        batch_id="batch-1",
        source_event_id="event-1",
        mode=TradingMode.REPLAY,
        timestamp=NOW,
        instrument=INSTRUMENT,
        regime=regime,
        risk_authorizations=(authorization,),
        marker=marker,
    )


async def test_multi_regime_batch_and_risk_decision_persist_exactly_once() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        assert await save_multi_regime_batch(session, _batch()) is True
    async with factory() as session:
        assert await save_multi_regime_batch(session, _batch()) is False
        batch_count = await session.scalar(
            select(func.count()).select_from(MultiRegimeDecisionRecord)
        )
        risk_count = await session.scalar(
            select(func.count()).select_from(RiskDecisionRecord)
        )

    await engine.dispose()
    assert batch_count == 1
    assert risk_count == 1


async def test_multi_regime_identity_collision_fails_closed() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        await save_multi_regime_batch(session, _batch())
    async with factory() as session:
        with pytest.raises(MultiRegimeDecisionIntegrityError, match="conflicting content"):
            await save_multi_regime_batch(session, _batch(marker="changed"))

    await engine.dispose()
