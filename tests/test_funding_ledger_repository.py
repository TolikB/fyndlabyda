from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import (
    LedgerPostingRecord,
    LedgerTransactionRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
)
from funding_arbitrage.database.repositories.ledger import (
    LedgerIntegrityError,
    append_funding_cashflow,
    backfill_paper_funding_ledger,
    infer_funding_settlement_asset,
)
from funding_arbitrage.database.repositories.market_data import (
    save_paper_funding_payment,
)
from funding_arbitrage.exchanges.base.models import FundingSnapshot

NOW = datetime(2026, 8, 31, 8, tzinfo=UTC)


async def test_funding_payment_and_double_entry_ledger_commit_atomically(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _engine, factory = database
    funding = _funding(NOW)
    async with factory() as session:
        first = await save_paper_funding_payment(
            session,
            "position-1",
            funding,
            Decimal("100"),
            Decimal("0.25"),
            ledger_asset="USDT",
            ledger_strategy_id="funding_basis",
        )
    async with factory() as session:
        duplicate = await save_paper_funding_payment(
            session,
            "position-1",
            funding,
            Decimal("999"),
            Decimal("999"),
            ledger_asset="USDT",
            ledger_strategy_id="funding_basis",
        )
        transactions = list(
            (
                await session.scalars(
                    select(LedgerTransactionRecord).order_by(
                        LedgerTransactionRecord.sequence
                    )
                )
            ).all()
        )
        postings = list(
            (
                await session.scalars(
                    select(LedgerPostingRecord).order_by(
                        LedgerPostingRecord.posting_index
                    )
                )
            ).all()
        )
        payment_count = await session.scalar(
            select(func.count()).select_from(PaperFundingPaymentRecord)
        )

    assert Decimal(str(first.pnl)) == Decimal("0.25")
    assert Decimal(str(duplicate.pnl)) == Decimal("0.25")
    assert payment_count == 1
    assert len(transactions) == 1
    assert transactions[0].sequence == 1
    assert transactions[0].reference_type == "FUNDING"
    assert len(postings) == 2
    assert [Decimal(str(posting.amount)) for posting in postings] == [
        Decimal("0.25"),
        Decimal("-0.25"),
    ]
    assert {posting.asset for posting in postings} == {"USDT"}
    assert {posting.position_id for posting in postings} == {"POSITION-1"}
    assert {posting.strategy_id for posting in postings} == {"FUNDING_BASIS"}


async def test_funding_ledger_rejects_same_event_with_different_content_and_rolls_back(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _engine, factory = database
    async with factory() as session:
        await append_funding_cashflow(
            session,
            position_id="position-conflict",
            venue="bybit",
            symbol="BTCUSDT",
            strategy_id="funding_basis",
            settlement_asset="USDT",
            amount=Decimal("0.10"),
            timestamp=NOW,
        )
        await session.commit()

    with pytest.raises(
        LedgerIntegrityError,
        match="reused with different content",
    ):
        async with factory() as session:
            await save_paper_funding_payment(
                session,
                "position-conflict",
                _funding(NOW),
                Decimal("100"),
                Decimal("0.20"),
                ledger_asset="USDT",
                ledger_strategy_id="funding_basis",
            )

    async with factory() as session:
        payment_count = await session.scalar(
            select(func.count()).select_from(PaperFundingPaymentRecord)
        )
        transaction_count = await session.scalar(
            select(func.count()).select_from(LedgerTransactionRecord)
        )
    assert payment_count == 0
    assert transaction_count == 1


async def test_restart_backfill_is_bounded_idempotent_and_hash_chained(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _engine, factory = database
    async with factory() as session:
        session.add_all(
            [
                _position("position-a", "spot_perp"),
                _position("position-b", "cross_exchange_funding"),
            ]
        )
        await session.commit()
        await save_paper_funding_payment(
            session,
            "position-a",
            _funding(NOW),
            Decimal("100"),
            Decimal("0.10"),
        )
        await save_paper_funding_payment(
            session,
            "position-b",
            _funding(NOW + timedelta(hours=8), symbol="ETH-USDT-SWAP"),
            Decimal("200"),
            Decimal("-0.20"),
        )
        inserted = await backfill_paper_funding_ledger(
            session,
            simulation_version="paper-v1",
            page_size=1,
        )
        replayed = await backfill_paper_funding_ledger(
            session,
            simulation_version="paper-v1",
            page_size=1,
        )

    async with factory() as session:
        transactions = list(
            (
                await session.scalars(
                    select(LedgerTransactionRecord).order_by(
                        LedgerTransactionRecord.sequence
                    )
                )
            ).all()
        )
    assert inserted == 2
    assert replayed == 0
    assert [item.sequence for item in transactions] == [1, 2]
    assert transactions[0].previous_hash == "0" * 64
    assert transactions[1].previous_hash == transactions[0].transaction_hash


def test_legacy_settlement_asset_inference_is_strict() -> None:
    assert infer_funding_settlement_asset("bybit", "BTCUSDT") == "USDT"
    assert infer_funding_settlement_asset("okx", "BTC-USDC-SWAP") == "USDC"
    assert infer_funding_settlement_asset("kucoin", "XBTUSDTM") == "USDT"
    assert infer_funding_settlement_asset("hyperliquid", "BTC") == "USDC"
    with pytest.raises(LedgerIntegrityError, match="settlement asset is absent"):
        infer_funding_settlement_asset("unknown", "BTC")


def _funding(timestamp: datetime, *, symbol: str = "BTCUSDT") -> FundingSnapshot:
    return FundingSnapshot(
        exchange="okx" if "-" in symbol else "bybit",
        symbol=symbol,
        funding_rate=Decimal("0.001"),
        funding_interval_hours=Decimal("8"),
        timestamp=timestamp,
    )


def _position(position_id: str, strategy: str) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=position_id,
        opportunity_id=f"opportunity-{position_id}",
        state="OPEN",
        asset="BTC",
        capital=Decimal("100"),
        simulation_version="paper-v1",
        opened_at=NOW - timedelta(hours=1),
        closed_at=None,
        payload={"strategy": strategy},
    )
