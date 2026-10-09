"""Atomic PostgreSQL/SQLite projections for the canonical trading ledger."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import (
    LedgerPostingRecord,
    LedgerTransactionRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
)
from funding_arbitrage.portfolio.ledger import (
    GENESIS_HASH,
    LedgerAccountKind,
    LedgerPosting,
    LedgerTransaction,
    build_ledger_transaction,
    ledger_transaction_hash,
)

_POSTGRES_LEDGER_LOCK_ID = 6_344_618_353_460_412_465
_SUPPORTED_DIALECTS = frozenset({"postgresql", "sqlite"})
_SETTLEMENT_ASSETS = ("USDT", "USDC", "USD")


class LedgerIntegrityError(RuntimeError):
    """A durable ledger row conflicts with its canonical event identity."""


@dataclass(frozen=True)
class LedgerAppendResult:
    transaction: LedgerTransaction | None
    inserted: bool


async def append_funding_cashflow(
    session: AsyncSession,
    *,
    position_id: str,
    venue: str,
    symbol: str,
    strategy_id: str,
    settlement_asset: str,
    amount: Decimal,
    timestamp: datetime,
) -> LedgerAppendResult:
    """Append one idempotent double-entry funding cashflow without committing."""

    normalized_timestamp = _utc(timestamp)
    transaction_id = funding_transaction_id(
        position_id=position_id,
        venue=venue,
        symbol=symbol,
        timestamp=normalized_timestamp,
    )
    if amount == 0:
        return LedgerAppendResult(transaction=None, inserted=False)
    normalized_venue = venue.strip().upper()
    normalized_position = position_id.strip().upper()
    postings = (
        LedgerPosting(
            account=f"ASSET:CASH:{normalized_venue}:{normalized_position}",
            account_kind=LedgerAccountKind.ASSET,
            asset=settlement_asset,
            amount=amount,
            venue=venue,
            strategy_id=strategy_id,
            position_id=position_id,
        ),
        LedgerPosting(
            account=f"REVENUE:FUNDING:{normalized_venue}:{normalized_position}",
            account_kind=LedgerAccountKind.REVENUE,
            asset=settlement_asset,
            amount=-amount,
            venue=venue,
            strategy_id=strategy_id,
            position_id=position_id,
        ),
    )
    return await append_ledger_transaction(
        session,
        transaction_id=transaction_id,
        timestamp=normalized_timestamp,
        reference_type="FUNDING",
        reference_id=transaction_id,
        description="Funding settlement",
        postings=postings,
    )


async def append_ledger_transaction(
    session: AsyncSession,
    *,
    transaction_id: str,
    timestamp: datetime,
    reference_type: str,
    reference_id: str,
    description: str,
    postings: tuple[LedgerPosting, ...],
) -> LedgerAppendResult:
    """Append one hash-chained transaction; an exact retry returns the original."""

    dialect = session.get_bind().dialect.name
    if dialect not in _SUPPORTED_DIALECTS:
        raise RuntimeError(f"canonical ledger is unsupported for {dialect}")
    if dialect == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _POSTGRES_LEDGER_LOCK_ID},
        )

    existing_record = await session.scalar(
        select(LedgerTransactionRecord).where(
            LedgerTransactionRecord.transaction_id == transaction_id
        )
    )
    if existing_record is not None:
        existing = await _load_transaction(session, existing_record)
        expected_identity = (
            _utc(timestamp),
            reference_type.strip().upper(),
            reference_id.strip().upper(),
            description,
            postings,
        )
        durable_identity = (
            existing.timestamp,
            existing.reference_type,
            existing.reference_id,
            existing.description,
            existing.postings,
        )
        if durable_identity != expected_identity:
            raise LedgerIntegrityError(
                "ledger transaction identity was reused with different content"
            )
        return LedgerAppendResult(transaction=existing, inserted=False)

    previous_record = await session.scalar(
        select(LedgerTransactionRecord)
        .order_by(LedgerTransactionRecord.sequence.desc())
        .limit(1)
    )
    if previous_record is None:
        sequence = 1
        previous_hash = GENESIS_HASH
    else:
        previous = await _load_transaction(session, previous_record)
        sequence = previous.sequence + 1
        previous_hash = previous.transaction_hash
    transaction = build_ledger_transaction(
        sequence=sequence,
        transaction_id=transaction_id,
        timestamp=timestamp,
        reference_type=reference_type,
        reference_id=reference_id,
        description=description,
        postings=postings,
        previous_hash=previous_hash,
    )
    transaction_record = LedgerTransactionRecord(
        sequence=transaction.sequence,
        transaction_id=transaction.transaction_id,
        timestamp=transaction.timestamp,
        reference_type=transaction.reference_type,
        reference_id=transaction.reference_id,
        description=transaction.description,
        previous_hash=transaction.previous_hash,
        transaction_hash=transaction.transaction_hash,
        payload=transaction.model_dump(mode="json"),
    )
    session.add(transaction_record)
    # These mappers deliberately do not expose a mutable ORM relationship.
    # Flush the immutable header first so PostgreSQL can enforce the posting FK
    # without depending on mapper insertion order. Both flushes remain inside
    # the caller's transaction and therefore commit or roll back atomically.
    await session.flush()
    session.add_all(
        [
            LedgerPostingRecord(
                transaction_id=transaction.transaction_id,
                posting_index=index,
                account=posting.account,
                account_kind=posting.account_kind.value,
                asset=posting.asset,
                amount=posting.amount,
                venue=posting.venue,
                strategy_id=posting.strategy_id,
                position_id=posting.position_id,
            )
            for index, posting in enumerate(transaction.postings)
        ]
    )
    await session.flush()
    return LedgerAppendResult(transaction=transaction, inserted=True)


async def backfill_paper_funding_ledger(
    session: AsyncSession,
    *,
    simulation_version: str,
    page_size: int = 500,
) -> int:
    """Idempotently project durable legacy paper payments after a restart."""

    if not simulation_version.strip():
        raise ValueError("paper funding ledger simulation version cannot be blank")
    if page_size <= 0:
        raise ValueError("paper funding ledger page size must be positive")
    inserted = 0
    after_id = 0
    expected_ids: set[str] = set()
    expected_totals: dict[str, Decimal] = {}
    while True:
        rows = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord, PaperPositionRecord)
                    .join(
                        PaperPositionRecord,
                        PaperPositionRecord.position_id
                        == PaperFundingPaymentRecord.position_id,
                    )
                    .where(
                        PaperFundingPaymentRecord.id > after_id,
                        PaperPositionRecord.simulation_version
                        == simulation_version,
                    )
                    .order_by(PaperFundingPaymentRecord.id)
                    .limit(page_size)
                )
            ).all()
        )
        if not rows:
            break
        for payment, position in rows:
            strategy_id = _paper_strategy_id(position.payload)
            settlement_asset = infer_funding_settlement_asset(
                payment.exchange, payment.symbol
            )
            result = await append_funding_cashflow(
                session,
                position_id=payment.position_id,
                venue=payment.exchange,
                symbol=payment.symbol,
                strategy_id=strategy_id,
                settlement_asset=settlement_asset,
                amount=Decimal(str(payment.pnl)),
                timestamp=payment.funding_timestamp,
            )
            inserted += int(result.inserted)
            if result.transaction is not None:
                expected_ids.add(result.transaction.transaction_id)
                expected_totals[settlement_asset] = expected_totals.get(
                    settlement_asset, Decimal("0")
                ) + Decimal(str(payment.pnl))
            after_id = payment.id
        await session.commit()
    await _verify_funding_projection(
        session,
        simulation_version=simulation_version,
        expected_ids=expected_ids,
        expected_totals=expected_totals,
    )
    return inserted


def funding_transaction_id(
    *,
    position_id: str,
    venue: str,
    symbol: str,
    timestamp: datetime,
) -> str:
    """Return the stable idempotency key for one position funding event."""

    event_identity = json.dumps(
        (
            position_id.strip(),
            venue.strip().upper(),
            symbol.strip().upper(),
            _utc(timestamp).isoformat(timespec="microseconds"),
        ),
        separators=(",", ":"),
    )
    return "funding_" + hashlib.sha256(event_identity.encode()).hexdigest()


def infer_funding_settlement_asset(exchange: str, symbol: str) -> str:
    """Infer only unambiguous stablecoin settlement symbols from legacy rows."""

    normalized_exchange = exchange.strip().lower()
    normalized_symbol = symbol.strip().upper()
    tokens = tuple(token for token in re.split(r"[-_/:]", normalized_symbol) if token)
    for asset in _SETTLEMENT_ASSETS:
        if asset in tokens:
            return asset
    compact = re.sub(r"[^A-Z0-9]", "", normalized_symbol)
    for asset in _SETTLEMENT_ASSETS:
        if compact.endswith(asset) or compact.endswith(f"{asset}M"):
            return asset
    if normalized_exchange == "hyperliquid" and normalized_symbol:
        return "USDC"
    raise LedgerIntegrityError(
        "funding settlement asset is absent from the durable legacy symbol"
    )


async def _load_transaction(
    session: AsyncSession,
    record: LedgerTransactionRecord,
) -> LedgerTransaction:
    posting_records = list(
        (
            await session.scalars(
                select(LedgerPostingRecord)
                .where(
                    LedgerPostingRecord.transaction_id == record.transaction_id
                )
                .order_by(LedgerPostingRecord.posting_index)
            )
        ).all()
    )
    if tuple(item.posting_index for item in posting_records) != tuple(
        range(len(posting_records))
    ):
        raise LedgerIntegrityError("ledger posting indexes are not contiguous")
    try:
        transaction = LedgerTransaction.model_validate(record.payload)
    except ValueError as error:
        raise LedgerIntegrityError(
            "ledger transaction payload is not a canonical transaction"
        ) from error
    header_identity = (
        transaction.sequence,
        transaction.transaction_id,
        transaction.timestamp,
        transaction.reference_type,
        transaction.reference_id,
        transaction.description,
        transaction.previous_hash,
        transaction.transaction_hash,
    )
    durable_header_identity = (
        record.sequence,
        record.transaction_id,
        _utc(record.timestamp),
        record.reference_type,
        record.reference_id,
        record.description,
        record.previous_hash,
        record.transaction_hash,
    )
    if header_identity != durable_header_identity:
        raise LedgerIntegrityError("ledger transaction payload conflicts with its columns")
    if len(posting_records) != len(transaction.postings):
        raise LedgerIntegrityError("ledger posting count conflicts with its payload")
    amount_tolerance = (
        Decimal("1e-12")
        if session.get_bind().dialect.name == "sqlite"
        else Decimal("0")
    )
    for durable, expected in zip(
        posting_records,
        transaction.postings,
        strict=True,
    ):
        durable_identity = (
            durable.account,
            durable.account_kind,
            durable.asset,
            durable.venue,
            durable.strategy_id,
            durable.position_id,
        )
        expected_identity = (
            expected.account,
            expected.account_kind.value,
            expected.asset,
            expected.venue,
            expected.strategy_id,
            expected.position_id,
        )
        if durable_identity != expected_identity or abs(
            Decimal(str(durable.amount)) - expected.amount
        ) > amount_tolerance:
            raise LedgerIntegrityError(
                "ledger posting columns conflict with their payload"
            )
    if transaction.transaction_hash != ledger_transaction_hash(transaction):
        raise LedgerIntegrityError("ledger transaction hash does not match its content")
    return transaction


async def _verify_funding_projection(
    session: AsyncSession,
    *,
    simulation_version: str,
    expected_ids: set[str],
    expected_totals: dict[str, Decimal],
) -> None:
    scoped_join = (
        LedgerPostingRecord.position_id
        == func.upper(PaperPositionRecord.position_id)
    )
    actual_ids = set(
        (
            await session.scalars(
                select(LedgerTransactionRecord.transaction_id)
                .join(
                    LedgerPostingRecord,
                    LedgerPostingRecord.transaction_id
                    == LedgerTransactionRecord.transaction_id,
                )
                .join(PaperPositionRecord, scoped_join)
                .where(
                    LedgerTransactionRecord.reference_type == "FUNDING",
                    PaperPositionRecord.simulation_version == simulation_version,
                )
                .distinct()
            )
        ).all()
    )
    if actual_ids != expected_ids:
        raise LedgerIntegrityError(
            "paper funding payments and canonical ledger identities diverged"
        )
    rows = list(
        (
            await session.execute(
                select(LedgerPostingRecord.asset, LedgerPostingRecord.amount)
                .join(
                    LedgerTransactionRecord,
                    LedgerTransactionRecord.transaction_id
                    == LedgerPostingRecord.transaction_id,
                )
                .join(PaperPositionRecord, scoped_join)
                .where(
                    LedgerTransactionRecord.reference_type == "FUNDING",
                    PaperPositionRecord.simulation_version == simulation_version,
                    LedgerPostingRecord.account.like("ASSET:CASH:%"),
                )
            )
        ).all()
    )
    actual_totals: dict[str, Decimal] = {}
    for asset, amount in rows:
        actual_totals[asset] = actual_totals.get(asset, Decimal("0")) + Decimal(
            str(amount)
        )
    tolerance = (
        Decimal("1e-12")
        if session.get_bind().dialect.name == "sqlite"
        else Decimal("0")
    )
    if set(actual_totals) != set(expected_totals) or any(
        abs(actual_totals[asset] - expected_totals[asset]) > tolerance
        for asset in expected_totals
    ):
        raise LedgerIntegrityError(
            "paper funding payments and canonical ledger totals diverged"
        )


def _paper_strategy_id(payload: dict[str, Any]) -> str:
    value = payload.get("strategy")
    if isinstance(value, str) and value.strip():
        return value
    return "LEGACY_FUNDING"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
