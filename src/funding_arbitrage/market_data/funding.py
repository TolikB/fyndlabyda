"""Funding history statistics used by ranking and risk controls."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import median

from pydantic import BaseModel, Field

from funding_arbitrage.exchanges.base.models import FundingHistoryPoint


class FundingStatistics(BaseModel):
    sample_count: int = Field(ge=0)
    mean: Decimal = Decimal("0")
    median: Decimal = Decimal("0")
    standard_deviation: Decimal = Field(default=Decimal("0"), ge=0)
    positive_ratio: Decimal = Field(default=Decimal("0"), ge=0, le=1)
    sign_changes: int = Field(default=0, ge=0)
    average_7d: Decimal = Decimal("0")
    average_30d: Decimal = Decimal("0")
    persistence_score: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    unstable_funding: bool = False


def funding_statistics(
    history: list[FundingHistoryPoint],
    current_rate: Decimal | None = None,
    now: datetime | None = None,
) -> FundingStatistics:
    if not history:
        return FundingStatistics(sample_count=0)
    values = [point.funding_rate for point in history]
    count = len(values)
    mean = sum(values, Decimal("0")) / Decimal(count)
    variance = sum((value - mean) ** 2 for value in values) / Decimal(count)
    positive = sum(value > 0 for value in values)
    changes = sum(
        first != 0 and second != 0 and (first > 0) != (second > 0)
        for first, second in zip(values, values[1:], strict=False)
    )
    ordered = sorted(history, key=lambda point: point.funding_timestamp)
    end = (now or ordered[-1].funding_timestamp).astimezone(UTC)
    seven_day = [
        point.funding_rate
        for point in ordered
        if point.funding_timestamp >= end - timedelta(days=7)
    ]
    thirty_day = [
        point.funding_rate
        for point in ordered
        if point.funding_timestamp >= end - timedelta(days=30)
    ]
    recent = values[-min(20, count) :]
    persistence = Decimal(sum(value > 0 for value in recent)) / Decimal(len(recent)) * 100
    if current_rate is None:
        unstable = False
    elif variance == 0:
        unstable = current_rate > mean
    else:
        unstable = current_rate > mean + Decimal("3") * variance.sqrt()
    return FundingStatistics(
        sample_count=count,
        mean=mean,
        median=Decimal(str(median(values))),
        standard_deviation=variance.sqrt(),
        positive_ratio=Decimal(positive) / Decimal(count),
        sign_changes=changes,
        average_7d=sum(seven_day, Decimal("0")) / Decimal(len(seven_day))
        if seven_day
        else Decimal("0"),
        average_30d=sum(thirty_day, Decimal("0")) / Decimal(len(thirty_day))
        if thirty_day
        else Decimal("0"),
        persistence_score=persistence,
        unstable_funding=bool(unstable),
    )
