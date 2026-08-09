"""Funding stability and mean-reversion warnings."""

from decimal import Decimal

from funding_arbitrage.exchanges.base.models import FundingHistoryPoint
from funding_arbitrage.market_data.funding import FundingStatistics, funding_statistics


def stability_score(statistics: FundingStatistics) -> Decimal:
    if statistics.sample_count == 0:
        return Decimal("0")
    volatility_penalty = min(Decimal("60"), statistics.standard_deviation * Decimal("10000"))
    change_penalty = min(
        Decimal("30"),
        Decimal(statistics.sign_changes)
        / Decimal(max(1, statistics.sample_count - 1))
        * Decimal("100"),
    )
    return max(Decimal("0"), statistics.persistence_score - volatility_penalty - change_penalty)


def assess_funding(
    history: list[FundingHistoryPoint], current_rate: Decimal | None = None
) -> tuple[FundingStatistics, Decimal]:
    statistics = funding_statistics(history, current_rate)
    return statistics, stability_score(statistics)
