"""Runtime wiring for the configurable Martingale, grid, and loss-averaging research.

The three research strategies were fully implemented and tested but structurally
unreachable: ``SupplementalStrategyContexts.dangerous_research`` was left at its
empty default by every production caller, and the suite always built them with
``enabled=False`` controls. This module supplies both halves — configuration
derived from the operator's explicit switches, and contexts built from real
broker state — while keeping the whole path inert unless a capability is both
enabled and separately authorized.
"""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import Side, TradingMode
from funding_arbitrage.execution.directional_paper import (
    DirectionalPaperPosition,
    DirectionalPaperStatus,
)
from funding_arbitrage.services.multi_regime import MultiRegimeStrategySnapshot
from funding_arbitrage.strategies.dangerous_research import (
    DangerousResearchContext,
    GridConfig,
    GridResearchStrategy,
    LossAveragingConfig,
    LossAveragingResearchStrategy,
    MartingaleConfig,
    MartingaleResearchStrategy,
)

ZERO = Decimal("0")
BPS = Decimal("10000")

DangerousResearchStrategies = tuple[
    MartingaleResearchStrategy,
    GridResearchStrategy,
    LossAveragingResearchStrategy,
]


def _controls(settings: Settings, *, enabled: bool, capability: str) -> dict[str, bool]:
    """Resolve the two independent switches guarding one research strategy.

    ``live_enabled`` is never true from a flag alone: the capability must also be
    named in ``DANGEROUS_CAPABILITY_AUTHORIZATION``.
    """

    authorized = capability in settings.authorized_dangerous_capabilities
    return {"enabled": enabled, "live_enabled": enabled and authorized}


def build_dangerous_research_strategies(
    settings: Settings,
) -> DangerousResearchStrategies:
    """Build the three research strategies from the operator's switches."""

    return (
        MartingaleResearchStrategy(
            MartingaleConfig(
                **_controls(
                    settings,
                    enabled=settings.martingale_research_enabled,
                    capability="martingale",
                )
            )
        ),
        GridResearchStrategy(
            GridConfig(
                **_controls(
                    settings,
                    enabled=settings.grid_research_enabled,
                    capability="grid_averaging",
                )
            )
        ),
        LossAveragingResearchStrategy(
            LossAveragingConfig(
                **_controls(
                    settings,
                    enabled=settings.loss_averaging_research_enabled,
                    capability="loss_averaging",
                )
            )
        ),
    )


def dangerous_research_enabled(settings: Settings) -> bool:
    return (
        settings.martingale_research_enabled
        or settings.grid_research_enabled
        or settings.loss_averaging_research_enabled
    )


def _closed_history(
    positions: tuple[DirectionalPaperPosition, ...],
) -> tuple[Decimal | None, int]:
    """Latest closed net PnL in basis points and the current loss streak."""

    closed = sorted(
        (
            position
            for position in positions
            if position.status is DirectionalPaperStatus.CLOSED
            and position.closed_at is not None
            and position.approved_notional > ZERO
        ),
        key=lambda position: (position.closed_at, position.position_id),
    )
    if not closed:
        return None, 0
    latest = closed[-1].net_pnl / closed[-1].approved_notional * BPS
    streak = 0
    for position in reversed(closed):
        if position.net_pnl < ZERO:
            streak += 1
        else:
            break
    return latest, streak


def _open_state(
    positions: tuple[DirectionalPaperPosition, ...],
) -> tuple[Side | None, Decimal | None, int]:
    """Reference side, average entry price, and prior additions for one instrument."""

    open_positions = tuple(
        position
        for position in positions
        if position.status is DirectionalPaperStatus.OPEN
        and position.entry_order.average_fill_price is not None
    )
    if not open_positions:
        return None, None, 0
    quantity = sum((abs(position.signed_quantity) for position in open_positions), ZERO)
    if quantity <= ZERO:
        return None, None, 0
    notional = sum(
        (
            abs(position.signed_quantity) * position.entry_order.average_fill_price
            for position in open_positions
            if position.entry_order.average_fill_price is not None
        ),
        ZERO,
    )
    signed = sum((position.signed_quantity for position in open_positions), ZERO)
    side = Side.BUY if signed > ZERO else Side.SELL if signed < ZERO else None
    return side, notional / quantity, len(open_positions) - 1


def build_dangerous_research_contexts(
    snapshot: MultiRegimeStrategySnapshot,
    *,
    settings: Settings,
    positions: tuple[DirectionalPaperPosition, ...],
    signed_quantity: Decimal,
    margin_available: bool,
    portfolio_drawdown_fraction: Decimal,
) -> tuple[DangerousResearchContext, ...]:
    """Project canonical state into one research context, or nothing at all.

    Returns an empty tuple whenever every research capability is switched off, so
    the default runtime never even constructs the input the strategies read.
    """

    if not dangerous_research_enabled(settings):
        return ()
    price = snapshot.technical.close
    if price <= ZERO or not price.is_finite():
        return ()
    instrument_positions = tuple(
        position for position in positions if position.instrument == snapshot.instrument
    )
    latest_pnl_bps, consecutive_losses = _closed_history(instrument_positions)
    side, average_entry_price, prior_additions = _open_state(instrument_positions)
    reference_side = side or _regime_reference_side(snapshot)
    live_mode = snapshot.mode in {TradingMode.LIMITED_LIVE, TradingMode.LIVE}
    operator_authorized = live_mode and settings.live_armed and settings.live_autotrade
    context = DangerousResearchContext(
        instrument=snapshot.instrument,
        price=price,
        market_timestamp=snapshot.book.exchange_timestamp,
        timestamp=snapshot.timestamp,
        mode=snapshot.mode,
        regime=snapshot.regime.regime,
        data_quality=snapshot.orderflow.data_quality,
        margin_available=margin_available,
        portfolio_drawdown_fraction=portfolio_drawdown_fraction,
        estimated_cost_bps=settings.multi_regime_estimated_cost_bps,
        operator_authorized=operator_authorized,
        reference_side=reference_side,
        latest_closed_trade_pnl_bps=latest_pnl_bps,
        consecutive_losses=consecutive_losses,
        anchor_price=average_entry_price or price,
        current_signed_quantity=signed_quantity,
        average_entry_price=average_entry_price,
        prior_additions=prior_additions,
    )
    return (context,)


def _regime_reference_side(snapshot: MultiRegimeStrategySnapshot) -> Side | None:
    """Without an open position the side comes from the observed book imbalance."""

    imbalance = snapshot.orderflow.book_imbalance_l5
    if imbalance is None or not imbalance.is_finite() or imbalance == ZERO:
        return None
    return Side.BUY if imbalance > ZERO else Side.SELL
