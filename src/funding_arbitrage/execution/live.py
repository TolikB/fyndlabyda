"""Durable two-leg execution with compensation and unknown-state handling."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Collection, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.repositories.live import (
    create_live_intent,
    save_live_order,
    save_live_position,
    save_pending_live_order,
    update_live_intent,
)
from funding_arbitrage.domain.decisions import LiveExecutionApproval
from funding_arbitrage.exchanges.base.models import InstrumentType, OrderBook
from funding_arbitrage.execution.trading import (
    LiveLeg,
    LiveOrderStatus,
    LivePosition,
    LivePositionState,
    TradingAdapter,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.market_data.venue_metadata import VenueMetadataRegistry
from funding_arbitrage.monitoring.metrics import (
    live_order_submission_latency_seconds,
    live_orders_total,
)
from funding_arbitrage.risk.live import LiveRiskController, LiveTradingPaused


class LiveExecutionError(RuntimeError):
    """A safely rejected or compensated live execution attempt."""


class LiveTradingExecutor:
    """Submit a hedged pair while preserving an auditable state machine."""

    def __init__(
        self,
        settings: Settings,
        adapters: dict[str, TradingAdapter],
        session_factory: async_sessionmaker[AsyncSession],
        risk: LiveRiskController,
        metadata_registry: VenueMetadataRegistry | None = None,
        private_reconciliation_coverage: Mapping[str, Collection[str]] | None = None,
    ) -> None:
        self.settings = settings
        self.adapters = adapters
        self.session_factory = session_factory
        self.risk = risk
        self.metadata_registry = metadata_registry
        self.private_reconciliation_coverage = {
            venue.lower(): frozenset(instrument_type.upper() for instrument_type in types)
            for venue, types in (private_reconciliation_coverage or {}).items()
        }

    async def open_position(
        self,
        approval: LiveExecutionApproval,
        snapshot: MarketSnapshot,
        balances: dict[str, VenueBalance],
        open_notional: Decimal,
    ) -> LivePosition:
        # Reject before fee lookups, derivative configuration, or durable intents.
        self.risk.assert_entry_enabled()
        self._validate_approval(approval)
        if snapshot.captured_at != approval.market_snapshot_at:
            raise LiveExecutionError("execution snapshot differs from approved snapshot")
        self._validate_dynamic_metadata(approval)
        await self._validate_account_fees(approval)
        capital_per_leg = approval.capital_per_leg
        intent_id = uuid4().hex
        position = LivePosition(
            position_id=uuid4().hex,
            intent_id=intent_id,
            opportunity_id=approval.opportunity_id,
            opportunity_key=approval.opportunity_key,
            strategy=approval.strategy,
            asset=approval.asset,
            capital_per_leg=capital_per_leg,
            state=LivePositionState.OPENING,
            target_settlements=approval.target_settlements,
        )
        async with self.session_factory() as session:
            await create_live_intent(session, intent_id, approval, capital_per_leg)
            await save_live_position(session, position)

        specifications = self._leg_specs(approval)
        instructions = tuple(
            sorted(approval.plan.instructions, key=lambda item: item.leg_index)
        )
        if specifications[1][3] is InstrumentType.SPOT:
            return await self._fail(position, "spot_leg_must_execute_first")
        prepared = []
        requested_base = approval.risk_decision.approved_quantity
        try:
            for specification, instruction in zip(
                specifications, instructions, strict=True
            ):
                label, exchange, symbol, instrument_type, side = specification
                adapter = self._adapter(exchange)
                quantity = await adapter.normalize_base_quantity(
                    symbol, instrument_type, requested_base
                )
                if quantity <= 0:
                    return await self._fail(position, "quantity_below_venue_minimum")
                if quantity > instruction.quantity:
                    return await self._fail(position, "quantity_exceeds_execution_plan")
                book = self._fresh_book(snapshot, exchange, symbol, instrument_type)
                limit_price = self._ioc_limit_price(book, side, quantity)
                limit_price = await adapter.normalize_price(
                    symbol, instrument_type, limit_price
                )
                self._assert_planned_price(instruction.limit_price, side, limit_price)
                prepared.append(
                    (
                        label,
                        adapter,
                        exchange,
                        symbol,
                        instrument_type,
                        side,
                        quantity,
                        limit_price,
                    )
                )
        except (LiveExecutionError, ValueError) as exc:
            return await self._fail(position, str(exc))
        quantities = [item[6] for item in prepared]
        if self._relative_drift(quantities[0], quantities[1]) > (
            self.settings.live_max_hedge_drift_percent
        ):
            return await self._fail(position, "venue_quantity_precision_mismatch")

        try:
            for _, adapter, _, symbol, instrument_type, _, _, _ in prepared:
                await adapter.configure_derivative(
                    symbol,
                    instrument_type,
                    self.settings.live_leverage,
                    self.settings.live_margin_mode,
                )
        except Exception as exc:
            return await self._fail(
                position, f"derivative_configuration:{type(exc).__name__}"
            )

        first = prepared[0]
        try:
            first_result = await self._submit(
                position,
                *first,
                reduce_only=False,
                balance=balances[first[2]],
                open_notional=open_notional,
            )
        except (LiveExecutionError, LiveTradingPaused, KeyError, ValueError) as exc:
            return await self._fail(position, f"first_leg_rejected:{exc}")
        if first_result.status is LiveOrderStatus.UNKNOWN:
            return await self._manual(position, "first_leg_order_state_unknown")
        if first_result.filled_base_quantity <= 0:
            return await self._fail(position, "first_leg_not_filled")
        if first[4] is InstrumentType.SPOT and first[5].upper() == "BUY":
            try:
                first_result = await self._resolve_spot_buy_fee(
                    position,
                    first_result,
                    first[1],
                    balances[first[2]],
                    approval.asset,
                )
            except Exception as exc:
                position.leg_a = self._to_leg(
                    first_result, first[7], approval.asset
                )
                await self._save_position(position)
                return await self._manual(
                    position, f"spot_balance_delta_unresolved:{type(exc).__name__}"
                )
        position.leg_a = self._to_leg(first_result, first[7], approval.asset)
        await self._save_position(position)

        second = prepared[1]
        second_adapter = second[1]
        try:
            hedge_quantity = await second_adapter.normalize_base_quantity(
                second[3], second[4], position.leg_a.filled_base_quantity
            )
            if hedge_quantity <= 0:
                return await self._compensate_open(
                    position, "hedge_quantity_below_minimum"
                )
            if hedge_quantity > instructions[1].quantity:
                return await self._compensate_open(
                    position, "hedge_quantity_exceeds_execution_plan"
                )
            second_book = self._fresh_book(snapshot, second[2], second[3], second[4])
            second_limit = self._ioc_limit_price(
                second_book, second[5], hedge_quantity
            )
            second_limit = await second_adapter.normalize_price(
                second[3], second[4], second_limit
            )
            self._assert_planned_price(
                instructions[1].limit_price, second[5], second_limit
            )
        except (LiveExecutionError, ValueError) as exc:
            return await self._compensate_open(
                position, f"second_leg_preparation:{exc}"
            )
        second = (*second[:6], hedge_quantity, second_limit)
        first_notional = first_result.filled_base_quantity * (
            first_result.average_price or first[7]
        )
        try:
            second_balance = await second_adapter.fetch_balance()
            second_result = await self._submit(
                position,
                *second,
                reduce_only=False,
                balance=second_balance,
                open_notional=open_notional + first_notional,
            )
        except (LiveExecutionError, LiveTradingPaused, KeyError, ValueError) as exc:
            return await self._compensate_open(
                position, f"second_leg_rejected:{exc}"
            )
        if second_result.status is LiveOrderStatus.UNKNOWN:
            return await self._manual(position, "second_leg_order_state_unknown")
        if second_result.filled_base_quantity <= 0:
            return await self._compensate_open(position, "second_leg_not_filled")
        position.leg_b = self._to_leg(second_result, second[7], approval.asset)
        if self._relative_drift(
            position.leg_a.filled_base_quantity,
            position.leg_b.filled_base_quantity,
        ) > self.settings.live_max_hedge_drift_percent:
            return await self._compensate_open(position, "post_fill_hedge_drift")

        position.state = LivePositionState.OPEN
        position.opened_at = datetime.now(UTC)
        await self._save_position(position)
        async with self.session_factory() as session:
            await update_live_intent(session, position.intent_id, position.state)
        return position

    async def close_position(
        self, position: LivePosition, snapshot: MarketSnapshot
    ) -> LivePosition:
        if position.state is not LivePositionState.OPEN:
            raise LiveExecutionError("only OPEN live positions can be closed")
        if position.leg_a is None or position.leg_b is None:
            return await self._manual(position, "open_position_missing_leg")
        position.state = LivePositionState.CLOSING
        await self._save_position(position)
        results: list[TradingOrderResult] = []
        for label, leg in (("close_a", position.leg_a), ("close_b", position.leg_b)):
            try:
                result, close_quantity, residual = await self._close_leg(
                    position, label, leg, snapshot
                )
            except Exception as exc:
                return await self._manual(
                    position, f"{label}_submission_failed:{type(exc).__name__}"
                )
            results.append(result)
            if result.status is LiveOrderStatus.UNKNOWN:
                return await self._manual(position, f"{label}_order_state_unknown")
            if result.filled_base_quantity < close_quantity:
                return await self._manual(position, f"{label}_not_fully_closed")
            leg.residual_base_quantity = residual
        position.leg_a.closing_order_ids = (results[0].client_order_id,)
        position.leg_b.closing_order_ids = (results[1].client_order_id,)
        position.state = LivePositionState.CLOSED
        position.closed_at = datetime.now(UTC)
        await self._save_position(position)
        async with self.session_factory() as session:
            await update_live_intent(session, position.intent_id, position.state)
        return position

    async def _close_leg(
        self,
        position: LivePosition,
        label: str,
        leg: LiveLeg,
        snapshot: MarketSnapshot,
    ) -> tuple[TradingOrderResult, Decimal, Decimal]:
        adapter = self._adapter(leg.exchange)
        side = self._opposite(leg.side)
        book = self._fresh_book(
            snapshot, leg.exchange, leg.exchange_symbol, leg.instrument_type
        )
        close_quantity = leg.filled_base_quantity
        residual = Decimal("0")
        if leg.instrument_type is InstrumentType.SPOT:
            close_quantity = await adapter.normalize_base_quantity(
                leg.exchange_symbol, leg.instrument_type, close_quantity
            )
            residual = leg.filled_base_quantity - close_quantity
            if close_quantity <= 0:
                raise LiveExecutionError("spot close quantity is below venue minimum")
            if residual * leg.average_price > self.settings.live_max_spot_residual_usd:
                raise LiveExecutionError("spot close would leave excessive residual value")
        limit = self._ioc_limit_price(book, side, close_quantity)
        limit = await adapter.normalize_price(
            leg.exchange_symbol, leg.instrument_type, limit
        )
        result = await self._submit(
            position,
            label,
            adapter,
            leg.exchange,
            leg.exchange_symbol,
            leg.instrument_type,
            side,
            close_quantity,
            limit,
            reduce_only=leg.instrument_type is not InstrumentType.SPOT,
        )
        return result, close_quantity, residual

    async def _submit(
        self,
        position: LivePosition,
        label: str,
        adapter: TradingAdapter,
        exchange: str,
        symbol: str,
        instrument_type: InstrumentType,
        side: str,
        quantity: Decimal,
        limit_price: Decimal,
        *,
        reduce_only: bool,
        balance: VenueBalance | None = None,
        open_notional: Decimal = Decimal("0"),
    ) -> TradingOrderResult:
        notional = quantity * limit_price
        if reduce_only or label.startswith(("close", "unwind")):
            self.risk.assert_can_reduce(order_notional=notional)
        else:
            if balance is None:
                raise LiveExecutionError("balance required for an opening order")
            self._validate_spot_inventory(
                balance, symbol, instrument_type, side, quantity, notional
            )
            self.risk.assert_can_open(
                order_notional=notional,
                open_notional=open_notional,
                free_collateral=balance.collateral_available(instrument_type),
            )
        request = TradingOrderRequest(
            intent_id=position.intent_id,
            client_order_id=self._client_order_id(exchange, position.intent_id, label),
            exchange=exchange,
            exchange_symbol=symbol,
            instrument_type=instrument_type,
            side=side,
            base_quantity=quantity,
            limit_price=limit_price,
            reduce_only=reduce_only,
        )
        async with self.session_factory() as session:
            await save_pending_live_order(
                session, request, position_id=position.position_id, leg=label
            )
        submission_started = time.monotonic()
        try:
            result = await adapter.submit_ioc_order(
                request, self.settings.live_order_timeout_seconds
            )
        except Exception as exc:
            # The request crossed the authenticated API boundary after its durable
            # PENDING record was committed. A timeout/transport exception therefore
            # has an unknown exchange outcome and must never be treated as a safe
            # rejection or retried automatically.
            result = TradingOrderResult(
                exchange=exchange,
                client_order_id=request.client_order_id,
                exchange_symbol=symbol,
                instrument_type=instrument_type,
                side=side,
                requested_base_quantity=quantity,
                filled_base_quantity=Decimal("0"),
                status=LiveOrderStatus.UNKNOWN,
                reduce_only=reduce_only,
                raw={"submission_error_type": type(exc).__name__},
            )
            self.risk.trip("order_submission_outcome_unknown")
        finally:
            live_order_submission_latency_seconds.labels(exchange).observe(
                time.monotonic() - submission_started
            )
        live_orders_total.labels(exchange, result.status.value).inc()
        try:
            async with self.session_factory() as session:
                await save_live_order(
                    session,
                    result,
                    intent_id=position.intent_id,
                    position_id=position.position_id,
                    leg=label,
                )
        except Exception:
            self.risk.trip("order_result_persistence_failed")
            return result.model_copy(update={"status": LiveOrderStatus.UNKNOWN})
        return result

    async def _compensate_open(
        self, position: LivePosition, reason: str
    ) -> LivePosition:
        for label, leg in (("unwind_a", position.leg_a), ("unwind_b", position.leg_b)):
            if leg is None:
                continue
            adapter = self._adapter(leg.exchange)
            # The last known fill price is widened only to form a bounded IOC limit.
            multiplier = (
                Decimal("1") + self.settings.live_max_slippage_percent
                if self._opposite(leg.side) == "BUY"
                else Decimal("1") - self.settings.live_max_slippage_percent
            )
            limit = await adapter.normalize_price(
                leg.exchange_symbol, leg.instrument_type, leg.average_price * multiplier
            )
            close_quantity = leg.filled_base_quantity
            residual = Decimal("0")
            if leg.instrument_type is InstrumentType.SPOT:
                close_quantity = await adapter.normalize_base_quantity(
                    leg.exchange_symbol, leg.instrument_type, close_quantity
                )
                residual = leg.filled_base_quantity - close_quantity
                if close_quantity <= 0:
                    return await self._manual(
                        position, f"{reason};compensation_below_venue_minimum"
                    )
            try:
                result = await self._submit(
                    position,
                    label,
                    adapter,
                    leg.exchange,
                    leg.exchange_symbol,
                    leg.instrument_type,
                    self._opposite(leg.side),
                    close_quantity,
                    limit,
                    reduce_only=leg.instrument_type is not InstrumentType.SPOT,
                )
            except Exception as exc:
                return await self._manual(
                    position,
                    f"{reason};compensation_failed:{type(exc).__name__}",
                )
            if (
                result.status is LiveOrderStatus.UNKNOWN
                or result.filled_base_quantity < close_quantity
            ):
                return await self._manual(position, f"{reason};compensation_incomplete")
            leg.residual_base_quantity = residual
            if residual * leg.average_price > self.settings.live_max_spot_residual_usd:
                return await self._manual(position, f"{reason};excessive_spot_residual")
        return await self._fail(position, reason)

    async def _resolve_spot_buy_fee(
        self,
        position: LivePosition,
        result: TradingOrderResult,
        adapter: TradingAdapter,
        before: VenueBalance,
        asset: str,
    ) -> TradingOrderResult:
        """Infer an omitted base-asset fee from the authoritative balance delta."""

        asset = asset.upper()
        before_total = before.total.get(asset, Decimal("0"))
        after_total = before_total
        for delay in (0.0, 0.2, 0.5):
            if delay:
                await asyncio.sleep(delay)
            after = await adapter.fetch_balance()
            after_total = after.total.get(asset, Decimal("0"))
            if after_total > before_total:
                break
        acquired = after_total - before_total
        reported = result.filled_base_quantity
        fee_rate = await adapter.fetch_taker_fee(
            result.exchange_symbol, result.instrument_type
        )
        maximum_deduction_rate = max(fee_rate * Decimal("2"), Decimal("0.005"))
        if (
            acquired <= 0
            or acquired > reported + Decimal("0.000000000001")
            or acquired < reported * (Decimal("1") - maximum_deduction_rate)
        ):
            raise LiveExecutionError("spot balance delta does not match the reported fill")
        inferred_fee = reported - acquired
        if inferred_fee <= 0:
            return result
        adjusted = result.model_copy(
            update={"fee": inferred_fee, "fee_currency": asset}
        )
        async with self.session_factory() as session:
            await save_live_order(
                session,
                adjusted,
                intent_id=position.intent_id,
                position_id=position.position_id,
                leg="open_a",
            )
        return adjusted

    async def _save_position(self, position: LivePosition) -> None:
        async with self.session_factory() as session:
            await save_live_position(session, position)

    async def _fail(self, position: LivePosition, reason: str) -> LivePosition:
        position.state = LivePositionState.FAILED
        position.failure_reason = reason
        await self._save_position(position)
        async with self.session_factory() as session:
            await update_live_intent(session, position.intent_id, position.state, reason)
        return position

    async def _manual(self, position: LivePosition, reason: str) -> LivePosition:
        self.risk.trip(reason)
        position.state = LivePositionState.MANUAL_INTERVENTION
        position.failure_reason = reason
        await self._save_position(position)
        async with self.session_factory() as session:
            await update_live_intent(session, position.intent_id, position.state, reason)
        return position

    def _validate_approval(self, approval: LiveExecutionApproval) -> None:
        if datetime.now(UTC) >= approval.plan.expires_at:
            raise LiveExecutionError("live execution approval expired")
        if approval.asset not in self.settings.live_allowed_asset_values:
            raise LiveExecutionError("asset is not live-allowlisted")
        if approval.strategy.lower() not in self.settings.live_allowed_strategy_values:
            raise LiveExecutionError("strategy is not live-allowlisted")
        specifications = self._leg_specs(approval)
        venues = {item[1] for item in specifications}
        if not venues.issubset(self.adapters):
            raise LiveExecutionError("execution plan uses a non-enabled live venue")
        for _, venue, _, instrument_type, _ in specifications:
            supported_types = self.private_reconciliation_coverage.get(venue)
            if (
                supported_types is None
                or instrument_type.value not in supported_types
            ):
                raise LiveExecutionError(
                    "instrument type lacks private reconciliation coverage"
                )
        if approval.expected_net_profit <= 0:
            raise LiveExecutionError("approved net profit must be positive")
        if approval.capital_per_leg <= 0:
            raise LiveExecutionError("capital must be positive")
        for _, _, _, instrument_type, side in specifications:
            if instrument_type is InstrumentType.SPOT and side == "SELL":
                raise LiveExecutionError("live spot borrowing is not implemented")
        if specifications[1][3] is InstrumentType.SPOT:
            raise LiveExecutionError("live spot leg must be leg A")

    def _validate_dynamic_metadata(
        self, approval: LiveExecutionApproval
    ) -> None:
        registry = self.metadata_registry
        if registry is None:
            return
        now = datetime.now(UTC)
        maximum_age = max(60.0, self.settings.public_metadata_refresh_seconds * 2)
        for specification, price in zip(
            self._leg_specs(approval), approval.reference_prices, strict=True
        ):
            _, venue, symbol, instrument_type, _ = specification
            snapshots = tuple(
                snapshot
                for snapshot in registry.snapshots()
                if snapshot.venue == venue.lower()
                and (now - snapshot.observed_at).total_seconds() <= maximum_age
            )
            if not snapshots:
                raise LiveExecutionError("dynamic venue metadata is unavailable")
            matches = tuple(
                metadata
                for snapshot in snapshots
                for metadata in snapshot.instruments
                if metadata.instrument.exchange_symbol.upper() == symbol.upper()
                and metadata.instrument.instrument_type.value == instrument_type.value
            )
            if len(matches) != 1:
                raise LiveExecutionError("dynamic instrument metadata is ambiguous")
            metadata = matches[0]
            if not metadata.active:
                raise LiveExecutionError("dynamic instrument metadata is inactive")
            requested_base = approval.capital_per_leg / price
            venue_amount = requested_base / metadata.contract_size
            if (
                metadata.minimum_amount is not None
                and venue_amount < metadata.minimum_amount
            ):
                raise LiveExecutionError("dynamic venue minimum amount is not met")
            if (
                metadata.minimum_cost is not None
                and requested_base * price < metadata.minimum_cost
            ):
                raise LiveExecutionError("dynamic venue minimum cost is not met")

    async def _validate_account_fees(
        self, approval: LiveExecutionApproval
    ) -> None:
        specifications = self._leg_specs(approval)
        try:
            actual_fees = await asyncio.gather(
                *(
                    self._adapter(exchange).fetch_taker_fee(symbol, instrument_type)
                    for _, exchange, symbol, instrument_type, _ in specifications
                )
            )
        except Exception as exc:
            raise LiveExecutionError("account taker-fee verification failed") from exc
        configured_fees = [
            self.settings.fee_schedules[exchange][1]
            for _, exchange, _, _, _ in specifications
        ]
        configured_roundtrip = (
            approval.capital_per_leg
            * Decimal("2")
            * sum(configured_fees, Decimal("0"))
        )
        actual_roundtrip = (
            approval.capital_per_leg
            * Decimal("2")
            * sum(actual_fees, Decimal("0"))
        )
        adjusted_profit = (
            approval.expected_net_profit
            + configured_roundtrip
            - actual_roundtrip
        )
        if adjusted_profit < self.settings.live_min_expected_profit_usd:
            raise LiveExecutionError("account-specific fees remove the expected net profit")

    @staticmethod
    def _leg_specs(
        approval: LiveExecutionApproval,
    ) -> tuple[tuple[str, str, str, InstrumentType, str], ...]:
        instructions = tuple(
            sorted(approval.plan.instructions, key=lambda item: item.leg_index)
        )
        return tuple(
            (
                f"open_{chr(ord('a') + instruction.leg_index)}",
                instruction.instrument.venue.lower(),
                instruction.instrument.exchange_symbol,
                InstrumentType(instruction.instrument.instrument_type.value),
                instruction.side.value,
            )
            for instruction in instructions
        )

    @staticmethod
    def _assert_planned_price(
        planned_limit: Decimal | None,
        side: str,
        actual_limit: Decimal,
    ) -> None:
        if planned_limit is None:
            raise LiveExecutionError("execution plan is missing a bounded limit price")
        if side == "BUY" and actual_limit > planned_limit:
            raise LiveExecutionError("IOC price exceeds execution-plan buy limit")
        if side == "SELL" and actual_limit < planned_limit:
            raise LiveExecutionError("IOC price exceeds execution-plan sell limit")
    def _fresh_book(
        self,
        snapshot: MarketSnapshot,
        exchange: str,
        symbol: str,
        instrument_type: InstrumentType,
    ) -> OrderBook:
        book = snapshot.orderbook(exchange, symbol, instrument_type)
        ticker = snapshot.ticker(exchange, symbol, instrument_type)
        if book is None or ticker is None:
            raise LiveExecutionError("fresh typed ticker and orderbook are required")
        if (snapshot.captured_at - book.timestamp).total_seconds() > (
            self.settings.market_data_stale_seconds
        ) or (snapshot.captured_at - ticker.timestamp).total_seconds() > (
            self.settings.market_data_stale_seconds
        ):
            raise LiveExecutionError("stale market data cannot submit live orders")
        return book

    def _ioc_limit_price(self, book: OrderBook, side: str, quantity: Decimal) -> Decimal:
        order_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL
        estimate = calculate_execution_price(book, order_side, quantity)
        if not estimate.is_fully_filled or estimate.average_price is None:
            raise LiveExecutionError("insufficient orderbook depth")
        if estimate.slippage_percent > self.settings.live_max_slippage_percent:
            raise LiveExecutionError("estimated slippage exceeds live limit")
        levels = book.asks if order_side is OrderSide.BUY else book.bids
        remaining = quantity
        worst = levels[0].price
        for level in levels:
            take = min(remaining, level.quantity)
            if take > 0:
                worst = level.price
                remaining -= take
            if remaining <= 0:
                break
        return worst

    def _adapter(self, exchange: str) -> TradingAdapter:
        try:
            return self.adapters[exchange]
        except KeyError as exc:
            raise LiveExecutionError(f"live adapter is not enabled: {exchange}") from exc

    def _client_order_id(self, exchange: str, intent_id: str, label: str) -> str:
        digest = hashlib.sha256(f"{intent_id}:{label}".encode()).hexdigest()
        prefix = self.settings.live_client_order_prefix.lower()
        if exchange == "hyperliquid":
            return "0x" + digest[:32]
        compact = f"{prefix}{digest[:18]}{label[-1]}"
        if exchange == "gate":
            return "t-" + compact[:25]
        return compact[:32]

    @staticmethod
    def _to_leg(
        result: TradingOrderResult, fallback_price: Decimal, asset: str
    ) -> LiveLeg:
        if result.filled_base_quantity <= 0:
            raise LiveExecutionError("cannot create a leg from an empty fill")
        base_quantity = result.filled_base_quantity
        if (
            result.instrument_type is InstrumentType.SPOT
            and result.side.upper() == "BUY"
            and (result.fee_currency or "").upper() == asset.upper()
        ):
            base_quantity -= result.fee
        if base_quantity <= 0:
            raise LiveExecutionError("spot fee consumed the acquired base quantity")
        return LiveLeg(
            exchange=result.exchange,
            exchange_symbol=result.exchange_symbol,
            instrument_type=result.instrument_type,
            side=result.side,
            requested_base_quantity=result.requested_base_quantity,
            filled_base_quantity=base_quantity,
            average_price=result.average_price or fallback_price,
            fee=result.fee,
            fee_currency=result.fee_currency,
            opening_order_ids=(result.client_order_id,),
        )

    @staticmethod
    def _relative_drift(left: Decimal, right: Decimal) -> Decimal:
        denominator = max(abs(left), abs(right))
        return abs(left - right) / denominator if denominator else Decimal("0")

    @staticmethod
    def _opposite(side: str) -> str:
        return "SELL" if side.upper() == "BUY" else "BUY"

    @staticmethod
    def _validate_spot_inventory(
        balance: VenueBalance,
        symbol: str,
        instrument_type: InstrumentType,
        side: str,
        quantity: Decimal,
        notional: Decimal,
    ) -> None:
        if instrument_type is not InstrumentType.SPOT:
            return
        if side.upper() == "SELL":
            base = symbol.replace("-", "_").replace("/", "_").split("_")[0]
            if balance.spot_available(base) < quantity:
                raise LiveExecutionError("insufficient spot base inventory")
        elif balance.collateral_available(InstrumentType.SPOT) < notional:
            raise LiveExecutionError("insufficient spot quote inventory")
