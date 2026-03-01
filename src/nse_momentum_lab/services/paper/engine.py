from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db.models import (
    PaperFill,
    PaperOrder,
    PaperPosition,
    Signal,
)
from nse_momentum_lab.services.backtest.engine import ExitReason, SlippageModel

logger = logging.getLogger(__name__)


class SignalState(Enum):
    NEW = "NEW"
    QUALIFIED = "QUALIFIED"
    ALERTED = "ALERTED"
    ENTERED = "ENTERED"
    MANAGED = "MANAGED"
    EXITED = "EXITED"
    ARCHIVED = "ARCHIVED"


class KillSwitchState(Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    DISABLED = "DISABLED"


@dataclass
class RiskConfig:
    max_daily_loss_pct: float = 0.05
    max_drawdown_pct: float = 0.15
    max_positions: int = 10
    max_position_size_pct: float = 0.10
    kill_switch_threshold: float = 0.20


class RiskGovernance:
    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()
        self._kill_switch = KillSwitchState.ACTIVE
        self._daily_loss = 0.0
        self._peak_equity = 100_000.0
        self._total_equity = 100_000.0

    def can_enter_position(self, position_size: float) -> tuple[bool, str]:
        if self._kill_switch == KillSwitchState.DISABLED:
            return False, "Kill switch disabled"

        if self._kill_switch == KillSwitchState.PAUSED:
            return False, "Kill switch paused"

        daily_loss_pct = self._daily_loss / self._peak_equity
        if daily_loss_pct >= self.config.max_daily_loss_pct:
            return False, f"Daily loss limit reached: {daily_loss_pct * 100:.1f}%"

        drawdown = (self._peak_equity - self._total_equity) / self._peak_equity
        if drawdown >= self.config.max_drawdown_pct:
            return False, f"Max drawdown reached: {drawdown * 100:.1f}%"

        if position_size > self._total_equity * self.config.max_position_size_pct:
            return False, f"Position size exceeds limit: {position_size:.0f}"

        return True, "OK"

    def check_risk(self, current_pnl: float) -> tuple[bool, str]:
        self._daily_loss += current_pnl
        self._total_equity += current_pnl

        if self._total_equity > self._peak_equity:
            self._peak_equity = self._total_equity

        daily_loss_pct = self._daily_loss / self._peak_equity
        if daily_loss_pct <= -self.config.max_daily_loss_pct:
            self._kill_switch = KillSwitchState.PAUSED
            return False, f"Daily loss limit breached: {daily_loss_pct * 100:.1f}%"

        drawdown = (self._peak_equity - self._total_equity) / self._peak_equity
        if drawdown >= self.config.max_drawdown_pct:
            self._kill_switch = KillSwitchState.PAUSED
            return False, f"Max drawdown breached: {drawdown * 100:.1f}%"

        return True, "OK"

    def get_kill_switch_state(self) -> KillSwitchState:
        return self._kill_switch

    def reset_daily(self) -> None:
        self._daily_loss = 0.0


class PaperTrader:
    def __init__(
        self,
        risk_config: RiskConfig | None = None,
        slippage_model: SlippageModel | None = None,
    ) -> None:
        self._risk = RiskGovernance(risk_config)
        self._slippage = slippage_model or SlippageModel()
        self._positions: dict[int, PaperPosition] = {}

    async def process_signals(
        self,
        signals: list[dict[str, Any]],
        prices: dict[int, dict[date, dict[str, float]]],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        results = []

        for sig in signals:
            state = sig.get("state", SignalState.NEW.value)

            if state == SignalState.NEW.value:
                can_enter, reason = self._risk.can_enter_position(sig.get("position_size", 0))
                if not can_enter:
                    logger.warning(f"Signal {sig['signal_id']} rejected: {reason}")
                    await self._update_signal_state(
                        session, sig["signal_id"], SignalState.ARCHIVED.value
                    )
                    continue

                await self._enter_position(session, sig, prices)

            elif state == SignalState.ENTERED.value:
                await self._manage_position(session, sig, prices)

            results.append({"signal_id": sig["signal_id"], "action": "processed"})

        return results

    async def _update_signal_state(
        self, session: AsyncSession, signal_id: int, new_state: SignalState
    ) -> None:
        await session.execute(
            update(Signal).where(Signal.signal_id == signal_id).values(state=new_state.value)
        )
        await session.commit()

    async def _enter_position(
        self,
        session: AsyncSession,
        sig: dict[str, Any],
        prices: dict[int, dict[date, dict[str, float]]],
    ) -> None:
        symbol_id = sig["symbol_id"]
        entry_date = sig.get("planned_entry_date") or date.today()

        if symbol_id not in prices or entry_date not in prices[symbol_id]:
            logger.warning(f"No price data for {symbol_id} on {entry_date}")
            return

        price_data = prices[symbol_id][entry_date]
        entry_price = price_data.get("close_adj", price_data.get("close", 0))

        position_size = sig.get("position_size")
        if position_size and position_size > 0:
            qty = int(position_size / entry_price)
        else:
            max_position_value = self._risk._total_equity * self._risk.config.max_position_size_pct
            qty = int(max_position_value / entry_price)

        if qty <= 0:
            logger.warning(f"Calculated qty is 0 for {symbol_id}")
            return

        slippage_bps = self._slippage.get_slippage_bps(
            price_data.get("value_traded_inr"), entry_price, qty
        )

        entry_price_adjusted = entry_price * (1 + slippage_bps / 10000)

        position = PaperPosition(
            symbol_id=symbol_id,
            opened_at=datetime.now(),
            closed_at=None,
            avg_entry=entry_price_adjusted,
            avg_exit=None,
            qty=qty,
            pnl=None,
            state=SignalState.ENTERED.value,
            metadata_json={"signal_id": sig["signal_id"]},
        )
        session.add(position)
        self._positions[symbol_id] = position

        order = PaperOrder(
            signal_id=sig["signal_id"],
            side="BUY",
            qty=qty,
            order_type="MARKET",
            limit_price=None,
            status="FILLED",
        )
        session.add(order)

        # Ensure primary key is assigned before referencing it from fills
        await session.flush()

        fill = PaperFill(
            order_id=order.order_id,
            fill_time=datetime.now(),
            fill_price=entry_price_adjusted,
            qty=qty,
            fees=entry_price_adjusted * qty * 0.001,
            slippage_bps=slippage_bps,
        )
        session.add(fill)

        await session.commit()
        await self._update_signal_state(session, sig["signal_id"], SignalState.ENTERED)

    async def _manage_position(
        self,
        session: AsyncSession,
        sig: dict[str, Any],
        prices: dict[int, dict[date, dict[str, float]]],
    ) -> None:
        symbol_id = sig["symbol_id"]

        if symbol_id not in self._positions:
            return

        position = self._positions[symbol_id]
        entry_date = position.opened_at.date()

        if symbol_id not in prices or entry_date not in prices[symbol_id]:
            return

        price_data = prices[symbol_id][entry_date]
        current_price = price_data.get("close_adj", price_data.get("close", 0))

        initial_stop = sig.get("initial_stop", position.avg_entry * 0.95)
        current_pnl = (current_price - position.avg_entry) * position.qty

        safe, reason = self._risk.check_risk(current_pnl)
        if not safe:
            logger.warning(f"Risk breach for {symbol_id}: {reason}")
            await self._exit_position(
                session, sig, position, ExitReason.STOP_INITIAL, current_price
            )
            return

        if current_price <= initial_stop:
            await self._exit_position(
                session, sig, position, ExitReason.STOP_INITIAL, current_price
            )
            return

        days_held = (date.today() - entry_date).days
        if days_held >= 3:
            await self._exit_position(session, sig, position, ExitReason.TIME_STOP, current_price)
            return

    async def _exit_position(
        self,
        session: AsyncSession,
        sig: dict[str, Any],
        position: PaperPosition,
        exit_reason: ExitReason,
        exit_price: float,
    ) -> None:
        slippage_bps = self._slippage.get_slippage_bps(None, exit_price, position.qty)
        exit_price_adjusted = exit_price * (1 - slippage_bps / 10000)

        pnl = (exit_price_adjusted - position.avg_entry) * position.qty

        await session.execute(
            update(PaperPosition)
            .where(PaperPosition.symbol_id == position.symbol_id)
            .values(
                closed_at=datetime.now(),
                avg_exit=exit_price_adjusted,
                pnl=pnl,
                state=SignalState.EXITED.value,
            )
        )

        order = PaperOrder(
            signal_id=sig["signal_id"],
            side="SELL",
            qty=position.qty,
            order_type="MARKET",
            limit_price=None,
            status="FILLED",
        )
        session.add(order)

        fill = PaperFill(
            order_id=order.order_id,
            fill_time=datetime.now(),
            fill_price=exit_price_adjusted,
            qty=position.qty,
            fees=exit_price_adjusted * position.qty * 0.001,
            slippage_bps=slippage_bps,
        )
        session.add(fill)

        del self._positions[position.symbol_id]
        await session.commit()
        await self._update_signal_state(session, sig["signal_id"], SignalState.EXITED)

    def get_open_positions(self) -> list[PaperPosition]:
        return list(self._positions.values())

    def get_kill_switch_state(self) -> KillSwitchState:
        return self._risk.get_kill_switch_state()
