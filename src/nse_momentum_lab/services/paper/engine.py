from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
from nse_momentum_lab.db.paper import upsert_paper_order_event
from nse_momentum_lab.services.backtest.cost_model import CostModel
from nse_momentum_lab.services.backtest.engine import ExitReason, SlippageModel

logger = logging.getLogger(__name__)
DEFAULT_FEE_RATE = 0.001
DEFAULT_TIME_STOP_DAYS = 3
DEFAULT_INITIAL_STOP_FRACTION = 0.95


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_today() -> date:
    return _utc_now().date()


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

    def __post_init__(self) -> None:
        pct_fields = {
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_position_size_pct": self.max_position_size_pct,
            "kill_switch_threshold": self.kill_switch_threshold,
        }
        for field_name, value in pct_fields.items():
            if not 0 < value <= 1:
                raise ValueError(f"{field_name} must be in the range (0, 1]")
        if self.max_positions <= 0:
            raise ValueError("max_positions must be greater than 0")


class RiskGovernance:
    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()
        self._kill_switch = KillSwitchState.ACTIVE
        self._daily_loss = 0.0
        self._peak_equity = 100_000.0
        self._total_equity = 100_000.0

    def can_enter_position(
        self,
        position_size: float,
        *,
        open_positions_count: int = 0,
    ) -> tuple[bool, str]:
        if self._kill_switch == KillSwitchState.DISABLED:
            return False, "Kill switch disabled"

        if self._kill_switch == KillSwitchState.PAUSED:
            return False, "Kill switch paused"

        daily_loss_pct = self._daily_loss / self._peak_equity
        if daily_loss_pct <= -self.config.max_daily_loss_pct:
            return False, f"Daily loss limit reached: {daily_loss_pct * 100:.1f}%"

        if open_positions_count >= self.config.max_positions:
            return False, f"Max positions reached: {self.config.max_positions}"

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
        cost_model: CostModel | None = None,
    ) -> None:
        self._risk = RiskGovernance(risk_config)
        self._slippage = slippage_model or SlippageModel()
        self._cost_model = cost_model or CostModel.zerodha()
        self._positions: dict[tuple[str | None, int], PaperPosition] = {}

    def _position_key(self, signal_id: int, session_id: str | None) -> tuple[str | None, int]:
        return session_id, signal_id

    def reset_daily(self) -> None:
        self._risk.reset_daily()

    def hydrate_positions(self, positions: list[PaperPosition], session_id: str | None) -> None:
        if session_id is not None:
            self._positions = {
                key: position for key, position in self._positions.items() if key[0] != session_id
            }

        for position in positions:
            metadata = position.metadata_json or {}
            signal_id = metadata.get("signal_id")
            if signal_id is None:
                continue
            self._positions[self._position_key(int(signal_id), position.session_id)] = position

    @staticmethod
    def _current_price_snapshot(
        prices: dict[int, dict[date, dict[str, float]]],
        symbol_id: int,
        preferred_date: date | None = None,
    ) -> tuple[date, dict[str, float]] | None:
        symbol_prices = prices.get(symbol_id)
        if not symbol_prices:
            return None
        if preferred_date is not None and preferred_date in symbol_prices:
            return preferred_date, symbol_prices[preferred_date]
        current_date = max(symbol_prices)
        return current_date, symbol_prices[current_date]

    async def process_signals(
        self,
        signals: list[dict[str, Any]],
        prices: dict[int, dict[date, dict[str, float]]],
        session: AsyncSession,
        paper_session_id: str | None = None,
    ) -> list[dict[str, Any]]:
        results = []

        for sig in signals:
            state = sig.get("state", SignalState.NEW.value)
            try:
                if state in (
                    SignalState.NEW.value,
                    SignalState.QUALIFIED.value,
                    SignalState.ALERTED.value,
                ):
                    can_enter, reason = self._risk.can_enter_position(
                        sig.get("position_size", 0),
                        open_positions_count=len(self._positions),
                    )
                    if not can_enter:
                        logger.warning("Signal %s rejected: %s", sig["signal_id"], reason)
                        await self._update_signal_state(
                            session, sig["signal_id"], SignalState.ARCHIVED
                        )
                        await session.commit()
                        continue

                    await self._enter_position(session, sig, prices, paper_session_id)

                elif state == SignalState.ENTERED.value:
                    await self._manage_position(session, sig, prices, paper_session_id)

                await session.commit()
                results.append({"signal_id": sig["signal_id"], "action": "processed"})
            except Exception:
                await session.rollback()
                logger.exception(
                    "Paper signal processing failed for session=%s signal_id=%s",
                    paper_session_id or sig.get("session_id"),
                    sig.get("signal_id"),
                )
                raise

        return results

    async def _update_signal_state(
        self, session: AsyncSession, signal_id: int, new_state: SignalState | str
    ) -> None:
        state_value = new_state.value if isinstance(new_state, SignalState) else new_state
        await session.execute(
            update(Signal).where(Signal.signal_id == signal_id).values(state=state_value)
        )

    async def _enter_position(
        self,
        session: AsyncSession,
        sig: dict[str, Any],
        prices: dict[int, dict[date, dict[str, float]]],
        paper_session_id: str | None = None,
    ) -> None:
        symbol_id = sig["symbol_id"]
        session_key = paper_session_id or sig.get("session_id")
        if not session_key:
            logger.warning("Skipping enter for signal_id=%s without session_id", sig["signal_id"])
            return
        entry_date = sig.get("planned_entry_date") or _utc_today()

        current_snapshot = self._current_price_snapshot(prices, symbol_id, entry_date)
        if current_snapshot is None:
            logger.warning("No price data for symbol_id=%s on %s", symbol_id, entry_date)
            return

        _, price_data = current_snapshot
        entry_price = price_data.get("close_adj", price_data.get("close", 0))
        if entry_price <= 0:
            logger.warning("Invalid entry price for symbol_id=%s on %s", symbol_id, entry_date)
            return

        position_size = sig.get("position_size")
        if position_size and position_size > 0:
            qty = int(position_size / entry_price)
        else:
            max_position_value = self._risk._total_equity * self._risk.config.max_position_size_pct
            qty = int(max_position_value / entry_price)

        if qty <= 0:
            logger.warning("Calculated qty is 0 for symbol_id=%s", symbol_id)
            return

        slippage_bps = self._slippage.get_slippage_bps(
            price_data.get("value_traded_inr"), entry_price, qty
        )

        entry_price_adjusted = entry_price * (1 + slippage_bps / 10000)
        entry_costs = self._cost_model.entry_order_cost(entry_price_adjusted, qty, direction="LONG")

        position = PaperPosition(
            session_id=session_key,
            symbol_id=symbol_id,
            opened_at=_utc_now(),
            closed_at=None,
            avg_entry=entry_price_adjusted,
            avg_exit=None,
            qty=qty,
            pnl=None,
            state=SignalState.ENTERED.value,
            metadata_json={
                "signal_id": sig["signal_id"],
                "session_id": session_key,
                "entry_costs": entry_costs,
                "gross_entry_price": entry_price,
            },
        )
        session.add(position)
        await session.flush()
        self._positions[self._position_key(sig["signal_id"], session_key)] = position

        order = PaperOrder(
            signal_id=sig["signal_id"],
            side="BUY",
            qty=qty,
            order_type="MARKET",
            limit_price=None,
            status="FILLED",
            session_id=session_key,
        )
        session.add(order)
        await session.flush()

        fill = PaperFill(
            session_id=session_key,
            order_id=order.order_id,
            fill_time=_utc_now(),
            fill_price=entry_price_adjusted,
            qty=qty,
            fees=entry_costs,
            slippage_bps=slippage_bps,
        )
        session.add(fill)
        await upsert_paper_order_event(
            session,
            session_id=session_key,
            event_type="POSITION_ENTERED",
            event_status="FILLED",
            order_id=order.order_id,
            signal_id=sig["signal_id"],
            payload_json={
                "qty": qty,
                "entry_price": entry_price_adjusted,
                "slippage_bps": slippage_bps,
            },
        )
        await self._update_signal_state(session, sig["signal_id"], SignalState.ENTERED)

    async def _manage_position(
        self,
        session: AsyncSession,
        sig: dict[str, Any],
        prices: dict[int, dict[date, dict[str, float]]],
        paper_session_id: str | None = None,
    ) -> None:
        symbol_id = sig["symbol_id"]
        session_key = paper_session_id or sig.get("session_id")
        if not session_key:
            logger.warning("Skipping exit for signal_id=%s without session_id", sig["signal_id"])
            return
        position = self._positions.get(self._position_key(sig["signal_id"], session_key))
        if position is None:
            return

        preferred_date = sig.get("planned_entry_date") or position.opened_at.date()
        current_snapshot = self._current_price_snapshot(prices, symbol_id, preferred_date)
        if current_snapshot is None:
            return

        current_date, price_data = current_snapshot
        current_price = price_data.get("close_adj", price_data.get("close", 0))

        initial_stop = sig.get("initial_stop", position.avg_entry * DEFAULT_INITIAL_STOP_FRACTION)
        metadata = dict(position.metadata_json or {})
        previous_mark_pnl = float(metadata.get("last_mark_pnl") or 0.0)
        current_pnl = (current_price - position.avg_entry) * position.qty
        delta_pnl = current_pnl - previous_mark_pnl

        safe, reason = self._risk.check_risk(delta_pnl)
        if not safe:
            logger.warning(f"Risk breach for {symbol_id}: {reason}")
            await self._exit_position(
                session,
                sig,
                position,
                ExitReason.STOP_INITIAL,
                current_price,
                paper_session_id,
            )
            return

        metadata.update(
            {
                "last_mark_price": current_price,
                "last_mark_pnl": current_pnl,
                "last_marked_at": _utc_now().isoformat(),
            }
        )
        position.metadata_json = metadata

        if current_price <= initial_stop:
            await self._exit_position(
                session, sig, position, ExitReason.STOP_INITIAL, current_price, paper_session_id
            )
            return

        days_held = (current_date - position.opened_at.date()).days
        if days_held >= DEFAULT_TIME_STOP_DAYS:
            await self._exit_position(
                session, sig, position, ExitReason.TIME_STOP, current_price, paper_session_id
            )
            return

    async def _exit_position(
        self,
        session: AsyncSession,
        sig: dict[str, Any],
        position: PaperPosition,
        exit_reason: ExitReason,
        exit_price: float,
        paper_session_id: str | None = None,
    ) -> None:
        slippage_bps = self._slippage.get_slippage_bps(None, exit_price, position.qty)
        exit_price_adjusted = exit_price * (1 - slippage_bps / 10000)
        exit_costs = self._cost_model.exit_order_cost(
            exit_price_adjusted, position.qty, direction="LONG"
        )

        gross_pnl = (exit_price_adjusted - position.avg_entry) * position.qty
        entry_costs = float((position.metadata_json or {}).get("entry_costs") or 0.0)
        pnl = gross_pnl - entry_costs - exit_costs
        session_key = paper_session_id or sig.get("session_id")
        position_key = self._position_key(sig["signal_id"], session_key)
        position_update = update(PaperPosition).where(
            PaperPosition.position_id == position.position_id
        )

        await session.execute(
            position_update.values(
                closed_at=_utc_now(),
                avg_exit=exit_price_adjusted,
                pnl=pnl,
                state=SignalState.EXITED.value,
            )
        )

        order = PaperOrder(
            session_id=session_key,
            signal_id=sig["signal_id"],
            side="SELL",
            qty=position.qty,
            order_type="MARKET",
            limit_price=None,
            status="FILLED",
        )
        session.add(order)
        await session.flush()

        fill = PaperFill(
            session_id=session_key,
            order_id=order.order_id,
            fill_time=_utc_now(),
            fill_price=exit_price_adjusted,
            qty=position.qty,
            fees=exit_costs,
            slippage_bps=slippage_bps,
        )
        session.add(fill)

        metadata = dict(position.metadata_json or {})
        metadata.update(
            {
                "gross_pnl": gross_pnl,
                "entry_costs": entry_costs,
                "exit_costs": exit_costs,
                "total_costs": entry_costs + exit_costs,
                "net_pnl": pnl,
            }
        )
        position.metadata_json = metadata

        self._positions.pop(position_key, None)
        await upsert_paper_order_event(
            session,
            session_id=session_key,
            event_type="POSITION_EXITED",
            event_status=exit_reason.value,
            order_id=order.order_id,
            signal_id=sig["signal_id"],
            payload_json={
                "qty": position.qty,
                "exit_price": exit_price_adjusted,
                "gross_pnl": gross_pnl,
                "net_pnl": pnl,
                "entry_costs": entry_costs,
                "exit_costs": exit_costs,
                "total_costs": entry_costs + exit_costs,
                "exit_reason": exit_reason.value,
            },
        )
        await self._update_signal_state(session, sig["signal_id"], SignalState.EXITED)

    def get_open_positions(self, session_id: str | None = None) -> list[PaperPosition]:
        if session_id is None:
            return list(self._positions.values())
        return [position for key, position in self._positions.items() if key[0] == session_id]

    def get_kill_switch_state(self) -> KillSwitchState:
        return self._risk.get_kill_switch_state()
