"""Canonical 5-step bar processing loop for paper trading.

Called once per closed candle batch by both replay and live runners.
This is the top-level orchestrator that guarantees identical behavior
across backtest, replay, and live modes.

Adapted from cpr-pivot-lab's paper_session_driver.py for NSE momentum strategies.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
    SessionPositionTracker,
    select_entries_for_bar,
    should_process_symbol,
)
from nse_momentum_lab.services.paper.engine.paper_runtime import (
    PaperRuntimeState,
    enforce_session_risk_controls,
    evaluate_candle,
    execute_entry,
)
from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
    AlertEvent,
    AlertType,
    format_partial_exit_alert,
    format_trade_closed_alert,
    format_trade_opened_alert,
)

logger = logging.getLogger(__name__)


async def process_closed_bar_group(
    *,
    session_id: str,
    session: dict[str, Any],
    bar_candles: list[dict[str, Any]],
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    strategy_config: Any,
    active_symbols: list[str],
    feed_source: str,
    paper_db: Any = None,
    alert_dispatcher: Any = None,
) -> dict[str, Any]:
    """Process one bar (all symbols) through the canonical 5-step loop.

    Args:
        session_id: Active session ID.
        session: Session dict from paper_db.
        bar_candles: List of candle dicts for this bar (one per symbol).
        runtime_state: Accumulated session state.
        tracker: Position tracker.
        strategy_config: PaperStrategyConfig.
        active_symbols: Current active symbol universe.
        feed_source: "replay" or "live".
        paper_db: PaperDB instance (optional, for DB writes).
        alert_dispatcher: AlertDispatcher instance (optional, for notifications).

    Returns:
        Dict with updated active_symbols, risk status, and completion flags.
    """
    if not bar_candles:
        return {
            "active_symbols": active_symbols,
            "last_price": 0.0,
            "triggered": False,
            "should_complete": False,
            "stop_reason": None,
        }

    # Wrap all DB writes for this bar in a single transaction so a mid-bar
    # crash leaves the DB in a consistent state (all-or-nothing per bar).
    _txn = paper_db.transaction() if paper_db is not None else contextlib.nullcontext()
    with _txn:
        # Build a per-symbol close price map for this bar group — used for unrealized P&L
        # and risk-flatten exit prices.  last_price (a single scalar) would be overwritten
        # by each candle and produce wrong unrealized P&L for all but the last symbol.
        close_prices: dict[str, float] = {}
        for c in bar_candles:
            sym = c.get("symbol", "")
            close = c.get("close")
            if sym and close is not None:
                close_prices[sym] = float(close)

        last_price = 0.0
        entry_cutoff_minutes = int(
            session.get("entry_cutoff_minutes")
            or getattr(strategy_config, "entry_cutoff_minutes", 60)
            or 60
        )
        slippage_bps = _resolve_slippage_bps(session)

        # ------------------------------------------------------------------
        # Step 1: Exits / advance trailing stops
        # ------------------------------------------------------------------
        for candle in bar_candles:
            symbol = candle.get("symbol", "")
            if not tracker.has_open_position(symbol):
                continue

            result = evaluate_candle(
                symbol=symbol,
                candle=candle,
                runtime_state=runtime_state,
                tracker=tracker,
                session=session,
                strategy_config=strategy_config,
                allow_entry_evaluation=False,
            )
            last_price = close_prices.get(symbol, last_price)

            if result.get("action") == "CLOSE":
                exit_price = result["exit_price"]
                # Capture position data BEFORE record_close() pops it from the tracker.
                tracked_before_close = tracker.get_open_position(symbol)
                exit_value = _compute_exit_value(tracker, symbol, exit_price)
                tracker.record_close(symbol, exit_value)
                state = runtime_state.for_symbol(symbol)
                state.position_closed_today = True

                _log_close(symbol, result["reason"], exit_price, tracked_before_close)

                # DB write.
                if paper_db is not None:
                    _record_close_in_db(paper_db, session_id, symbol, exit_price, result["reason"])

                # Alert.
                if alert_dispatcher is not None:
                    _dispatch_trade_closed(
                        alert_dispatcher,
                        session_id,
                        symbol,
                        result,
                        session,
                        tracked_before_close,
                    )

            elif result.get("action") == "HOLD":
                trail_state = result.get("next_trail_state", {})
                tracker.update_trail_state(symbol, trail_state)
                # Persist mark price and current stop so flatten() and eod-carry
                # have accurate exit prices and trail state on crash recovery.
                if paper_db is not None:
                    mark = close_prices.get(symbol)
                    if mark is not None:
                        tracked_pos = tracker.get_open_position(symbol)
                        if tracked_pos and tracked_pos.position_id:
                            paper_db.patch_position_metadata(
                                tracked_pos.position_id,
                                last_mark_price=mark,
                                current_sl=trail_state.get("current_sl"),
                            )

            elif result.get("action") == "PARTIAL_EXIT":
                exit_price = float(result["exit_price"])
                partial_fraction = float(result.get("partial_fraction", 0.80))
                carry_stop = float(result["carry_stop"])
                trail_state = result.get("updated_trail_state", {})
                tracked_pos = tracker.get_open_position(symbol)
                if tracked_pos is not None:
                    total_qty = tracked_pos.current_qty
                    exit_qty = max(1, int(total_qty * partial_fraction))
                    remain_qty = total_qty - exit_qty
                    if remain_qty <= 0:
                        # qty too small to split — treat as full close
                        exit_value = _compute_exit_value(tracker, symbol, exit_price)
                        tracker.record_close(symbol, exit_value)
                        state = runtime_state.for_symbol(symbol)
                        state.position_closed_today = True
                        _log_close(symbol, result["reason"], exit_price, tracked_pos)
                        if paper_db is not None:
                            _record_close_in_db(
                                paper_db, session_id, symbol, exit_price, result["reason"]
                            )
                    else:
                        exit_value = exit_price * exit_qty
                        entry_price = float(tracked_pos.entry_price)
                        realized_pnl = (
                            (exit_price - entry_price) * exit_qty
                            if tracked_pos.direction == "LONG"
                            else (entry_price - exit_price) * exit_qty
                        )
                        realized_pnl -= round(entry_price * total_qty * 0.001, 4)
                        realized_pnl -= round(exit_price * exit_qty * 0.001, 4)
                        tracker.partial_close(
                            symbol,
                            exit_qty=exit_qty,
                            exit_value=exit_value,
                            new_stop=carry_stop,
                            new_trail_state=trail_state,
                        )
                        logger.info(
                            "PARTIAL_EXIT %s %s @ %.2f qty=%d remain=%d carry_stop=%.2f",
                            tracked_pos.direction,
                            symbol,
                            exit_price,
                            exit_qty,
                            remain_qty,
                            carry_stop,
                        )
                        if paper_db is not None and tracked_pos.position_id:
                            paper_db.partial_close_position(
                                tracked_pos.position_id,
                                partial_exit_price=exit_price,
                                partial_exit_qty=exit_qty,
                                carry_stop=carry_stop,
                                reason="PARTIAL_EXIT",
                                closed_at=datetime.now(tz=UTC),
                            )
                        if alert_dispatcher is not None:
                            subject, body = format_partial_exit_alert(
                                symbol=symbol,
                                direction=tracked_pos.direction,
                                entry_price=entry_price,
                                exit_price=exit_price,
                                realized_pnl=realized_pnl,
                                exited_qty=exit_qty,
                                remaining_qty=remain_qty,
                                carry_stop=carry_stop,
                                session_id=session_id,
                                strategy=str(session.get("strategy_name", "")),
                                event_time=datetime.now(tz=UTC),
                            )
                            alert_dispatcher.enqueue(
                                AlertEvent(
                                    alert_type=AlertType.PARTIAL_EXIT,
                                    session_id=session_id,
                                    subject=subject,
                                    body=body,
                                )
                            )

        # ------------------------------------------------------------------
        # Step 2: Evaluate entry candidates
        # ------------------------------------------------------------------
        entry_candidates: list[dict[str, Any]] = []
        for candle in bar_candles:
            symbol = candle.get("symbol", "")
            # Skip symbols being managed in Step 1 (exits).
            if tracker.has_open_position(symbol):
                continue
            bar_end = candle.get("bar_end") or candle.get("ts")
            bar_time_minutes = _minutes_from_open(bar_end) if bar_end else 0
            state = runtime_state.for_symbol(symbol)

            if not should_process_symbol(
                bar_time_minutes=bar_time_minutes,
                entry_cutoff_minutes=entry_cutoff_minutes,
                tracker=tracker,
                symbol=symbol,
                setup_status=state.setup_status,
            ):
                continue

            result = evaluate_candle(
                symbol=symbol,
                candle=candle,
                runtime_state=runtime_state,
                tracker=tracker,
                session=session,
                strategy_config=strategy_config,
                allow_entry_evaluation=True,
            )

            if result.get("action") == "ENTRY_CANDIDATE":
                entry_candidates.append(result)
            last_price = close_prices.get(symbol, last_price)

        # ------------------------------------------------------------------
        # Step 3: Select + execute entries
        # ------------------------------------------------------------------
        selected = select_entries_for_bar(entry_candidates, tracker)
        for candidate in selected:
            result = execute_entry(
                candidate=candidate,
                tracker=tracker,
                session_id=session_id,
                session=session,
                paper_db=paper_db,
                slippage_bps=slippage_bps,
                strategy_config=strategy_config,
            )
            if result and result.get("status") == "opened" and alert_dispatcher is not None:
                _dispatch_trade_opened(alert_dispatcher, session_id, session, result)

        # ------------------------------------------------------------------
        # Step 4: NSEML-specific filtering (placeholder for strategy filters)
        # ------------------------------------------------------------------
        # NSEML does not use the separate Stage B direction filter from the pivot workflow.
        # Strategy-specific post-entry filters can be added here per strategy type.

        # ------------------------------------------------------------------
        # Risk controls  (must run before prune, per spec Step 4)
        # ------------------------------------------------------------------
        triggered = False
        if paper_db is not None:
            positions = paper_db.list_open_positions(session_id)
        else:
            positions = []

        # Per-symbol unrealized P&L: use each symbol's own bar close price.
        unrealized_pnl = 0.0
        for sym in tracker.open_symbols():
            tracked = tracker.get_open_position(sym)
            if tracked is None:
                continue
            mark = close_prices.get(sym, tracked.entry_price)
            if tracked.direction == "LONG":
                unrealized_pnl += (mark - tracked.entry_price) * tracked.current_qty
            elif tracked.direction == "SHORT":
                unrealized_pnl += (tracked.entry_price - mark) * tracked.current_qty

        # Realized P&L from all closed positions in this session (from DB).
        session_realized_pnl = paper_db.get_session_realized_pnl(session_id) if paper_db else 0.0

        portfolio_value = session.get("risk_config", {}).get("portfolio_value", 1_000_000)
        raw_ts = bar_candles[0].get("bar_end") or bar_candles[0].get("ts")
        if isinstance(raw_ts, (int, float)):
            as_of_dt = datetime.fromtimestamp(raw_ts, tz=UTC)
        elif isinstance(raw_ts, datetime):
            as_of_dt = raw_ts
        else:
            as_of_dt = datetime.now(tz=UTC)
        risk_result = enforce_session_risk_controls(
            session=session,
            positions=positions,
            as_of=as_of_dt,
            portfolio_value=portfolio_value,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=session_realized_pnl,
        )

        if risk_result["triggered"]:
            triggered = True
            logger.warning(
                "Risk triggered session=%s reasons=%s", session_id, risk_result["reasons"]
            )
            # Flatten all open positions using each symbol's own bar close price.
            for symbol in list(tracker.open_symbols()):
                tracked = tracker.get_open_position(symbol)
                if tracked is None:
                    continue
                exit_price = close_prices.get(symbol) or (
                    last_price if last_price > 0 else tracked.entry_price
                )
                exit_value = _compute_exit_value(tracker, symbol, exit_price)
                tracker.record_close(symbol, exit_value)
                if paper_db is not None:
                    _record_close_in_db(paper_db, session_id, symbol, exit_price, "RISK_BREACH")

        # ------------------------------------------------------------------
        # Step 5: Prune symbol universe
        # ------------------------------------------------------------------
        pruned = []
        for symbol in active_symbols:
            # Find the candle for this symbol to get bar time.
            bar_end = None
            for c in bar_candles:
                if c.get("symbol") == symbol:
                    bar_end = c.get("bar_end") or c.get("ts")
                    break
            bar_time_minutes = _minutes_from_open(bar_end) if bar_end else 0
            state = runtime_state.for_symbol(symbol)

            if should_process_symbol(
                bar_time_minutes=bar_time_minutes,
                entry_cutoff_minutes=entry_cutoff_minutes,
                tracker=tracker,
                symbol=symbol,
                setup_status=state.setup_status,
            ):
                pruned.append(symbol)

        active_symbols_changed = len(pruned) != len(active_symbols)
        updated_active = pruned if active_symbols_changed else active_symbols

        # ------------------------------------------------------------------
        # Checkpoint (write after every bar so replay can resume)
        # ------------------------------------------------------------------
        if paper_db is not None:
            bar_end_raw = bar_candles[0].get("bar_end") or bar_candles[0].get("ts")
            if bar_end_raw is not None:
                try:
                    if isinstance(bar_end_raw, (int, float)):
                        bar_end_dt = datetime.fromtimestamp(bar_end_raw, tz=UTC)
                    elif isinstance(bar_end_raw, datetime):
                        bar_end_dt = (
                            bar_end_raw if bar_end_raw.tzinfo else bar_end_raw.replace(tzinfo=UTC)
                        )
                    else:
                        bar_end_dt = None
                    if bar_end_dt is not None:
                        paper_db.insert_bar_checkpoint(
                            session_id=session_id,
                            bar_end_ts=bar_end_dt,
                            committed_symbol_count=len(bar_candles),
                            fill_count=len(selected),
                            state_hash=None,
                        )
                except Exception:
                    logger.exception("Failed to write bar checkpoint session=%s", session_id)

    # ------------------------------------------------------------------
    # Completion check (outside transaction — read-only)
    # ------------------------------------------------------------------
    bar_end_ts = bar_candles[0].get("bar_end") or bar_candles[0].get("ts")
    bar_time_minutes = _minutes_from_open(bar_end_ts) if bar_end_ts else 0
    should_complete = (
        bar_time_minutes >= entry_cutoff_minutes and tracker.open_count == 0 and not triggered
    )

    return {
        "active_symbols": updated_active,
        "last_price": last_price,
        "triggered": triggered,
        "should_complete": should_complete,
        "stop_reason": "RISK_BREACH" if triggered else None,
        "risk_reasons": risk_result.get("reasons", []),
    }


async def complete_session(
    *,
    session_id: str,
    paper_db: Any,
    status: str = "COMPLETED",
    notes: str | None = None,
    replica_sync: Any = None,
) -> None:
    """Finalize a session in the DB and trigger replica snapshot sync."""
    if paper_db is not None:
        paper_db.update_session(session_id, status=status, notes=notes)
        logger.info("Session %s completed with status=%s", session_id, status)
    if replica_sync is not None:
        try:
            replica_sync.force_sync()
        except Exception:
            logger.exception("Replica sync failed after session completion session=%s", session_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_IST = timezone(timedelta(hours=5, minutes=30))


def _minutes_from_open(bar_end: Any) -> int:
    """Return minutes since NSE market open (09:15 IST). Handles float epoch seconds or datetime."""
    if bar_end is None:
        return 0
    if isinstance(bar_end, (int, float)):
        dt: datetime = datetime.fromtimestamp(bar_end, tz=_IST)
    elif isinstance(bar_end, datetime):
        dt = bar_end.astimezone(_IST) if bar_end.tzinfo else bar_end.replace(tzinfo=_IST)
    else:
        return 0
    market_open = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    return max(0, int((dt - market_open).total_seconds() / 60))


def _resolve_slippage_bps(session: dict[str, Any]) -> float:
    return session.get("risk_config", {}).get("slippage_bps", 5.0)


def _compute_exit_value(tracker: SessionPositionTracker, symbol: str, exit_price: float) -> float:
    tracked = tracker.get_open_position(symbol)
    if tracked is None:
        return 0.0
    qty = tracked.current_qty
    direction = tracked.direction
    if direction == "SHORT":
        # SHORT P&L: qty * (2 * entry - exit)
        entry = tracked.entry_price
        return qty * (2 * entry - exit_price)
    return qty * exit_price


def _record_close_in_db(
    paper_db: Any, session_id: str, symbol: str, exit_price: float, reason: str
) -> None:
    try:
        positions = paper_db.list_open_positions(session_id)
        for p in positions:
            if p.get("symbol") == symbol:
                qty = p.get("qty", 0)
                entry = p.get("avg_entry", 0.0)
                direction = p.get("direction", "LONG")
                if direction == "SHORT":
                    pnl = qty * (entry - exit_price)
                else:
                    pnl = qty * (exit_price - entry)

                # Read signal_id before any metadata write.
                meta = p.get("metadata_json") or {}
                signal_id = meta.get("signal_id") or ""

                # Update scalar close fields; keep metadata intact via patch.
                paper_db.update_position(
                    p["position_id"],
                    closed_at=datetime.now(tz=UTC),
                    avg_exit=exit_price,
                    pnl=pnl,
                    state="CLOSED",
                )
                # Merge exit_reason without clobbering signal_id / trail state.
                paper_db.patch_position_metadata(p["position_id"], exit_reason=reason)

                # Write close order + fill to maintain full trade history.
                close_side = "BUY" if direction == "SHORT" else "SELL"
                now = datetime.now(tz=UTC)
                order = paper_db.insert_order(
                    session_id=session_id,
                    signal_id=signal_id or None,
                    symbol=symbol,
                    side=close_side,
                    qty=qty,
                    order_type="MARKET",
                    status="FILLED",
                )
                if order:
                    paper_db.insert_fill(
                        session_id=session_id,
                        order_id=order["order_id"],
                        symbol=symbol,
                        fill_time=now,
                        fill_price=exit_price,
                        qty=qty,
                        fees=round(exit_price * qty * 0.001, 4),
                        slippage_bps=0.0,
                        side=close_side,
                    )
                if signal_id:
                    paper_db.update_signal_state(signal_id, "EXITED")
                break
    except Exception:
        logger.exception("Failed to record close in DB for %s", symbol)


def _log_close(symbol: str, reason: str, exit_price: float, tracked: Any) -> None:
    if tracked:
        direction = tracked.direction
        entry = tracked.entry_price
        pnl = (
            tracked.current_qty * (exit_price - entry)
            if direction == "LONG"
            else tracked.current_qty * (entry - exit_price)
        )
        logger.info(
            "EXIT %s %s @ %.2f reason=%s pnl=%.2f",
            direction,
            symbol,
            exit_price,
            reason,
            pnl,
        )


def _dispatch_trade_opened(
    dispatcher: Any,
    session_id: str,
    session: dict[str, Any],
    result: dict[str, Any],
) -> None:
    try:
        subject, body = format_trade_opened_alert(
            symbol=str(result.get("symbol", "")),
            direction=str(result.get("direction", "LONG")).upper(),
            entry_price=float(result.get("entry_price", 0.0) or 0.0),
            initial_stop=float(result.get("initial_stop", 0.0) or 0.0),
            qty=int(result.get("qty", 0) or 0),
            target_price=(
                float(result["target_price"]) if result.get("target_price") is not None else None
            ),
            session_id=session_id,
            strategy=str(session.get("strategy_name", "")),
            event_time=datetime.now(tz=UTC),
        )
        event = AlertEvent(
            alert_type=AlertType.TRADE_OPENED,
            session_id=session_id,
            subject=subject,
            body=body,
        )
        dispatcher.enqueue(event)
    except Exception:
        logger.exception("Failed to dispatch TRADE_OPENED alert")


def _classify_exit_alert_type(reason: str) -> AlertType:
    """Map exit reason to the most specific AlertType available."""
    reason_upper = reason.upper()
    if "TRAIL" in reason_upper:
        return AlertType.TRAIL_STOP
    if any(kw in reason_upper for kw in ("STOP", "SL_", "BREAKEVEN")):
        return AlertType.SL_HIT
    return AlertType.TRADE_CLOSED


def _dispatch_trade_closed(
    dispatcher: Any,
    session_id: str,
    symbol: str,
    result: dict[str, Any],
    session: dict[str, Any],
    tracked_before_close: Any,
) -> None:
    try:
        exit_price = float(result.get("exit_price", 0.0) or 0.0)
        reason = str(result.get("reason", ""))
        alert_type = _classify_exit_alert_type(reason)
        entry_price = float(getattr(tracked_before_close, "entry_price", 0.0) or 0.0)
        direction = str(getattr(tracked_before_close, "direction", "LONG")).upper()
        qty = int(getattr(tracked_before_close, "current_qty", 0) or 0)
        gross_pnl = (
            qty * (exit_price - entry_price)
            if direction == "LONG"
            else qty * (entry_price - exit_price)
        )
        # Match the current paper-engine fee model used in entry/exit fills so
        # breakeven stops still show the expected small rupee loss in alerts.
        entry_fees = round(entry_price * qty * 0.001, 4)
        exit_fees = round(exit_price * qty * 0.001, 4)
        realized_pnl = gross_pnl - entry_fees - exit_fees
        subject, body = format_trade_closed_alert(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_price,
            reason=reason,
            realized_pnl=realized_pnl,
            qty=qty,
            session_id=session_id,
            strategy=str(session.get("strategy_name", "")),
            event_time=datetime.now(tz=UTC),
        )
        event = AlertEvent(
            alert_type=alert_type,
            session_id=session_id,
            subject=subject,
            body=body,
        )
        dispatcher.enqueue(event)
    except Exception:
        logger.exception("Failed to dispatch trade closed alert")
