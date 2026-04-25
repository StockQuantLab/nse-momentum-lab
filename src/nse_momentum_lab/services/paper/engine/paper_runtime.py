"""Paper trading runtime state, risk controls, and position lifecycle.

Holds per-symbol accumulated state, session-level risk metrics,
and the core evaluate/execute/flatten functions used by the session driver.

The evaluate_candle and advance_position functions delegate to existing
backtest primitives (strategy_registry, intraday_execution, candidate_builder)
rather than re-encoding strategy rules.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta, timezone
from typing import Any

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams
from nse_momentum_lab.services.backtest.intraday_execution import evaluate_held_position_bar
from nse_momentum_lab.services.paper.candidate_builder import (
    apply_breakdown_selection_ranking,
    apply_breakout_selection_ranking,
)
from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
    SessionPositionTracker,
    TrackedPosition,
)
from nse_momentum_lab.services.paper.engine.shared_eval import evaluate_entry_trigger

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parity trace logging (env-gated, zero overhead when disabled)
# ---------------------------------------------------------------------------

_PARITY_TRACE = os.environ.get("PARITY_TRACE", "0") == "1"


def _parity_trace(**fields: Any) -> None:
    """Log a structured parity trace line. Only active when PARITY_TRACE=1."""
    if _PARITY_TRACE:
        logger.info("PARITY_TRACE %s", json.dumps(fields, default=str))


# IST market open time.
NSE_OPEN = time(9, 15)

# Signal states in lifecycle order.
OPEN_SIGNAL_STATES = {"NEW", "QUALIFIED", "ALERTED", "ENTERED", "MANAGED"}
ACTIVE_SESSION_STATUSES = {"ACTIVE", "RUNNING", "PAUSED", "PLANNING", "STOPPING"}
FINAL_SESSION_STATUSES = {"COMPLETED", "FAILED", "ARCHIVED", "CANCELLED"}


def _now_utc() -> datetime:
    return datetime.now(UTC)


_IST = timezone(timedelta(hours=5, minutes=30))


def _minutes_from_open(bar_end: datetime | float | int) -> int:
    """Return minutes elapsed since NSE market open (09:15 IST).

    Accepts a timezone-aware/naive datetime or an epoch-seconds float/int
    (as produced by ClosedCandle.bar_end).
    """
    if isinstance(bar_end, (int, float)):
        dt: datetime = datetime.fromtimestamp(bar_end, tz=_IST)
    elif bar_end.tzinfo is None:
        dt = bar_end.replace(tzinfo=_IST)
    else:
        dt = bar_end.astimezone(_IST)
    market_open = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    return max(0, int((dt - market_open).total_seconds() / 60))


# ---------------------------------------------------------------------------
# Runtime state dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SymbolRuntimeState:
    """Per-symbol state accumulated during a session."""

    trade_date: str | None = None
    candles: list[dict[str, Any]] = field(default_factory=list)
    setup_row: dict[str, Any] | None = None
    setup_status: str = "pending"  # "pending" | "candidate" | "rejected"
    position_closed_today: bool = False
    entry_window_closed: bool = False
    candle_count: int = 0


@dataclass(slots=True)
class PaperRuntimeState:
    """Session-level state: per-symbol runtime, risk metrics, alert dedup."""

    symbols: dict[str, SymbolRuntimeState] = field(default_factory=dict)
    skipped_setup_rows: int = 0
    invalid_setup_rows: int = 0
    alerts_sent: set[str] = field(default_factory=set)

    def for_symbol(self, symbol: str) -> SymbolRuntimeState:
        if symbol not in self.symbols:
            self.symbols[symbol] = SymbolRuntimeState()
        return self.symbols[symbol]

    def reset_for_new_date(self, symbol: str, trade_date: str) -> None:
        state = self.for_symbol(symbol)
        if state.trade_date != trade_date:
            state.trade_date = trade_date
            state.candles.clear()
            state.setup_row = None
            state.setup_status = "pending"
            state.position_closed_today = False
            state.entry_window_closed = False
            state.candle_count = 0


# ---------------------------------------------------------------------------
# Evaluate candle — the core entry/exit decision point
# ---------------------------------------------------------------------------


def evaluate_candle(
    *,
    symbol: str,
    candle: dict[str, Any],
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    session: dict[str, Any],
    strategy_config: Any,
    allow_entry_evaluation: bool = True,
) -> dict[str, Any]:
    """Evaluate a single closed candle for entry or exit.

    This function is called from the 5-step bar processing loop.
    For symbols with open positions, it advances the position (exits/trails).
    For symbols without positions, it evaluates entry candidacy.

    Returns:
        {"action": "CLOSE", ...} — position should be closed
        {"action": "HOLD", ...} — position continues, trail state updated
        {"action": "ENTRY_CANDIDATE", ...} — new entry candidate found
        {"action": "SKIP", "reason": ...} — symbol filtered out
    """
    state = runtime_state.for_symbol(symbol)
    bar_end = candle.get("bar_end") or candle.get("ts")

    # Accumulate candle.
    state.candles.append(candle)
    state.candle_count += 1

    # --- Open position: advance (exit/trail) ---
    if tracker.has_open_position(symbol):
        result = _advance_open_position(
            symbol=symbol,
            candle=candle,
            state=state,
            tracker=tracker,
            session=session,
            strategy_config=strategy_config,
        )
        _parity_trace(
            event="advance_position",
            symbol=symbol,
            action=result.get("action"),
            reason=result.get("reason"),
            bar_end=str(bar_end),
            candle_close=candle.get("close"),
        )
        return result

    # --- Position closed today: no re-entry ---
    if state.position_closed_today:
        return {"action": "SKIP", "reason": "position_closed_today"}

    # --- Entry start gate: skip bars before entry_start_minutes (matches backtest default=5) ---
    entry_start = getattr(strategy_config, "entry_start_minutes", 0)
    if bar_end is not None and entry_start > 0 and _minutes_from_open(bar_end) < entry_start:
        return {"action": "SKIP", "reason": "entry_start_not_reached"}

    # --- Entry window closed ---
    if bar_end is not None and _minutes_from_open(bar_end) >= strategy_config.entry_cutoff_minutes:
        state.entry_window_closed = True
        return {"action": "SKIP", "reason": "entry_window_closed"}

    # --- Setup not ready ---
    if state.setup_status not in ("candidate",):
        return {"action": "SKIP", "reason": f"setup_{state.setup_status}"}

    # --- Entry evaluation ---
    if not allow_entry_evaluation:
        return {"action": "SKIP", "reason": "entry_not_allowed"}

    result = _evaluate_entry(
        symbol=symbol,
        candle=candle,
        state=state,
        session=session,
        strategy_config=strategy_config,
    )
    _parity_trace(
        event="evaluate_entry",
        symbol=symbol,
        action=result.get("action"),
        reason=result.get("reason"),
        bar_end=str(bar_end),
        candle_close=candle.get("close"),
        setup_status=state.setup_status,
    )
    return result


def _advance_open_position(
    *,
    symbol: str,
    candle: dict[str, Any],
    state: SymbolRuntimeState,
    tracker: SessionPositionTracker,
    session: dict[str, Any],
    strategy_config: Any = None,
) -> dict[str, Any]:
    """Advance an open position via the shared intraday_execution evaluator.

    Delegates entirely to evaluate_held_position_bar() — the single source of
    truth for 5-min bar stop management shared by paper-live, paper-replay, and
    (future) the 5-min backtest hold-day loop.
    """
    tracked = tracker.get_open_position(symbol)
    if tracked is None:
        return {"action": "SKIP", "reason": "no_position"}

    close = candle.get("close", 0.0)
    high = candle.get("high", close)
    low = candle.get("low", close)
    open_px = candle.get("open", close)

    trail_state = dict(tracked.trail_state)
    stop_level = trail_state.get("current_sl", tracked.stop_loss)

    same_day_partial_exit_pct = (
        getattr(strategy_config, "same_day_partial_exit_pct", None) if strategy_config else None
    )
    same_day_partial_exit_carry_stop_pct = (
        getattr(strategy_config, "same_day_partial_exit_carry_stop_pct", 0.05)
        if strategy_config
        else 0.05
    )
    is_entry_day = not (int(getattr(tracked, "days_held", 0) or 0) > 0)

    result = evaluate_held_position_bar(
        open_px=open_px,
        high_px=high,
        low_px=low,
        close_px=close,
        entry_price=tracked.entry_price,
        stop_level=stop_level,
        direction=tracked.direction,
        trail_state=trail_state,
        is_first_bar_of_session=state.candle_count <= 1,
        is_carried_position=int(getattr(tracked, "days_held", 0) or 0) > 0,
        same_day_partial_exit_pct=same_day_partial_exit_pct,
        same_day_partial_exit_carry_stop_pct=same_day_partial_exit_carry_stop_pct,
        is_entry_day=is_entry_day,
    )

    if result["action"] == "CLOSE":
        return {"action": "CLOSE", "exit_price": result["exit_price"], "reason": result["reason"]}
    if result["action"] == "PARTIAL_EXIT":
        return result  # pass through with all keys intact
    return {"action": "HOLD", "next_trail_state": result["updated_trail_state"]}


def _classify_stop_reason(entry_price: float, stop_level: float, direction: str) -> str:
    """Classify stop hit as initial, breakeven, or trailing — matches backtest intraday_execution."""
    # Use a relative epsilon for near-zero comparison (matches np.isclose default rtol=1e-5).
    eps = max(abs(entry_price) * 1e-5, 1e-6)
    if direction == "LONG":
        if stop_level > entry_price + eps:
            return "STOP_TRAIL"
        if stop_level >= entry_price - eps:
            return "STOP_BREAKEVEN"
        return "STOP_INITIAL"
    else:  # SHORT
        if stop_level < entry_price - eps:
            return "STOP_TRAIL"
        if stop_level <= entry_price + eps:
            return "STOP_BREAKEVEN"
        return "STOP_INITIAL"


def _evaluate_entry(
    *,
    symbol: str,
    candle: dict[str, Any],
    state: SymbolRuntimeState,
    session: dict[str, Any],
    strategy_config: Any,
) -> dict[str, Any]:
    """Evaluate entry using the shared evaluate_entry_trigger() helper.

    Delegates threshold check, stop placement, and max-stop-dist guard to
    the shared pure function — same code path as the backtest engine.
    """
    setup_row = state.setup_row
    if setup_row is None:
        return {"action": "SKIP", "reason": "no_setup_row"}

    prev_close = setup_row.get("prev_close") or setup_row.get("close", 0.0)
    if prev_close <= 0:
        return {"action": "SKIP", "reason": "invalid_prev_close"}

    threshold = getattr(strategy_config, "breakout_threshold", 0.04)
    direction = getattr(strategy_config, "direction", "LONG")
    close = candle.get("close", 0.0)
    high = candle.get("high", close)
    low = candle.get("low", close)
    atr = setup_row.get("atr_20", 0.0) or setup_row.get("atr", 0.0)

    # Session extremes across all accumulated bars.
    lows = [
        float(v)
        for c in state.candles
        if (v := c.get("low")) is not None and math.isfinite(float(v))
    ]
    highs = [
        float(v)
        for c in state.candles
        if (v := c.get("high")) is not None and math.isfinite(float(v))
    ]
    session_low = min(lows) if lows else float(low)
    session_high = max(highs) if highs else float(high)

    is_short = direction == "SHORT"
    trigger_price = prev_close * (1.0 - threshold) if is_short else prev_close * (1.0 + threshold)

    short_max = getattr(strategy_config, "short_max_stop_dist_pct", None)
    max_stop_dist = (
        float(short_max)
        if is_short and short_max is not None
        else getattr(strategy_config, "max_stop_dist_pct", 0.08)
    )
    short_stop_atr_mult = (
        strategy_config.extra_params.get("short_initial_stop_atr_cap_mult")
        if is_short and hasattr(strategy_config, "extra_params")
        else None
    )

    result = evaluate_entry_trigger(
        candle_high=float(high),
        candle_low=float(low),
        candle_open=float(candle.get("open", close)),
        session_low=session_low,
        session_high=session_high,
        trigger_price=trigger_price,
        is_short=is_short,
        max_stop_dist_pct=max_stop_dist,
        short_initial_stop_atr_cap_mult=short_stop_atr_mult,
        atr=float(atr) if atr else 0.0,
    )

    if result is None:
        return {"action": "SKIP", "reason": "no_trigger"}

    trigger_key = "breakdown_price" if is_short else "breakout_price"
    return {
        "action": "ENTRY_CANDIDATE",
        "symbol": symbol,
        "direction": direction,
        "entry_price": result.entry_price,
        "initial_stop": result.initial_stop,
        trigger_key: trigger_price,
        "setup_row": setup_row,
        "signal_id": setup_row.get("signal_id", ""),
    }


# ---------------------------------------------------------------------------
# Execute entry — open a position
# ---------------------------------------------------------------------------


def execute_entry(
    *,
    candidate: dict[str, Any],
    tracker: SessionPositionTracker,
    session_id: str,
    session: dict[str, Any],
    paper_db: Any,
    slippage_bps: float = 5.0,
    strategy_config: Any = None,
) -> dict[str, Any] | None:
    """Execute an entry from a candidate dict.

    Returns the position dict on success, None on failure.
    """
    symbol = candidate["symbol"]
    entry_price = candidate["entry_price"]
    initial_stop = candidate["initial_stop"]
    direction = candidate.get("direction", "LONG")

    # Apply slippage.
    slip = entry_price * slippage_bps / 10_000
    if direction == "LONG":
        entry_price += slip
    else:
        entry_price -= slip

    qty = tracker.compute_position_qty(entry_price=entry_price)
    if qty < 1:
        logger.warning("execute_entry: no cash for %s qty=0", symbol)
        return {"status": "skipped", "reason": "no_cash"}

    position_value = entry_price * qty

    # Trail state initialization — resolve direction-aware values from strategy_config.
    is_short = direction == "SHORT"
    _trail_activation = (
        getattr(strategy_config, "short_trail_activation_pct", None)
        if strategy_config is not None and is_short
        else None
    ) or getattr(strategy_config, "trail_activation_pct", 0.08)
    _trail_stop = getattr(strategy_config, "trail_stop_pct", 0.02)
    trail_state = {
        "entry_price": entry_price,
        "direction": direction,
        "initial_sl": initial_stop,
        "current_sl": initial_stop,
        "phase": "PROTECT",
        "trail_activation_pct": _trail_activation,
        "trail_stop_pct": _trail_stop,
        "highest_since_entry": entry_price if direction == "LONG" else None,
        "lowest_since_entry": entry_price if direction == "SHORT" else None,
        "candle_count": 0,
        "signal_id": candidate.get("signal_id", ""),
    }

    tracked = TrackedPosition(
        position_id="",
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        stop_loss=initial_stop,
        target_price=None,
        entry_time="",
        quantity=qty,
        current_qty=qty,
        status="OPEN",
        trail_state=trail_state,
    )

    # Write to DB if paper_db provided.
    if paper_db is not None:
        try:
            pos = paper_db.insert_position(
                session_id=session_id,
                symbol=symbol,
                direction=direction,
                avg_entry=entry_price,
                qty=qty,
                state="OPEN",
                metadata_json=trail_state,
            )
            tracked.position_id = pos.get("position_id", "")
            tracked.raw_position = pos
        except Exception:
            logger.exception("execute_entry: DB write failed for %s", symbol)
            return {"status": "error", "reason": "db_write_failed"}

        # Write entry order + fill to maintain full trade history (parity with close path).
        signal_id = trail_state.get("signal_id") or ""
        entry_side = "BUY" if direction == "LONG" else "SELL"
        try:
            now = datetime.now(tz=UTC)
            order = paper_db.insert_order(
                session_id=session_id,
                signal_id=signal_id or None,
                symbol=symbol,
                side=entry_side,
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
                    fill_price=entry_price,
                    qty=qty,
                    fees=round(entry_price * qty * 0.001, 4),
                    slippage_bps=slippage_bps,
                    side=entry_side,
                )
            if signal_id:
                paper_db.update_signal_state(signal_id, "ENTERED")
        except Exception:
            logger.exception("execute_entry: order/fill write failed for %s (non-fatal)", symbol)

    tracker.record_open(tracked, position_value)
    logger.info(
        "ENTRY %s %s @ %.2f qty=%d stop=%.2f",
        direction,
        symbol,
        entry_price,
        qty,
        initial_stop,
    )

    return {
        "status": "opened",
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "qty": qty,
        "initial_stop": initial_stop,
        "position_id": tracked.position_id,
    }


# ---------------------------------------------------------------------------
# Risk controls
# ---------------------------------------------------------------------------


def enforce_session_risk_controls(
    *,
    session: dict[str, Any],
    positions: list[dict[str, Any]],
    as_of: datetime,
    portfolio_value: float,
    unrealized_pnl: float = 0.0,
    realized_pnl: float = 0.0,
) -> dict[str, Any]:
    """Check session risk limits. Returns triggered=True if flatten required.

    ``realized_pnl`` should be the cumulative closed-position P&L for the session
    (from paper_db.get_session_realized_pnl).  ``unrealized_pnl`` is the mark-to-
    market gain/loss on currently open positions, computed per-symbol by the caller.
    ``positions`` is retained for backward compatibility but its P&L is not summed
    internally — pass explicit ``realized_pnl`` for correct accounting.
    """
    risk_config = session.get("risk_config", {})
    reasons: list[str] = []

    # Flatten time.
    flatten_time_str = risk_config.get("flatten_time", "15:15:00")
    flatten_time = time.fromisoformat(flatten_time_str)
    bar_time = as_of.time() if hasattr(as_of, "time") else None
    if bar_time is not None and bar_time >= flatten_time:
        reasons.append(f"flatten_time:{flatten_time_str}")

    # Net P&L = realized (closed trades, caller-supplied) + unrealized (open positions, caller-supplied).
    net_pnl = realized_pnl + unrealized_pnl

    # Daily loss limit.
    max_daily_loss_pct = risk_config.get("max_daily_loss_pct", 0.05)
    if net_pnl <= -(portfolio_value * max_daily_loss_pct):
        reasons.append(f"daily_loss_limit:{net_pnl:.2f}")

    # Max drawdown.
    max_drawdown_pct = risk_config.get("max_drawdown_pct", 0.15)
    if net_pnl <= -(portfolio_value * max_drawdown_pct):
        reasons.append(f"max_drawdown:{net_pnl:.2f}")

    triggered = len(reasons) > 0
    return {
        "triggered": triggered,
        "reasons": reasons,
        "daily_pnl_used": net_pnl,
    }


def build_summary_feed_state(
    *,
    session_id: str,
    tracker: SessionPositionTracker,
    last_bar_ts: datetime | None,
    feed_source: str,
    runtime_state: PaperRuntimeState,
) -> dict[str, Any]:
    """Build feed state dict for dashboard consumption."""
    return {
        "session_id": session_id,
        "source": feed_source,
        "mode": "paper",
        "status": "OK",
        "is_stale": False,
        "open_positions": tracker.open_count,
        "cash_available": tracker.cash_available,
        "current_equity": tracker.current_equity,
        "last_bar_ts": last_bar_ts.isoformat() if last_bar_ts else None,
        "symbols_tracked": len(runtime_state.symbols),
    }


def seed_candidates_from_market_db(
    market_db: Any,
    runtime_state: PaperRuntimeState,
    symbols: list[str],
    trade_date: str,
    direction: str = "LONG",
    strategy_config: Any | None = None,
    paper_db: Any = None,
    session_id: str | None = None,
) -> int:
    """Seed runtime state setup_rows from feat_daily for the trade date.

    Loads prev_close, atr_20, and other signal features for each symbol
    from feat_daily in the market DB.  Applies the same candidate-builder
    ranking as the backtest (apply_breakout_selection_ranking for LONG,
    apply_breakdown_selection_ranking for SHORT) so that selection_score /
    selection_rank in setup_row match backtest parity.

    ``direction`` should be "LONG" or "SHORT".  All candidates pass (no
    budget cap) — slot enforcement happens later in select_entries_for_bar.

    Returns the count of symbols successfully seeded from feat_daily.
    """
    from datetime import date as _date

    import polars as pl

    from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy

    if not symbols:
        return 0
    symbol_set = set(symbols)
    found: set[str] = set()
    ranked_rows: dict[str, dict[str, Any]] = {}

    try:
        extra_params = getattr(strategy_config, "extra_params", {}) or {}
        params = BacktestParams(
            breakout_threshold=float(getattr(strategy_config, "breakout_threshold", 0.04)),
            min_price=int(extra_params.get("min_price", 10)),
            min_value_traded_inr=float(extra_params.get("min_value_traded_inr", 3_000_000)),
            min_volume=int(extra_params.get("min_volume", 50_000)),
            breakout_daily_candidate_budget=0,
            breakdown_daily_candidate_budget=0,
        )
        try:
            strategy_key = str(getattr(strategy_config, "strategy_key", "") or "")
            if strategy_key:
                strategy = resolve_strategy(strategy_key)
                strategy.build_candidate_query(params, symbols, trade_date, trade_date)
        except Exception:
            logger.debug(
                "seed_candidates_from_market_db: strategy builder hook unavailable",
                exc_info=True,
            )
        symbols_placeholders = ",".join("?" for _ in symbols)
        query = f"""
            WITH prior_day AS (
                SELECT MAX(date) AS watch_date
                FROM v_daily
                WHERE date < CAST(? AS DATE)
            ),
            watch_rows AS (
                SELECT
                    d.symbol,
                    CAST(? AS DATE) AS trading_date,
                    p.watch_date,
                    d.close AS prev_close,
                    d.high AS prev_high,
                    d.low AS prev_low,
                    d.open AS prev_open,
                    d.volume AS prev_volume,
                    d.close * d.volume AS value_traded_inr
                FROM v_daily d
                CROSS JOIN prior_day p
                WHERE d.date = p.watch_date
                  AND d.symbol IN ({symbols_placeholders})
            )
            SELECT
                w.symbol,
                w.trading_date,
                w.watch_date,
                w.prev_close,
                w.prev_high,
                w.prev_low,
                w.prev_open,
                w.prev_volume,
                w.value_traded_inr,
                f.close_pos_in_range,
                f.ma_20,
                f.ret_5d,
                f.atr_20,
                f.vol_dryup_ratio,
                f.atr_compress_ratio,
                f.range_percentile,
                f.prior_breakouts_30d,
                f.prior_breakouts_90d,
                f.prior_breakdowns_90d,
                f.r2_65,
                f.ma_7,
                f.ma_65_sma,
                f.rs_252
            FROM watch_rows w
            LEFT JOIN feat_daily f
              ON w.symbol = f.symbol
             AND w.watch_date = f.date
            WHERE w.prev_close >= ?
              AND w.value_traded_inr >= ?
              AND w.prev_volume >= ?
              AND f.close_pos_in_range IS NOT NULL
            ORDER BY w.symbol
        """
        query_params = [
            trade_date,
            trade_date,
            *symbols,
            float(params.min_price),
            float(params.min_value_traded_inr),
            int(params.min_volume),
        ]
        df: pl.DataFrame = market_db.con.execute(query, query_params).pl()

        if not df.is_empty():
            if direction.upper() == "SHORT":
                ranked, _ = apply_breakdown_selection_ranking(df, params)
            else:
                ranked, _ = apply_breakout_selection_ranking(df, params)

            # Convert ranked DataFrame rows to dicts keyed by symbol.
            for row in ranked.to_dicts():
                sym = row.get("symbol", "")
                if sym in symbol_set:
                    ranked_rows[sym] = row
                    found.add(sym)

    except Exception as exc:
        logger.warning("seed_candidates_from_market_db: feat_daily query failed: %s", exc)

    # Populate runtime state from ranked rows; insert paper_signals rows.
    for sym in found:
        state = runtime_state.for_symbol(sym)
        state.setup_status = "candidate"
        row = ranked_rows[sym]
        state.setup_row = row
        if paper_db is not None and session_id is not None:
            try:
                from datetime import date as _date

                sig = paper_db.insert_signal(
                    session_id=session_id,
                    symbol=sym,
                    asof_date=_date.fromisoformat(trade_date),
                    state="NEW",
                    entry_mode=direction.lower(),
                    metadata_json={
                        "selection_score": row.get("selection_score"),
                        "selection_rank": row.get("selection_rank"),
                    },
                )
                state.setup_row["signal_id"] = sig.get("signal_id", "")
            except Exception:
                logger.warning("seed_candidates: could not insert signal for %s", sym)

    # Symbols missing from feat_daily: still mark candidate so engine can run.
    # evaluate_candle skips cleanly when prev_close is absent.
    for sym in symbol_set - found:
        state = runtime_state.for_symbol(sym)
        state.setup_status = "candidate"
        state.setup_row = {}
        if paper_db is not None and session_id is not None:
            try:
                from datetime import date as _date

                sig = paper_db.insert_signal(
                    session_id=session_id,
                    symbol=sym,
                    asof_date=_date.fromisoformat(trade_date),
                    state="NEW",
                    entry_mode=direction.lower(),
                    metadata_json={},
                )
                state.setup_row["signal_id"] = sig.get("signal_id", "")
            except Exception:
                logger.warning(
                    "seed_candidates: could not insert signal for %s (no feat_daily)", sym
                )

    logger.info(
        "seed_candidates_from_market_db: %d/%d symbols seeded from feat_daily for %s (direction=%s)",
        len(found),
        len(symbol_set),
        trade_date,
        direction,
    )
    return len(found)
