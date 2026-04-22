"""Post-market EOD carry decision engine.

Called after market close (once daily ingest + feature build complete) to decide
whether each open position should be carried overnight or exited at today's close.

Decision logic — mirrors the backtest H-carry filter exactly:
  - TIME_EXIT:       days_held increments only on carries AFTER the entry day.
                     Exit when new_days_held >= time_stop_days.
  - WEAK_CLOSE_EXIT: filter_h fails (close not in favourable end of range)
  - CARRY:           filter_h passes → keep position, increment days_held

days_held parity rule:
  The backtest exits at bar (entry_idx + time_stop_days), so a 3D time stop exits
  3 trading days after entry.  To match this, days_held must NOT be incremented on
  the EOD carry that runs the same night the position was opened.  Only subsequent
  nightly carries count toward the time stop.

Operator workflow (live sessions):
    1. nseml-kite-ingest --today
    2. nseml-build-features --since TODAY
    3. nseml-paper eod-carry --strategy STRATEGY --trade-date TODAY

For replay sessions, called automatically at the end of each day's replay loop.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core carry decision logic
# ---------------------------------------------------------------------------


def prefetch_daily_features(
    market_db: Any,
    symbols: list[str],
    trade_date: str,
) -> dict[str, dict[str, Any]]:
    """Query feat_daily for all symbols on trade_date.

    Returns symbol → {close_pos_in_range, low, high, close}.
    Symbols with no feat_daily row are excluded (position will use fallback).
    """
    if not symbols:
        return {}
    placeholders = ", ".join(f"'{s}'" for s in symbols)
    sql = f"""
        SELECT symbol, close_pos_in_range, low, high, close
        FROM feat_daily
        WHERE trade_date = '{trade_date}'
          AND symbol IN ({placeholders})
    """
    try:
        rows = market_db.con.execute(sql).fetchall()
    except Exception:
        logger.exception("prefetch_daily_features failed trade_date=%s", trade_date)
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        sym, close_pos, low, high, close = row
        result[sym] = {
            "close_pos_in_range": float(close_pos) if close_pos is not None else None,
            "low": float(low) if low is not None else None,
            "high": float(high) if high is not None else None,
            "close": float(close) if close is not None else None,
        }
    return result


def _eod_close_position(
    *,
    paper_db: Any,
    pos: dict[str, Any],
    session_id: str,
    exit_price: float,
    exit_reason: str,
    now: datetime,
) -> None:
    """Close a position at EOD with full order/fill/signal side effects.

    Mirrors _record_close_in_db() in paper_session_driver.py — keeps order/fill/signal
    tables consistent so dashboard and reconciliation see complete trade history.
    """
    position_id = pos["position_id"]
    symbol = pos["symbol"]
    qty = int(pos.get("qty", 0))
    avg_entry = float(pos.get("avg_entry", 0))
    direction = str(pos.get("direction", "LONG")).upper()
    meta = pos.get("metadata_json") or {}
    if not isinstance(meta, dict):
        meta = {}
    signal_id = meta.get("signal_id") or ""

    pnl = (exit_price - avg_entry) * qty if direction == "LONG" else (avg_entry - exit_price) * qty

    paper_db.update_position(
        position_id,
        closed_at=now,
        avg_exit=exit_price,
        pnl=pnl,
        state="CLOSED",
    )
    paper_db.patch_position_metadata(position_id, exit_reason=exit_reason)

    close_side = "BUY" if direction == "SHORT" else "SELL"
    try:
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
    except Exception:
        logger.exception("eod-carry: order/fill/signal write failed for %s", symbol)


def apply_eod_carry_decisions(
    *,
    session_id: str,
    trade_date: str,
    paper_db: Any,
    daily_features: dict[str, dict[str, Any]],
    strategy_config: Any,
) -> dict[str, Any]:
    """Apply H-carry decisions to all open positions in the session.

    Decision logic — mirrors the backtest H-carry rule exactly:
      - TIME_EXIT:            days_held + 1 >= time_stop_days → exit at feat_daily.close
      - H=True:               filter_h passes → carry; tighten stop to breakeven
      - H=False + losing/flat: WEAK_CLOSE_EXIT → exit at feat_daily.close
      - H=False + profitable:  carry; tighten stop to breakeven (same as H=True carry)

    Returns a summary dict: {carried, time_exit, weak_close_exit, no_data, total}.
    """
    open_positions = paper_db.list_open_positions(session_id)
    if not open_positions:
        return {"carried": 0, "time_exit": 0, "weak_close_exit": 0, "no_data": 0, "total": 0}

    time_stop_days: int = getattr(strategy_config, "time_stop_days", 5)
    h_carry_enabled: bool = getattr(strategy_config, "h_carry_enabled", True)
    h_filter_threshold: float = getattr(strategy_config, "h_filter_close_pos_threshold", 0.70)

    carried = time_exit = weak_close_exit = no_data = 0
    now = datetime.now(UTC)
    trade_date_obj = date.fromisoformat(trade_date)

    for pos in open_positions:
        symbol = pos["symbol"]
        direction = str(pos.get("direction", "LONG")).upper()
        meta = pos.get("metadata_json") or {}
        if not isinstance(meta, dict):
            meta = {}
        days_held = int(meta.get("days_held", 0))

        # Parity rule: the backtest exits at bar (entry_idx + time_stop_days), so a 3D
        # stop exits 3 full trading days after entry.  Do NOT increment days_held on the
        # carry pass that runs the same night the position was opened — that first carry
        # is "overnight 0 → day 1" and must not count as a completed hold day.
        opened_at_str = pos.get("opened_at") or ""
        is_entry_day = False
        if opened_at_str:
            try:
                opened_date = datetime.fromisoformat(str(opened_at_str)).date()
                is_entry_day = opened_date >= trade_date_obj
            except ValueError, TypeError:
                pass
        new_days_held = days_held if is_entry_day else days_held + 1

        avg_entry = float(pos.get("avg_entry", 0))

        feat = daily_features.get(symbol)
        if feat is None or feat.get("close") is None:
            logger.warning(
                "eod-carry: no feat_daily for %s trade_date=%s — skipping carry decision",
                symbol,
                trade_date,
            )
            no_data += 1
            continue

        daily_close = float(feat["close"])
        close_pos = feat.get("close_pos_in_range")
        prior_day_low = feat.get("low")
        prior_day_high = feat.get("high")

        # --- TIME_EXIT (highest priority) ---
        if new_days_held >= time_stop_days:
            logger.info(
                "eod-carry: TIME_EXIT %s days_held=%d time_stop=%d price=%.2f",
                symbol,
                days_held,
                time_stop_days,
                daily_close,
            )
            _eod_close_position(
                paper_db=paper_db,
                pos=pos,
                session_id=session_id,
                exit_price=daily_close,
                exit_reason="TIME_EXIT",
                now=now,
            )
            time_exit += 1
            continue

        # --- filter_h check ---
        # Matches filters.py check_h(): None → H fails (parity with backtest).
        if not h_carry_enabled:
            filter_h_pass = True
        elif close_pos is None:
            filter_h_pass = False  # zero-range bar or missing feature → treat as H failed
        elif direction == "LONG":
            filter_h_pass = close_pos >= h_filter_threshold
        else:  # SHORT
            filter_h_pass = close_pos <= (1.0 - h_filter_threshold)

        # --- H=False + losing/flat → WEAK_CLOSE_EXIT ---
        if not filter_h_pass:
            is_short = direction == "SHORT"
            # Losing/flat: SHORT when close >= entry; LONG when close <= entry.
            position_losing = (daily_close >= avg_entry) if is_short else (daily_close <= avg_entry)
            if position_losing:
                logger.info(
                    "eod-carry: WEAK_CLOSE_EXIT %s close_pos=%.3f price=%.2f",
                    symbol,
                    close_pos or 0.0,
                    daily_close,
                )
                _eod_close_position(
                    paper_db=paper_db,
                    pos=pos,
                    session_id=session_id,
                    exit_price=daily_close,
                    exit_reason="WEAK_CLOSE_EXIT",
                    now=now,
                )
                weak_close_exit += 1
                continue
            # H=False but profitable → fall through to carry with breakeven stop tightening.

        # --- CARRY: tighten stop to at least breakeven, update metadata ---
        # Mirrors backtest: H=True or (H=False + profitable) both carry with BE stop.
        current_sl = float(meta.get("current_sl") or pos.get("stop_loss") or avg_entry)
        if direction == "SHORT":
            tightened_sl = min(current_sl, avg_entry)  # SHORT stop must not exceed entry
        else:
            tightened_sl = max(current_sl, avg_entry)  # LONG stop must not fall below entry

        updated_meta = {
            **meta,
            "days_held": new_days_held,
            "current_sl": tightened_sl,
            "prior_day_low": prior_day_low,
            "prior_day_high": prior_day_high,
        }
        paper_db.update_position(pos["position_id"], metadata_json=updated_meta)
        logger.info(
            "eod-carry: CARRY %s days_held=%d close_pos=%s price=%.2f sl=%.2f→%.2f",
            symbol,
            new_days_held,
            f"{close_pos:.3f}" if close_pos is not None else "N/A",
            daily_close,
            current_sl,
            tightened_sl,
        )
        carried += 1

    summary = {
        "carried": carried,
        "time_exit": time_exit,
        "weak_close_exit": weak_close_exit,
        "no_data": no_data,
        "total": len(open_positions),
    }
    logger.info("eod-carry session=%s trade_date=%s summary=%s", session_id, trade_date, summary)
    return summary


# ---------------------------------------------------------------------------
# Run entry point (for CLI)
# ---------------------------------------------------------------------------


def run_eod_carry(
    *,
    session_id: str,
    trade_date: str,
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
    strategy: str | None = None,
) -> dict[str, Any]:
    """Run EOD carry decisions for a session. Returns summary dict."""
    from nse_momentum_lab.db.market_db import MarketDataDB
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB
    from nse_momentum_lab.services.paper.engine.shared_engine import get_paper_strategy_config

    paper_db = PaperDB(paper_db_path)
    market_db = MarketDataDB(Path(market_db_path))

    try:
        session = paper_db.get_session(session_id)
        if session is None:
            return {"error": f"Session {session_id} not found"}

        strategy_name = strategy or session.get("strategy_name", "2lynchbreakout")
        strategy_params = session.get("strategy_params") or {}
        strategy_config = get_paper_strategy_config(strategy_name, overrides=strategy_params)

        open_positions = paper_db.list_open_positions(session_id)
        if not open_positions:
            logger.info("eod-carry: no open positions for session %s", session_id)
            paper_db.update_session(session_id, status="COMPLETED")
            return {"session_id": session_id, "carried": 0, "status": "COMPLETED"}

        symbols = [p["symbol"] for p in open_positions]
        daily_features = prefetch_daily_features(market_db, symbols, trade_date)

        summary = apply_eod_carry_decisions(
            session_id=session_id,
            trade_date=trade_date,
            paper_db=paper_db,
            daily_features=daily_features,
            strategy_config=strategy_config,
        )

        # Pause or complete session based on remaining open positions.
        remaining = paper_db.list_open_positions(session_id)
        if remaining:
            paper_db.update_session(session_id, status="PAUSED")
            final_status = "PAUSED"
        else:
            paper_db.update_session(session_id, status="COMPLETED")
            final_status = "COMPLETED"

        return {"session_id": session_id, "status": final_status, **summary}

    finally:
        paper_db.close()
        market_db.close()
