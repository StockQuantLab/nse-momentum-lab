"""Paper trading live session script.

Connects to Kite WebSocket for real-time data and runs the shared paper engine.
Supports both live (Kite WebSocket) and local (DuckDB replay) modes.

Usage:
    python -m nse_momentum_lab.services.paper.scripts.paper_live \\
        --session-id SESSION_ID

Adapted from cpr-pivot-lab's paper_live.py pattern.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.market_db import MarketDataDB
from nse_momentum_lab.db.versioned_replica_sync import DEFAULT_PAPER_TABLES, VersionedReplicaSync
from nse_momentum_lab.services.kite.auth import KiteAuth
from nse_momentum_lab.services.paper.db.paper_db import PaperDB
from nse_momentum_lab.services.paper.engine.shared_engine import (
    PaperRuntimeState,
    SessionPositionTracker,
    complete_session,
    get_paper_strategy_config,
    process_closed_bar_group,
    seed_candidates_from_market_db,
)
from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
from nse_momentum_lab.services.paper.feeds.kite_ticker_adapter import KiteTickerAdapter
from nse_momentum_lab.services.paper.feeds.local_ticker_adapter import LocalTickerAdapter
from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
    AlertDispatcher,
    AlertEvent,
    AlertType,
    enqueue_daily_pnl_summary,
    get_alert_config,
)
from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 1.0
_CANDLE_INTERVAL = 5  # minutes
_STALE_TIMEOUT = 300  # seconds
_WEBSOCKET_RECOVERY_AFTER_SEC = 20.0
_FEED_STALE_ALERT_COOLDOWN_SEC = 300.0
_IST = ZoneInfo("Asia/Kolkata")


def _build_alert_dispatcher(
    paper_db: PaperDB | None = None, *, enabled: bool = True
) -> AlertDispatcher:
    """Build an AlertDispatcher wired from Doppler settings (TELEGRAM_BOT_TOKEN/CHAT_IDS)."""
    config = get_alert_config()
    return AlertDispatcher(paper_db=paper_db, config=config, enabled=enabled)


def _resolve_kite_credentials(
    api_key: str | None = None, access_token: str | None = None
) -> tuple[str | None, str | None]:
    """Resolve Kite credentials from explicit args, settings, or env vars.

    Settings construction is allowed to fail in unit tests that only exercise the
    retry wrapper. In that case we fall back to direct environment variables.
    """
    settings = None
    try:
        settings = get_settings()
    except Exception:
        logger.debug("Kite settings unavailable; falling back to environment")

    resolved_api_key = (
        api_key or getattr(settings, "kite_api_key", None) or os.getenv("KITE_API_KEY")
    )
    resolved_access_token = (
        access_token
        or getattr(settings, "kite_access_token", None)
        or os.getenv("KITE_ACCESS_TOKEN")
    )
    return resolved_api_key, resolved_access_token


def _resolve_kite_instrument_map(symbols: list[str]) -> dict[str, int]:
    """Resolve NSE symbols to Kite instrument tokens."""
    auth = KiteAuth()
    symbol_to_token: dict[str, int] = {}
    missing: list[str] = []
    for symbol in sorted({s.strip().upper() for s in symbols if s.strip()}):
        token = auth.get_instrument_token(symbol, exchange="NSE")
        if token is None:
            missing.append(symbol)
            continue
        symbol_to_token[symbol] = int(token)
    if missing:
        logger.warning(
            "Missing Kite instrument token(s) for %d/%d symbol(s): %s",
            len(missing),
            len(symbol_to_token) + len(missing),
            ", ".join(sorted(missing)[:25]),
        )
    if not symbol_to_token:
        raise RuntimeError("No Kite instrument tokens found for live session symbols")
    return symbol_to_token


def _sync_replica_after_write(
    replica: VersionedReplicaSync, paper_db: PaperDB, *, force: bool = False
) -> None:
    """Mark the paper replica dirty and sync it after a DB write."""
    replica.mark_dirty()
    if force:
        replica.force_sync(source_conn=paper_db.con)
    else:
        replica.maybe_sync(source_conn=paper_db.con)


def _terminal_session_status(final_status: str, tracker: SessionPositionTracker | None) -> str:
    """Map loop exit states to the DB session status contract."""
    normalized = str(final_status or "").upper()
    open_count = tracker.open_count if tracker is not None else 0
    if normalized in {"COMPLETED", "NO_SYMBOLS"}:
        return "COMPLETED"
    if normalized == "STOPPING" and open_count == 0:
        return "COMPLETED"
    if normalized in {"CANCELLED", "FAILED"}:
        return normalized
    if normalized == "RISK_BREACH":
        return "FAILED"
    if normalized == "MAX_CYCLES":
        return "COMPLETED"
    return normalized or "FAILED"


def _session_alert_already_sent(paper_db: PaperDB, session_id: str, alert_type: AlertType) -> bool:
    try:
        return paper_db.has_alert_log(session_id, str(alert_type.value), status="sent")
    except Exception:
        logger.debug(
            "Failed to query alert_log dedup state session=%s alert=%s",
            session_id,
            alert_type,
            exc_info=True,
        )
        return False


def _feed_alert_state(feed_state: dict[str, Any] | None) -> dict[str, Any]:
    raw_state = {}
    if isinstance(feed_state, dict):
        raw_state = feed_state.get("raw_state") or {}
    if not isinstance(raw_state, dict):
        raw_state = {}
    alert_state = raw_state.get("alert_state") or {}
    if not isinstance(alert_state, dict):
        alert_state = {}
    last_emitted_state = str(alert_state.get("last_emitted_state") or "").upper()
    if last_emitted_state not in {"OK", "STALE"}:
        feed_status = str((feed_state or {}).get("status", "")).upper()
        alert_state["last_emitted_state"] = "STALE" if feed_status == "STALE" else "OK"
    return alert_state


def _feed_raw_state(alert_state: dict[str, Any]) -> dict[str, Any]:
    return {"alert_state": dict(alert_state)}


def _live_tick_feed_status(
    *,
    now_ts: float,
    last_tick_ts: float | None,
    stale_timeout_sec: float = _STALE_TIMEOUT,
) -> tuple[str, bool, float | None]:
    """Classify live feed health from tick age, not from closed-bar cadence."""
    if last_tick_ts is None:
        return "OK", False, None
    tick_age_sec = max(0.0, float(now_ts) - float(last_tick_ts))
    if tick_age_sec >= stale_timeout_sec:
        return "STALE", True, tick_age_sec
    return "OK", False, tick_age_sec


def _build_open_position_manual_lines(tracker: SessionPositionTracker | None) -> list[str]:
    if tracker is None:
        return []
    lines: list[str] = []
    for symbol in sorted(tracker.open_symbols()):
        tracked = tracker.get_open_position(symbol)
        if tracked is None:
            continue
        target = f"₹{tracked.target_price:,.2f}" if tracked.target_price is not None else "-"
        risk_rupees = abs(tracked.entry_price - tracked.stop_loss) * float(tracked.current_qty)
        icon = "🟢" if str(tracked.direction).upper() == "LONG" else "🔴"
        lines.append(
            f"{icon} <code>{escape(symbol)}</code> {escape(str(tracked.direction))}\n"
            f"   Entry: <code>₹{tracked.entry_price:,.2f}</code> | "
            f"SL: <code>₹{tracked.stop_loss:,.2f}</code> | "
            f"Target: <code>{target}</code> | Qty: <code>{int(tracked.current_qty):,}</code>\n"
            f"   Risk: <code>₹{risk_rupees:,.0f}</code>"
        )
    return lines


def _format_ist_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=UTC).astimezone(_IST).strftime("%H:%M:%S IST")


def _format_feed_stale_details(
    *,
    session_id: str = "",
    transport: str,
    streak: int,
    tick_age_sec: float | None,
    last_tick_ts: float | None,
    tracker: SessionPositionTracker | None,
) -> str:
    body_lines = [
        "📡 <b>Feed stale</b>",
        f"Session: <code>{escape(session_id[:16])}</code>",
        f"Transport: <code>{escape(transport)}</code>",
        f"Streak: <code>{int(streak)}</code>",
    ]
    if tick_age_sec is not None:
        body_lines.append(f"Last tick age: <code>{tick_age_sec:.0f}s</code>")
    last_tick = _format_ist_ts(last_tick_ts)
    if last_tick is not None:
        body_lines.append(f"Last tick: <code>{last_tick}</code>")
    open_pos_lines = _build_open_position_manual_lines(tracker)
    if open_pos_lines:
        body_lines.append("")
        body_lines.append("⚡ <b>Open positions</b> — place manual SL orders now:")
        body_lines.extend(open_pos_lines)
    else:
        body_lines.append("")
        body_lines.append("No open positions.")
    return "\n".join(body_lines)


def _format_feed_recovered_details(
    *,
    session_id: str = "",
    stale_cycles: int,
    tracker: SessionPositionTracker | None,
    reconnect_count: int | None = None,
    down_duration_sec: float | None = None,
) -> str:
    open_count = tracker.open_count if tracker is not None else 0
    body_lines = [
        "✅ <b>Feed recovered</b>",
        f"Session: <code>{escape(session_id[:16])}</code>",
        f"Stale cycles: <code>{int(stale_cycles)}</code>",
        f"Monitoring: <code>{int(open_count)}</code> open position(s)",
    ]
    if down_duration_sec is not None:
        body_lines.append(f"WebSocket down: <code>{down_duration_sec:.0f}s</code>")
    if reconnect_count is not None:
        body_lines.append(f"Reconnects: <code>{int(reconnect_count)}</code>")
    body_lines.append("")
    body_lines.append("Market data is live again.")
    return "\n".join(body_lines)


def _log_ticker_health(
    *,
    session_id: str,
    ticker_adapter: Any,
    active_symbols: list[str],
) -> dict[str, Any] | None:
    """Emit one structured line of ticker health telemetry for logs."""
    if ticker_adapter is None or not hasattr(ticker_adapter, "health_stats"):
        return None
    try:
        stats = ticker_adapter.health_stats() or {}
        coverage = ticker_adapter.symbol_coverage(active_symbols, within_sec=_STALE_TIMEOUT) or {}
    except Exception:
        logger.debug("ticker health_stats failed", exc_info=True)
        return None
    logger.info(
        "TICKER_HEALTH session=%s connected=%s ticks=%d last_tick_age=%s reconnects=%d subs=%d coverage=%.0f%% (%d/%d) stale=%d missing=%d",
        session_id,
        stats.get("connected"),
        int(stats.get("tick_count", 0) or 0),
        (
            f"{float(stats['last_tick_age_sec']):.0f}s"
            if stats.get("last_tick_age_sec") is not None
            else "none"
        ),
        int(stats.get("reconnect_count", 0) or 0),
        int(stats.get("subscribed_tokens", 0) or 0),
        float(coverage.get("coverage_pct", 100.0) or 100.0),
        int(coverage.get("covered", 0) or 0),
        int(coverage.get("total", len(active_symbols)) or len(active_symbols)),
        int(coverage.get("stale", 0) or 0),
        int(coverage.get("missing", 0) or 0),
    )
    return {"stats": stats, "coverage": coverage}


def _write_feed_state(
    paper_db: PaperDB,
    *,
    session_id: str,
    source: str,
    mode: str,
    status: str,
    is_stale: bool,
    subscription_count: int,
    alert_state: dict[str, Any],
    heartbeat_at: datetime | None = None,
    last_quote_at: datetime | None = None,
    last_tick_at: datetime | None = None,
    last_bar_at: datetime | None = None,
) -> dict[str, Any]:
    return paper_db.upsert_feed_state(
        session_id=session_id,
        source=source,
        mode=mode,
        status=status,
        is_stale=is_stale,
        subscription_count=subscription_count,
        heartbeat_at=heartbeat_at,
        last_quote_at=last_quote_at,
        last_tick_at=last_tick_at,
        last_bar_at=last_bar_at,
        raw_state=_feed_raw_state(alert_state),
    )


def _maybe_emit_feed_transition(
    *,
    paper_db: PaperDB,
    session_id: str,
    alert_dispatcher: AlertDispatcher,
    alert_state: dict[str, Any],
    next_state: str,
    details: str,
    now_ts: datetime | None = None,
) -> bool:
    """Emit a FEED_STALE/FEED_RECOVERED alert once per transition episode."""
    normalized = str(next_state or "").strip().upper()
    if normalized not in {"OK", "STALE"}:
        return False

    now_dt = now_ts or datetime.now(UTC)
    current_state = str(alert_state.get("last_emitted_state") or "OK").upper()
    if current_state == normalized:
        return False
    if normalized == "STALE":
        last_stale_alert_at_raw = alert_state.get("last_stale_alert_at")
        last_stale_alert_at: datetime | None = None
        if isinstance(last_stale_alert_at_raw, str):
            try:
                last_stale_alert_at = datetime.fromisoformat(last_stale_alert_at_raw)
            except ValueError:
                last_stale_alert_at = None
        if last_stale_alert_at is not None:
            cooldown_elapsed = (now_dt - last_stale_alert_at).total_seconds()
            if cooldown_elapsed < _FEED_STALE_ALERT_COOLDOWN_SEC:
                return False

    alert_type = AlertType.FEED_STALE if normalized == "STALE" else AlertType.FEED_RECOVERED
    short_session = session_id[:16]
    subject = (
        f"⚠️ Feed Stale — {short_session}"
        if alert_type == AlertType.FEED_STALE
        else f"✅ Feed Recovered — {short_session}"
    )
    alert_dispatcher.enqueue(
        AlertEvent(
            alert_type=alert_type,
            session_id=session_id,
            subject=subject,
            body=details,
            level="warning" if alert_type == AlertType.FEED_STALE else "info",
        )
    )
    alert_state["last_emitted_state"] = normalized
    alert_state["last_transition_at"] = now_dt.isoformat()
    if normalized == "STALE":
        alert_state["last_stale_alert_at"] = now_dt.isoformat()
    # Keep the durable marker on the feed row so process restarts do not repeat
    # the same lifecycle alert for the same stale/healthy episode.
    try:
        feed_state = paper_db.get_feed_state(session_id) or {}
        _write_feed_state(
            paper_db,
            session_id=session_id,
            source=str(feed_state.get("source", "kite")),
            mode=str(feed_state.get("mode", "paper")),
            status=normalized,
            is_stale=(normalized == "STALE"),
            subscription_count=int(feed_state.get("subscription_count", 0) or 0),
            alert_state=alert_state,
            heartbeat_at=now_dt if normalized == "OK" else None,
        )
    except Exception:
        logger.debug(
            "Failed to persist feed transition marker session=%s state=%s",
            session_id,
            normalized,
            exc_info=True,
        )
    return True


async def run_live_session(
    *,
    session_id: str,
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
    paper_db: PaperDB | None = None,
    market_db: MarketDataDB | None = None,
    alert_dispatcher: AlertDispatcher | None = None,
    replica: VersionedReplicaSync | None = None,
    ticker_adapter: KiteTickerAdapter | LocalTickerAdapter | None = None,
    api_key: str | None = None,
    access_token: str | None = None,
    poll_interval: float = _POLL_INTERVAL,
    max_cycles: int | None = None,
    no_alerts: bool = False,
    auto_flatten_on_error: bool = True,
    alerts_sent: set[str] | None = None,
) -> dict[str, Any]:
    """Run a paper trading session — live or replay.

    Args:
        auto_flatten_on_error: When True (default), flatten open positions on
            unhandled exceptions. Set to False when wrapping in a retry loop
            so positions survive across retries.
    """
    if paper_db is None:
        paper_db = PaperDB(paper_db_path)
        own_paper_db = True
    else:
        own_paper_db = False
    if market_db is None:
        market_db = MarketDataDB(Path(market_db_path))
        own_market_db = True
    else:
        own_market_db = False
    if alert_dispatcher is None:
        alert_dispatcher = _build_alert_dispatcher(paper_db, enabled=not no_alerts)
        own_alert_dispatcher = True
    else:
        own_alert_dispatcher = False
    paper_path = Path(paper_db_path)
    if replica is None:
        replica = VersionedReplicaSync(
            source_path=paper_path,
            replica_dir=paper_path.parent / "paper_replica",
            prefix="paper_replica",
            min_interval_sec=2.0,
            tables=DEFAULT_PAPER_TABLES,
        )
        own_replica = True
    else:
        own_replica = False

    # Seed setup rows from market DB into runtime state.
    runtime_state = PaperRuntimeState()
    runtime_state.alerts_sent = alerts_sent if alerts_sent is not None else set()
    tracker: SessionPositionTracker | None = None
    local_feed = False
    final_status = "COMPLETED"
    created_adapter = False
    api_key, access_token = _resolve_kite_credentials(api_key, access_token)
    feed_state = None
    feed_alert_state: dict[str, Any] = {}

    try:
        # Load session.
        session = paper_db.get_session(session_id)
        if session is None:
            return {"error": f"Session {session_id} not found"}

        strategy = session.get("strategy_name", "2lynchbreakout")
        strategy_params = session.get("strategy_params") or {}
        strategy_config = get_paper_strategy_config(strategy, overrides=strategy_params)
        symbols = session.get("symbols", [])
        if not symbols:
            return {"error": "No symbols in session"}

        trade_date = session.get("trade_date", "")

        logger.info(
            "Session starting id=%s strategy=%s symbols=%d",
            session_id,
            strategy,
            len(symbols),
        )

        # Set session active.
        paper_db.update_session(session_id, status="ACTIVE")
        # Sync immediately so the session appears in the dashboard before first bar.
        replica.mark_dirty()
        replica.force_sync(source_conn=paper_db.con)

        # Start alert dispatcher.
        await alert_dispatcher.start()

        # Dispatch session started alert (dedup-guarded for retry scenarios).
        if not _session_alert_already_sent(paper_db, session_id, AlertType.SESSION_STARTED):
            _started_key = f"SESSION_STARTED:{session_id}"
            if _started_key not in runtime_state.alerts_sent:
                runtime_state.alerts_sent.add(_started_key)
                _session_body = (
                    f"🚀 <b>Session started</b>\n"
                    f"Strategy: <code>{escape(strategy)}</code>\n"
                    f"Session: <code>{session_id[:16]}</code>\n"
                    f"Symbols: <code>{len(symbols)}</code>\n"
                    f"Date: <code>{trade_date}</code>\n"
                    f"Mode: <code>live</code>"
                )
                alert_dispatcher.enqueue(
                    AlertEvent(
                        alert_type=AlertType.SESSION_STARTED,
                        session_id=session_id,
                        subject=f"🚀 Session started — {escape(strategy)}",
                        body=_session_body,
                    )
                )
        # Seed durable feed-alert state so stale/recovered alerts survive restarts.
        feed_state = paper_db.get_feed_state(session_id)
        feed_alert_state = _feed_alert_state(feed_state)
        if "last_emitted_state" not in feed_alert_state:
            feed_alert_state["last_emitted_state"] = "OK"
        if feed_state is None:
            _write_feed_state(
                paper_db,
                session_id=session_id,
                source="replay" if local_feed else "kite",
                mode="paper",
                status="OK",
                is_stale=False,
                subscription_count=len(symbols),
                alert_state=feed_alert_state,
            )

        # Setup engine state.
        tracker = SessionPositionTracker(
            max_positions=strategy_config.max_positions,
            portfolio_value=session.get("risk_config", {}).get("portfolio_value", 1_000_000),
            max_position_pct=strategy_config.max_position_pct,
        )

        # Adopt open positions from prior sessions for this strategy (cross-day carry).
        paper_db.adopt_open_positions_from_strategy(session_id, strategy)

        # Seed existing positions (carried from prior session or intra-day resume).
        existing = paper_db.list_open_positions(session_id)
        if existing:
            tracker.seed_open_positions(existing)

        # Expand symbol universe to include any carried position symbols not in today's list.
        carried_symbols = {p["symbol"] for p in existing}
        symbols = list(set(symbols) | carried_symbols)

        # Seed candidate setup_rows from feat_daily so evaluate_candle can open trades.
        seed_candidates_from_market_db(
            market_db,
            runtime_state,
            list(symbols),
            trade_date,
            direction=strategy_config.direction,
            strategy_config=strategy_config,
            paper_db=paper_db,
            session_id=session_id,
        )

        # Setup ticker adapter.
        if ticker_adapter is None:
            local_feed = False
            ticker_adapter = KiteTickerAdapter(api_key=api_key, access_token=access_token)
            created_adapter = True
        else:
            local_feed = getattr(ticker_adapter, "_local_feed", False)

        builder = FiveMinuteCandleBuilder(interval_minutes=_CANDLE_INTERVAL)

        if isinstance(ticker_adapter, KiteTickerAdapter):
            instrument_map = _resolve_kite_instrument_map(symbols)
            if instrument_map:
                ticker_adapter.set_instrument_map(instrument_map)
            ticker_adapter.register_session(session_id, symbols, builder)
        else:
            ticker_adapter.register_session(session_id, symbols, builder)

        # Main loop.
        active_symbols = list(symbols)
        cycles = 0
        last_bar_ts: float | None = None
        closed_bars = 0
        last_bucket_start: float = 0.0
        no_snapshot_streak = 0
        last_close_prices: dict[str, float] = {}

        while True:
            if max_cycles is not None and cycles >= max_cycles:
                final_status = "MAX_CYCLES"
                break

            # Reload session for status changes.
            session = paper_db.get_session(session_id)
            if session is None:
                final_status = "MISSING"
                break
            status = session.get("status", "ACTIVE")
            if status in ("STOPPING", "COMPLETED", "CANCELLED", "FAILED"):
                final_status = status
                break
            if status == "PAUSED":
                _write_feed_state(
                    paper_db,
                    session_id=session_id,
                    source="replay" if local_feed else "kite",
                    mode="paper",
                    status="PAUSED",
                    is_stale=False,
                    subscription_count=len(active_symbols),
                    alert_state=feed_alert_state,
                )
                _sync_replica_after_write(replica, paper_db)
                await asyncio.sleep(poll_interval)
                cycles += 1
                continue

            # Drain closed candles.
            closed_candles: list = []
            if local_feed:
                closed_candles = ticker_adapter.drain_closed(session_id)
                if getattr(ticker_adapter, "exhausted", False):
                    final_status = "COMPLETED"
                    break
            else:
                # Live: only drain when bucket boundary advances.
                now = time.time()
                current_bucket = (int(now) // (_CANDLE_INTERVAL * 60)) * (_CANDLE_INTERVAL * 60)
                if current_bucket > last_bucket_start:
                    # Synthesize quiet symbols before draining.
                    ticker_adapter.synthesize_quiet_symbols(session_id, active_symbols, now)
                    closed_candles = ticker_adapter.drain_closed(session_id)
                    last_bucket_start = current_bucket

                    if isinstance(ticker_adapter, KiteTickerAdapter):
                        _log_ticker_health(
                            session_id=session_id,
                            ticker_adapter=ticker_adapter,
                            active_symbols=active_symbols,
                        )

                    # WebSocket recovery watchdog.
                    if isinstance(ticker_adapter, KiteTickerAdapter):
                        recovery = ticker_adapter.recover_connection(
                            now=now, reconnect_after_sec=_WEBSOCKET_RECOVERY_AFTER_SEC
                        )
                        if recovery["action"] == "recovered":
                            logger.info("WebSocket recovered")
                            _maybe_emit_feed_transition(
                                paper_db=paper_db,
                                session_id=session_id,
                                alert_dispatcher=alert_dispatcher,
                                alert_state=feed_alert_state,
                                next_state="OK",
                                details=_format_feed_recovered_details(
                                    session_id=session_id,
                                    stale_cycles=no_snapshot_streak,
                                    tracker=tracker,
                                    reconnect_count=int(ticker_adapter.reconnect_count),
                                    down_duration_sec=float(recovery.get("down_sec", 0) or 0),
                                ),
                                now_ts=datetime.fromtimestamp(now, tz=UTC),
                            )
                        elif recovery["action"] == "failed":
                            if not _session_alert_already_sent(
                                paper_db, session_id, AlertType.SESSION_ERROR
                            ):
                                alert_dispatcher.enqueue(
                                    AlertEvent(
                                        alert_type=AlertType.SESSION_ERROR,
                                        session_id=session_id,
                                        subject=f"WebSocket recovery failed: {session_id}",
                                        body=f"Error: {recovery.get('error', 'unknown')}",
                                        level="error",
                                    )
                                )

            # Group by bar_end and process.
            if closed_candles:
                bars_by_end: dict[float, list] = {}
                for c in closed_candles:
                    bars_by_end.setdefault(c.bar_end, []).append(c)

                no_snapshot_streak = 0

                for bar_end in sorted(bars_by_end):
                    bar_candles = bars_by_end[bar_end]

                    # Record feed audit (before engine conversion so ClosedCandle fields are intact).
                    record_closed_candles(
                        bar_candles=bar_candles,
                        session_id=session_id,
                        trade_date=str(session.get("trade_date", trade_date)),
                        feed_source="replay" if local_feed else "kite",
                        paper_db=paper_db,
                        transport="local" if local_feed else "websocket",
                    )

                    candle_dicts = [
                        {
                            "symbol": c.symbol,
                            "bar_end": bar_end,
                            "ts": bar_end,
                            "open": c.open,
                            "high": c.high,
                            "low": c.low,
                            "close": c.close,
                            "volume": c.volume,
                        }
                        for c in bar_candles
                    ]
                    # Track latest close prices for crash-recovery flatten.
                    for c in bar_candles:
                        last_close_prices[c.symbol] = c.close

                    result = await process_closed_bar_group(
                        session_id=session_id,
                        session=session,
                        bar_candles=candle_dicts,
                        runtime_state=runtime_state,
                        tracker=tracker,
                        strategy_config=strategy_config,
                        active_symbols=active_symbols,
                        feed_source="live" if not local_feed else "replay",
                        paper_db=paper_db,
                        alert_dispatcher=alert_dispatcher,
                    )

                    active_symbols = result["active_symbols"]
                    last_bar_ts = bar_end
                    closed_bars += 1

                    _maybe_emit_feed_transition(
                        paper_db=paper_db,
                        session_id=session_id,
                        alert_dispatcher=alert_dispatcher,
                        alert_state=feed_alert_state,
                        next_state="OK",
                        details=_format_feed_recovered_details(
                            session_id=session_id,
                            stale_cycles=no_snapshot_streak,
                            tracker=tracker,
                            reconnect_count=(
                                int(ticker_adapter.reconnect_count)
                                if isinstance(ticker_adapter, KiteTickerAdapter)
                                else None
                            ),
                        ),
                        now_ts=datetime.fromtimestamp(bar_end, tz=UTC)
                        if isinstance(bar_end, (int, float))
                        else datetime.now(UTC),
                    )

                    # Write feed heartbeat for dashboard visibility.
                    _write_feed_state(
                        paper_db,
                        session_id=session_id,
                        source="replay" if local_feed else "kite",
                        mode="paper",
                        status="OK",
                        is_stale=False,
                        subscription_count=len(active_symbols),
                        last_bar_at=datetime.fromtimestamp(bar_end, tz=UTC)
                        if isinstance(bar_end, (int, float))
                        else None,
                        alert_state=feed_alert_state,
                    )
                    _sync_replica_after_write(replica, paper_db)

                    if result["should_complete"]:
                        final_status = "COMPLETED"
                        break

                    if result["triggered"]:
                        final_status = "RISK_BREACH"
                        risk_reasons = result.get("risk_reasons", [])
                        # Dispatch the most specific risk alert type.
                        is_drawdown = any("max_drawdown" in r for r in risk_reasons)
                        alert_type = (
                            AlertType.DRAWDOWN_LIMIT if is_drawdown else AlertType.DAILY_LOSS_LIMIT
                        )
                        alert_dispatcher.enqueue(
                            AlertEvent(
                                alert_type=alert_type,
                                session_id=session_id,
                                subject=f"Risk breach: {session_id}",
                                body=f"Reasons: {risk_reasons}",
                                level="error",
                            )
                        )
                        break

                    if not active_symbols:
                        final_status = "NO_SYMBOLS"
                        break
            else:
                # Stale detection (live only).
                if not local_feed:
                    live_status = "STALE"
                    tick_age_sec: float | None = None
                    if isinstance(ticker_adapter, KiteTickerAdapter):
                        live_status, is_stale, tick_age_sec = _live_tick_feed_status(
                            now_ts=now,
                            last_tick_ts=ticker_adapter.last_tick_ts,
                            stale_timeout_sec=_STALE_TIMEOUT,
                        )
                    else:
                        is_stale = True
                    if is_stale:
                        no_snapshot_streak += 1
                        _write_feed_state(
                            paper_db,
                            session_id=session_id,
                            source="kite",
                            mode="paper",
                            status=live_status,
                            is_stale=True,
                            subscription_count=len(active_symbols),
                            last_tick_at=datetime.fromtimestamp(ticker_adapter.last_tick_ts, tz=UTC)
                            if isinstance(ticker_adapter, KiteTickerAdapter)
                            and ticker_adapter.last_tick_ts is not None
                            else None,
                            last_bar_at=datetime.fromtimestamp(last_bar_ts, tz=UTC)
                            if isinstance(last_bar_ts, (int, float))
                            else None,
                            alert_state=feed_alert_state,
                        )
                        _sync_replica_after_write(replica, paper_db)
                    else:
                        no_snapshot_streak = 0
                        _maybe_emit_feed_transition(
                            paper_db=paper_db,
                            session_id=session_id,
                            alert_dispatcher=alert_dispatcher,
                            alert_state=feed_alert_state,
                            next_state="OK",
                            details=_format_feed_recovered_details(
                                session_id=session_id,
                                stale_cycles=no_snapshot_streak,
                                tracker=tracker,
                                reconnect_count=(
                                    int(ticker_adapter.reconnect_count)
                                    if isinstance(ticker_adapter, KiteTickerAdapter)
                                    else None
                                ),
                            ),
                            now_ts=datetime.fromtimestamp(now, tz=UTC),
                        )
                        # Live heartbeat: keep the dashboard replica current even when
                        # no 5-minute bar has closed yet.
                        _write_feed_state(
                            paper_db,
                            session_id=session_id,
                            source="kite",
                            mode="paper",
                            status="OK",
                            is_stale=False,
                            subscription_count=len(active_symbols),
                            heartbeat_at=datetime.fromtimestamp(now, tz=UTC),
                            last_tick_at=datetime.fromtimestamp(ticker_adapter.last_tick_ts, tz=UTC)
                            if isinstance(ticker_adapter, KiteTickerAdapter)
                            and ticker_adapter.last_tick_ts is not None
                            else None,
                            last_bar_at=datetime.fromtimestamp(last_bar_ts, tz=UTC)
                            if isinstance(last_bar_ts, (int, float))
                            else None,
                            alert_state=feed_alert_state,
                        )
                        _sync_replica_after_write(replica, paper_db)
                    if is_stale and no_snapshot_streak >= 3:
                        details = _format_feed_stale_details(
                            session_id=session_id,
                            transport="websocket",
                            streak=no_snapshot_streak,
                            tick_age_sec=tick_age_sec,
                            last_tick_ts=(
                                ticker_adapter.last_tick_ts
                                if isinstance(ticker_adapter, KiteTickerAdapter)
                                else None
                            ),
                            tracker=tracker,
                        )
                        _maybe_emit_feed_transition(
                            paper_db=paper_db,
                            session_id=session_id,
                            alert_dispatcher=alert_dispatcher,
                            alert_state=feed_alert_state,
                            next_state="STALE",
                            details=details,
                            now_ts=datetime.fromtimestamp(now, tz=UTC),
                        )

            await asyncio.sleep(poll_interval)
            cycles += 1

        # Finalize session — flatten any open positions for operator stops / feed death.
        # Crash/exception path does this in the except block; normal exits need it too.
        if final_status in ("STOPPING", "CANCELLED", "RISK_BREACH", "NO_SYMBOLS"):
            if tracker is not None and tracker.open_count > 0:
                paper_db.flatten_open_positions(session_id, mark_prices=last_close_prices)
                for symbol in list(tracker.open_symbols()):
                    tracked = tracker.get_open_position(symbol)
                    if tracked:
                        tracker.record_close(symbol, tracked.entry_price * tracked.current_qty)

        db_status = _terminal_session_status(final_status, tracker)
        await complete_session(session_id=session_id, paper_db=paper_db, status=db_status)
        replica.force_sync(source_conn=paper_db.con)

        # Dispatch DAILY_PNL_SUMMARY with realized + unrealized breakdown (swing-trading daily scorecard).
        _dispatch_daily_pnl_summary(
            alert_dispatcher=alert_dispatcher,
            session_id=session_id,
            paper_db=paper_db,
            strategy=strategy,
            trade_date=trade_date,
            portfolio_value=session.get("risk_config", {}).get("portfolio_value", 1_000_000),
            alerts_sent=runtime_state.alerts_sent,
            mark_prices=last_close_prices,
        )

        if not _session_alert_already_sent(paper_db, session_id, AlertType.SESSION_COMPLETED):
            alert_dispatcher.enqueue(
                AlertEvent(
                    alert_type=AlertType.SESSION_COMPLETED,
                    session_id=session_id,
                    subject=f"Session {session_id} completed: {final_status}",
                    body=f"Bars: {closed_bars}\nCycles: {cycles}",
                )
            )

        logger.info(
            "Session complete id=%s status=%s bars=%d cycles=%d",
            session_id,
            final_status,
            closed_bars,
            cycles,
        )

        return {
            "session_id": session_id,
            "status": final_status,
            "closed_bars": closed_bars,
            "cycles": cycles,
            "last_bar_ts": last_bar_ts,
        }

    except Exception as e:
        logger.exception("Live session failed for %s", session_id)
        if auto_flatten_on_error and tracker is not None:
            # Persist flattened positions to DB before marking FAILED.
            paper_db.flatten_open_positions(session_id, mark_prices=last_close_prices)
            # Also update in-memory tracker.
            for symbol in list(tracker.open_symbols()):
                tracked = tracker.get_open_position(symbol)
                if tracked:
                    tracker.record_close(symbol, tracked.entry_price * tracked.current_qty)
        await complete_session(session_id=session_id, paper_db=paper_db, status="FAILED")
        replica.force_sync(source_conn=paper_db.con)
        return {"error": str(e), "session_id": session_id}
    finally:
        # Final sync before teardown.
        try:
            if own_replica:
                replica.force_sync(source_conn=paper_db.con)
        except Exception:
            logger.debug("Final replica sync failed — ignoring")
        # Teardown.
        if ticker_adapter is not None:
            ticker_adapter.unregister_session(session_id)
            if created_adapter:
                ticker_adapter.close()
        if own_alert_dispatcher:
            await alert_dispatcher.shutdown()
        # Purge old feed audit rows (retention housekeeping).
        try:
            if own_paper_db:
                retention = get_settings().feed_audit_retention_days
                paper_db.purge_old_feed_audit_rows(retention)
        except Exception:
            logger.exception("feed_audit: purge failed session=%s", session_id)
        if own_paper_db:
            paper_db.close()
        if own_market_db:
            market_db.close()


# ---------------------------------------------------------------------------
# Auto-retry wrapper — keeps swing positions alive across transient failures
# ---------------------------------------------------------------------------

_RETRY_MAX = 5
_RETRY_WAIT_BASE_SEC = 10.0
_RETRYABLE_STATUSES = {"FAILED", "STALE"}


def _should_retry(result: dict[str, Any] | None, attempt: int, max_attempts: int) -> bool:
    """Decide if a failed session should be retried.

    Acceptance: this function never changes entry/exit/carry/stop logic.
    It only decides whether to restart the session runner.
    """
    if attempt >= max_attempts:
        return False
    if result is None:
        return True
    status = result.get("status", "")
    if status in _RETRYABLE_STATUSES:
        return True
    error = str(result.get("error", "")).strip().lower()
    if error:
        if any(
            token in error
            for token in ("not found", "no symbols", "invalid session", "permission denied")
        ):
            return False
        return True
    return False


async def run_live_session_with_retry(
    *,
    session_id: str,
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
    paper_db: PaperDB | None = None,
    market_db: MarketDataDB | None = None,
    alert_dispatcher: AlertDispatcher | None = None,
    replica: VersionedReplicaSync | None = None,
    api_key: str | None = None,
    access_token: str | None = None,
    poll_interval: float = _POLL_INTERVAL,
    max_cycles: int | None = None,
    no_alerts: bool = False,
    ticker_adapter: KiteTickerAdapter | None = None,
    max_retries: int = _RETRY_MAX,
) -> dict[str, Any]:
    """Run a live session with automatic retry on transient failures.

    KEY CONSTRAINT: Positions are NOT flattened between retries. The session
    re-seeds from DB state on each restart, so carried positions survive.
    Only the final exhaustion (all retries used) triggers flatten-on-error.
    """
    result: dict[str, Any] = {}
    alerts_sent: set[str] = set()

    for attempt in range(1, max_retries + 1):
        is_final_attempt = attempt == max_retries

        try:
            result = await run_live_session(
                session_id=session_id,
                paper_db_path=paper_db_path,
                market_db_path=market_db_path,
                paper_db=paper_db,
                market_db=market_db,
                alert_dispatcher=alert_dispatcher,
                replica=replica,
                ticker_adapter=ticker_adapter,
                api_key=api_key,
                access_token=access_token,
                poll_interval=poll_interval,
                max_cycles=max_cycles,
                no_alerts=no_alerts,
                # Only flatten on the final attempt — earlier retries keep positions alive.
                auto_flatten_on_error=is_final_attempt,
                alerts_sent=alerts_sent,
            )
        except Exception as e:
            result = {"error": str(e), "session_id": session_id, "status": "FAILED"}

        # Check if retry is warranted.
        if not _should_retry(result, attempt, max_retries):
            return result

        wait_sec = _RETRY_WAIT_BASE_SEC * attempt
        logger.warning(
            "Session %s attempt %d/%d failed (status=%s), retrying in %.0fs — "
            "positions NOT flattened, will re-seed from DB",
            session_id,
            attempt,
            max_retries,
            result.get("status", result.get("error", "unknown")),
            wait_sec,
        )
        await asyncio.sleep(wait_sec)

    return result


async def run_live_session_group(
    *,
    session_ids: list[str],
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
    api_key: str | None = None,
    access_token: str | None = None,
    poll_interval: float = _POLL_INTERVAL,
    max_cycles: int | None = None,
    no_alerts: bool = False,
    max_retries: int = _RETRY_MAX,
) -> dict[str, dict[str, Any]]:
    """Run multiple live sessions in one process with shared DB/feed resources.

    This is the CPR-style orchestration path: one writer process, one websocket,
    one alert dispatcher, many session contexts.
    """
    if not session_ids:
        return {}

    paper_db = PaperDB(paper_db_path)
    market_db = MarketDataDB(Path(market_db_path))
    alert_dispatcher = _build_alert_dispatcher(paper_db, enabled=not no_alerts)
    paper_path = Path(paper_db_path)
    resolved_api_key, resolved_access_token = _resolve_kite_credentials(api_key, access_token)
    replica = VersionedReplicaSync(
        source_path=paper_path,
        replica_dir=paper_path.parent / "paper_replica",
        prefix="paper_replica",
        min_interval_sec=5.0,
        tables=DEFAULT_PAPER_TABLES,
    )
    shared_adapter = KiteTickerAdapter(api_key=resolved_api_key, access_token=resolved_access_token)
    initial_results: dict[str, dict[str, Any]] = {}
    valid_session_ids: list[str] = []
    all_symbols: list[str] = []
    for session_id in session_ids:
        session = paper_db.get_session(session_id)
        if session is None:
            initial_results[session_id] = {
                "error": f"Session {session_id} not found",
                "session_id": session_id,
            }
            continue
        valid_session_ids.append(session_id)
        all_symbols.extend(list(session.get("symbols") or []))

    if not valid_session_ids:
        try:
            await alert_dispatcher.start()
        finally:
            await alert_dispatcher.shutdown()
            paper_db.close()
            market_db.close()
        return initial_results

    instrument_map = _resolve_kite_instrument_map(all_symbols)
    if instrument_map:
        shared_adapter.set_instrument_map(instrument_map)
    try:
        shared_adapter.connect()
    except Exception:
        logger.exception("Failed to connect shared Kite websocket for multi-session run")
        raise

    tasks: list[asyncio.Task[dict[str, Any]]] = []
    try:
        await alert_dispatcher.start()
        for session_id in valid_session_ids:
            tasks.append(
                asyncio.create_task(
                    run_live_session_with_retry(
                        session_id=session_id,
                        paper_db_path=paper_db_path,
                        market_db_path=market_db_path,
                        paper_db=paper_db,
                        market_db=market_db,
                        alert_dispatcher=alert_dispatcher,
                        replica=replica,
                        ticker_adapter=shared_adapter,
                        api_key=api_key,
                        access_token=access_token,
                        poll_interval=poll_interval,
                        max_cycles=max_cycles,
                        no_alerts=no_alerts,
                        max_retries=max_retries,
                    )
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        grouped: dict[str, dict[str, Any]] = dict(initial_results)
        for session_id, result in zip(valid_session_ids, results, strict=True):
            if isinstance(result, Exception):
                grouped[session_id] = {"error": str(result), "session_id": session_id}
            else:
                grouped[session_id] = result
        return grouped
    finally:
        try:
            replica.force_sync(source_conn=paper_db.con)
        except Exception:
            logger.debug("Multi-session replica sync failed — ignoring")
        try:
            await alert_dispatcher.shutdown()
        except Exception:
            logger.debug("Multi-session alert dispatcher shutdown failed", exc_info=True)
        try:
            shared_adapter.close()
        except Exception:
            logger.debug("Multi-session adapter close failed", exc_info=True)
        try:
            retention = get_settings().feed_audit_retention_days
            paper_db.purge_old_feed_audit_rows(retention)
        except Exception:
            logger.exception("feed_audit: purge failed for multi-session run")
        paper_db.close()
        market_db.close()


def _dispatch_daily_pnl_summary(
    *,
    alert_dispatcher: AlertDispatcher,
    session_id: str,
    paper_db: PaperDB,
    strategy: str,
    trade_date: str,
    portfolio_value: float,
    mark_prices: dict[str, float] | None = None,
    alerts_sent: set[str] | None = None,
) -> None:
    """Compute and enqueue DAILY_PNL_SUMMARY with shared notifier logic."""
    enqueue_daily_pnl_summary(
        alert_dispatcher=alert_dispatcher,
        session_id=session_id,
        paper_db=paper_db,
        strategy=strategy,
        trade_date=trade_date,
        portfolio_value=portfolio_value,
        mark_prices=mark_prices,
        alerts_sent=alerts_sent,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper trading live session")
    parser.add_argument("--session-id", required=True, help="Paper session ID")
    parser.add_argument("--paper-db", default="data/paper.duckdb")
    parser.add_argument("--market-db", default="data/market.duckdb")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--max-cycles", type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    result = asyncio.run(
        run_live_session_with_retry(
            session_id=args.session_id,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
            poll_interval=args.poll_interval,
            max_cycles=args.max_cycles,
        )
    )
    print(result)
    sys.exit(0 if "error" not in result else 1)


if __name__ == "__main__":
    main()
