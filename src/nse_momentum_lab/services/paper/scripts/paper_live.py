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
from pathlib import Path
from typing import Any

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.market_db import MarketDataDB
from nse_momentum_lab.db.versioned_replica_sync import DEFAULT_PAPER_TABLES, VersionedReplicaSync
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
    format_daily_pnl_summary,
    get_alert_config,
)
from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 1.0
_CANDLE_INTERVAL = 5  # minutes
_STALE_TIMEOUT = 300  # seconds
_WEBSOCKET_RECOVERY_AFTER_SEC = 20.0


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


async def run_live_session(
    *,
    session_id: str,
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
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
    paper_db = PaperDB(paper_db_path)
    market_db = MarketDataDB(Path(market_db_path))
    alert_dispatcher = _build_alert_dispatcher(paper_db, enabled=not no_alerts)
    paper_path = Path(paper_db_path)
    replica = VersionedReplicaSync(
        source_path=paper_path,
        replica_dir=paper_path.parent / "paper_replica",
        prefix="paper_replica",
        min_interval_sec=5.0,
        tables=DEFAULT_PAPER_TABLES,
    )

    # Seed setup rows from market DB into runtime state.
    runtime_state = PaperRuntimeState()
    runtime_state.alerts_sent = alerts_sent if alerts_sent is not None else set()
    tracker: SessionPositionTracker | None = None
    local_feed = False
    final_status = "COMPLETED"
    created_adapter = False
    api_key, access_token = _resolve_kite_credentials(api_key, access_token)

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
        _started_key = f"SESSION_STARTED:{session_id}"
        if _started_key not in runtime_state.alerts_sent:
            runtime_state.alerts_sent.add(_started_key)
            alert_dispatcher.enqueue(
                AlertEvent(
                    alert_type=AlertType.SESSION_STARTED,
                    session_id=session_id,
                    subject=f"Session {session_id} started",
                    body=f"Strategy: {strategy}\nSymbols: {len(symbols)}\nDate: {trade_date}",
                )
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
                paper_db.upsert_feed_state(
                    session_id=session_id,
                    source="replay" if local_feed else "kite",
                    mode="paper",
                    status="PAUSED",
                    is_stale=False,
                    subscription_count=len(active_symbols),
                )
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

                    # WebSocket recovery watchdog.
                    if isinstance(ticker_adapter, KiteTickerAdapter):
                        recovery = ticker_adapter.recover_connection(
                            now=now, reconnect_after_sec=_WEBSOCKET_RECOVERY_AFTER_SEC
                        )
                        if recovery["action"] == "recovered":
                            logger.info("WebSocket recovered")
                            alert_dispatcher.enqueue(
                                AlertEvent(
                                    alert_type=AlertType.FEED_RECOVERED,
                                    session_id=session_id,
                                    subject=f"Feed recovered: {session_id}",
                                    body="WebSocket reconnection successful",
                                )
                            )
                        elif recovery["action"] == "failed":
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
                    replica.mark_dirty()
                    replica.maybe_sync(source_conn=paper_db.con)

                    # Write feed heartbeat for dashboard visibility.
                    paper_db.upsert_feed_state(
                        session_id=session_id,
                        source="replay" if local_feed else "kite",
                        mode="paper",
                        status="OK",
                        is_stale=False,
                        subscription_count=len(active_symbols),
                        last_bar_ts=datetime.fromtimestamp(bar_end, tz=UTC)
                        if isinstance(bar_end, (int, float))
                        else None,
                    )

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
                    no_snapshot_streak += 1
                    # Write stale feed state for dashboard.
                    paper_db.upsert_feed_state(
                        session_id=session_id,
                        source="kite",
                        mode="paper",
                        status="STALE",
                        is_stale=True,
                        subscription_count=len(active_symbols),
                    )
                    if no_snapshot_streak >= 3:
                        alert_dispatcher.enqueue(
                            AlertEvent(
                                alert_type=AlertType.FEED_STALE,
                                session_id=session_id,
                                subject=f"Feed stale: {session_id}",
                                body=f"No data for {no_snapshot_streak} cycles",
                                level="warning",
                            )
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

        await complete_session(session_id=session_id, paper_db=paper_db, status=final_status)
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
            replica.force_sync(source_conn=paper_db.con)
        except Exception:
            logger.debug("Final replica sync failed — ignoring")
        # Teardown.
        if ticker_adapter is not None:
            ticker_adapter.unregister_session(session_id)
            if created_adapter:
                ticker_adapter.close()
        await alert_dispatcher.shutdown()
        # Purge old feed audit rows (retention housekeeping).
        try:
            retention = get_settings().feed_audit_retention_days
            paper_db.purge_old_feed_audit_rows(retention)
        except Exception:
            logger.exception("feed_audit: purge failed session=%s", session_id)
        paper_db.close()
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
    api_key: str | None = None,
    access_token: str | None = None,
    poll_interval: float = _POLL_INTERVAL,
    max_cycles: int | None = None,
    no_alerts: bool = False,
    max_retries: int = _RETRY_MAX,
) -> dict[str, Any]:
    """Run a live session with automatic retry on transient failures.

    KEY CONSTRAINT: Positions are NOT flattened between retries. The session
    re-seeds from DB state on each restart, so carried positions survive.
    Only the final exhaustion (all retries used) triggers flatten-on-error.
    """
    result: dict[str, Any] = {}
    ticker_adapter: KiteTickerAdapter | None = None
    alerts_sent: set[str] = set()

    for attempt in range(1, max_retries + 1):
        is_final_attempt = attempt == max_retries

        try:
            result = await run_live_session(
                session_id=session_id,
                paper_db_path=paper_db_path,
                market_db_path=market_db_path,
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
    """Compute and enqueue DAILY_PNL_SUMMARY with realized + unrealized breakdown."""
    if not alert_dispatcher._enabled:
        return
    # Dedup guard: only send once per session per invocation.
    _summary_key = f"DAILY_PNL_SUMMARY:{session_id}"
    if alerts_sent is not None and _summary_key in alerts_sent:
        return
    if alerts_sent is not None:
        alerts_sent.add(_summary_key)
    try:
        realized_pnl = paper_db.get_session_realized_pnl(session_id)

        # Compute unrealized from live marks first, then persisted metadata, then entry.
        unrealized_pnl = 0.0
        open_pos_details: list[dict] = []
        for p in paper_db.list_open_positions(session_id):
            meta = p.get("metadata_json") or {}
            sym = p.get("symbol", "")
            qty = int(p.get("qty", 0))
            avg_entry = float(p.get("avg_entry", 0))
            direction = str(p.get("direction", "LONG")).upper()
            mark = (mark_prices or {}).get(sym)
            if mark is None:
                mark = float(meta.get("last_mark_price", avg_entry))
            if direction == "LONG":
                upnl = (mark - avg_entry) * qty
            else:
                upnl = (avg_entry - mark) * qty
            unrealized_pnl += upnl
            open_pos_details.append(
                {
                    "symbol": sym,
                    "unrealized_pnl": upnl,
                    "days_held": meta.get("days_held", 0),
                }
            )

        # Count today's closed trades and winners/losers.
        closed_positions = paper_db.list_positions(session_id)
        trades_closed_today = 0
        winners = 0
        losers = 0
        for p in closed_positions:
            if p.get("state") == "CLOSED" and p.get("pnl") is not None:
                trades_closed_today += 1
                if p["pnl"] >= 0:
                    winners += 1
                else:
                    losers += 1

        max_dd_used_pct = (
            abs(realized_pnl + unrealized_pnl) / portfolio_value * 100 if portfolio_value else 0.0
        )

        subject, body = format_daily_pnl_summary(
            session_id=session_id,
            strategy=strategy,
            trade_date=trade_date,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            trades_closed_today=trades_closed_today,
            winners=winners,
            losers=losers,
            open_positions=open_pos_details,
            portfolio_value=portfolio_value,
            max_dd_used_pct=max_dd_used_pct,
        )
        alert_dispatcher.enqueue(
            AlertEvent(
                alert_type=AlertType.DAILY_PNL_SUMMARY,
                session_id=session_id,
                subject=subject,
                body=body,
            )
        )
    except Exception:
        logger.exception("Failed to dispatch DAILY_PNL_SUMMARY session=%s", session_id)


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
