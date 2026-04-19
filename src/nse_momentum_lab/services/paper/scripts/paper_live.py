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
import sys
import time
from pathlib import Path
from typing import Any

from nse_momentum_lab.db.market_db import MarketDataDB
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
    get_alert_config,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 1.0
_CANDLE_INTERVAL = 5  # minutes
_STALE_TIMEOUT = 300  # seconds
_WEBSOCKET_RECOVERY_AFTER_SEC = 20.0


def _build_alert_dispatcher(paper_db: PaperDB | None = None) -> AlertDispatcher:
    """Build an AlertDispatcher wired from Doppler settings (TELEGRAM_BOT_TOKEN/CHAT_IDS)."""
    config = get_alert_config()
    return AlertDispatcher(paper_db=paper_db, config=config)


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
) -> dict[str, Any]:
    """Run a paper trading session — live or replay."""
    paper_db = PaperDB(paper_db_path)
    market_db = MarketDataDB(Path(market_db_path))
    alert_dispatcher = _build_alert_dispatcher(paper_db)

    # Seed setup rows from market DB into runtime state.
    runtime_state = PaperRuntimeState()
    tracker: SessionPositionTracker | None = None
    local_feed = False
    final_status = "COMPLETED"
    created_adapter = False

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

        # Start alert dispatcher.
        await alert_dispatcher.start()

        # Dispatch session started alert.
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

        # Seed existing positions if resuming.
        existing = paper_db.list_open_positions(session_id)
        if existing:
            tracker.seed_open_positions(existing)

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

                    if result["should_complete"]:
                        final_status = "COMPLETED"
                        break

                    if result["triggered"]:
                        final_status = "RISK_BREACH"
                        alert_dispatcher.enqueue(
                            AlertEvent(
                                alert_type=AlertType.DAILY_LOSS_LIMIT,
                                session_id=session_id,
                                subject=f"Risk breach: {session_id}",
                                body=f"Reasons: {result.get('risk_reasons', [])}",
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
        if tracker is not None:
            # Persist flattened positions to DB before marking FAILED.
            paper_db.flatten_open_positions(session_id, mark_prices=last_close_prices)
            # Also update in-memory tracker.
            for symbol in list(tracker.open_symbols()):
                tracked = tracker.get_open_position(symbol)
                if tracked:
                    tracker.record_close(symbol, tracked.entry_price * tracked.current_qty)
        await complete_session(session_id=session_id, paper_db=paper_db, status="FAILED")
        return {"error": str(e), "session_id": session_id}
    finally:
        # Teardown.
        if ticker_adapter is not None:
            ticker_adapter.unregister_session(session_id)
            if created_adapter:
                ticker_adapter.close()
        await alert_dispatcher.shutdown()
        paper_db.close()
        market_db.close()


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
        run_live_session(
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
