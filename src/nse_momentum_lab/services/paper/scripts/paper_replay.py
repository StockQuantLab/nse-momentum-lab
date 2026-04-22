"""Paper trading replay script.

Replays historical 5-min candles from DuckDB through the shared paper engine.
Used for strategy validation, parity testing against backtest, and debugging.

Usage:
    python -m nse_momentum_lab.services.paper.scripts.paper_replay \\
        --session-id SESSION_ID --trade-date 2025-01-15
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

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
from nse_momentum_lab.services.paper.feeds.local_ticker_adapter import LocalTickerAdapter
from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
    AlertDispatcher,
    get_alert_config,
)
from nse_momentum_lab.services.paper.scripts.paper_eod_carry import (
    apply_eod_carry_decisions,
    prefetch_daily_features,
)
from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

logger = logging.getLogger(__name__)


async def run_replay(
    *,
    session_id: str,
    trade_date: str,
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
    max_cycles: int | None = None,
    no_alerts: bool = False,
) -> dict[str, Any]:
    """Run a paper replay session from start to finish."""
    paper_db = PaperDB(paper_db_path)
    market_db = MarketDataDB(Path(market_db_path))
    alert_dispatcher = AlertDispatcher(
        paper_db=paper_db, config=get_alert_config(), enabled=not no_alerts
    )
    paper_path = Path(paper_db_path)
    replica = VersionedReplicaSync(
        source_path=paper_path,
        replica_dir=paper_path.parent / "paper_replica",
        prefix="paper_replica",
        min_interval_sec=5.0,
        tables=DEFAULT_PAPER_TABLES,
    )

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

        logger.info(
            "Replay starting session=%s strategy=%s symbols=%d date=%s",
            session_id,
            strategy,
            len(symbols),
            trade_date,
        )

        # Set session active.
        paper_db.update_session(session_id, status="ACTIVE")
        # Sync immediately so the session appears in the dashboard before first bar.
        replica.mark_dirty()
        replica.force_sync(source_conn=paper_db.con)

        # Setup engine state.
        runtime_state = PaperRuntimeState()
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

        # Load checkpoint for resume — skip bars already processed.
        checkpoint = paper_db.get_latest_checkpoint(session_id)
        resume_after_ts: float | None = None
        if checkpoint is not None:
            resume_after_ts = checkpoint.get("bar_end_ts")
            if isinstance(resume_after_ts, str):
                from datetime import datetime

                resume_after_ts = datetime.fromisoformat(resume_after_ts).timestamp()
            elif hasattr(resume_after_ts, "timestamp"):
                resume_after_ts = resume_after_ts.timestamp()
            logger.info(
                "Resuming from checkpoint bar_end_ts=%s session=%s", resume_after_ts, session_id
            )

        # Create local feed adapter.
        adapter = LocalTickerAdapter(
            trade_date=trade_date,
            symbols=symbols,
            market_db=market_db,
        )
        adapter.register_session(session_id, symbols)

        # Main loop: drain one bar group per cycle.
        active_symbols = list(symbols)
        cycles = 0
        last_bar_ts = None
        closed_bars = 0
        final_status = "COMPLETED"

        while True:
            if max_cycles is not None and cycles >= max_cycles:
                final_status = "MAX_CYCLES"
                break

            closed_candles = adapter.drain_closed(session_id)

            if adapter.exhausted:
                logger.info("Local feed exhausted after %d bars", closed_bars)
                break

            if not closed_candles:
                await asyncio.sleep(0.01)
                cycles += 1
                continue

            # Group by bar_end.
            bars_by_end: dict[float, list] = {}
            for c in closed_candles:
                bars_by_end.setdefault(c.bar_end, []).append(c)

            for bar_end in sorted(bars_by_end):
                # Skip bars already committed in a prior run.
                if resume_after_ts is not None and bar_end <= resume_after_ts:
                    continue

                bar_candles = bars_by_end[bar_end]

                # Record feed audit before engine conversion.
                record_closed_candles(
                    bar_candles=bar_candles,
                    session_id=session_id,
                    trade_date=trade_date,
                    feed_source="replay",
                    paper_db=paper_db,
                    transport="local",
                )

                # Convert ClosedCandle to dict for engine.
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

                result = await process_closed_bar_group(
                    session_id=session_id,
                    session=session,
                    bar_candles=candle_dicts,
                    runtime_state=runtime_state,
                    tracker=tracker,
                    strategy_config=strategy_config,
                    active_symbols=active_symbols,
                    feed_source="replay",
                    paper_db=paper_db,
                    alert_dispatcher=alert_dispatcher,
                )

                active_symbols = result["active_symbols"]
                last_bar_ts = bar_end
                closed_bars += 1
                replica.mark_dirty()
                replica.maybe_sync(source_conn=paper_db.con)

                if result["should_complete"]:
                    logger.info("Session should complete (no positions, window closed)")
                    break

                if result["triggered"]:
                    final_status = "RISK_BREACH"
                    logger.warning("Risk breach: %s", result.get("risk_reasons", []))
                    break

                if not active_symbols:
                    final_status = "NO_SYMBOLS"
                    break

            cycles += 1

        # Finalize.
        if final_status == "MAX_CYCLES":
            # Partial/debug replay — skip EOD carry, leave positions open, pause session.
            paper_db.update_session(session_id, status="PAUSED")
            logger.info(
                "Partial replay (max_cycles=%d): EOD carry skipped, session paused session=%s",
                max_cycles,
                session_id,
            )
        else:
            # Apply EOD H-carry decisions using feat_daily (same source as backtest).
            open_before_carry = paper_db.list_open_positions(session_id)
            if open_before_carry:
                carry_symbols = [p["symbol"] for p in open_before_carry]
                daily_features = prefetch_daily_features(market_db, carry_symbols, trade_date)
                carry_summary = apply_eod_carry_decisions(
                    session_id=session_id,
                    trade_date=trade_date,
                    paper_db=paper_db,
                    daily_features=daily_features,
                    strategy_config=strategy_config,
                )
                logger.info("EOD carry summary session=%s: %s", session_id, carry_summary)

            # PAUSE if positions remain open (carried overnight), COMPLETE otherwise.
            remaining_open = paper_db.list_open_positions(session_id)
            if remaining_open and final_status not in ("RISK_BREACH",):
                paper_db.update_session(session_id, status="PAUSED")
                logger.info(
                    "Session PAUSED with %d open position(s) for overnight carry session=%s",
                    len(remaining_open),
                    session_id,
                )
            else:
                await complete_session(
                    session_id=session_id, paper_db=paper_db, status=final_status
                )
        replica.force_sync(source_conn=paper_db.con)

        # Purge old feed audit rows (retention housekeeping).
        try:
            from nse_momentum_lab.config import get_settings

            retention = get_settings().feed_audit_retention_days
            paper_db.purge_old_feed_audit_rows(retention)
        except Exception:
            logger.exception("feed_audit: purge failed session=%s", session_id)

        adapter.close()

        logger.info(
            "Replay complete session=%s status=%s bars=%d cycles=%d",
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
        logger.exception("Replay failed for session %s", session_id)
        await complete_session(session_id=session_id, paper_db=paper_db, status="FAILED")
        replica.force_sync(source_conn=paper_db.con)
        return {"error": str(e), "session_id": session_id}
    finally:
        try:
            replica.force_sync(source_conn=paper_db.con)
        except Exception:
            logger.debug("Final replica sync failed — ignoring")
        await alert_dispatcher.shutdown()
        paper_db.close()
        market_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper trading replay")
    parser.add_argument("--session-id", required=True, help="Paper session ID")
    parser.add_argument("--trade-date", required=True, help="Trade date YYYY-MM-DD")
    parser.add_argument("--paper-db", default="data/paper.duckdb")
    parser.add_argument("--market-db", default="data/market.duckdb")
    parser.add_argument("--max-cycles", type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    result = asyncio.run(
        run_replay(
            session_id=args.session_id,
            trade_date=args.trade_date,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
            max_cycles=args.max_cycles,
        )
    )
    print(result)
    sys.exit(0 if "error" not in result else 1)


if __name__ == "__main__":
    main()
