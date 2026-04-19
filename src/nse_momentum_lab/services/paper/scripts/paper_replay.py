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

logger = logging.getLogger(__name__)


async def run_replay(
    *,
    session_id: str,
    trade_date: str,
    paper_db_path: str = "data/paper.duckdb",
    market_db_path: str = "data/market.duckdb",
    max_cycles: int | None = None,
) -> dict[str, Any]:
    """Run a paper replay session from start to finish."""
    paper_db = PaperDB(paper_db_path)
    market_db = MarketDataDB(Path(market_db_path))
    alert_dispatcher = AlertDispatcher(paper_db=paper_db, config=get_alert_config())

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

        # Setup engine state.
        runtime_state = PaperRuntimeState()
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
        await complete_session(session_id=session_id, paper_db=paper_db, status=final_status)

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
        return {"error": str(e), "session_id": session_id}
    finally:
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
