"""CLI for DuckDB-backed paper trading (v2 engine).

Streamlined commands that use the new shared engine, DuckDB-only storage,
and feed adapters. Replaces nseml-paper for v2 sessions.

Commands:
    nseml-paper-v2 prepare       — Create/resume a paper session in DuckDB
    nseml-paper-v2 replay        — Replay historical candles through the engine
    nseml-paper-v2 live          — Run a live paper session with Kite WebSocket
    nseml-paper-v2 plan          — Plan multi-variant sessions
    nseml-paper-v2 status        — Show session status
    nseml-paper-v2 stop          — Stop (mark COMPLETED) a running session
    nseml-paper-v2 pause         — Pause an active session
    nseml-paper-v2 resume        — Resume a paused session
    nseml-paper-v2 flatten       — Flatten open positions and pause session
    nseml-paper-v2 archive       — Archive a session
    nseml-paper-v2 daily-prepare — Prepare today's paper session
    nseml-paper-v2 daily-replay  — Replay today's candles
    nseml-paper-v2 daily-live    — Run live session for today
    nseml-paper-v2 daily-sim     — Fast daily simulation probe
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _run_async(coro: Any) -> Any:
    if sys.platform == "win32":
        selector_loop_cls = getattr(asyncio, "SelectorEventLoop", None)
        if selector_loop_cls is None:
            raise RuntimeError("asyncio.SelectorEventLoop is not available on this platform")
        with asyncio.Runner(loop_factory=selector_loop_cls) as runner:
            return runner.run(coro)
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _cmd_prepare(args: argparse.Namespace) -> None:
    """Create a new paper session in DuckDB, or return an existing resumable one."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB
    from nse_momentum_lab.services.paper.engine.shared_engine import resolve_strategy_key

    strategy = resolve_strategy_key(args.strategy)
    mode = getattr(args, "mode", "replay") or "replay"
    trade_date = (
        args.trade_date
        if isinstance(args.trade_date, date)
        else (date.fromisoformat(args.trade_date) if args.trade_date else date.today())
    )
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else []
    risk_config = _parse_json(args.risk_config) or {
        "portfolio_value": args.portfolio_value,
        "max_daily_loss_pct": 0.05,
        "max_drawdown_pct": 0.15,
        "flatten_time": "15:15:00",
        "slippage_bps": 5.0,
    }
    metadata = _parse_json(args.metadata)

    db = PaperDB(args.paper_db)
    try:
        # Idempotent: return existing resumable session rather than creating a duplicate.
        existing = db.find_resumable_session(
            strategy_name=strategy, trade_date=trade_date, mode=mode
        )
        if existing is not None:
            session_id = existing["session_id"]
            logger.info(
                "Resuming existing session %s strategy=%s date=%s mode=%s status=%s",
                session_id,
                strategy,
                trade_date,
                mode,
                existing.get("status"),
            )
            print(
                json.dumps(
                    {
                        "session_id": session_id,
                        "strategy": strategy,
                        "symbols": len(existing.get("symbols") or []),
                        "resumed": True,
                        "status": existing.get("status"),
                    }
                )
            )
            return

        session = db.create_session(
            strategy_name=strategy,
            mode=mode,
            trade_date=trade_date,
            status="PLANNED",
            symbols=symbols,
            risk_config=risk_config,
            strategy_params=metadata,
            notes=None,
        )
        session_id = session["session_id"]
        print(
            json.dumps(
                {
                    "session_id": session_id,
                    "strategy": strategy,
                    "symbols": len(symbols),
                    "resumed": False,
                }
            )
        )
    finally:
        db.close()


def _resolve_session_id(
    args: argparse.Namespace,
    db_path: str,
    *,
    mode: str,
    trade_date: str | None = None,
) -> str:
    """Resolve a session_id: use explicit arg, or auto-discover by strategy + date + mode."""
    if getattr(args, "session_id", None):
        return args.session_id

    strategy_raw = getattr(args, "strategy", None)
    if not strategy_raw:
        print(
            "Error: --session-id or (--strategy + --trade-date) required",
            file=sys.stderr,
        )
        sys.exit(1)

    from nse_momentum_lab.services.paper.db.paper_db import PaperDB
    from nse_momentum_lab.services.paper.engine.shared_engine import resolve_strategy_key

    strategy = resolve_strategy_key(strategy_raw)
    td = trade_date or getattr(args, "trade_date", None)
    db = PaperDB(db_path)
    try:
        session = db.find_resumable_session(strategy_name=strategy, trade_date=td, mode=mode)
    finally:
        db.close()

    if session is None:
        print(
            f"Error: no resumable {mode} session found for strategy={strategy} date={td}. "
            "Run `prepare` first.",
            file=sys.stderr,
        )
        sys.exit(1)

    session_id = session["session_id"]
    logger.info(
        "Auto-discovered session %s strategy=%s date=%s mode=%s status=%s",
        session_id,
        strategy,
        td,
        mode,
        session.get("status"),
    )
    return session_id


def _cmd_replay(args: argparse.Namespace) -> None:
    """Replay historical candles through the shared engine."""
    from nse_momentum_lab.services.paper.scripts.paper_replay import run_replay

    session_id = _resolve_session_id(args, args.paper_db, mode="replay", trade_date=args.trade_date)
    result = _run_async(
        run_replay(
            session_id=session_id,
            trade_date=args.trade_date,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
        )
    )
    print(json.dumps(result, default=str))
    sys.exit(0 if "error" not in result else 1)


def _cmd_live(args: argparse.Namespace) -> None:
    """Run a live paper session."""
    from nse_momentum_lab.services.paper.scripts.paper_live import run_live_session

    session_id = _resolve_session_id(
        args, args.paper_db, mode="live", trade_date=getattr(args, "trade_date", None)
    )
    result = _run_async(
        run_live_session(
            session_id=session_id,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
            poll_interval=args.poll_interval,
            max_cycles=args.max_cycles,
        )
    )
    print(json.dumps(result, default=str))
    sys.exit(0 if "error" not in result else 1)


def _cmd_plan(args: argparse.Namespace) -> None:
    """Plan multi-variant sessions."""
    from nse_momentum_lab.services.paper.scripts.multi_variant import plan_variants

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else []
    if not symbols:
        print("Error: --symbols required", file=sys.stderr)
        sys.exit(1)

    plans = plan_variants(
        strategy=args.strategy,
        trade_date=args.trade_date,
        symbols=symbols,
        num_variants=args.variants,
        portfolio_value=args.portfolio_value,
    )

    if args.dry_run:
        for plan in plans:
            print(json.dumps(plan, indent=2, default=str))
    else:
        from nse_momentum_lab.services.paper.db.paper_db import PaperDB
        from nse_momentum_lab.services.paper.scripts.multi_variant import create_variant_sessions

        db = PaperDB(args.paper_db)
        try:
            session_ids = create_variant_sessions(
                paper_db=db,
                strategy=args.strategy,
                trade_date=args.trade_date,
                symbols=symbols,
                num_variants=args.variants,
                portfolio_value=args.portfolio_value,
            )
            print(json.dumps({"created": len(session_ids), "session_ids": session_ids}, indent=2))
        finally:
            db.close()


def _cmd_status(args: argparse.Namespace) -> None:
    """Show session status."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    db = PaperDB(args.paper_db)
    try:
        if args.session_id:
            session = db.get_session(args.session_id)
            if session is None:
                print(f"Session {args.session_id} not found")
                sys.exit(1)
            print(json.dumps(session, indent=2, default=str))
        else:
            sessions = db.list_sessions(
                status=args.status,
                limit=args.limit,
            )
            for s in sessions:
                print(
                    f"{s.get('session_id', '?')[:12]}  "
                    f"{s.get('status', '?'):10s}  "
                    f"{s.get('strategy', '?'):20s}  "
                    f"{s.get('trade_date', '?')}"
                )
    finally:
        db.close()


def _cmd_stop(args: argparse.Namespace) -> None:
    """Stop (mark COMPLETED) a running session."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        db.update_session(session_id, status="COMPLETED")
        print(json.dumps({"session_id": session_id, "status": "COMPLETED"}))
    finally:
        db.close()


def _cmd_pause(args: argparse.Namespace) -> None:
    """Pause an active session."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        db.update_session(session_id, status="PAUSED")
        print(json.dumps({"session_id": session_id, "status": "PAUSED"}))
    finally:
        db.close()


def _cmd_resume(args: argparse.Namespace) -> None:
    """Resume a paused session (mark ACTIVE)."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        db.update_session(session_id, status="ACTIVE")
        print(json.dumps({"session_id": session_id, "status": "ACTIVE"}))
    finally:
        db.close()


def _cmd_flatten(args: argparse.Namespace) -> None:
    """Flatten (close) all open positions for a session and pause it."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        flattened = db.flatten_open_positions(session_id)
        db.update_session(session_id, status="PAUSED")
        print(json.dumps({"session_id": session_id, "flattened": flattened, "status": "PAUSED"}))
    finally:
        db.close()


def _cmd_archive(args: argparse.Namespace) -> None:
    """Archive a session (mark ARCHIVED)."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        db.update_session(session_id, status="ARCHIVED")
        print(json.dumps({"session_id": session_id, "status": "ARCHIVED"}))
    finally:
        db.close()


def _cmd_daily_prepare(args: argparse.Namespace) -> None:
    """Prepare a paper session for today (shortcut for prepare --trade-date=today)."""
    args.trade_date = str(date.today())
    _cmd_prepare(args)


def _cmd_daily_replay(args: argparse.Namespace) -> None:
    """Replay today's candles (shortcut for replay --trade-date=today)."""
    args.trade_date = str(date.today())
    _cmd_replay(args)


def _cmd_daily_live(args: argparse.Namespace) -> None:
    """Run a live session for today (shortcut for live --trade-date=today)."""
    args.trade_date = str(date.today())
    _cmd_live(args)


def _cmd_daily_sim(args: argparse.Namespace) -> None:
    """Fast daily simulation probe — runs a single-day replay and prints summary."""
    from nse_momentum_lab.services.paper.scripts.paper_replay import run_replay

    result = _run_async(
        run_replay(
            session_id=args.session_id,
            trade_date=args.trade_date,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
        )
    )

    if "error" in result:
        print(f"ERROR: {result['error']}")
        sys.exit(1)

    print(f"Session: {result.get('session_id', '?')}")
    print(f"Status:  {result.get('status', '?')}")
    print(f"Bars:    {result.get('closed_bars', 0)}")
    print(f"Cycles:  {result.get('cycles', 0)}")


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-paper-v2",
        description="DuckDB-backed paper trading (v2 engine)",
    )
    parser.add_argument("--paper-db", default="data/paper.duckdb", help="Paper DuckDB path")
    parser.add_argument("--market-db", default="data/market.duckdb", help="Market DuckDB path")

    sub = parser.add_subparsers(dest="command", required=True)

    # prepare
    prepare = sub.add_parser("prepare", help="Create a new paper session")
    prepare.add_argument("--strategy", default="2lynchbreakout")
    prepare.add_argument(
        "--mode", default="replay", choices=["replay", "live"], help="Session mode"
    )
    prepare.add_argument("--trade-date", default=None, help="YYYY-MM-DD")
    prepare.add_argument("--symbols", default="", help="Comma-separated symbols")
    prepare.add_argument("--portfolio-value", type=float, default=1_000_000)
    prepare.add_argument("--risk-config", default=None, help="JSON risk config")
    prepare.add_argument("--metadata", default=None, help="JSON metadata")
    prepare.set_defaults(handler=_cmd_prepare)

    # replay
    replay = sub.add_parser("replay", help="Replay historical candles")
    replay.add_argument(
        "--session-id",
        default=None,
        help="Session ID. If omitted, auto-discovers via --strategy + --trade-date.",
    )
    replay.add_argument(
        "--strategy",
        default=None,
        help="Strategy key for auto-discovery (used when --session-id is not provided).",
    )
    replay.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    replay.set_defaults(handler=_cmd_replay)

    # live
    live = sub.add_parser("live", help="Run a live paper session")
    live.add_argument(
        "--session-id",
        default=None,
        help="Session ID. If omitted, auto-discovers via --strategy + --trade-date.",
    )
    live.add_argument(
        "--strategy",
        default=None,
        help="Strategy key for auto-discovery (used when --session-id is not provided).",
    )
    live.add_argument(
        "--trade-date",
        default=None,
        help="YYYY-MM-DD (used with --strategy for auto-discovery).",
    )
    live.add_argument("--poll-interval", type=float, default=1.0)
    live.add_argument("--max-cycles", type=int, default=None)
    live.set_defaults(handler=_cmd_live)

    # plan
    plan = sub.add_parser("plan", help="Plan multi-variant sessions")
    plan.add_argument("--strategy", default="2lynchbreakout")
    plan.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    plan.add_argument("--symbols", required=True, help="Comma-separated")
    plan.add_argument("--variants", type=int, default=3)
    plan.add_argument("--portfolio-value", type=float, default=1_000_000)
    plan.add_argument("--dry-run", action="store_true")
    plan.set_defaults(handler=_cmd_plan)

    # status
    status = sub.add_parser("status", help="Show session status")
    status.add_argument("--session-id", default=None, help="Specific session")
    status.add_argument("--status", default=None, help="Filter by status")
    status.add_argument("--limit", type=int, default=20)
    status.set_defaults(handler=_cmd_status)

    # daily-sim
    daily_sim = sub.add_parser("daily-sim", help="Fast daily simulation probe")
    daily_sim.add_argument("--session-id", required=True)
    daily_sim.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    daily_sim.set_defaults(handler=_cmd_daily_sim)

    # stop
    _stop = sub.add_parser("stop", help="Stop (mark COMPLETED) a running session")
    _stop.add_argument("--session-id", default=None)
    _stop.add_argument("--strategy", default=None)
    _stop.add_argument("--trade-date", default=None)
    _stop.add_argument("--mode", default="replay")
    _stop.set_defaults(handler=_cmd_stop)

    # pause
    _pause = sub.add_parser("pause", help="Pause an active session")
    _pause.add_argument("--session-id", default=None)
    _pause.add_argument("--strategy", default=None)
    _pause.add_argument("--trade-date", default=None)
    _pause.add_argument("--mode", default="replay")
    _pause.set_defaults(handler=_cmd_pause)

    # resume
    _resume = sub.add_parser("resume", help="Resume a paused session")
    _resume.add_argument("--session-id", default=None)
    _resume.add_argument("--strategy", default=None)
    _resume.add_argument("--trade-date", default=None)
    _resume.add_argument("--mode", default="replay")
    _resume.set_defaults(handler=_cmd_resume)

    # flatten
    _flatten = sub.add_parser("flatten", help="Flatten open positions and pause session")
    _flatten.add_argument("--session-id", default=None)
    _flatten.add_argument("--strategy", default=None)
    _flatten.add_argument("--trade-date", default=None)
    _flatten.add_argument("--mode", default="replay")
    _flatten.set_defaults(handler=_cmd_flatten)

    # archive
    _archive = sub.add_parser("archive", help="Archive a session")
    _archive.add_argument("--session-id", default=None)
    _archive.add_argument("--strategy", default=None)
    _archive.add_argument("--trade-date", default=None)
    _archive.add_argument("--mode", default="replay")
    _archive.set_defaults(handler=_cmd_archive)

    # daily-prepare
    daily_prepare = sub.add_parser("daily-prepare", help="Prepare today's paper session")
    daily_prepare.add_argument("--strategy", default="2lynchbreakout")
    daily_prepare.add_argument("--mode", default="replay", choices=["replay", "live"])
    daily_prepare.add_argument("--symbols", default="", help="Comma-separated symbols")
    daily_prepare.add_argument("--portfolio-value", type=float, default=1_000_000)
    daily_prepare.add_argument("--risk-config", default=None)
    daily_prepare.add_argument("--metadata", default=None)
    daily_prepare.set_defaults(handler=_cmd_daily_prepare, trade_date=None)

    # daily-replay
    daily_replay = sub.add_parser("daily-replay", help="Replay today's candles")
    daily_replay.add_argument("--session-id", default=None)
    daily_replay.add_argument("--strategy", default=None)
    daily_replay.set_defaults(handler=_cmd_daily_replay)

    # daily-live
    daily_live = sub.add_parser("daily-live", help="Run live session for today")
    daily_live.add_argument("--session-id", default=None)
    daily_live.add_argument("--strategy", default=None)
    daily_live.add_argument("--poll-interval", type=float, default=1.0)
    daily_live.add_argument("--max-cycles", type=int, default=None)
    daily_live.set_defaults(handler=_cmd_daily_live)

    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = build_parser()
    args = parser.parse_args()
    handler = getattr(args, "handler", None)
    if handler is None:
        raise SystemExit("No command specified")
    handler(args)


if __name__ == "__main__":
    main()
