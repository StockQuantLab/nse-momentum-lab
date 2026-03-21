"""CLI entry point for paper-session management and walk-forward runs."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
from dataclasses import asdict
from datetime import UTC, date, datetime
from statistics import mean, median
from typing import Any
from uuid import uuid4

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.paper import (
    create_or_update_paper_session,
    get_paper_session_summary,
    list_paper_sessions,
    set_paper_session_status,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)
from nse_momentum_lab.services.backtest.walkforward import WalkForwardFramework
from nse_momentum_lab.services.kite.client import KiteConnectClient
from nse_momentum_lab.services.kite.stream import KiteStreamConfig, KiteStreamRunner
from nse_momentum_lab.services.paper.runtime import PaperRuntimePlan, PaperRuntimeScaffold

logger = logging.getLogger(__name__)


def _utc_today() -> date:
    return datetime.now(UTC).date()


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid ISO date: {value}") from exc


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected integer value") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Expected integer greater than 0")
    return parsed


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("Expected JSON object")
    return parsed


def _parse_int_csv(value: str | None) -> list[int]:
    if not value:
        return []
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _default_session_id(prefix: str, *parts: str) -> str:
    safe = "-".join(part.strip().lower().replace(" ", "-") for part in parts if part.strip())
    return f"{prefix}-{safe}" if safe else f"{prefix}-{uuid4().hex[:8]}"


def _session_to_json(session: Any) -> dict[str, Any]:
    if isinstance(session, dict):
        return session
    return asdict(session)


def _build_runtime_plan(
    args: argparse.Namespace, *, mode: str, feed_source: str
) -> PaperRuntimePlan:
    settings = get_settings()
    trade_date = getattr(args, "trade_date", None) or (_utc_today() if mode == "live" else None)
    session_id = args.session_id or _default_session_id(
        "paper", args.strategy, trade_date.isoformat() if trade_date else "na", mode
    )
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    return PaperRuntimePlan(
        session_id=session_id,
        strategy_name=args.strategy,
        trade_date=trade_date,
        mode=mode,  # type: ignore[arg-type]
        symbols=symbols,
        experiment_id=args.experiment_id,
        notes=args.notes,
        strategy_params=_parse_json(args.strategy_params)
        if hasattr(args, "strategy_params")
        else {},
        risk_config=_parse_json(args.risk_config) if hasattr(args, "risk_config") else {},
        feed_mode=getattr(args, "feed_mode", "full"),
        feed_source=feed_source,
        kite_api_key=settings.kite_api_key,
        kite_access_token=settings.kite_access_token,
        instrument_tokens=_parse_int_csv(getattr(args, "instrument_tokens", None)),
    )


def _build_stream_runner(args: argparse.Namespace, session_id: str) -> KiteStreamRunner:
    settings = get_settings()
    sessionmaker = get_sessionmaker()
    return KiteStreamRunner(
        sessionmaker=sessionmaker,
        session_id=session_id,
        config=KiteStreamConfig(
            api_key=settings.kite_api_key or "",
            access_token=settings.kite_access_token or "",
            instrument_tokens=_parse_int_csv(getattr(args, "instrument_tokens", None)),
            mode=getattr(args, "feed_mode", "full"),
        ),
    )


def _summarize_folds(folds: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [
        float(fold["total_return_pct"])
        for fold in folds
        if fold.get("total_return_pct") is not None
    ]
    drawdowns = [
        float(fold["max_drawdown_pct"])
        for fold in folds
        if fold.get("max_drawdown_pct") is not None
    ]
    trades = [int(fold.get("total_trades") or 0) for fold in folds]
    completed = [fold for fold in folds if str(fold.get("status") or "").lower() == "completed"]
    profitable = [value for value in returns if value > 0]
    return {
        "folds_total": len(folds),
        "folds_completed": len(completed),
        "folds_profitable": len(profitable),
        "avg_return_pct": round(mean(returns), 4) if returns else None,
        "median_return_pct": round(median(returns), 4) if returns else None,
        "worst_drawdown_pct": round(max(drawdowns), 4) if drawdowns else None,
        "total_trades": sum(trades),
    }


async def _cmd_prepare(args: argparse.Namespace) -> None:
    session_id = args.session_id or _default_session_id(
        "paper",
        args.strategy,
        args.trade_date.isoformat() if args.trade_date else "na",
        args.mode,
    )
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        row = await create_or_update_paper_session(
            db_session,
            session_id=session_id,
            trade_date=args.trade_date,
            strategy_name=args.strategy,
            mode=args.mode,
            status=args.status,
            experiment_id=args.experiment_id,
            symbols=[s.strip().upper() for s in args.symbols.split(",") if s.strip()],
            strategy_params=_parse_json(args.strategy_params),
            risk_config=_parse_json(args.risk_config),
            notes=args.notes,
        )
    print(json.dumps(_session_to_json(row), default=str, indent=2))


async def _cmd_status(args: argparse.Namespace) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        if args.session_id:
            summary = await get_paper_session_summary(db_session, args.session_id)
            print(json.dumps(summary or {}, default=str, indent=2))
            return

        sessions = await list_paper_sessions(db_session, status=args.status, limit=args.limit)
    print(json.dumps({"sessions": sessions}, default=str, indent=2))


async def _transition_session(args: argparse.Namespace, status: str) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        row = await set_paper_session_status(
            db_session,
            session_id=args.session_id,
            status=status,
            notes=args.notes,
        )
    print(json.dumps(_session_to_json(row) if row else {}, default=str, indent=2))


async def _cmd_pause(args: argparse.Namespace) -> None:
    await _transition_session(args, "PAUSED")


async def _cmd_resume(args: argparse.Namespace) -> None:
    await _transition_session(args, "ACTIVE")


async def _cmd_stop(args: argparse.Namespace) -> None:
    await _transition_session(args, "COMPLETED" if args.complete else "STOPPING")


async def _cmd_flatten(args: argparse.Namespace) -> None:
    await _transition_session(args, "STOPPING")


async def _cmd_archive(args: argparse.Namespace) -> None:
    await _transition_session(args, "ARCHIVED")


async def _cmd_replay_day(args: argparse.Namespace) -> None:
    plan = _build_runtime_plan(args, mode="replay", feed_source="duckdb")
    runtime = PaperRuntimeScaffold(feed_batch_size=get_settings().kite_ws_max_tokens)
    sessionmaker = get_sessionmaker()
    result = await runtime.prepare_session(sessionmaker, plan, status="RUNNING")
    execution = None
    if getattr(args, "execute", False):
        execution = await runtime.execute_replay_cycle(sessionmaker, plan.session_id)
    print(
        json.dumps(
            {
                "session_id": plan.session_id,
                "mode": "replay",
                "result": result,
                "execution": execution,
            },
            default=str,
            indent=2,
        )
    )


async def _cmd_live(args: argparse.Namespace) -> None:
    settings = get_settings()
    plan = _build_runtime_plan(args, mode="live", feed_source="kite")
    runtime = PaperRuntimeScaffold(feed_batch_size=settings.kite_ws_max_tokens)
    sessionmaker = get_sessionmaker()
    status = "ACTIVE" if settings.has_kite_credentials() else "PLANNING"
    result = await runtime.prepare_session(sessionmaker, plan, status=status)
    execution = None
    if getattr(args, "execute", False):
        if not settings.has_kite_credentials():
            raise SystemExit("Kite credentials are required to execute live paper entries")
        with KiteConnectClient(
            api_key=settings.kite_api_key or "",
            access_token=settings.kite_access_token,
            api_secret=settings.kite_api_secret,
            login_url=settings.kite_login_url,
            api_root=settings.kite_api_root,
        ) as kite_client:
            execution = await runtime.execute_live_cycle(
                sessionmaker,
                plan.session_id,
                kite_client=kite_client,
            )
    print(
        json.dumps(
            {
                "session_id": plan.session_id,
                "mode": "live",
                "status": status,
                "kite_ready": settings.has_kite_credentials(),
                "result": result,
                "execution": execution,
            },
            default=str,
            indent=2,
        )
    )

    if getattr(args, "run", False):
        if not settings.has_kite_credentials():
            raise SystemExit("Kite credentials are required to start the live stream")
        runner = _build_stream_runner(args, plan.session_id)
        await runner.run()


async def _cmd_stream(args: argparse.Namespace) -> None:
    args.run = True
    await _cmd_live(args)


async def _cmd_walk_forward(args: argparse.Namespace) -> None:
    framework = WalkForwardFramework(strategy_name=args.strategy)
    windows = list(
        framework.generate_rolling_windows(
            args.start_date,
            args.end_date,
            train_days=args.train_days,
            test_days=args.test_days,
            roll_interval_days=args.roll_interval_days,
        )
    )
    if args.max_folds is not None:
        windows = windows[: args.max_folds]
    if not windows:
        raise SystemExit("No walk-forward windows generated for the requested date range")

    session_id = args.session_id or _default_session_id(
        "wf", args.strategy, args.start_date.isoformat(), args.end_date.isoformat()
    )
    base_params = asdict(BacktestParams(strategy=args.strategy))
    base_params.update(_parse_json(args.params_json))
    base_params["strategy"] = args.strategy

    sessionmaker = get_sessionmaker()
    runner = DuckDBBacktestRunner()
    folds: list[dict[str, Any]] = []

    summary: dict[str, Any] | None = None
    decision: dict[str, Any] | None = None
    async with sessionmaker() as db_session:
        await create_or_update_paper_session(
            db_session,
            session_id=session_id,
            trade_date=args.end_date,
            strategy_name=args.strategy,
            mode="walk_forward",
            status="RUNNING",
            experiment_id=None,
            symbols=[],
            strategy_params={
                "train_days": args.train_days,
                "test_days": args.test_days,
                "roll_interval_days": args.roll_interval_days,
                "base_params": base_params,
            },
            risk_config={},
            notes=args.notes,
        )

        try:
            for window in windows:
                fold_params = dict(base_params)
                fold_params["start_date"] = window.test_start.isoformat()
                fold_params["end_date"] = window.test_end.isoformat()
                exp_id = runner.run(
                    BacktestParams(**fold_params),
                    force=args.force,
                    snapshot=args.snapshot,
                )
                exp = runner.results_db.get_experiment(exp_id) or {}
                folds.append(
                    {
                        "train_start": window.train_start.isoformat(),
                        "train_end": window.train_end.isoformat(),
                        "test_start": window.test_start.isoformat(),
                        "test_end": window.test_end.isoformat(),
                        "exp_id": exp_id,
                        "status": exp.get("status"),
                        "total_return_pct": exp.get("total_return_pct"),
                        "max_drawdown_pct": exp.get("max_drawdown_pct"),
                        "profit_factor": exp.get("profit_factor"),
                        "total_trades": exp.get("total_trades"),
                    }
                )

            summary = _summarize_folds(folds)
            decision = {
                "status": (
                    "PASS"
                    if summary["folds_total"] > 0
                    and summary["folds_completed"] == summary["folds_total"]
                    else "FAIL"
                ),
                "reason": (
                    "all_folds_completed"
                    if summary["folds_total"] > 0
                    and summary["folds_completed"] == summary["folds_total"]
                    else "incomplete_or_empty_walk_forward"
                ),
            }
            await set_paper_session_status(
                db_session,
                session_id=session_id,
                status="COMPLETED" if decision["status"] == "PASS" else "FAILED",
                notes=args.notes,
            )
        except Exception:
            logger.exception("Walk-forward session failed: session_id=%s", session_id)
            await set_paper_session_status(
                db_session,
                session_id=session_id,
                status="FAILED",
                notes=args.notes,
            )
            raise

    print(
        json.dumps(
            {
                "session_id": session_id,
                "strategy": args.strategy,
                "windows": len(windows),
                "summary": summary,
                "decision": decision,
                "folds": folds,
            },
            default=str,
            indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Paper session and walk-forward workflow")
    sub = parser.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser("prepare", help="Create or update a paper session")
    prepare.add_argument("--session-id", default=None)
    prepare.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    prepare.add_argument("--strategy", default="indian_2lynch")
    prepare.add_argument("--mode", default="replay", choices=["replay", "live", "walk_forward"])
    prepare.add_argument("--status", default="PLANNING")
    prepare.add_argument("--experiment-id", default=None)
    prepare.add_argument("--symbols", default="")
    prepare.add_argument("--strategy-params", default=None)
    prepare.add_argument("--risk-config", default=None)
    prepare.add_argument("--notes", default=None)
    prepare.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    prepare.add_argument("--instrument-tokens", default="")
    prepare.set_defaults(handler=_cmd_prepare)

    status = sub.add_parser("status", help="Show one session or list sessions")
    status.add_argument("--session-id", default=None)
    status.add_argument("--status", default=None)
    status.add_argument("--limit", type=_positive_int, default=20)
    status.set_defaults(handler=_cmd_status)

    walk_forward = sub.add_parser("walk-forward", help="Run rolling walk-forward backtests")
    walk_forward.add_argument("--session-id", default=None)
    walk_forward.add_argument("--strategy", default="indian_2lynch")
    walk_forward.add_argument("--start-date", required=True, type=_parse_iso_date)
    walk_forward.add_argument("--end-date", required=True, type=_parse_iso_date)
    walk_forward.add_argument("--train-days", type=_positive_int, default=252)
    walk_forward.add_argument("--test-days", type=_positive_int, default=63)
    walk_forward.add_argument("--roll-interval-days", type=_positive_int, default=63)
    walk_forward.add_argument("--max-folds", type=_positive_int, default=None)
    walk_forward.add_argument("--params-json", default=None)
    walk_forward.add_argument("--force", action="store_true")
    walk_forward.add_argument("--snapshot", action="store_true")
    walk_forward.add_argument("--notes", default=None)
    walk_forward.set_defaults(handler=_cmd_walk_forward)

    replay = sub.add_parser("replay-day", help="Bootstrap a replay-day paper session")
    replay.add_argument("--session-id", default=None)
    replay.add_argument("--trade-date", required=True, type=_parse_iso_date, help="YYYY-MM-DD")
    replay.add_argument("--strategy", default="indian_2lynch")
    replay.add_argument("--experiment-id", default=None)
    replay.add_argument("--symbols", default="")
    replay.add_argument("--strategy-params", default=None)
    replay.add_argument("--risk-config", default=None)
    replay.add_argument("--notes", default=None)
    replay.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    replay.add_argument("--instrument-tokens", default="")
    replay.add_argument("--execute", action="store_true", help="Execute the replay queue once")
    replay.set_defaults(handler=_cmd_replay_day)

    live = sub.add_parser("live", help="Bootstrap a live Kite-backed paper session")
    live.add_argument("--session-id", default=None)
    live.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    live.add_argument("--strategy", default="indian_2lynch")
    live.add_argument("--experiment-id", default=None)
    live.add_argument("--symbols", default="")
    live.add_argument("--strategy-params", default=None)
    live.add_argument("--risk-config", default=None)
    live.add_argument("--notes", default=None)
    live.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    live.add_argument("--instrument-tokens", default="")
    live.add_argument("--execute", action="store_true", help="Execute live paper entries once")
    live.add_argument("--run", action="store_true", help="Start the live Kite websocket loop")
    live.set_defaults(handler=_cmd_live)

    stream = sub.add_parser("stream", help="Start the live Kite websocket loop")
    stream.add_argument("--session-id", default=None)
    stream.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    stream.add_argument("--strategy", default="indian_2lynch")
    stream.add_argument("--experiment-id", default=None)
    stream.add_argument("--symbols", default="")
    stream.add_argument("--strategy-params", default=None)
    stream.add_argument("--risk-config", default=None)
    stream.add_argument("--notes", default=None)
    stream.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    stream.add_argument("--instrument-tokens", default="")
    stream.add_argument(
        "--execute", action="store_true", help="Execute live paper entries once before streaming"
    )
    stream.set_defaults(handler=_cmd_stream)

    pause = sub.add_parser("pause", help="Pause a paper session")
    pause.add_argument("--session-id", required=True)
    pause.add_argument("--notes", default=None)
    pause.set_defaults(handler=_cmd_pause)

    resume = sub.add_parser("resume", help="Resume a paused paper session")
    resume.add_argument("--session-id", required=True)
    resume.add_argument("--notes", default=None)
    resume.set_defaults(handler=_cmd_resume)

    stop = sub.add_parser("stop", help="Stop a paper session")
    stop.add_argument("--session-id", required=True)
    stop.add_argument("--notes", default=None)
    stop.add_argument("--complete", action="store_true")
    stop.set_defaults(handler=_cmd_stop)

    flatten = sub.add_parser("flatten", help="Request a session flatten")
    flatten.add_argument("--session-id", required=True)
    flatten.add_argument("--notes", default=None)
    flatten.set_defaults(handler=_cmd_flatten)

    archive = sub.add_parser("archive", help="Archive a completed session")
    archive.add_argument("--session-id", required=True)
    archive.add_argument("--notes", default=None)
    archive.set_defaults(handler=_cmd_archive)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "command", None) == "walk-forward" and args.end_date < args.start_date:
        parser.error("--end-date must be on or after --start-date")
    handler = getattr(args, "handler", None)
    if handler is None:
        raise SystemExit("No command specified")

    if inspect.iscoroutinefunction(handler):
        asyncio.run(handler(args))
    else:
        handler(args)
