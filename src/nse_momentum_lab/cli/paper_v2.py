"""CLI for DuckDB-backed paper trading (v2 engine).

Streamlined commands that use the new shared engine, DuckDB-only storage,
and feed adapters. Replaces nseml-paper for v2 sessions.

Commands:
    nseml-paper-v2 prepare       — Create/resume a paper session in DuckDB
    nseml-paper-v2 replay        — Replay historical candles through the engine
    nseml-paper-v2 live          — Run a live paper session with Kite WebSocket
    nseml-paper-v2 multi-live    — Run multiple live sessions in one writer process
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
    nseml-paper-v2 eod-carry     — Post-market H-carry decisions for open positions
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
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


def _kill_stale_paper_writer_processes() -> list[int]:
    """Best-effort cleanup for orphaned Windows paper-writer processes."""
    if os.name != "nt":
        return []

    current_pid = os.getpid()
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-Command",
        (
            "Get-CimInstance Win32_Process | "
            "Where-Object { "
            f"$_.ProcessId -ne {current_pid} -and ("
            "$_.CommandLine -match 'nseml-paper' -or "
            "$_.CommandLine -match 'paper_live' -or "
            "$_.CommandLine -match 'multi-live' -or "
            "$_.CommandLine -match 'daily-live'"
            ") } | "
            "Select-Object ProcessId | ConvertTo-Json -Depth 2"
        ),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False)
    except Exception:
        logger.exception("Failed to inspect orphaned paper writer processes")
        return []

    payload = (result.stdout or "").strip()
    if not payload or payload == "null":
        return []

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        logger.debug("Unexpected process query output: %s", payload[:500])
        return []

    if isinstance(parsed, dict):
        parsed = [parsed]

    killed: list[int] = []
    for item in parsed:
        try:
            pid = int(item.get("ProcessId"))
        except Exception:
            continue
        if pid == current_pid:
            continue
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            killed.append(pid)
        except Exception:
            logger.exception("Failed to terminate stale writer pid=%s", pid)
    return killed


def _open_paper_db_with_orphan_cleanup(paper_db_path: str):
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    try:
        return PaperDB(paper_db_path)
    except Exception as exc:
        message = str(exc).lower()
        if "being used by another process" not in message and "cannot open file" not in message:
            raise
        killed = _kill_stale_paper_writer_processes()
        if not killed:
            raise
        logger.warning("Killed stale paper writer processes: %s", killed)
        return PaperDB(paper_db_path)


def _build_paper_replica_sync(paper_db_path: str) -> Any:
    from nse_momentum_lab.db.versioned_replica_sync import (
        DEFAULT_PAPER_TABLES,
        VersionedReplicaSync,
    )

    source_path = Path(paper_db_path)
    prefix = f"{source_path.stem}_replica"
    replica_dir = source_path.parent / prefix
    return VersionedReplicaSync(
        source_path=source_path,
        replica_dir=replica_dir,
        prefix=prefix,
        tables=DEFAULT_PAPER_TABLES,
    )


def _sync_paper_replica_after_cli_write(paper_db_path: str, db: Any) -> None:
    try:
        replica = _build_paper_replica_sync(paper_db_path)
        replica.force_sync(source_conn=db.con)
    except Exception:
        logger.exception("Failed to sync paper replica after CLI write db=%s", paper_db_path)


def _dispatch_manual_flatten_notifications(
    *,
    db: Any,
    session_id: str,
    flattened_count: int,
    session_status: str,
) -> None:
    if flattened_count <= 0:
        return

    from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
        AlertDispatcher,
        AlertEvent,
        AlertType,
        enqueue_daily_pnl_summary,
        format_manual_flatten_alert,
        get_alert_config,
        summarize_session_pnl,
    )

    session = db.get_session(session_id) or {}
    strategy = str(session.get("strategy_name", "") or "")
    trade_date = str(session.get("trade_date", "") or "")
    risk_config = session.get("risk_config") or {}
    portfolio_value = float(risk_config.get("portfolio_value") or 0.0)
    summary = summarize_session_pnl(paper_db=db, session_id=session_id)
    net_pnl = float(summary["realized_pnl"]) + float(summary["unrealized_pnl"])

    async def _send() -> None:
        dispatcher = AlertDispatcher(paper_db=db, config=get_alert_config())
        await dispatcher.start()
        subject, body = format_manual_flatten_alert(
            session_id=session_id,
            strategy=strategy,
            trade_date=trade_date,
            flattened_positions=flattened_count,
            net_pnl=net_pnl,
            session_status=session_status,
        )
        dispatcher.enqueue(
            AlertEvent(
                alert_type=AlertType.FLATTEN_EOD,
                session_id=session_id,
                subject=subject,
                body=body,
            )
        )
        enqueue_daily_pnl_summary(
            alert_dispatcher=dispatcher,
            session_id=session_id,
            paper_db=db,
            strategy=strategy,
            trade_date=trade_date,
            portfolio_value=portfolio_value,
        )
        await dispatcher.shutdown()

    _run_async(_send())


def _serialize_strategy_params(
    config: Any,
    *,
    preset_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a paper strategy config in session.strategy_params."""
    payload = dict(dataclasses.asdict(config))
    extra = dict(payload.pop("extra_params", {}) or {})
    if preset_name:
        payload["preset_name"] = preset_name
    for key, value in (metadata or {}).items():
        if key not in payload:
            extra[key] = value
    if extra:
        payload["extra_params"] = extra
    return payload


def _load_default_symbols(market_db_path: str, trade_date: date) -> list[str]:
    """Default to the latest available full daily universe at or before trade_date."""
    from nse_momentum_lab.db.market_db import LIVE_BLOCKING_DQ_CODES, MarketDataDB

    market_db = MarketDataDB(Path(market_db_path), read_only=True)
    try:
        dq_placeholders = ",".join("?" for _ in LIVE_BLOCKING_DQ_CODES)
        rows = market_db.con.execute(
            f"""
            WITH ref_day AS (
                SELECT max(date) AS ref_date
                FROM v_daily
                WHERE date <= $1
            )
            SELECT DISTINCT symbol
            FROM v_daily
            WHERE date = (SELECT ref_date FROM ref_day)
              AND symbol NOT IN (
                  SELECT symbol
                  FROM data_quality_issues
                  WHERE is_active = TRUE
                    AND issue_code IN ({dq_placeholders})
              )
            ORDER BY symbol
            """,
            [trade_date, *LIVE_BLOCKING_DQ_CODES],
        ).fetchall()
        return [str(row[0]) for row in rows if row and row[0]]
    finally:
        market_db.close()


def _find_matching_resumable_session(
    db: Any,
    *,
    strategy_name: str,
    trade_date: date,
    mode: str,
    preset_name: str | None = None,
) -> dict[str, Any] | None:
    """Find a resumable session, matching preset identity when provided."""
    resumable = {"PLANNED", "ACTIVE", "PAUSED", "RUNNING"}
    sessions = db.list_sessions(limit=200)
    for session in sessions:
        if str(session.get("status", "")).upper() not in resumable:
            continue
        if session.get("strategy_name") != strategy_name:
            continue
        if session.get("mode") != mode:
            continue
        if session.get("trade_date") != trade_date.isoformat():
            continue
        if preset_name is not None:
            strategy_params = session.get("strategy_params") or {}
            if strategy_params.get("preset_name") != preset_name:
                continue
        return session
    return None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _cmd_prepare(args: argparse.Namespace) -> None:
    """Create a new paper session in DuckDB, or return an existing resumable one."""
    from nse_momentum_lab.services.backtest.backtest_presets import build_params_from_preset
    from nse_momentum_lab.services.backtest.engine import PositionSide
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB
    from nse_momentum_lab.services.paper.engine.shared_engine import resolve_strategy_key
    from nse_momentum_lab.services.paper.paper_backtest_bridge import build_paper_config_from_preset

    mode = getattr(args, "mode", "replay") or "replay"
    trade_date = (
        args.trade_date
        if isinstance(args.trade_date, date)
        else (date.fromisoformat(args.trade_date) if args.trade_date else date.today())
    )
    raw_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else []
    risk_config = _parse_json(args.risk_config) or {
        "portfolio_value": args.portfolio_value,
        "max_daily_loss_pct": 0.05,
        "max_drawdown_pct": 0.15,
        "flatten_time": "15:15:00",
        "slippage_bps": 5.0,
    }
    metadata = _parse_json(args.metadata)
    preset_name = getattr(args, "preset", None)

    if preset_name:
        params = build_params_from_preset(preset_name)
        direction = (
            PositionSide.SHORT if "breakdown" in str(params.strategy).lower() else PositionSide.LONG
        )
        strategy_config = build_paper_config_from_preset(preset_name, direction)
        strategy = strategy_config.strategy_key
        strategy_params = _serialize_strategy_params(
            strategy_config,
            preset_name=preset_name,
            metadata=metadata,
        )
    else:
        strategy = resolve_strategy_key(args.strategy)
        strategy_params = metadata

    symbols = raw_symbols or _load_default_symbols(args.market_db, trade_date)

    db = PaperDB(args.paper_db)
    try:
        # Idempotent: return existing resumable session rather than creating a duplicate.
        existing = _find_matching_resumable_session(
            db,
            strategy_name=strategy,
            trade_date=trade_date,
            mode=mode,
            preset_name=preset_name,
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
                        "preset": preset_name,
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
            strategy_params=strategy_params,
            notes=None,
        )
        session_id = session["session_id"]
        print(
            json.dumps(
                {
                    "session_id": session_id,
                    "strategy": strategy,
                    "preset": preset_name,
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
            no_alerts=getattr(args, "no_alerts", False),
        )
    )
    print(json.dumps(result, default=str))
    sys.exit(0 if "error" not in result else 1)


def _cmd_live(args: argparse.Namespace) -> None:
    """Run a live paper session."""
    from nse_momentum_lab.services.paper.scripts.paper_live import run_live_session_with_retry

    session_id = _resolve_session_id(
        args, args.paper_db, mode="live", trade_date=getattr(args, "trade_date", None)
    )
    result = _run_async(
        run_live_session_with_retry(
            session_id=session_id,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
            poll_interval=args.poll_interval,
            max_cycles=args.max_cycles,
            no_alerts=getattr(args, "no_alerts", False),
        )
    )
    print(json.dumps(result, default=str))
    sys.exit(0 if "error" not in result else 1)


def _resolve_session_ids(
    args: argparse.Namespace,
    db_path: str,
    *,
    mode: str,
    trade_date: str | None = None,
) -> list[str]:
    """Resolve explicit session_ids or auto-discover one session per strategy."""
    session_ids = list(getattr(args, "session_ids", None) or [])
    if session_ids:
        return session_ids

    strategies = list(getattr(args, "strategies", None) or [])
    if not strategies:
        strategies = ["2lynchbreakout", "2lynchbreakdown"]

    resolved: list[str] = []
    seen: set[str] = set()
    for strategy_raw in strategies:
        session_id = _resolve_session_id(
            argparse.Namespace(session_id=None, strategy=strategy_raw, trade_date=trade_date),
            db_path,
            mode=mode,
            trade_date=trade_date,
        )
        if session_id not in seen:
            resolved.append(session_id)
            seen.add(session_id)
    return resolved


def _cmd_multi_live(args: argparse.Namespace) -> None:
    """Run multiple live sessions inside one process and one DuckDB writer."""
    from nse_momentum_lab.services.paper.scripts.paper_live import run_live_session_group

    trade_date = getattr(args, "trade_date", None) or str(date.today())
    session_ids = _resolve_session_ids(args, args.paper_db, mode="live", trade_date=trade_date)
    result = _run_async(
        run_live_session_group(
            session_ids=session_ids,
            paper_db_path=args.paper_db,
            market_db_path=args.market_db,
            poll_interval=args.poll_interval,
            max_cycles=args.max_cycles,
            no_alerts=getattr(args, "no_alerts", False),
        )
    )
    print(json.dumps(result, default=str))
    sys.exit(0 if all("error" not in v for v in result.values()) else 1)


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
    from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
        AlertDispatcher,
        AlertEvent,
        AlertType,
        format_session_alert,
        get_alert_config,
    )

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        current = db.get_session(session_id)
        if current is None:
            print("{}")
            return
        status_before = str(current.get("status", "")).upper()
        if status_before != "PAUSED":
            db.update_session(session_id, status="PAUSED")
            # Dispatch session paused alert.
            try:
                config = get_alert_config()
                dispatcher = AlertDispatcher(paper_db=db, config=config)
                import asyncio

                async def _send():
                    await dispatcher.start()
                    subject, body = format_session_alert(session_id=session_id, event="PAUSED")
                    dispatcher.enqueue(
                        AlertEvent(
                            alert_type=AlertType.SESSION_PAUSED,
                            session_id=session_id,
                            subject=subject,
                            body=body,
                        )
                    )
                    await dispatcher.shutdown()

                asyncio.run(_send())
            except Exception:
                pass  # Alert is best-effort, don't block the command.
        print(json.dumps({"session_id": session_id, "status": "PAUSED"}))
    finally:
        db.close()


def _cmd_resume(args: argparse.Namespace) -> None:
    """Resume a paused session (mark ACTIVE)."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB
    from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
        AlertDispatcher,
        AlertEvent,
        AlertType,
        format_session_alert,
        get_alert_config,
    )

    session_id = _resolve_session_id(
        args, args.paper_db, mode=getattr(args, "mode", "replay") or "replay"
    )
    db = PaperDB(args.paper_db)
    try:
        current = db.get_session(session_id)
        if current is None:
            print("{}")
            return
        status_before = str(current.get("status", "")).upper()
        if status_before != "ACTIVE":
            db.update_session(session_id, status="ACTIVE")
            # Dispatch session resumed alert.
            try:
                config = get_alert_config()
                dispatcher = AlertDispatcher(paper_db=db, config=config)
                import asyncio

                async def _send():
                    await dispatcher.start()
                    subject, body = format_session_alert(session_id=session_id, event="RESUMED")
                    dispatcher.enqueue(
                        AlertEvent(
                            alert_type=AlertType.SESSION_RESUMED,
                            session_id=session_id,
                            subject=subject,
                            body=body,
                        )
                    )
                    await dispatcher.shutdown()

                asyncio.run(_send())
            except Exception:
                pass  # Alert is best-effort, don't block the command.
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
        flattened_count = len(flattened) if isinstance(flattened, list) else 0
        try:
            _dispatch_manual_flatten_notifications(
                db=db,
                session_id=session_id,
                flattened_count=flattened_count,
                session_status="PAUSED",
            )
        except Exception:
            logger.exception("Failed to dispatch manual flatten alerts session=%s", session_id)
        _sync_paper_replica_after_cli_write(args.paper_db, db)
        print(json.dumps({"session_id": session_id, "flattened": flattened, "status": "PAUSED"}))
    finally:
        db.close()


def _cmd_flatten_all(args: argparse.Namespace) -> None:
    """EMERGENCY KILL SWITCH — force-close all positions across all active sessions for a trade date.

    This is NOT part of normal daily operations. Use only when you need to immediately
    exit everything (system going offline, market event, etc.). Swing positions that would
    normally carry overnight are force-closed.
    """
    from nse_momentum_lab.services.paper.db.paper_db import (
        ACTIVE_SESSION_STATUSES,
        PaperDB,
    )

    trade_date = args.trade_date
    if trade_date is None:
        trade_date = str(date.today())

    db = PaperDB(args.paper_db)
    try:
        # Find all sessions in active statuses for the trade date.
        active_statuses = ", ".join(f"'{s}'" for s in ACTIVE_SESSION_STATUSES)
        rows = db._query_all(
            f"SELECT session_id, strategy_name FROM paper_sessions "
            f"WHERE status IN ({active_statuses}) AND trade_date = $1",
            [trade_date],
        )

        if not rows:
            print(
                json.dumps(
                    {
                        "action": "flatten_all",
                        "sessions_processed": 0,
                        "message": "No active sessions found",
                    }
                )
            )
            return

        total_flattened = 0
        results = []
        for row in rows:
            sid = row["session_id"]
            flattened = db.flatten_open_positions(sid)
            db.update_session(sid, status="COMPLETED")
            count = len(flattened) if isinstance(flattened, list) else 0
            total_flattened += count
            try:
                _dispatch_manual_flatten_notifications(
                    db=db,
                    session_id=sid,
                    flattened_count=count,
                    session_status="COMPLETED",
                )
            except Exception:
                logger.exception("Failed to dispatch flatten-all alerts session=%s", sid)
            results.append({"session_id": sid, "positions_flattened": count})

        _sync_paper_replica_after_cli_write(args.paper_db, db)
        summary = {
            "action": "flatten_all",
            "trade_date": trade_date,
            "sessions_processed": len(results),
            "total_positions_flattened": total_flattened,
            "sessions": results,
        }
        print(json.dumps(summary, default=str))
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
            no_alerts=getattr(args, "no_alerts", False),
        )
    )

    if "error" in result:
        print(f"ERROR: {result['error']}")
        sys.exit(1)

    print(f"Session: {result.get('session_id', '?')}")
    print(f"Status:  {result.get('status', '?')}")
    print(f"Bars:    {result.get('closed_bars', 0)}")
    print(f"Cycles:  {result.get('cycles', 0)}")


def _cmd_eod_carry(args: argparse.Namespace) -> None:
    """Post-market H-carry decisions: TIME_EXIT + WEAK_CLOSE_EXIT or carry forward."""
    from nse_momentum_lab.services.paper.scripts.paper_eod_carry import run_eod_carry

    session_id = getattr(args, "session_id", None)
    if not session_id:
        session_id = _resolve_session_id(
            args, args.paper_db, mode="live", trade_date=getattr(args, "trade_date", None)
        )
    result = run_eod_carry(
        session_id=session_id,
        trade_date=args.trade_date,
        paper_db_path=args.paper_db,
        market_db_path=args.market_db,
        no_alerts=getattr(args, "no_alerts", False),
    )
    print(json.dumps(result, default=str))
    sys.exit(0 if "error" not in result else 1)


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
        "--preset", default=None, help="Canonical backtest preset, e.g. BREAKOUT_2PCT"
    )
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
    replay.add_argument("--no-alerts", action="store_true", help="Disable Telegram/email alerts")
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
    live.add_argument("--no-alerts", action="store_true", help="Disable Telegram/email alerts")
    live.set_defaults(handler=_cmd_live)

    # multi-live
    multi_live = sub.add_parser(
        "multi-live",
        help="Run multiple live sessions in one writer process",
    )
    multi_live.add_argument(
        "--session-id",
        dest="session_ids",
        action="append",
        default=[],
        help="Explicit session ID (repeatable)",
    )
    multi_live.add_argument(
        "--strategy",
        dest="strategies",
        action="append",
        default=[],
        help="Strategy key to auto-discover (repeatable)",
    )
    multi_live.add_argument(
        "--trade-date",
        default=None,
        help="YYYY-MM-DD used to auto-discover live sessions (default: today)",
    )
    multi_live.add_argument("--poll-interval", type=float, default=1.0)
    multi_live.add_argument("--max-cycles", type=int, default=None)
    multi_live.add_argument(
        "--no-alerts", action="store_true", help="Disable Telegram/email alerts"
    )
    multi_live.set_defaults(handler=_cmd_multi_live)

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
    daily_sim.add_argument("--no-alerts", action="store_true", help="Disable Telegram/email alerts")
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

    # flatten-all (emergency kill switch)
    _flatten_all = sub.add_parser(
        "flatten-all",
        help="EMERGENCY: force-close all positions across all sessions for a trade date",
    )
    _flatten_all.add_argument("--trade-date", default=None, help="YYYY-MM-DD (default: today)")
    _flatten_all.set_defaults(handler=_cmd_flatten_all)

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
    daily_prepare.add_argument(
        "--preset", default=None, help="Canonical backtest preset, e.g. BREAKOUT_2PCT"
    )
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
    daily_replay.add_argument(
        "--no-alerts", action="store_true", help="Disable Telegram/email alerts"
    )
    daily_replay.set_defaults(handler=_cmd_daily_replay)

    # daily-live
    daily_live = sub.add_parser("daily-live", help="Run live session for today")
    daily_live.add_argument("--session-id", default=None)
    daily_live.add_argument("--strategy", default=None)
    daily_live.add_argument("--poll-interval", type=float, default=1.0)
    daily_live.add_argument("--max-cycles", type=int, default=None)
    daily_live.add_argument(
        "--no-alerts", action="store_true", help="Disable Telegram/email alerts"
    )
    daily_live.set_defaults(handler=_cmd_daily_live)

    # eod-carry
    eod_carry = sub.add_parser(
        "eod-carry",
        help="Post-market H-carry decisions (TIME_EXIT / carry) — run after nseml-build-features",
    )
    eod_carry.add_argument(
        "--session-id",
        default=None,
        help="Session ID. If omitted, auto-discovers via --strategy + --trade-date.",
    )
    eod_carry.add_argument("--strategy", default=None, help="Strategy key for auto-discovery")
    eod_carry.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    eod_carry.add_argument("--no-alerts", action="store_true", help="Disable Telegram/email alerts")
    eod_carry.set_defaults(handler=_cmd_eod_carry)

    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    parser = build_parser()
    args = parser.parse_args()

    # Startup cleanup: cancel orphaned STOPPING sessions from previous runs.
    try:
        db = _open_paper_db_with_orphan_cleanup(args.paper_db)
        stale = db.cleanup_stale_sessions()
        if stale:
            print(f"Cleaned up {stale} stale session(s) from previous run(s)", flush=True)
        db.close()
    except Exception:
        pass  # Non-critical — don't block startup if DB is unavailable.

    handler = getattr(args, "handler", None)
    if handler is None:
        raise SystemExit("No command specified")
    handler(args)


if __name__ == "__main__":
    main()
