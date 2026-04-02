"""Paper session planner — checks readiness, bootstraps session records, outputs a manifest.

This is a planning/bootstrap step only. It does NOT execute live sessions, flatten
positions, or archive sessions. The websocket loop in _cmd_daily_live is blocking,
so actually running sessions must happen in separate processes (one per session).

Usage flow:
  1. nseml-paper-plan          -> produces manifest JSON
  2. nseml-paper-live --session-id <id> --execute   (per session, separate process)
  3. nseml-paper-flatten --session-id <id>           (at EOD)
  4. nseml-paper-archive --session-id <id>           (post-session)
"""

from __future__ import annotations

import argparse
import io
import json as _json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class SessionPlan:
    """Configuration for paper session planning."""

    trade_date: date
    strategy_variants: list[tuple[str, float]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.strategy_variants:
            # Default: 4 variants matching current daily workflow
            self.strategy_variants = [
                ("thresholdbreakout", 0.04),
                ("thresholdbreakout", 0.02),
                ("thresholdbreakdown", 0.04),
                ("thresholdbreakdown", 0.02),
            ]


@dataclass
class SessionEntry:
    """A single session in the plan manifest."""

    strategy: str
    threshold: float
    session_id: str | None = None
    status: str = "PLANNED"


@dataclass
class PlanManifest:
    """Machine-readable manifest of planned sessions."""

    trade_date: str
    verdict: str
    coverage_ready: bool
    readiness_reasons: list[str] = field(default_factory=list)
    sessions: list[SessionEntry] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "verdict": self.verdict,
            "coverage_ready": self.coverage_ready,
            "readiness_reasons": self.readiness_reasons,
            "sessions": [asdict(s) for s in self.sessions],
        }


async def check_readiness(trade_date: date) -> dict[str, Any]:
    """Check daily readiness by calling _build_daily_prepare_report directly.

    Returns dict with 'verdict' key: READY, OBSERVE_ONLY, or BLOCKED.
    """
    from nse_momentum_lab.cli.paper import _build_daily_prepare_report, _resolve_daily_symbols

    args = argparse.Namespace(symbols=None, all_symbols=True)
    symbols = _resolve_daily_symbols(args, trade_date, live=True)

    report = _build_daily_prepare_report(
        trade_date=trade_date,
        symbols=symbols,
        mode="live",
    )
    verdict = report.get("verdict", "BLOCKED")
    return {
        "verdict": verdict,
        "coverage_ready": report.get("coverage_ready", False),
        "reasons": report.get("reasons", []),
        "remediation": report.get("remediation", []),
    }


async def bootstrap_session(trade_date: date, strategy: str, threshold: float) -> SessionEntry:
    """Bootstrap a session record for one strategy variant.

    Calls _cmd_daily_live with execute=False, run=False to create the
    session record without starting the blocking websocket loop.
    Returns a SessionEntry with the session_id if successful.
    """
    from nse_momentum_lab.cli.paper import _cmd_daily_live

    strategy_params = f'{{"breakout_threshold": {threshold}}}'

    args = argparse.Namespace(
        trade_date=trade_date,
        strategy=strategy,
        experiment_id=None,
        symbols=None,
        all_symbols=True,
        session_id=None,
        strategy_params=strategy_params,
        risk_config=None,
        notes=None,
        feed_mode=None,
        execute=False,
        run=False,
        observe=False,
        watchlist=False,
    )

    captured = io.StringIO()
    real_stdout = sys.stdout
    try:
        sys.stdout = captured
        await _cmd_daily_live(args)
    except SystemExit:
        pass
    except Exception as e:
        return SessionEntry(strategy=strategy, threshold=threshold, status=f"ERROR: {e}")

    finally:
        sys.stdout = real_stdout

    output = captured.getvalue()
    json_start = None
    for i, line in enumerate(output.split("\n")):
        stripped = line.lstrip()
        if stripped.startswith("{"):
            json_start = i
            break
    if json_start is not None:
        json_blob = "\n".join(output.split("\n")[json_start:])
        try:
            parsed = _json.loads(json_blob)
            return SessionEntry(
                strategy=strategy,
                threshold=threshold,
                session_id=parsed.get("session_id"),
                status=parsed.get("status", "PLANNING"),
            )
        except _json.JSONDecodeError, ValueError:
            pass

    return SessionEntry(
        strategy=strategy,
        threshold=threshold,
        status="UNKNOWN",
    )


async def plan_sessions(config: SessionPlan) -> PlanManifest:
    """Plan sessions for a trading day.

    Checks readiness, then bootstraps session records for each variant.
    Returns a PlanManifest suitable for JSON serialization and scheduler consumption.
    Raises SystemExit if any session bootstrap fails.
    """
    readiness = await check_readiness(config.trade_date)
    manifest = PlanManifest(
        trade_date=config.trade_date.isoformat(),
        verdict=readiness["verdict"],
        coverage_ready=readiness["coverage_ready"],
        readiness_reasons=readiness["reasons"],
    )

    if readiness["verdict"] != "READY":
        logger.warning("Readiness verdict: %s - skipping session bootstrap", readiness["verdict"])
        return manifest

    errors: list[str] = []
    for strategy_name, threshold in config.strategy_variants:
        entry = await bootstrap_session(config.trade_date, strategy_name, threshold)
        manifest.sessions.append(entry)
        if entry.session_id is None:
            errors.append(f"{strategy_name}@{threshold}: {entry.status}")
        logger.info(
            "Planned session for %s @ %.0f%%: %s (session_id=%s)",
            strategy_name,
            threshold * 100,
            entry.status,
            entry.session_id,
        )

    if errors:
        raise SystemExit(
            f"Session bootstrap failed for {len(errors)} variant(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
    return manifest
