"""CLI entrypoint for nseml-paper-plan — bootstrap paper session records and output manifest.

This is a planning step only. It does NOT execute live sessions.
Use nseml-paper --session-id <id> --execute to actually run each session.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import logging
from datetime import date

from nse_momentum_lab.services.paper.session_planner import (
    SessionPlan,
    check_readiness,
    plan_sessions,
)

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-paper-plan",
        description="Plan paper sessions: check readiness, bootstrap session records, output manifest.",
    )
    parser.add_argument("--date", "-d", type=str, help="Trading date (YYYY-MM-DD)")
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=[],
        help="Strategy names (default: thresholdbreakout + thresholdbreakdown at 2pct and 4pct)",
    )
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        default=[],
        help="Threshold values (default: 0.02, 0.04). Cartesian product with --strategies",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Show resolved variants without bootstrapping sessions",
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Run readiness check and show verdict, without bootstrapping sessions",
    )
    return parser


def _resolve_variants(args: argparse.Namespace) -> list[tuple[str, float]]:
    """Build (strategy, threshold) variant list from CLI args."""
    strategies = args.strategies or []
    thresholds = args.thresholds or []
    if strategies and thresholds:
        return list(itertools.product(strategies, thresholds))
    if strategies:
        return [(s, t) for s in strategies for t in [0.02, 0.04]]
    return []  # SessionPlan.__post_init__ fills defaults


async def _run(args: argparse.Namespace) -> None:
    trade_date = date.fromisoformat(args.date) if args.date else date.today()
    variants = _resolve_variants(args)
    config = SessionPlan(trade_date=trade_date, strategy_variants=variants)

    if args.preview:
        print(f"[PREVIEW] Would plan sessions for {trade_date}")
        print(f"  Variants: {config.strategy_variants}")
        return

    if args.preflight:
        readiness = await check_readiness(trade_date)
        print(json.dumps(readiness, indent=2))
        if readiness["verdict"] != "READY":
            raise SystemExit(f"Readiness: {readiness['verdict']}")
        return

    manifest = await plan_sessions(config)
    print(json.dumps(manifest.to_dict(), indent=2))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
