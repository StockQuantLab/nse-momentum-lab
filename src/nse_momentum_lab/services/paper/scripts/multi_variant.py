"""Multi-variant paper session planner.

Creates multiple paper sessions with different strategy configurations
for side-by-side comparison. Each variant gets its own session_id
and runs independently.

Usage:
    python -m nse_momentum_lab.services.paper.scripts.multi_variant \\
        --strategy 2lynchbreakout --trade-date 2025-01-15 \\
        --variants 3 --universe-size 50
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from typing import Any

from nse_momentum_lab.services.paper.db.paper_db import PaperDB
from nse_momentum_lab.services.paper.engine.shared_engine import (
    get_paper_strategy_config,
    resolve_strategy_key,
)

logger = logging.getLogger(__name__)

# Pre-defined variant profiles for common comparison axes.
_VARIANT_PROFILES: dict[str, list[dict[str, Any]]] = {
    "2lynchbreakout": [
        {
            "label": "conservative",
            "breakout_threshold": 0.03,
            "max_positions": 5,
            "entry_cutoff_minutes": 20,
        },
        {
            "label": "baseline",
            "breakout_threshold": 0.04,
            "max_positions": 10,
            "entry_cutoff_minutes": 30,
        },
        {
            "label": "aggressive",
            "breakout_threshold": 0.05,
            "max_positions": 15,
            "entry_cutoff_minutes": 45,
        },
    ],
    "2lynchbreakdown": [
        {
            "label": "conservative",
            "breakout_threshold": 0.03,
            "max_positions": 5,
            "entry_cutoff_minutes": 20,
        },
        {
            "label": "baseline",
            "breakout_threshold": 0.04,
            "max_positions": 10,
            "entry_cutoff_minutes": 30,
        },
        {
            "label": "aggressive",
            "breakout_threshold": 0.05,
            "max_positions": 15,
            "entry_cutoff_minutes": 45,
        },
    ],
    "episodicpivot": [
        {"label": "tight", "max_positions": 5, "entry_cutoff_minutes": 20},
        {"label": "baseline", "max_positions": 10, "entry_cutoff_minutes": 30},
        {"label": "wide", "max_positions": 15, "entry_cutoff_minutes": 45},
    ],
}


def plan_variants(
    *,
    strategy: str,
    trade_date: str,
    symbols: list[str],
    num_variants: int = 3,
    portfolio_value: float = 1_000_000.0,
    risk_overrides: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Plan variant sessions for a strategy.

    Returns a list of session creation dicts, each with:
    - label, strategy, strategy_config overrides, symbols, risk_config
    """
    canonical = resolve_strategy_key(strategy)
    base_config = get_paper_strategy_config(canonical)
    profiles = _VARIANT_PROFILES.get(canonical, [])

    # If we have fewer profiles than requested, pad with the last one.
    while len(profiles) < num_variants:
        if profiles:
            last = dict(profiles[-1])
            last["label"] = f"variant_{len(profiles)}"
            profiles.append(last)
        else:
            # No profiles defined — generate generic variants.
            profiles.append(
                {
                    "label": f"variant_{len(profiles)}",
                    "max_positions": 10,
                    "entry_cutoff_minutes": 30,
                }
            )

    profiles = profiles[:num_variants]

    sessions: list[dict[str, Any]] = []
    for profile in profiles:
        overrides = {k: v for k, v in profile.items() if k != "label"}
        overrides.setdefault("strategy_key", canonical)
        overrides.setdefault("direction", base_config.direction)

        risk_config: dict[str, Any] = {
            "portfolio_value": portfolio_value,
            "max_daily_loss_pct": 0.05,
            "max_drawdown_pct": 0.15,
            "flatten_time": "15:15:00",
            "slippage_bps": 5.0,
        }
        if risk_overrides:
            risk_config.update(risk_overrides)

        sessions.append(
            {
                "label": profile["label"],
                "strategy": canonical,
                "strategy_overrides": overrides,
                "trade_date": trade_date,
                "symbols": list(symbols),
                "risk_config": risk_config,
            }
        )

    return sessions


def create_variant_sessions(
    *,
    paper_db: PaperDB,
    strategy: str,
    trade_date: str,
    symbols: list[str],
    num_variants: int = 3,
    portfolio_value: float = 1_000_000.0,
) -> list[str]:
    """Create variant sessions in the paper DB. Returns session IDs."""
    plans = plan_variants(
        strategy=strategy,
        trade_date=trade_date,
        symbols=symbols,
        num_variants=num_variants,
        portfolio_value=portfolio_value,
    )

    session_ids: list[str] = []
    for plan in plans:
        trade_date_obj = (
            date.fromisoformat(plan["trade_date"])
            if isinstance(plan["trade_date"], str)
            else plan["trade_date"]
        )
        session = paper_db.create_session(
            strategy_name=plan["strategy"],
            mode="replay",
            trade_date=trade_date_obj,
            status="PLANNED",
            symbols=plan["symbols"],
            risk_config=plan["risk_config"],
            strategy_params=plan["strategy_overrides"],
            notes=f"{plan['label']} (variant of {strategy})",
        )
        session_ids.append(session["session_id"])
        logger.info(
            "Created variant session id=%s label=%s strategy=%s",
            session["session_id"],
            plan["label"],
            plan["strategy"],
        )

    return session_ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-variant paper session planner")
    parser.add_argument("--strategy", required=True, help="Strategy name")
    parser.add_argument("--trade-date", required=True, help="Trade date YYYY-MM-DD")
    parser.add_argument("--variants", type=int, default=3, help="Number of variants")
    parser.add_argument("--symbols-file", help="File with one symbol per line")
    parser.add_argument("--symbols", help="Comma-separated symbols")
    parser.add_argument("--portfolio-value", type=float, default=1_000_000)
    parser.add_argument("--paper-db", default="data/paper.duckdb")
    parser.add_argument("--dry-run", action="store_true", help="Print plans without creating")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    # Resolve symbols.
    symbols: list[str] = []
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    elif args.symbols_file:
        with open(args.symbols_file) as f:
            symbols = [line.strip() for line in f if line.strip()]

    if not symbols:
        logger.error("No symbols provided. Use --symbols or --symbols-file")
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
        sys.exit(0)

    paper_db = PaperDB(args.paper_db)
    try:
        session_ids = create_variant_sessions(
            paper_db=paper_db,
            strategy=args.strategy,
            trade_date=args.trade_date,
            symbols=symbols,
            num_variants=args.variants,
            portfolio_value=args.portfolio_value,
        )
        print(f"Created {len(session_ids)} variant sessions:")
        for sid in session_ids:
            print(f"  {sid}")
    finally:
        paper_db.close()


if __name__ == "__main__":
    main()
