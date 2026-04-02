"""CLI entrypoint for nseml-sweep — YAML-driven backtest parameter sweeps."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from nse_momentum_lab.services.backtest.comparison import format_comparison_table
from nse_momentum_lab.services.backtest.sweep_runner import run_sweep_with_comparison
from nse_momentum_lab.services.backtest.sweep_schema import load_sweep_config

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-sweep",
        description="Run parameter sweeps defined in YAML and auto-compare results.",
    )
    parser.add_argument("config", type=Path, help="Path to sweep YAML config")
    parser.add_argument("--dry-run", action="store_true", help="Show combinations without running")
    parser.add_argument("--force", action="store_true", help="Re-run even if cached")
    parser.add_argument("--snapshot", action="store_true", help="Publish DuckDB snapshot per run")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args()

    config = load_sweep_config(args.config)
    logger.info("Sweep: %s (%d combinations)", config.name, len(config.combinations()))

    results, ranked = run_sweep_with_comparison(
        config,
        force=args.force,
        snapshot=args.snapshot,
        dry_run=args.dry_run,
    )

    if args.json:
        output = {
            "sweep": config.name,
            "total_combinations": len(config.combinations()),
            "results": [
                {"exp_id": r.exp_id, "label": r.label, "params": r.params} for r in results
            ],
            "ranked": [
                {
                    "exp_id": s.exp_id,
                    "label": s.label,
                    "calmar_ratio": s.calmar_ratio,
                    "win_rate": s.win_rate,
                    "annualised_return": s.annualised_return,
                    "max_drawdown": s.max_drawdown,
                }
                for s in ranked
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"\n{'=' * 60}")
        print(f"Sweep: {config.name}")
        print(f"{'=' * 60}")
        print(f"Total combinations: {len(config.combinations())}")
        print(f"Completed: {len(results)}")
        print()
        for r in results:
            print(f"  {r.label:40s} -> {r.exp_id}")
        if ranked:
            print(f"\n{'=' * 60}")
            print(f"Ranked Results (top {config.compare.top_n}):")
            print(f"{'=' * 60}")
            print(format_comparison_table(ranked))


if __name__ == "__main__":
    main()
