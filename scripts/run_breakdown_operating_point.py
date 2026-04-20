"""Run the approved breakdown operating point as a single repeatable command.

Operating point:
- 4% breakdown: Option-B short-side tuning (BREAKDOWN_4PCT preset)
- 2% breakdown: Phase-1 canonical flags (BREAKDOWN_2PCT preset)

All strategy parameters come from named presets in backtest_presets.py.
Only infrastructure flags (universe, date window, parallelism) are accepted here.
"""

from __future__ import annotations

import argparse

from nse_momentum_lab.services.backtest.backtest_presets import (
    build_params_from_preset,
    list_preset_names,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import DuckDBBacktestRunner

_BREAKDOWN_PRESETS = ["BREAKDOWN_4PCT", "BREAKDOWN_2PCT"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run 4% tuned + 2% canonical breakdown backtests in one command",
        epilog=(
            f"Runs presets: {', '.join(_BREAKDOWN_PRESETS)}. "
            f"All available presets: {', '.join(list_preset_names())}."
        ),
    )
    # Infrastructure args only — strategy params come from presets.
    parser.add_argument("--start-year", type=int, default=2025)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--start-date", type=str, default="2025-04-01")
    parser.add_argument("--end-date", type=str, default="2026-03-10")
    parser.add_argument("--universe-size", type=int, default=2000)
    parser.add_argument("--min-price", type=int, default=10)
    parser.add_argument("--min-filters", type=int, default=5)
    parser.add_argument("--entry-timeframe", type=str, default="5min", choices=["5min", "daily"])
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Number of parallel worker threads per leg (default: 1)",
    )
    parser.add_argument("--force", action="store_true", help="Force rerun even if cached")
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Publish DuckDB snapshot artifact after each run",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    infra: dict[str, object] = {
        "universe_size": args.universe_size,
        "min_price": args.min_price,
        "min_filters": args.min_filters,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "entry_timeframe": args.entry_timeframe,
        "parallel_workers": args.parallel_workers,
    }

    results: dict[str, str] = {}
    try:
        runner = DuckDBBacktestRunner()
        for preset_name in _BREAKDOWN_PRESETS:
            params = build_params_from_preset(preset_name, infra_overrides=infra)
            print(f"[RUN] {preset_name} start")
            exp_id = runner.run(params, force=args.force, snapshot=args.snapshot)
            print(f"[RUN] {preset_name} done  exp_id={exp_id}")
            results[preset_name] = exp_id
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(f"[BACKTEST BLOCKED] {exc}") from exc

    print()
    print("Breakdown operating point completed")
    for preset_name, exp_id in results.items():
        print(f"  {preset_name:<20} exp_id={exp_id}")


if __name__ == "__main__":
    main()
