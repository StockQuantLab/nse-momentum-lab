"""Run the approved breakdown operating point as a single repeatable command.

Operating point:
- 4% breakdown: short-side Option-B tuning enabled
- 2% breakdown: canonical short baseline
"""

from __future__ import annotations

import argparse

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run 4% tuned + 2% canonical breakdown backtests in one command"
    )
    parser.add_argument("--start-year", type=int, default=2025)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--start-date", type=str, default="2025-04-01")
    parser.add_argument("--end-date", type=str, default="2026-03-10")
    parser.add_argument("--universe-size", type=int, default=2000)
    parser.add_argument("--min-price", type=int, default=10)
    parser.add_argument("--min-filters", type=int, default=5)
    parser.add_argument("--entry-timeframe", type=str, default="5min", choices=["5min", "daily"])
    parser.add_argument("--force", action="store_true", help="Force rerun even if cached")
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Publish DuckDB snapshot artifact after each run",
    )
    return parser


def _run_variant(
    runner: DuckDBBacktestRunner,
    *,
    label: str,
    breakout_threshold: float,
    force: bool,
    snapshot: bool,
    common: dict[str, object],
    short_option_b: bool,
) -> str:
    params = BacktestParams(
        strategy="thresholdbreakdown",
        breakout_threshold=breakout_threshold,
        abnormal_gap_mode="trail_after_gap",
        same_day_r_ladder=True,
        same_day_r_ladder_start_r=2,
        **common,
    )

    if short_option_b:
        params.short_trail_activation_pct = 0.04
        params.short_time_stop_days = 3
        params.short_max_stop_dist_pct = 0.05
        params.short_abnormal_profit_pct = 0.05

    print(f"[RUN] {label} start")
    exp_id = runner.run(params, force=force, snapshot=snapshot)
    print(f"[RUN] {label} done exp_id={exp_id}")
    return exp_id


def main() -> None:
    args = build_parser().parse_args()

    common = {
        "universe_size": args.universe_size,
        "min_price": args.min_price,
        "min_filters": args.min_filters,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "entry_timeframe": args.entry_timeframe,
    }

    runner = DuckDBBacktestRunner()

    exp_4 = _run_variant(
        runner,
        label="4% breakdown (Option-B tuned)",
        breakout_threshold=0.04,
        force=args.force,
        snapshot=args.snapshot,
        common=common,
        short_option_b=True,
    )
    exp_2 = _run_variant(
        runner,
        label="2% breakdown (canonical)",
        breakout_threshold=0.02,
        force=args.force,
        snapshot=args.snapshot,
        common=common,
        short_option_b=False,
    )

    print()
    print("Operating point completed")
    print(f"4% tuned exp_id: {exp_4}")
    print(f"2% canonical exp_id: {exp_2}")


if __name__ == "__main__":
    main()
