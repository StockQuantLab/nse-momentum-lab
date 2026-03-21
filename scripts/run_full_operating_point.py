"""Run the full 2LYNCH operating point in one command.

Includes:
- Breakout 4% (canonical, no daily ranking budget cap)
- Breakout 2% (canonical, no daily ranking budget cap)
- Breakdown 4% (Option-B short tuning)
- Breakdown 2% (Phase-1 flags: strict_filter_l, narrow_n, skip_gap_down, rs_min=-0.10, budget=5)
"""

from __future__ import annotations

import argparse

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run breakout+breakdown 2%/4% operating point backtests in one command"
    )
    parser.add_argument("--start-year", type=int, default=2025)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--start-date", type=str, default="2025-04-01")
    parser.add_argument("--end-date", type=str, default="2026-03-10")
    parser.add_argument("--universe-size", type=int, default=2000)
    parser.add_argument("--min-price", type=int, default=10)
    parser.add_argument("--min-filters", type=int, default=5)
    parser.add_argument("--entry-timeframe", type=str, default="5min", choices=["5min", "daily"])
    parser.add_argument(
        "--breakout-daily-candidate-budget",
        type=int,
        default=0,
        help=(
            "Daily candidate cap for breakout runs. "
            "Use 0 to disable and run canonical unbudgeted breakout selection."
        ),
    )
    parser.add_argument(
        "--breakout-c-quality-source",
        type=str,
        default="current",
        choices=["current", "prev"],
        help="Breakout ranking C-quality source for breakout legs.",
    )
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
    strategy: str,
    breakout_threshold: float,
    force: bool,
    snapshot: bool,
    common: dict[str, object],
    breakout_budget: int | None = None,
    short_option_b: bool = False,
    breakdown_phase1: bool = False,
) -> str:
    params = BacktestParams(
        strategy=strategy,
        breakout_threshold=breakout_threshold,
        abnormal_gap_mode="trail_after_gap",
        same_day_r_ladder=True,
        same_day_r_ladder_start_r=2,
        **common,
    )

    if breakout_budget is not None:
        params.breakout_daily_candidate_budget = breakout_budget

    if short_option_b:
        params.short_trail_activation_pct = 0.04
        params.short_time_stop_days = 3
        params.short_max_stop_dist_pct = 0.05
        params.short_abnormal_profit_pct = 0.05

    if breakdown_phase1:
        params.breakdown_daily_candidate_budget = 5
        params.breakdown_rs_min = -0.10
        params.breakdown_strict_filter_l = True
        params.breakdown_filter_n_narrow_only = True
        params.breakdown_skip_gap_down = True

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
        "breakout_use_current_day_c_quality": args.breakout_c_quality_source == "current",
    }

    try:
        runner = DuckDBBacktestRunner()

        exp_bo_4 = _run_variant(
            runner,
            label="4% breakout (canonical)",
            strategy="thresholdbreakout",
            breakout_threshold=0.04,
            force=args.force,
            snapshot=args.snapshot,
            common=common,
            breakout_budget=args.breakout_daily_candidate_budget,
        )
        exp_bo_2 = _run_variant(
            runner,
            label="2% breakout (canonical)",
            strategy="thresholdbreakout",
            breakout_threshold=0.02,
            force=args.force,
            snapshot=args.snapshot,
            common=common,
            breakout_budget=args.breakout_daily_candidate_budget,
        )
        exp_bd_4 = _run_variant(
            runner,
            label="4% breakdown (Option-B tuned)",
            strategy="thresholdbreakdown",
            breakout_threshold=0.04,
            force=args.force,
            snapshot=args.snapshot,
            common=common,
            short_option_b=True,
        )
        exp_bd_2 = _run_variant(
            runner,
            label="2% breakdown (Phase-1 flags + R²-ranking)",
            strategy="thresholdbreakdown",
            breakout_threshold=0.02,
            force=args.force,
            snapshot=args.snapshot,
            common=common,
            breakdown_phase1=True,
        )

        print()
        print("Full operating point completed")
        print(f"breakout_4pct_exp_id: {exp_bo_4}")
        print(f"breakout_2pct_exp_id: {exp_bo_2}")
        print(f"breakdown_4pct_exp_id: {exp_bd_4}")
        print(f"breakdown_2pct_exp_id: {exp_bd_2}")
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(f"[BACKTEST BLOCKED] {exc}") from exc


if __name__ == "__main__":
    main()
