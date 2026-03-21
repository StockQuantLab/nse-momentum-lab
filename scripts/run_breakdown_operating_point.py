"""Run the approved breakdown operating point as a single repeatable command.

Operating point:
- 4% breakdown: short-side Option-B tuning enabled
- 2% breakdown: short-side tuning with configurable profile

Profiles for 2% BD (--short-profile):
- option-b:     trail 4%, time-stop 3d, max-stop 5%, abnormal 5% (same as 4% BD)
- aggressive:   trail 3%, time-stop 2d, max-stop 4%, abnormal 4%
- quick-scalp:  trail 2%, time-stop 2d, max-stop 3%, abnormal 3%
- legacy:       trail 8%, time-stop 5d (original long-side defaults — NOT recommended)
"""

from __future__ import annotations

import argparse

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)

SHORT_PROFILES: dict[str, dict[str, float | int]] = {
    "option-b": {
        "short_trail_activation_pct": 0.04,
        "short_time_stop_days": 3,
        "short_max_stop_dist_pct": 0.05,
        "short_abnormal_profit_pct": 0.05,
    },
    "aggressive": {
        "short_trail_activation_pct": 0.03,
        "short_time_stop_days": 2,
        "short_max_stop_dist_pct": 0.04,
        "short_abnormal_profit_pct": 0.04,
    },
    "quick-scalp": {
        "short_trail_activation_pct": 0.02,
        "short_time_stop_days": 2,
        "short_max_stop_dist_pct": 0.03,
        "short_abnormal_profit_pct": 0.03,
    },
    "legacy": {},  # uses long-side defaults (8% trail, 5d time-stop)
}


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
    parser.add_argument(
        "--breakout-c-quality-source",
        type=str,
        default="current",
        choices=["current", "prev"],
        help="Breakout ranking C-quality source (symmetry flag; breakdown runs ignore breakout ranking).",
    )
    parser.add_argument("--breakdown-budget", type=int, default=5,
                        help="Max short candidates per day (default: 5)")
    parser.add_argument("--breakdown-rs-min", type=float, default=0.0,
                        help="rs_252 threshold for 2%% BD filter_y (default 0.0 = rs_252<0). "
                             "E.g. -0.10 requires >= 10%% YTD underperformance.")
    parser.add_argument(
        "--breakdown-ti65-mode",
        type=str,
        default="off",
        choices=["off", "bearish"],
        help="Optional TI65 gate for 2%% BD trend filter_l: off (default) or bearish (ma_7/ma_65_sma <= 0.95).",
    )
    parser.add_argument(
        "--short-initial-stop-atr-cap-mult",
        type=float,
        default=None,
        help=(
            "Optional short-side intraday initial stop cap in ATR_20 multiples "
            "(e.g. 1.5). None keeps session-high stop."
        ),
    )
    parser.add_argument(
        "--short-same-day-r-ladder-start-r",
        type=int,
        default=None,
        help=(
            "Optional short-side same-day R-ladder start override "
            "(e.g. 1). None keeps default start."
        ),
    )
    parser.add_argument(
        "--short-same-day-take-profit-pct",
        type=float,
        default=None,
        help=(
            "Optional short-side same-day take-profit threshold "
            "(e.g. 0.02 for +2%% favorable move)."
        ),
    )
    parser.add_argument(
        "--breakdown-breadth-threshold",
        type=float,
        default=None,
        help=(
            "Optional market-breadth gate for breakdown (e.g. 0.55). "
            "Requires pct_below_ma20 > threshold for a trade day."
        ),
    )
    parser.add_argument(
        "--breakdown-require-atr-expansion",
        action="store_true",
        help=(
            "Require breakdown day ATR20 to be above SMA20(ATR20). "
            "Use as a hard freshness gate (off by default)."
        ),
    )
    parser.add_argument("--short-profile", type=str, default="option-b",
                        choices=list(SHORT_PROFILES.keys()),
                        help="Short engine param profile for 2%% BD (default: option-b)")
    parser.add_argument("--sweep-profiles", action="store_true",
                        help="Run ALL short profiles for 2%% BD (overrides --short-profile)")
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
    short_profile: dict[str, float | int] | None = None,
) -> str:
    params = BacktestParams(
        strategy="thresholdbreakdown",
        breakout_threshold=breakout_threshold,
        abnormal_gap_mode="trail_after_gap",
        same_day_r_ladder=True,
        same_day_r_ladder_start_r=2,
        **common,
    )

    if short_profile:
        for key, val in short_profile.items():
            setattr(params, key, val)

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
        "breakdown_daily_candidate_budget": args.breakdown_budget,
        "short_initial_stop_atr_cap_mult": args.short_initial_stop_atr_cap_mult,
        "short_same_day_r_ladder_start_r": args.short_same_day_r_ladder_start_r,
        "short_same_day_take_profit_pct": args.short_same_day_take_profit_pct,
    }
    common_2pct = {
        **common,
        "breakdown_rs_min": args.breakdown_rs_min,
        "breakdown_strict_filter_l": True,
        "breakdown_filter_n_narrow_only": True,
        "breakdown_skip_gap_down": True,
        "breakdown_max_prior_breakdowns": -1,
        "breakdown_ti65_mode": args.breakdown_ti65_mode,
        "breakdown_breadth_threshold": args.breakdown_breadth_threshold,
        "breakdown_require_atr_expansion": args.breakdown_require_atr_expansion,
    }

    try:
        runner = DuckDBBacktestRunner()

        # 4% BD always uses Option-B
        exp_4 = _run_variant(
            runner,
            label="4% breakdown (Option-B tuned)",
            breakout_threshold=0.04,
            force=args.force,
            snapshot=args.snapshot,
            common=common,
            short_profile=SHORT_PROFILES["option-b"],
        )

        # 2% BD: sweep all profiles or run single profile
        profiles_to_run = (
            SHORT_PROFILES if args.sweep_profiles
            else {args.short_profile: SHORT_PROFILES[args.short_profile]}
        )

        results_2pct: dict[str, str] = {}
        for profile_name, profile_params in profiles_to_run.items():
            exp_2 = _run_variant(
                runner,
                label=f"2% breakdown ({profile_name})",
                breakout_threshold=0.02,
                force=args.force,
                snapshot=args.snapshot,
                common=common_2pct,
                short_profile=profile_params or None,
            )
            results_2pct[profile_name] = exp_2

        print()
        print("Operating point completed")
        print(f"4% tuned exp_id: {exp_4}")
        for profile_name, exp_id in results_2pct.items():
            print(f"2% ({profile_name}) exp_id: {exp_id}")
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(f"[BACKTEST BLOCKED] {exc}") from exc


if __name__ == "__main__":
    main()
