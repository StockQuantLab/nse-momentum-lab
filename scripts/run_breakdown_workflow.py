"""Reusable breakdown optimization workflow for repeated experimentation.

Use this as the single entry-point for planned breakdown runs so ad-hoc command
chains are replaced by named presets.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = ROOT / "scripts" / "run_breakdown_operating_point.py"

STEP_PRESETS: dict[str, dict[str, object]] = {
    "baseline": {
        "description": "phase-1 canonical (budget=5, rs_min=-0.10)",
        "breakdown_daily_candidate_budget": 5,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": False,
    },
    "ti65": {
        "description": "TI65 bearish gate",
        "breakdown_daily_candidate_budget": 5,
        "breakdown_ti65_mode": "bearish",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": False,
    },
    "breadth": {
        "description": "market breadth gate",
        "breakdown_daily_candidate_budget": 5,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": False,
    },
    "atr-expansion": {
        "description": "volatility expansion hard gate",
        "breakdown_daily_candidate_budget": 5,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": True,
    },
    "atr-cap": {
        "description": "ATR-capped short initial stop (1.5x ATR_20)",
        "breakdown_daily_candidate_budget": 5,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": False,
        "short_initial_stop_atr_cap_mult": 1.5,
    },
    "day0-profit": {
        "description": "same-day short profit taking at +2%",
        "breakdown_daily_candidate_budget": 5,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": False,
        "short_same_day_take_profit_pct": 0.02,
    },
    "budget8": {
        "description": "budget stress test at 8 candidates/day",
        "breakdown_daily_candidate_budget": 8,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": True,
    },
    "budget10": {
        "description": "budget stress test at 10 candidates/day",
        "breakdown_daily_candidate_budget": 10,
        "breakdown_ti65_mode": "off",
        "breakdown_breadth_threshold": None,
        "breakdown_require_atr_expansion": True,
    },
}

BASE_FILTERS = {
    "breakdown_strict_filter_l": True,
    "breakdown_filter_n_narrow_only": True,
    "breakdown_skip_gap_down": True,
}

PLAN_ALIASES = {
    "phase3a": ["baseline"],
    "phase3b": ["ti65", "breadth"],
    "phase3c": ["atr-expansion"],
    "phase3d": ["budget8"],
    "phase3f": ["budget10"],
}

DEFAULT_WORKFLOW_RS_MIN = -0.10
DEFAULT_WORKFLOW_BREADTH_THRESHOLD = 0.55


def _load_runner_module():
    spec = importlib.util.spec_from_file_location("run_breakdown_operating_point", _MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load breakdown runner module from {_MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_parser():
    module = _load_runner_module()

    parser = module.build_parser()
    parser.description = "Run repeated breakdown optimization presets for reproducible experiments."
    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.add_argument(
        "--step",
        action="append",
        choices=sorted(STEP_PRESETS),
        help=(
            "Preset to run:\n"
            "  baseline       = phase-1 canonical (budget=5, rs_min=-0.10)\n"
            "  ti65           = phase-3d TI65 bearish gate\n"
            "  breadth        = phase-3d breadth gate\n"
            "  atr-expansion  = phase-3e ATR expansion hard gate\n"
            "  atr-cap        = phase-3b ATR-capped short initial stop\n"
            "  day0-profit    = phase-3c short same-day profit target\n"
            "  budget8        = budget=8 (with phase-1 + phase-3e)\n"
            "  budget10       = budget=10 (with phase-1 + phase-3e)"
        ),
    )
    parser.add_argument(
        "--plan",
        action="append",
        choices=sorted(PLAN_ALIASES),
        help="Named experiment sequence alias (e.g. phase3a, phase3b, phase3c)",
    )
    parser.add_argument(
        "--compare-baseline",
        action="store_true",
        help="Print baseline comparison command lines after each step",
    )
    parser.add_argument(
        "--baseline-4pct-exp",
        help="Optional 4%% baseline exp_id used in compare hints",
    )
    parser.add_argument(
        "--baseline-2pct-exp",
        help="Optional 2%% baseline exp_id used in compare hints",
    )
    return parser


def _resolve_steps(args_step: list[str] | None, args_plan: list[str] | None) -> list[str]:
    steps: list[str] = []
    if args_step:
        steps.extend(args_step)
    if args_plan:
        for plan in args_plan:
            steps.extend(PLAN_ALIASES[plan])
    if not steps:
        return ["baseline"]
    return steps


def _apply_step_params(common_2pct: dict[str, object], step: str, args) -> dict[str, object]:
    step = step.lower()
    preset = STEP_PRESETS.get(step)
    if preset is None:
        raise ValueError(f"Unknown step {step!r}")

    overrides = dict(preset)
    overrides.pop("description", None)
    overrides.update(BASE_FILTERS)
    overrides["breakdown_rs_min"] = args.workflow_breakdown_rs_min
    if step == "breadth":
        overrides["breakdown_breadth_threshold"] = args.workflow_breadth_threshold

    next_cfg = dict(common_2pct)
    next_cfg.update(overrides)
    return next_cfg


def main() -> None:
    module = _load_runner_module()
    parser = build_parser()
    args = parser.parse_args()
    args.workflow_breakdown_rs_min = (
        args.breakdown_rs_min if "--breakdown-rs-min" in sys.argv[1:] else DEFAULT_WORKFLOW_RS_MIN
    )
    args.workflow_breadth_threshold = (
        args.breakdown_breadth_threshold
        if "--breakdown-breadth-threshold" in sys.argv[1:]
        else DEFAULT_WORKFLOW_BREADTH_THRESHOLD
    )

    steps = _resolve_steps(args.step, args.plan)

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
        "breakdown_daily_candidate_budget": 5,
        "short_initial_stop_atr_cap_mult": args.short_initial_stop_atr_cap_mult,
        "short_same_day_r_ladder_start_r": args.short_same_day_r_ladder_start_r,
        "short_same_day_take_profit_pct": args.short_same_day_take_profit_pct,
    }

    common_2pct = dict(common)

    if args.short_profile not in module.SHORT_PROFILES:
        raise SystemExit(f"Unknown short profile: {args.short_profile}")

    runner = module.DuckDBBacktestRunner()

    for step in steps:
        common_2pct = _apply_step_params(common_2pct, step, args)

        # Keep 4%% leg stable: Option-B profile + no phase-1 filter overrides.
        common_4pct = dict(common)

        print(f"\n===== Running breakdown step: {step} ({STEP_PRESETS[step]['description']}) =====")
        exp_4 = module._run_variant(
            runner,
            label=f"4% breakdown ({step})",
            breakout_threshold=0.04,
            force=args.force,
            snapshot=args.snapshot,
            common=common_4pct,
            short_profile=module.SHORT_PROFILES["option-b"],
        )

        short_profile = module.SHORT_PROFILES[args.short_profile]
        exp_2 = module._run_variant(
            runner,
            label=f"2% breakdown ({step}/{args.short_profile})",
            breakout_threshold=0.02,
            force=args.force,
            snapshot=args.snapshot,
            common=common_2pct,
            short_profile=short_profile,
        )

        print(f"step={step} 4%={exp_4} 2%={exp_2}")

        if args.compare_baseline:
            print("\nSuggested compare command(s):")
            if args.baseline_4pct_exp:
                print(
                    "  "
                    "doppler run -- uv run python scripts/compare_backtest_runs.py "
                    f"--baseline {args.baseline_4pct_exp} --exps {step}-4:{exp_4}"
                )
            if args.baseline_2pct_exp:
                print(
                    "  "
                    "doppler run -- uv run python scripts/compare_backtest_runs.py "
                    f"--baseline {args.baseline_2pct_exp} --exps {step}-2:{exp_2}"
                )
            if not (args.baseline_4pct_exp or args.baseline_2pct_exp):
                print(
                    "  no baseline exp ids provided. "
                    "Pass --baseline-4pct-exp / --baseline-2pct-exp if you want ready-to-run compare stubs."
                )


if __name__ == "__main__":
    main()
