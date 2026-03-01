"""CLI entry point for the DuckDB-backed Indian 2LYNCH backtest."""

from __future__ import annotations

import argparse
from pathlib import Path

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Indian 2LYNCH backtest")
    parser.add_argument("--force", action="store_true", help="Re-run even if cached")
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Publish DuckDB snapshot artifact to MinIO after run",
    )
    parser.add_argument("--universe-size", type=int, default=500)
    parser.add_argument("--min-price", type=int, default=10)
    parser.add_argument("--min-filters", type=int, default=5)
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument(
        "--start-date", type=str, default=None, help="Optional YYYY-MM-DD lower bound"
    )
    parser.add_argument(
        "--end-date", type=str, default=None, help="Optional YYYY-MM-DD upper bound"
    )
    parser.add_argument("--entry-timeframe", type=str, default="5min", choices=["5min", "daily"])
    parser.add_argument("--trail-activation", type=float, default=0.08)
    parser.add_argument("--trail-stop", type=float, default=0.02)
    parser.add_argument("--min-hold-days", type=int, default=3)
    parser.add_argument("--time-stop-days", type=int, default=5)
    parser.add_argument("--abnormal-profit-pct", type=float, default=0.10)
    parser.add_argument("--abnormal-gap-exit-pct", type=float, default=0.20)
    parser.add_argument(
        "--progress-file",
        type=str,
        default=None,
        help="Optional NDJSON file to append run progress heartbeats",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    params = BacktestParams(
        universe_size=args.universe_size,
        min_price=args.min_price,
        min_filters=args.min_filters,
        start_year=args.start_year,
        end_year=args.end_year,
        start_date=args.start_date,
        end_date=args.end_date,
        entry_timeframe=args.entry_timeframe,
        trail_activation_pct=args.trail_activation,
        trail_stop_pct=args.trail_stop,
        min_hold_days=args.min_hold_days,
        time_stop_days=args.time_stop_days,
        abnormal_profit_pct=args.abnormal_profit_pct,
        abnormal_gap_exit_pct=args.abnormal_gap_exit_pct,
    )

    runner = DuckDBBacktestRunner()
    progress_file = Path(args.progress_file).expanduser() if args.progress_file else None
    try:
        exp_id = runner.run(
            params,
            force=args.force,
            snapshot=args.snapshot,
            progress_file=progress_file,
        )
    except RuntimeError as exc:
        raise SystemExit(f"[BACKTEST FAILED] {exc}") from exc
    print(f"\nExperiment ID: {exp_id}")


if __name__ == "__main__":
    main()
