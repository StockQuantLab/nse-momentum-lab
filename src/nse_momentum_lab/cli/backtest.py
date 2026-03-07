"""CLI entry point for the DuckDB-backed backtest runner."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)
from nse_momentum_lab.services.backtest.strategy_registry import list_strategies

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DuckDB-backed backtest")
    parser.add_argument(
        "--strategy",
        type=str,
        default="indian_2lynch",
        help="Strategy name resolved from registry",
    )
    parser.add_argument(
        "--list-strategies",
        action="store_true",
        help="List available strategies and exit",
    )
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
        "--breakout-threshold",
        type=float,
        default=0.04,
        help="Breakout threshold (e.g., 0.04 for 4%%). Used by threshold_breakout/breakdown.",
    )
    parser.add_argument(
        "--min-value-traded",
        type=int,
        default=3_000_000,
        help="Minimum 20-day average value traded in INR",
    )
    parser.add_argument(
        "--min-volume",
        type=int,
        default=50_000,
        help="Minimum daily volume",
    )
    parser.add_argument(
        "--progress-file",
        type=str,
        default=None,
        help="Optional NDJSON file to append run progress heartbeats",
    )
    return parser


def _print_summary(exp_id: str) -> None:
    """Query DuckDB and print a compact backtest summary to stdout."""
    db = get_market_db()
    row = db.con.execute(
        """SELECT strategy_name, start_year, end_year, total_trades, win_rate_pct,
                  annualized_return_pct, max_drawdown_pct, total_return_pct, profit_factor
           FROM bt_experiment WHERE exp_id = ?""",
        [exp_id],
    ).fetchone()
    if not row:
        print("  (no summary available — experiment not persisted)")
        return

    strategy, start_yr, end_yr, trades, win_pct, ann_ret, max_dd, tot_ret, pf = row

    print()
    print(f"  Strategy   : {strategy}  ({start_yr}-{end_yr})")
    print(f"  Trades     : {trades:,}")
    print(f"  Win Rate   : {win_pct:.1f}%")
    print(f"  Ann Return : {ann_ret:.1f}%")
    print(f"  Max DD     : {max_dd:.2f}%")
    print(f"  Calmar     : {ann_ret / max_dd:.2f}" if max_dd else "  Calmar     : N/A")
    print(f"  Total Ret  : {tot_ret:.1f}%")
    print(f"  Prof Factor: {pf:.2f}")

    # Yearly breakdown
    yearly = db.con.execute(
        "SELECT year, return_pct, trades, win_rate_pct, max_dd_pct "
        "FROM bt_yearly_metric WHERE exp_id = ? ORDER BY year",
        [exp_id],
    ).fetchall()
    if yearly:
        print()
        print(f"  {'Year':>4}  {'Return':>8}  {'Trades':>6}  {'Win%':>6}  {'MaxDD':>6}")
        print(f"  {'-' * 4}  {'-' * 8}  {'-' * 6}  {'-' * 6}  {'-' * 6}")
        for yr, ret, tr, wr, dd in yearly:
            print(f"  {yr:>4}  {ret:>7.1f}%  {tr:>6,}  {wr:>5.1f}%  {dd:>5.2f}%")
    print()


def main() -> None:
    args = build_parser().parse_args()

    if args.list_strategies:
        for strategy in list_strategies():
            print(f"{strategy.name} ({strategy.version}) - {strategy.description}")
        return

    params = BacktestParams(
        strategy=args.strategy,
        universe_size=args.universe_size,
        min_price=args.min_price,
        min_filters=args.min_filters,
        breakout_threshold=args.breakout_threshold,
        min_value_traded_inr=args.min_value_traded,
        min_volume=args.min_volume,
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
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(f"[BACKTEST FAILED] {exc}") from exc

    print(f"\nExperiment ID: {exp_id}")
    _print_summary(exp_id)


if __name__ == "__main__":
    main()
