"""Year-by-year batch runner with checkpoint/resume for long backtest programs."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)
from nse_momentum_lab.utils import compute_short_hash


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run backtests year-by-year with checkpoint/resume support"
    )
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument(
        "--start-date", type=str, default=None, help="Optional YYYY-MM-DD lower bound"
    )
    parser.add_argument(
        "--end-date", type=str, default=None, help="Optional YYYY-MM-DD upper bound"
    )

    parser.add_argument("--universe-size", type=int, default=500)
    parser.add_argument("--min-price", type=int, default=10)
    parser.add_argument("--min-filters", type=int, default=5)
    parser.add_argument("--entry-timeframe", type=str, default="5min", choices=["5min", "daily"])
    parser.add_argument("--trail-activation", type=float, default=0.08)
    parser.add_argument("--trail-stop", type=float, default=0.02)
    parser.add_argument("--min-hold-days", type=int, default=3)
    parser.add_argument("--time-stop-days", type=int, default=5)
    parser.add_argument("--abnormal-profit-pct", type=float, default=0.10)
    parser.add_argument("--abnormal-gap-exit-pct", type=float, default=0.20)

    parser.add_argument("--snapshot", action="store_true", help="Publish DuckDB snapshot per-year")
    parser.add_argument("--force", action="store_true", help="Force re-run each year")
    parser.add_argument(
        "--continue-on-error", action="store_true", help="Continue when one year fails"
    )
    parser.add_argument(
        "--checkpoint-file", type=str, default=None, help="Custom checkpoint JSON path"
    )
    parser.add_argument(
        "--progress-dir",
        type=str,
        default="data/progress",
        help="Directory for per-year progress NDJSON files",
    )
    return parser


def _parse_optional_iso_date(value: str | None, field_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def _compute_batch_id(payload: dict[str, Any]) -> str:
    return compute_short_hash(payload, length=16)


def _write_checkpoint(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as tmp:
        json.dump(state, tmp, sort_keys=True, indent=2)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _load_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _year_window(
    year: int, global_start: date | None, global_end: date | None
) -> tuple[date, date] | None:
    start_d = date(year, 1, 1)
    end_d = date(year, 12, 31)
    if global_start and global_start > start_d:
        start_d = global_start
    if global_end and global_end < end_d:
        end_d = global_end
    if start_d > end_d:
        return None
    return start_d, end_d


def main() -> None:
    args = build_parser().parse_args()
    global_start = _parse_optional_iso_date(args.start_date, "start_date")
    global_end = _parse_optional_iso_date(args.end_date, "end_date")
    if global_start and global_end and global_start > global_end:
        raise SystemExit("start_date must be <= end_date")
    if args.start_year > args.end_year:
        raise SystemExit("start_year must be <= end_year")

    batch_fingerprint = {
        "start_year": args.start_year,
        "end_year": args.end_year,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "universe_size": args.universe_size,
        "min_price": args.min_price,
        "min_filters": args.min_filters,
        "entry_timeframe": args.entry_timeframe,
        "trail_activation": args.trail_activation,
        "trail_stop": args.trail_stop,
        "min_hold_days": args.min_hold_days,
        "time_stop_days": args.time_stop_days,
        "abnormal_profit_pct": args.abnormal_profit_pct,
        "abnormal_gap_exit_pct": args.abnormal_gap_exit_pct,
        "snapshot": bool(args.snapshot),
    }
    batch_id = _compute_batch_id(batch_fingerprint)
    checkpoint_file = (
        Path(args.checkpoint_file).expanduser()
        if args.checkpoint_file
        else Path(args.progress_dir) / f"backtest_batch_{batch_id}.json"
    )
    progress_dir = Path(args.progress_dir)
    progress_dir.mkdir(parents=True, exist_ok=True)

    state = _load_checkpoint(checkpoint_file) or {
        "batch_id": batch_id,
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
        "fingerprint": batch_fingerprint,
        "completed_years": {},
        "failed_years": {},
    }
    if str(state.get("batch_id")) != batch_id:
        raise SystemExit(
            f"Checkpoint mismatch for {checkpoint_file}. Use --checkpoint-file for explicit override."
        )

    runner = DuckDBBacktestRunner()
    years = list(range(args.start_year, args.end_year + 1))
    completed = state.get("completed_years", {})
    failed = state.get("failed_years", {})

    print(f"[BATCH] id={batch_id} years={args.start_year}-{args.end_year}")
    print(f"[BATCH] checkpoint={checkpoint_file}")
    print(f"[BATCH] already completed={len(completed)} failed={len(failed)}")

    for year in years:
        key = str(year)
        if not args.force and key in completed:
            print(f"[BATCH] skip year {year} (already complete: exp_id={completed[key]})")
            continue

        window = _year_window(year, global_start, global_end)
        if window is None:
            print(f"[BATCH] skip year {year} (outside date window)")
            continue
        year_start, year_end = window

        year_params = BacktestParams(
            universe_size=args.universe_size,
            min_price=args.min_price,
            min_filters=args.min_filters,
            start_year=year,
            end_year=year,
            start_date=year_start.isoformat(),
            end_date=year_end.isoformat(),
            entry_timeframe=args.entry_timeframe,
            trail_activation_pct=args.trail_activation,
            trail_stop_pct=args.trail_stop,
            min_hold_days=args.min_hold_days,
            time_stop_days=args.time_stop_days,
            abnormal_profit_pct=args.abnormal_profit_pct,
            abnormal_gap_exit_pct=args.abnormal_gap_exit_pct,
        )
        progress_file = progress_dir / f"backtest_batch_{batch_id}_{year}.ndjson"

        print(f"[BATCH] year {year} start ({year_start} -> {year_end})")
        try:
            exp_id = runner.run(
                year_params,
                force=args.force,
                snapshot=args.snapshot,
                progress_file=progress_file,
            )
        except Exception as exc:
            failed[key] = {"error": str(exc), "failed_at": datetime.now(UTC).isoformat()}
            state["failed_years"] = failed
            state["updated_at"] = datetime.now(UTC).isoformat()
            _write_checkpoint(checkpoint_file, state)
            print(f"[BATCH] year {year} failed: {exc}")
            if args.continue_on_error:
                continue
            raise

        completed[key] = exp_id
        failed.pop(key, None)
        state["completed_years"] = completed
        state["failed_years"] = failed
        state["updated_at"] = datetime.now(UTC).isoformat()
        _write_checkpoint(checkpoint_file, state)
        print(f"[BATCH] year {year} complete exp_id={exp_id}")

    print(
        f"[BATCH] done completed={len(state['completed_years'])} "
        f"failed={len(state['failed_years'])}"
    )


if __name__ == "__main__":
    main()
