#!/usr/bin/env python3
"""
Repair NSE 5-minute parquet files using CPR as the source of truth.

This targets only files that still fail the timestamp alignment check in the
NSE lake. CPR is treated as the repair source because it preserves the original
IST wall-clock timestamps for the legacy files we need to fix.

Usage:
    uv run python scripts/repair_5min_parquet_from_cpr.py [--dry-run] [--symbols SYM1,SYM2] [--limit N]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import polars as pl

from nse_momentum_lab.services.kite.parquet_repair import (
    IST_OPEN_TIME,
    _first_candle_timestamp,
    scan_5min_timestamp_alignment,
)

NSE_BASE = Path(r"C:\Users\kanna\github\nse-momentum-lab\data\parquet")
CPR_BASE = Path(r"C:\Users\kanna\github\cpr-pivot-lab\data\parquet")

NSE_5MIN = NSE_BASE / "5min"
CPR_5MIN = CPR_BASE / "5min"
NSE_COLS = ["symbol", "date", "candle_time", "open", "high", "low", "close", "volume"]


def _parse_symbols(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    symbols = {part.strip().upper() for part in raw.split(",") if part.strip()}
    return symbols or None


def _validate_cpr_file(path: Path, *, expected_year: int) -> tuple[bool, str]:
    first = _first_candle_timestamp(path)
    if first is None:
        return False, "CPR file empty or unreadable"
    first_time = first.time() if hasattr(first, "time") else first
    first_year = first.date().year if hasattr(first, "date") else None
    if first_year is not None and first_year != expected_year:
        return False, f"CPR year mismatch ({first_year} != {expected_year})"
    if first_time is None or first_time < IST_OPEN_TIME:
        return False, f"CPR starts before 09:15 IST ({first_time})"
    return True, "ok"


def _repair_one(nse_path: Path, cpr_path: Path, dry_run: bool) -> str:
    df = pl.read_parquet(cpr_path)
    if df.is_empty() or "candle_time" not in df.columns:
        return "SKIP — CPR file empty or missing candle_time"

    df = df.drop("true_range") if "true_range" in df.columns else df
    df = df.with_columns(pl.col("candle_time").cast(pl.Datetime("ns")))
    if "date" in df.columns:
        df = df.drop("date")
    df = df.with_columns(pl.col("candle_time").dt.date().alias("date"))
    df = df.select(NSE_COLS).sort("candle_time")

    if dry_run:
        first = df["candle_time"][0]
        return f"would repair {len(df):,} rows (first={first})"

    nse_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(nse_path) + ".tmp"
    df.write_parquet(tmp, compression="snappy")
    os.replace(tmp, str(nse_path))
    return f"{len(df):,} rows -> OK"


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair NSE 5-min parquet from CPR copies")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without writing")
    parser.add_argument("--symbols", help="Comma-separated symbols to repair")
    parser.add_argument("--limit", type=int, help="Limit number of files processed")
    args = parser.parse_args()

    symbols_filter = _parse_symbols(args.symbols)
    issues = scan_5min_timestamp_alignment(NSE_5MIN, limit=args.limit)
    if symbols_filter is not None:
        issues = [issue for issue in issues if issue.symbol in symbols_filter]

    if not issues:
        print("No misaligned NSE 5-min parquet files found.")
        return 0

    print(
        f"{'DRY RUN — ' if args.dry_run else ''}Repairing {len(issues)} NSE 5-min parquet files from CPR"
    )
    print(f"NSE source : {NSE_5MIN}")
    print(f"CPR source : {CPR_5MIN}")
    print()

    repaired = skipped = failed = 0
    for idx, issue in enumerate(issues, start=1):
        nse_path = Path(issue.path)
        cpr_path = CPR_5MIN / issue.symbol / f"{issue.year}.parquet"
        if not cpr_path.exists():
            print(f"  [{idx}/{len(issues)}] {issue.symbol} {issue.year}: SKIP — CPR file missing")
            skipped += 1
            continue

        ok, reason = _validate_cpr_file(cpr_path, expected_year=issue.year)
        if not ok:
            print(f"  [{idx}/{len(issues)}] {issue.symbol} {issue.year}: SKIP — {reason}")
            skipped += 1
            continue

        try:
            result = _repair_one(nse_path, cpr_path, args.dry_run)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"  [{idx}/{len(issues)}] {issue.symbol} {issue.year}: ERROR — {exc}")
            failed += 1
            continue

        print(f"  [{idx}/{len(issues)}] {issue.symbol} {issue.year}: {result}")
        if result.startswith("would repair") or result.endswith("-> OK"):
            repaired += 1
        else:
            skipped += 1

    print(f"\nDone: {repaired} repaired, {skipped} skipped, {failed} errors")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
