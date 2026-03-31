#!/usr/bin/env python3
"""Repair legacy 5-minute parquet files whose timestamps still start at 03:45.

The canonical parquet representation in this repo is IST wall-clock time with a
naive ``timestamp`` column.  Older files were written as UTC-naive values, which
shifts the open from 09:15 IST to 03:45.  This script scans the year-partitioned
``data/parquet/5min/<SYMBOL>/<YEAR>.parquet`` tree and rewrites only the files
that still need the IST shift.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from nse_momentum_lab.services.kite.parquet_repair import (
    repair_legacy_utc_naive_5min_parquet,
    scan_5min_timestamp_alignment,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair legacy 5-minute parquet timestamps")
    parser.add_argument(
        "--parquet-dir",
        default="data/parquet/5min",
        help="Year-partitioned 5-minute parquet root",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rewrite the affected parquet files in place",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only scan the first N parquet files (useful for spot checks)",
    )
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir)
    if not args.apply:
        issues = scan_5min_timestamp_alignment(parquet_dir, limit=args.limit)
        print(f"Scanning: {parquet_dir}")
        print(f"Issues found: {len(issues):,}")
        legacy = sum(1 for issue in issues if issue.status == "legacy_utc_naive")
        unexpected = len(issues) - legacy
        print(f"  Legacy UTC-naive files: {legacy:,}")
        print(f"  Unexpected open times: {unexpected:,}")
        for issue in issues[:10]:
            print(
                f"  - {issue.symbol} {issue.year}: {issue.first_candle_time} "
                f"({issue.status}) {issue.path}"
            )
        if len(issues) > 10:
            print(f"  ... {len(issues) - 10:,} more")
    else:
        print(f"Repairing: {parquet_dir}")

    stats = repair_legacy_utc_naive_5min_parquet(parquet_dir, apply=args.apply, limit=args.limit)
    print("Repair stats")
    print(f"  Scanned files: {stats.scanned_files:,}")
    print(f"  Flagged files: {stats.flagged_files:,}")
    print(f"  Repaired files: {stats.repaired_files:,}")
    print(f"  Skipped files: {stats.skipped_files:,}")
    print(f"  Unexpected files: {stats.unexpected_files:,}")
    print(f"  Rows rewritten: {stats.rows_rewritten:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
