#!/usr/bin/env python3
"""Show recent scan results"""

import asyncio
import io
import sys
from pathlib import Path

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import desc, func, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import RefSymbol, ScanResult, ScanRun


async def show_results():
    sm = get_sessionmaker()
    async with sm() as session:
        # Count total scan runs
        total_runs = await session.execute(select(func.count(ScanRun.scan_run_id)))
        print(f"\n{'=' * 70}")
        print(f"TOTAL SCAN RUNS IN DATABASE: {total_runs.scalar_one()}")
        print(f"{'=' * 70}\n")

        # Get recent scan runs (last 5)
        runs = await session.execute(select(ScanRun).order_by(desc(ScanRun.started_at)).limit(5))

        print("RECENT SCAN RUNS (Last 5):")
        print("-" * 70)

        for run in runs.scalars():
            print(f"\nRun ID: {run.scan_run_id}")
            print(f"Date: {run.asof_date}")
            print(f"Status: {run.status}")
            print(f"Started: {run.started_at}")
            print(f"Dataset hash: {run.dataset_hash}")

            # Get results for this run
            results = await session.execute(
                select(ScanResult, RefSymbol)
                .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
                .where(ScanResult.scan_run_id == run.scan_run_id)
                .order_by(desc(ScanResult.score))
            )

            scan_results = results.all()
            print(f"Results: {len(scan_results)}")

            # Show all results
            passed = 0
            for sr, rs in scan_results:
                status = "PASS" if sr.passed else "FAIL"
                score = f"{sr.score:.2f}" if sr.score else "N/A"
                print(f"  [{status}] {rs.symbol:15s} Score: {score:>6s}")
                if sr.passed:
                    passed += 1

            print(f"Passed: {passed}/{len(scan_results)}")

        print("\n" + "=" * 70)
        print("SUMMARY ACROSS ALL SCANS:")
        print("=" * 70)

        # Get totals
        total_results = await session.execute(select(func.count(ScanResult.scan_run_id)))
        total_passed = await session.execute(
            select(func.count(ScanResult.scan_run_id)).where(ScanResult.passed)
        )

        print(f"Total scan results: {total_results.scalar_one()}")
        print(f"Total passed: {total_passed.scalar_one()}")
        print(
            f"Pass rate: {total_passed.scalar_one() / total_results.scalar_one() * 100:.2f}%"
            if total_results.scalar_one() > 0
            else "Pass rate: 0%"
        )


if __name__ == "__main__":
    asyncio.run(show_results(), loop_factory=asyncio.SelectorEventLoop)
