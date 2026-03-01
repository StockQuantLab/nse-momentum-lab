#!/usr/bin/env python3
"""Quick scan results viewer - no emoji issues"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

from sqlalchemy import desc, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import RefSymbol, ScanResult, ScanRun


async def main():
    sm = get_sessionmaker()
    async with sm() as session:
        print("\n" + "=" * 70)
        print("SCAN RESULTS IN DATABASE")
        print("=" * 70 + "\n")

        # Get recent scan runs
        runs = await session.execute(select(ScanRun).order_by(desc(ScanRun.started_at)).limit(3))

        print("RECENT SCAN RUNS (Last 3):\n")

        for run in runs.scalars():
            print(f"Date: {run.asof_date}")
            print(f"Status: {run.status}")
            print(f"Hash: {run.dataset_hash}")

            # Get results
            results = await session.execute(
                select(ScanResult, RefSymbol)
                .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
                .where(ScanResult.scan_run_id == run.scan_run_id)
                .order_by(desc(ScanResult.score))
            )

            scan_results = results.all()

            print(f"\nResults ({len(scan_results)} stocks):")
            for sr, rs in scan_results:
                passed = "PASS" if sr.passed else "FAIL"
                score = f"{sr.score:.2f}" if sr.score else "N/A"
                print(f"  [{passed}] {rs.symbol:15s} Score: {score}")

            print("\n" + "-" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
