#!/usr/bin/env python3
"""Show detailed scan results with which criteria passed/failed"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import desc, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import RefSymbol, ScanResult, ScanRun


async def show_detailed_results():
    sm = get_sessionmaker()
    async with sm() as session:
        # Get most recent scan run
        run = await session.execute(select(ScanRun).order_by(desc(ScanRun.started_at)).limit(1))
        run = run.scalar_one()

        if not run:
            print("No scan runs found!")
            return

        print(f"\n{'=' * 70}")
        print(f"SCAN RESULTS FOR: {run.asof_date}")
        print(f"{'=' * 70}\n")

        # Get results with full details
        results = await session.execute(
            select(ScanResult, RefSymbol)
            .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
            .where(ScanResult.scan_run_id == run.scan_run_id)
            .order_by(desc(ScanResult.score))
        )

        print("SYMBOL        SCORE   STATUS    CHECKS PASSED (7 total)")
        print("-" * 70)

        for sr, rs in results.all():
            # Parse reason_json to get individual check results

            checks_data = sr.reason_json if isinstance(sr.reason_json, dict) else {}
            checks_list = checks_data.get("checks", [])

            # Format checks
            check_results = []
            for check in checks_list:
                letter = check.get("letter", "?")
                passed = check.get("passed", False)
                reason = check.get("reason", "")
                check_results.append(f"{letter}:{'+' if passed else '-'}")

            checks_str = ", ".join(check_results) if check_results else "N/A"
            status = "PASS" if sr.passed else "FAIL"

            print(
                f"{rs.symbol:12s}  {sr.score if sr.score else 0:.2f}   {status:<6}  [{checks_str}]"
            )

            print("\n  Detailed Checks:")
            for check in checks_list:
                letter = check.get("letter", "?")
                passed = check.get("passed", False)
                reason = check.get("reason", "")
                status_icon = "+" if passed else "-"
                print(f"    [{status_icon}] {letter}: {reason}")

            print()


asyncio.run(show_detailed_results(), loop_factory=asyncio.SelectorEventLoop)


if __name__ == "__main__":
    asyncio.run(show_detailed_results(), loop_factory=asyncio.SelectorEventLoop)
