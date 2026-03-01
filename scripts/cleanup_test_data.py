#!/usr/bin/env python3
"""Clean up test data from database."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import delete

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import BtTrade, ExpMetric, ExpRun, ScanResult, ScanRun


async def cleanup():
    """Clean up test data."""
    sm = get_sessionmaker()
    async with sm() as session:
        # Delete in proper order due to foreign keys
        await session.execute(delete(BtTrade))
        await session.execute(delete(ExpMetric))
        await session.execute(delete(ExpRun))
        await session.execute(delete(ScanResult))
        await session.execute(delete(ScanRun))
        await session.commit()

        print("Cleaned up test data successfully!")


if __name__ == "__main__":
    # Windows async fix
    asyncio.run(cleanup(), loop_factory=asyncio.SelectorEventLoop)
