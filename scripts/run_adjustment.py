#!/usr/bin/env python3
"""Run adjustment worker."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.services.adjust.worker import AdjustmentWorker


async def run():
    worker = AdjustmentWorker()
    results = await worker.run_all()
    print(f"Adjusted {len(results)} symbols")
    await worker.close()


if __name__ == "__main__":
    asyncio.run(run(), loop_factory=asyncio.SelectorEventLoop)
