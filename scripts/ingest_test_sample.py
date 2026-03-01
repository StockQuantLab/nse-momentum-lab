#!/usr/bin/env python3
"""
Ingest Zerodha test sample with Windows-compatible event loop.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Set Windows event loop policy
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from scripts.ingest_vendor_candles import main

if __name__ == "__main__":
    # Set command line args
    sys.argv = [
        "ingest_vendor_candles.py",
        "data/vendor/zerodda/test_sample",
        "--timeframe",
        "day",
        "--vendor",
        "zerodha",
    ]

    main()
