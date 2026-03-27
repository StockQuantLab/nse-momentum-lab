"""nse-momentum-lab.

Deterministic research + paper trading system for NSE momentum bursts.
"""

import os

# Legacy parquet in this repo uses fixed-offset timezones like ``+05:30`` for
# 5-minute candle timestamps. Polars rejects those offsets by default, so enable
# its documented compatibility fallback before any submodule imports Polars.
os.environ.setdefault("POLARS_IGNORE_TIMEZONE_PARSE_ERROR", "1")

__all__ = ["__version__"]

__version__ = "0.1.0"
