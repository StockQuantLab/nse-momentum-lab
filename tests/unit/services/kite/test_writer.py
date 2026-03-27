from __future__ import annotations

import os

os.environ.setdefault("POLARS_IGNORE_TIMEZONE_PARSE_ERROR", "1")

import pandas as pd
import polars as pl

from nse_momentum_lab.services.kite.writer import KiteWriter


def test_normalize_5min_frame_uses_naive_ist_timestamp() -> None:
    writer = KiteWriter()
    sample = pd.DataFrame(
        [
            {
                "symbol": "RELIANCE",
                "date": pd.Timestamp("2026-03-23").date(),
                "candle_time": pd.Timestamp("2026-03-23 09:15:00", tz="Asia/Kolkata"),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
            }
        ]
    )

    frame = writer._normalize_5min_frame(sample)

    assert frame.schema["candle_time"] == pl.Datetime("ns")
    assert frame["candle_time"][0].isoformat() == "2026-03-23T09:15:00"


def test_normalize_ist_candle_time_converts_legacy_offset_to_naive_ist() -> None:
    writer = KiteWriter()
    frame = pl.DataFrame(
        {
            "symbol": ["RELIANCE"],
            "date": [pd.Timestamp("2026-03-23").date()],
            "candle_time": [pd.Timestamp("2026-03-23 09:15:00+05:30")],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
        }
    )

    normalized = writer._normalize_ist_candle_time(frame)

    assert normalized.schema["candle_time"] == pl.Datetime("ns")
    assert normalized["candle_time"][0].isoformat() == "2026-03-23T09:15:00"
