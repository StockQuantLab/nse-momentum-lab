from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path

import polars as pl

from nse_momentum_lab.services.kite.parquet_repair import (
    repair_legacy_utc_naive_5min_parquet,
    scan_5min_timestamp_alignment,
)


def _write_5min_parquet(path: Path, first_candle_time: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "symbol": ["RELIANCE", "RELIANCE"],
            "date": [date(2025, 1, 1), date(2025, 1, 1)],
            "candle_time": [
                first_candle_time,
                first_candle_time.replace(
                    hour=first_candle_time.hour, minute=first_candle_time.minute + 5
                ),
            ],
            "open": [1.0, 2.0],
            "high": [1.5, 2.5],
            "low": [0.5, 1.5],
            "close": [1.2, 2.2],
            "volume": [10, 11],
        }
    )
    frame.write_parquet(path, compression="zstd")


def test_scan_5min_timestamp_alignment_detects_legacy_utc_naive_files() -> None:
    base_dir = Path.home() / ".codex" / "memories" / "pytest-temp"
    base_dir.mkdir(parents=True, exist_ok=True)
    run_dir = base_dir / f"scan-{uuid.uuid4().hex}"
    run_dir.mkdir(parents=True, exist_ok=False)
    parquet_dir = run_dir / "5min"
    _write_5min_parquet(parquet_dir / "RELIANCE" / "2025.parquet", datetime(2025, 1, 1, 3, 45))
    _write_5min_parquet(parquet_dir / "20MICRONS" / "2025.parquet", datetime(2025, 1, 1, 9, 15))
    _write_5min_parquet(parquet_dir / "3PLAND" / "2025.parquet", datetime(2025, 1, 1, 9, 20))

    issues = scan_5min_timestamp_alignment(parquet_dir)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.symbol == "RELIANCE"
    assert issue.year == 2025
    assert issue.first_candle_time == "03:45:00"
    assert issue.status == "legacy_utc_naive"


def test_scan_5min_timestamp_alignment_detects_previous_day_files() -> None:
    base_dir = Path.home() / ".codex" / "memories" / "pytest-temp"
    base_dir.mkdir(parents=True, exist_ok=True)
    run_dir = base_dir / f"scan-prev-day-{uuid.uuid4().hex}"
    run_dir.mkdir(parents=True, exist_ok=False)
    parquet_dir = run_dir / "5min"
    _write_5min_parquet(parquet_dir / "RELIANCE" / "2025.parquet", datetime(2024, 12, 31, 22, 15))

    issues = scan_5min_timestamp_alignment(parquet_dir)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.symbol == "RELIANCE"
    assert issue.year == 2025
    assert issue.first_candle_time == "22:15:00"
    assert issue.status == "year_mismatch"


def test_repair_legacy_utc_naive_5min_parquet_rewrites_only_bad_files() -> None:
    base_dir = Path.home() / ".codex" / "memories" / "pytest-temp"
    base_dir.mkdir(parents=True, exist_ok=True)
    run_dir = base_dir / f"repair-{uuid.uuid4().hex}"
    run_dir.mkdir(parents=True, exist_ok=False)
    parquet_dir = run_dir / "5min"
    bad_path = parquet_dir / "RELIANCE" / "2025.parquet"
    good_path = parquet_dir / "20MICRONS" / "2025.parquet"
    _write_5min_parquet(bad_path, datetime(2025, 1, 1, 3, 45))
    _write_5min_parquet(good_path, datetime(2025, 1, 1, 9, 15))

    stats = repair_legacy_utc_naive_5min_parquet(parquet_dir, apply=True)

    assert stats.scanned_files == 2
    assert stats.flagged_files == 1
    assert stats.repaired_files == 1
    assert stats.unexpected_files == 0

    repaired = pl.read_parquet(bad_path)
    assert repaired["candle_time"][0].isoformat() == "2025-01-01T09:15:00"
    assert repaired["date"][0].isoformat() == "2025-01-01"

    untouched = pl.read_parquet(good_path)
    assert untouched["candle_time"][0].isoformat() == "2025-01-01T09:15:00"
    assert untouched["date"][0].isoformat() == "2025-01-01"
