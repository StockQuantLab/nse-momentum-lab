from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

IST_OPEN_TIME = time(9, 15)
LEGACY_UTC_OPEN_TIME = time(3, 45)
IST_SHIFT = timedelta(hours=5, minutes=30)


@dataclass(slots=True)
class TimestampAlignmentIssue:
    symbol: str
    year: int
    path: str
    first_candle_time: str
    status: str

    def as_dict(self) -> dict[str, str | int]:
        return {
            "symbol": self.symbol,
            "year": self.year,
            "path": self.path,
            "first_candle_time": self.first_candle_time,
            "status": self.status,
        }


@dataclass(slots=True)
class TimestampRepairStats:
    scanned_files: int = 0
    flagged_files: int = 0
    repaired_files: int = 0
    skipped_files: int = 0
    unexpected_files: int = 0
    rows_rewritten: int = 0


def iter_5min_parquet_files(parquet_dir: Path) -> list[Path]:
    """Return the year-partitioned 5-minute parquet files under ``parquet_dir``."""
    if not parquet_dir.exists():
        return []
    return sorted(path for path in parquet_dir.glob("*/*.parquet") if path.is_file())


def _first_candle_timestamp(path: Path) -> datetime | time | None:
    parquet_file = pq.ParquetFile(path)
    if parquet_file.metadata is not None and parquet_file.metadata.num_rows == 0:
        return None
    try:
        table = parquet_file.read_row_group(0, columns=["candle_time"])
    except IndexError:
        return None
    if table.num_rows == 0:
        return None
    value = table.column(0)[0].as_py()
    if value is None:
        return None
    if isinstance(value, (datetime, time)):
        return value
    if hasattr(value, "date") and hasattr(value, "time"):
        return value
    return None


def _first_candle_time(path: Path) -> time | None:
    value = _first_candle_timestamp(path)
    if isinstance(value, datetime):
        return value.time()
    if isinstance(value, time):
        return value
    return None


def _format_time(value: time | None) -> str:
    return value.strftime("%H:%M:%S") if value is not None else "unknown"


def scan_5min_timestamp_alignment(
    parquet_dir: Path,
    *,
    limit: int | None = None,
) -> list[TimestampAlignmentIssue]:
    """Return 5-minute parquet files whose first candle does not align to 09:15 IST."""
    issues: list[TimestampAlignmentIssue] = []
    for idx, path in enumerate(iter_5min_parquet_files(parquet_dir), start=1):
        if limit is not None and idx > limit:
            break
        first_value = _first_candle_timestamp(path)
        if first_value is None:
            continue
        symbol = path.parent.name.strip().upper()
        try:
            year = int(path.stem)
        except ValueError:
            year = 0
        if isinstance(first_value, datetime):
            first_time = first_value.time()
            first_year = first_value.date().year
        elif isinstance(first_value, time):
            first_time = first_value
            first_year = None
        else:
            first_time = None
            first_year = None

        if first_time is not None and first_time >= IST_OPEN_TIME and first_year in (None, year):
            continue
        if first_time == LEGACY_UTC_OPEN_TIME:
            status = "legacy_utc_naive"
        elif first_year is not None and first_year != year:
            status = "year_mismatch"
        else:
            status = "first_candle_not_0915"
        issues.append(
            TimestampAlignmentIssue(
                symbol=symbol,
                year=year,
                path=str(path),
                first_candle_time=_format_time(first_time),
                status=status,
            )
        )
    return issues


def _repair_one_file(path: Path) -> int:
    frame = pl.read_parquet(path)
    if frame.is_empty() or "candle_time" not in frame.columns:
        return 0

    frame = frame.with_columns(
        (pl.col("candle_time") + pl.duration(seconds=int(IST_SHIFT.total_seconds()))).alias(
            "candle_time"
        )
    )
    frame = frame.with_columns(pl.col("candle_time").dt.date().alias("date"))
    frame = frame.sort("candle_time")

    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.write_parquet(tmp, compression="zstd")
    tmp.replace(path)
    return frame.height


def repair_legacy_utc_naive_5min_parquet(
    parquet_dir: Path,
    *,
    apply: bool = False,
    limit: int | None = None,
) -> TimestampRepairStats:
    """Repair 5-minute parquet files that still start at 03:45 instead of 09:15."""
    stats = TimestampRepairStats()
    for idx, path in enumerate(iter_5min_parquet_files(parquet_dir), start=1):
        if limit is not None and idx > limit:
            break
        stats.scanned_files += 1
        first = _first_candle_time(path)
        if first is None:
            continue
        if first != LEGACY_UTC_OPEN_TIME:
            continue
        stats.flagged_files += 1
        if not apply:
            stats.skipped_files += 1
            continue
        stats.rows_rewritten += _repair_one_file(path)
        stats.repaired_files += 1
    return stats
