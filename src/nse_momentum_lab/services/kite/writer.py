from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

# Historical 5-minute parquet files in this lake were written with a fixed-offset
# timezone annotation (+05:30). Polars validates that metadata on read, so keep
# the fallback enabled for merge/read-back compatibility.
os.environ.setdefault("POLARS_IGNORE_TIMEZONE_PARSE_ERROR", "1")

import polars as pl

from nse_momentum_lab.services.kite.fetcher import KiteFetcher, get_kite_fetcher

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
RAW_KITE_DIR = DATA_DIR / "raw" / "kite"
KITE_DAILY_FILENAME = "kite.parquet"
DAILY_PARQUET_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume"]
FIVE_MIN_PARQUET_COLUMNS = [
    "symbol",
    "date",
    "candle_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
]
PARQUET_DAILY_DTYPE = {
    "symbol": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
}
PARQUET_5MIN_DTYPE = {
    "symbol": pl.Utf8,
    "date": pl.Date,
    "candle_time": pl.Datetime(time_unit="ns"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
}


class KiteWriter:
    def __init__(self, fetcher: KiteFetcher | None = None) -> None:
        self.fetcher = fetcher or get_kite_fetcher()

    def fetch_and_write_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        mode: str = "append",
        save_raw: bool = False,
    ) -> int:
        effective_start, skip_fetch = self._effective_fetch_window(
            dataset="daily",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if skip_fetch:
            return 0
        frame = self.fetcher.fetch_daily_ohlcv(symbol, effective_start, end_date)
        if frame.is_empty():
            return 0
        if save_raw:
            self._save_raw_csv("daily", symbol, frame, start_date, end_date)
        return self.write_daily(symbol, frame, mode=mode)

    def fetch_and_write_5min(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        mode: str = "append",
        save_raw: bool = False,
    ) -> int:
        effective_start, skip_fetch = self._effective_fetch_window(
            dataset="5min",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if skip_fetch:
            return 0
        frame = self.fetcher.fetch_5min_ohlcv(symbol, effective_start, end_date)
        if frame.is_empty():
            return 0
        if save_raw:
            self._save_raw_csv("5min", symbol, frame, start_date, end_date)
        return self.write_5min(symbol, frame, mode=mode)

    def write_daily(self, symbol: str, df: Any, mode: str = "append") -> int:
        frame = self._normalize_daily_frame(df)
        if frame.is_empty():
            return 0

        path = PARQUET_DIR / "daily" / symbol / KITE_DAILY_FILENAME
        combined = self._merge_existing(
            path=path,
            new_rows=frame,
            subset=["symbol", "date"],
            sort_columns=["symbol", "date"],
            mode=mode,
        )
        self._write_parquet(path, combined)
        return frame.height

    def write_5min(self, symbol: str, df: Any, mode: str = "append") -> int:
        frame = self._normalize_5min_frame(df)
        if frame.is_empty():
            return 0

        total_written = 0
        yearly_frames = frame.with_columns(pl.col("date").dt.year().alias("_year")).partition_by(
            "_year", maintain_order=True
        )
        for yearly_frame in yearly_frames:
            year = int(yearly_frame.get_column("_year")[0])
            path = PARQUET_DIR / "5min" / symbol / f"{year}.parquet"
            payload = yearly_frame.drop("_year")
            combined = self._merge_existing(
                path=path,
                new_rows=payload,
                subset=["symbol", "candle_time"],
                sort_columns=["symbol", "candle_time"],
                mode=mode,
            )
            self._write_parquet(path, combined)
            total_written += payload.height
        return total_written

    def _normalize_daily_frame(self, df: Any) -> pl.DataFrame:
        frame = self._to_polars_frame(df)
        if frame.is_empty():
            return pl.DataFrame(schema=PARQUET_DAILY_DTYPE)
        return frame.cast(PARQUET_DAILY_DTYPE, strict=False).sort(["symbol", "date"])

    def _normalize_5min_frame(self, df: Any) -> pl.DataFrame:
        frame = self._to_polars_frame(df)
        if frame.is_empty():
            return pl.DataFrame(schema=PARQUET_5MIN_DTYPE)
        frame = self._normalize_ist_candle_time(frame)
        frame = frame.cast(PARQUET_5MIN_DTYPE, strict=False)
        return frame.sort(["symbol", "candle_time"])

    def _to_polars_frame(self, df: Any) -> pl.DataFrame:
        if isinstance(df, pl.DataFrame):
            frame = df
        else:
            frame = pl.DataFrame(df)
        columns = (
            DAILY_PARQUET_COLUMNS
            if "candle_time" not in frame.columns
            else FIVE_MIN_PARQUET_COLUMNS
        )
        available_columns = [column for column in columns if column in frame.columns]
        return frame.select(available_columns)

    def _effective_fetch_window(
        self,
        *,
        dataset: str,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> tuple[date, bool]:
        if start_date > end_date:
            return start_date, True
        existing_max = self._get_existing_max_date(dataset=dataset, symbol=symbol)
        if existing_max is None or existing_max < start_date:
            return start_date, False
        if existing_max >= end_date:
            return end_date, True
        return date.fromordinal(existing_max.toordinal() + 1), False

    def _get_existing_max_date(self, *, dataset: str, symbol: str) -> date | None:
        if dataset == "daily":
            path = PARQUET_DIR / "daily" / symbol / KITE_DAILY_FILENAME
            if not path.exists():
                return None
            frame = pl.scan_parquet(path).select(pl.max("date").alias("max_date")).collect()
        else:
            symbol_dir = PARQUET_DIR / "5min" / symbol
            if not symbol_dir.exists():
                return None
            files = sorted(symbol_dir.glob("*.parquet"))
            if not files:
                return None
            frame = (
                pl.scan_parquet([str(file) for file in files])
                .select(pl.max("date").alias("max_date"))
                .collect()
            )
        if frame.is_empty():
            return None
        return frame.get_column("max_date")[0]

    def _merge_existing(
        self,
        *,
        path: Path,
        new_rows: pl.DataFrame,
        subset: list[str],
        sort_columns: list[str],
        mode: str,
    ) -> pl.DataFrame:
        if mode == "overwrite" or not path.exists():
            return new_rows.unique(subset=subset, keep="last").sort(sort_columns)

        existing = pl.read_parquet(path)
        existing = self._normalize_ist_candle_time(existing)
        append_key = sort_columns[-1]
        if not existing.is_empty() and not new_rows.is_empty():
            existing_max = existing.get_column(append_key).max()
            new_min = new_rows.get_column(append_key).min()
            if existing_max is not None and new_min is not None and new_min > existing_max:  # type: ignore[operator]
                return pl.concat([existing, new_rows], how="vertical_relaxed")
        merged = pl.concat([existing, new_rows], how="vertical_relaxed")
        return merged.unique(subset=subset, keep="last").sort(sort_columns)

    def _normalize_ist_candle_time(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "candle_time" not in frame.columns:
            return frame
        candle_type = frame.schema.get("candle_time")
        candle_expr = pl.col("candle_time")
        if isinstance(candle_type, pl.Datetime) and candle_type.time_zone:
            candle_expr = candle_expr.dt.convert_time_zone("Asia/Kolkata").dt.replace_time_zone(
                None
            )
        return frame.with_columns(candle_expr.cast(PARQUET_5MIN_DTYPE["candle_time"], strict=False))

    def _write_parquet(self, path: Path, frame: pl.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        frame.write_parquet(temp_path, compression="zstd")
        temp_path.replace(path)

    def _save_raw_csv(
        self,
        dataset: str,
        symbol: str,
        df: Any,
        start_date: date,
        end_date: date,
    ) -> None:
        target_dir = RAW_KITE_DIR / dataset / symbol
        target_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
        self._to_polars_frame(df).write_csv(target_dir / file_name)


_kite_writer: KiteWriter | None = None


def get_kite_writer() -> KiteWriter:
    global _kite_writer
    if _kite_writer is None:
        _kite_writer = KiteWriter()
    return _kite_writer
