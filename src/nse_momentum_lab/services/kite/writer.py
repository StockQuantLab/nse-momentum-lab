from __future__ import annotations

import os
from datetime import date
from pathlib import Path

# Historical 5-minute parquet files in this lake were written with a fixed-offset
# timezone annotation (+05:30). Polars validates that metadata on read, so keep
# the fallback enabled for merge/read-back compatibility.
os.environ.setdefault("POLARS_IGNORE_TIMEZONE_PARSE_ERROR", "1")

import pandas as pd
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
        frame = self.fetcher.fetch_daily_ohlcv(symbol, start_date, end_date)
        if frame.empty:
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
        frame = self.fetcher.fetch_5min_ohlcv(symbol, start_date, end_date)
        if frame.empty:
            return 0
        if save_raw:
            self._save_raw_csv("5min", symbol, frame, start_date, end_date)
        return self.write_5min(symbol, frame, mode=mode)

    def write_daily(self, symbol: str, df: pd.DataFrame, mode: str = "append") -> int:
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

    def write_5min(self, symbol: str, df: pd.DataFrame, mode: str = "append") -> int:
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

    def _normalize_daily_frame(self, df: pd.DataFrame) -> pl.DataFrame:
        if df.empty:
            return pl.DataFrame(schema=PARQUET_DAILY_DTYPE)
        frame = pl.from_pandas(df, include_index=False).select(DAILY_PARQUET_COLUMNS)
        return frame.cast(PARQUET_DAILY_DTYPE, strict=False).sort(["symbol", "date"])

    def _normalize_5min_frame(self, df: pd.DataFrame) -> pl.DataFrame:
        if df.empty:
            return pl.DataFrame(schema=PARQUET_5MIN_DTYPE)
        frame = pl.from_pandas(df, include_index=False).select(FIVE_MIN_PARQUET_COLUMNS)
        frame = self._normalize_ist_candle_time(frame)
        frame = frame.cast(PARQUET_5MIN_DTYPE, strict=False)
        return frame.sort(["symbol", "candle_time"])

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
        df: pd.DataFrame,
        start_date: date,
        end_date: date,
    ) -> None:
        target_dir = RAW_KITE_DIR / dataset / symbol
        target_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
        df.to_csv(target_dir / file_name, index=False)


_kite_writer: KiteWriter | None = None


def get_kite_writer() -> KiteWriter:
    global _kite_writer
    if _kite_writer is None:
        _kite_writer = KiteWriter()
    return _kite_writer
