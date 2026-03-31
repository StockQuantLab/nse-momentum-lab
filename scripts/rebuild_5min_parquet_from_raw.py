#!/usr/bin/env python3
"""Rebuild 5-minute Parquet files from raw intraday CSV inputs.

Input format (per symbol CSV, possibly under part subfolders):
  Date,Open,High,Low,Close,Volume
  2015-04-01T09:15:00+0530,194.5,194.5,193.05,194.2,474840

Output format:
  data/parquet/5min/<SYMBOL>/<YEAR>.parquet
  columns: candle_time, open, high, low, close, volume, true_range, date, symbol
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class BuildStats:
    files: int = 0
    symbols: int = 0
    rows_in: int = 0
    rows_out: int = 0
    parquet_files: int = 0


def _infer_symbol(path: Path) -> str:
    stem = path.stem
    if "_" in stem:
        return stem.split("_", 1)[1].strip().upper()
    return stem.strip().upper()


def _parse_ist_timestamp(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    else:
        parsed = parsed.astimezone(IST)
    return parsed.replace(tzinfo=None)


def _normalize_5min_csv(path: Path, symbol: str) -> pl.DataFrame:
    df = pl.read_csv(path, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "candle_time": pl.Datetime(time_unit="us"),
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "true_range": pl.Float64,
                "date": pl.Date,
                "symbol": pl.Utf8,
            }
        )

    out = (
        df.with_columns(
            pl.col("Date")
            .map_elements(_parse_ist_timestamp, return_dtype=pl.Datetime(time_unit="us"))
            .alias("candle_time"),
            pl.col("Open").cast(pl.Float64, strict=False).alias("open"),
            pl.col("High").cast(pl.Float64, strict=False).alias("high"),
            pl.col("Low").cast(pl.Float64, strict=False).alias("low"),
            pl.col("Close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("Volume").cast(pl.Int64, strict=False).fill_null(0).alias("volume"),
            pl.lit(symbol).alias("symbol"),
        )
        .drop_nulls(["candle_time", "open", "high", "low", "close"])
        .with_columns(
            (pl.col("high") - pl.col("low")).alias("true_range"),
            pl.col("candle_time").dt.date().alias("date"),
        )
        .select(["candle_time", "open", "high", "low", "close", "volume", "true_range", "date", "symbol"])
        .unique(subset=["candle_time"], keep="last")
        .sort("candle_time")
    )
    return out


def rebuild_5min_parquet(
    *,
    raw_dir: Path,
    out_dir: Path,
    clean: bool,
    limit: int | None,
) -> BuildStats:
    csv_files = sorted(raw_dir.rglob("*.csv"))
    if limit is not None:
        csv_files = csv_files[:limit]

    if not csv_files:
        raise SystemExit(f"No CSV files found in {raw_dir}")

    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = BuildStats(files=len(csv_files))
    seen_symbols: set[str] = set()

    for idx, path in enumerate(csv_files, start=1):
        symbol = _infer_symbol(path)
        seen_symbols.add(symbol)
        df = _normalize_5min_csv(path, symbol)
        stats.rows_in += df.height
        stats.rows_out += df.height

        if not df.is_empty():
            symbol_dir = out_dir / symbol
            symbol_dir.mkdir(parents=True, exist_ok=True)
            yearly_frames = df.with_columns(pl.col("date").dt.year().alias("year")).partition_by(
                "year", maintain_order=True
            )
            for year_df in yearly_frames:
                year = int(year_df.get_column("year")[0])
                target = symbol_dir / f"{year}.parquet"
                tmp = symbol_dir / f"{year}.parquet.tmp"
                year_df.drop("year").write_parquet(tmp)
                tmp.replace(target)
                stats.parquet_files += 1

        if idx % 100 == 0 or idx == len(csv_files):
            print(f"[{idx}/{len(csv_files)}] wrote {symbol}")

    stats.symbols = len(seen_symbols)
    return stats


def validate_5min_parquet(out_dir: Path) -> None:
    glob = str((out_dir / "*" / "*.parquet").resolve()).replace("\\", "/")
    con = duckdb.connect(":memory:")
    totals = con.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(date)::VARCHAR, MAX(date)::VARCHAR
        FROM read_parquet('{glob}', hive_partitioning=false)
        """
    ).fetchone()
    dow = con.execute(
        f"""
        SELECT strftime(date::DATE, '%w') AS dow, COUNT(*) AS rows
        FROM read_parquet('{glob}', hive_partitioning=false)
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    print("Parquet validation")
    print(f"  Rows: {int(totals[0]):,}")
    print(f"  Symbols: {int(totals[1]):,}")
    print(f"  Date range: {totals[2]} to {totals[3]}")
    print(f"  DOW rows: {dow}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild 5-minute parquet from raw CSV files")
    parser.add_argument(
        "--raw-dir", default="data/raw/5min", help="Raw 5-minute CSV root directory"
    )
    parser.add_argument(
        "--out-dir", default="data/parquet/5min", help="5-minute parquet output directory"
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit number of CSV files")
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not delete output directory before rebuild",
    )
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    clean = not args.no_clean

    print("Rebuilding 5-minute parquet")
    print(f"  Raw dir: {raw_dir}")
    print(f"  Out dir: {out_dir}")
    print(f"  Clean: {clean}")
    if args.limit:
        print(f"  Limit: {args.limit}")

    stats = rebuild_5min_parquet(
        raw_dir=raw_dir,
        out_dir=out_dir,
        clean=clean,
        limit=args.limit,
    )
    print("Build complete")
    print(f"  Files processed: {stats.files}")
    print(f"  Symbols written: {stats.symbols}")
    print(f"  Input rows: {stats.rows_in:,}")
    print(f"  Output rows: {stats.rows_out:,}")
    print(f"  Parquet files: {stats.parquet_files:,}")
    validate_5min_parquet(out_dir)


if __name__ == "__main__":
    main()
