#!/usr/bin/env python3
"""Rebuild daily Parquet files from raw vendor daily CSV inputs.

Input format (per symbol CSV):
  Date,Open,High,Low,Close,Volume
  2015-04-01T00:00:00+0530,196.6,199.9,193.75,199.2,13993680

Output format:
  data/parquet/daily/<SYMBOL>/all.parquet
  columns: open, high, low, close, volume, date, symbol
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl


@dataclass
class BuildStats:
    files: int = 0
    symbols: int = 0
    rows_in: int = 0
    rows_out: int = 0


def _infer_symbol(path: Path) -> str:
    stem = path.stem
    if "_" in stem:
        return stem.split("_", 1)[1].strip().upper()
    return stem.strip().upper()


def _normalize_daily_csv(path: Path, symbol: str) -> pl.DataFrame:
    df = pl.read_csv(path, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "date": pl.Date,
                "symbol": pl.Utf8,
            }
        )

    out = (
        df.with_columns(
            pl.col("Date").cast(pl.Utf8).str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
            pl.col("Open").cast(pl.Float64, strict=False).alias("open"),
            pl.col("High").cast(pl.Float64, strict=False).alias("high"),
            pl.col("Low").cast(pl.Float64, strict=False).alias("low"),
            pl.col("Close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("Volume").cast(pl.Int64, strict=False).fill_null(0).alias("volume"),
            pl.lit(symbol).alias("symbol"),
        )
        .drop_nulls(["date", "open", "high", "low", "close"])
        .select(["open", "high", "low", "close", "volume", "date", "symbol"])
        .unique(subset=["date"], keep="last")
        .sort("date")
    )
    return out


def rebuild_daily_parquet(
    *,
    raw_dir: Path,
    out_dir: Path,
    clean: bool,
    limit: int | None,
) -> BuildStats:
    csv_files = sorted(raw_dir.glob("*.csv"))
    if limit is not None:
        csv_files = csv_files[:limit]

    if not csv_files:
        raise SystemExit(f"No CSV files found in {raw_dir}")

    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = BuildStats(files=len(csv_files))

    for idx, path in enumerate(csv_files, start=1):
        symbol = _infer_symbol(path)
        df = _normalize_daily_csv(path, symbol)
        stats.rows_in += df.height
        stats.rows_out += df.height
        stats.symbols += 1

        symbol_dir = out_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        target = symbol_dir / "all.parquet"
        tmp = symbol_dir / "all.parquet.tmp"

        df.write_parquet(tmp)
        tmp.replace(target)

        if idx % 200 == 0 or idx == len(csv_files):
            print(f"[{idx}/{len(csv_files)}] wrote {symbol}")

    return stats


def validate_daily_parquet(out_dir: Path) -> None:
    glob = str((out_dir / "*" / "*.parquet").resolve()).replace("\\", "/")
    con = duckdb.connect(":memory:")
    dow = con.execute(
        f"""
        SELECT strftime(date::DATE, '%w') AS dow, COUNT(*) AS rows
        FROM read_parquet('{glob}', hive_partitioning=false)
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    total = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM read_parquet('{glob}', hive_partitioning=false)"
    ).fetchone()
    if total is None:
        raise SystemExit("No parquet rows found")
    print("Parquet validation")
    print(f"  Rows: {int(total[0]):,}")
    print(f"  Symbols: {int(total[1]):,}")
    print(f"  Date range: {total[2]} to {total[3]}")
    print(f"  DOW rows: {dow}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild daily parquet from raw CSV files")
    parser.add_argument("--raw-dir", default="data/raw/daily", help="Raw daily CSV directory")
    parser.add_argument(
        "--out-dir", default="data/parquet/daily", help="Daily parquet output directory"
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit number of symbols")
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not delete output directory before rebuild",
    )
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    clean = not args.no_clean

    print("Rebuilding daily parquet")
    print(f"  Raw dir: {raw_dir}")
    print(f"  Out dir: {out_dir}")
    print(f"  Clean: {clean}")
    if args.limit:
        print(f"  Limit: {args.limit}")

    stats = rebuild_daily_parquet(
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
    validate_daily_parquet(out_dir)


if __name__ == "__main__":
    main()
