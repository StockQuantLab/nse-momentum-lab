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
import pandas as pd


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


def _normalize_daily_csv(path: Path, symbol: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["Date", "Open", "High", "Low", "Close", "Volume"])
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "date", "symbol"])

    # Parse calendar date directly from YYYY-MM-DD prefix. This intentionally
    # ignores timezone offset in vendor strings to avoid UTC date shifting.
    date_str = df["Date"].astype(str).str.slice(0, 10)
    parsed = pd.to_datetime(date_str, format="%Y-%m-%d", errors="coerce")
    bad = parsed.isna().sum()
    if bad:
        parsed_fallback = pd.to_datetime(df["Date"], errors="coerce")
        parsed = parsed.fillna(parsed_fallback)

    out = pd.DataFrame(
        {
            "open": pd.to_numeric(df["Open"], errors="coerce"),
            "high": pd.to_numeric(df["High"], errors="coerce"),
            "low": pd.to_numeric(df["Low"], errors="coerce"),
            "close": pd.to_numeric(df["Close"], errors="coerce"),
            "volume": pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64"),
            "date": parsed.dt.date,
            "symbol": symbol,
        }
    )

    out = out.dropna(subset=["date", "open", "high", "low", "close"])
    out = out.sort_values("date")
    out = out.drop_duplicates(subset=["date"], keep="last")
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
        stats.rows_in += int(pd.read_csv(path, usecols=["Date"]).shape[0])
        stats.rows_out += int(len(df))
        stats.symbols += 1

        symbol_dir = out_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        target = symbol_dir / "all.parquet"
        tmp = symbol_dir / "all.parquet.tmp"

        df.to_parquet(tmp, index=False)
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
    print("Parquet validation")
    print(f"  Rows: {int(total[0]):,}")
    print(f"  Symbols: {int(total[1]):,}")
    print(f"  Date range: {total[2]} to {total[3]}")
    print(f"  DOW rows: {dow}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild daily parquet from raw CSV files")
    parser.add_argument("--raw-dir", default="data/raw/daily", help="Raw daily CSV directory")
    parser.add_argument("--out-dir", default="data/parquet/daily", help="Daily parquet output directory")
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
