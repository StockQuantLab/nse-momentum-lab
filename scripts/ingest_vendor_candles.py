#!/usr/bin/env python3
"""Ingest vendor candle CSVs (daily or minute) into nseml.md_ohlcv_raw.

This supports datasets you already have downloaded (e.g., from shared drives / Jio Cloud
links / broker exports). For Phase 1, daily data is sufficient; minute data is optional
and will be deterministically aggregated to daily.

Assumptions:
- Each CSV is a single symbol, columns: Date/Datetime, Open, High, Low, Close, Volume.
- Minute/5-min files are in non-decreasing time order.

Examples:
  doppler run -- uv run python scripts/ingest_vendor_candles.py data/vendor/day --timeframe day
  doppler run -- uv run python scripts/ingest_vendor_candles.py data/vendor/1min --timeframe minute
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.services.ingest.repository import IngestionRepository

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.services.ingest.candle_csv import (
    aggregate_to_daily,
    file_sha256,
    infer_symbol_from_filename,
    iter_candles_csv,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _iter_csv_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    return sorted([p for p in root.rglob("*.csv") if p.is_file()])


async def ingest_path(
    root: Path,
    *,
    timeframe: str,
    series: str,
    vendor: str,
    dry_run: bool,
    limit: int | None = None,
) -> dict:
    files = _iter_csv_files(root)
    if not files:
        raise SystemExit(f"No .csv files found under {root}")

    if limit:
        files = files[:limit]
        logger.info(f"Limited to first {limit} files")

    sessionmaker = get_sessionmaker()
    totals = {"files": len(files), "symbols": 0, "days": 0, "rows": 0}

    async with sessionmaker() as session:
        repo = IngestionRepository(session)

        for idx, path in enumerate(files, 1):
            symbol = infer_symbol_from_filename(path)
            symbol_id = await repo.get_symbol_id(symbol, series=series, create_if_missing=True)
            if symbol_id is None:
                logger.warning("Skipping %s (cannot resolve symbol_id)", symbol)
                continue

            sha = file_sha256(path)[:16]
            source_uri = f"vendor:{vendor}:{path.as_posix()}"
            ingest_run_id = f"vendor-{vendor}-{timeframe}-{sha}"

            rows = list(iter_candles_csv(path, timeframe=timeframe))
            is_minuteish = any(r.ts is not None for r in rows)

            if timeframe == "minute" or (timeframe == "auto" and is_minuteish):
                daily = aggregate_to_daily(rows)
                daily_sorted = sorted(daily, key=lambda x: x.trading_date)
            else:
                # already-daily; keep stable ordering
                daily_sorted = sorted(rows, key=lambda x: x.trading_date)

            if not daily_sorted:
                continue

            totals["symbols"] += 1
            totals["days"] += len(daily_sorted)
            totals["rows"] += len(rows)

            logger.info(
                "[%s/%s] %s: %s daily bars (%s raw rows)",
                idx,
                len(files),
                symbol,
                len(daily_sorted),
                len(rows),
            )

            if dry_run:
                continue

            for d in daily_sorted:
                await repo.upsert_ohlcv_row(
                    symbol_id=symbol_id,
                    trading_date=d.trading_date,
                    row={
                        "open": float(d.open),
                        "high": float(d.high),
                        "low": float(d.low),
                        "close": float(d.close),
                        "volume": int(d.volume),
                    },
                    source_file_uri=source_uri,
                    ingest_run_id=ingest_run_id,
                )

            await session.commit()

    return totals


def main() -> None:
    p = argparse.ArgumentParser(description="Ingest vendor candle CSVs into md_ohlcv_raw")
    p.add_argument("path", type=str, help="CSV file or folder containing per-symbol CSV files")
    p.add_argument(
        "--timeframe",
        choices=["auto", "day", "minute"],
        default="auto",
        help="Interpret Date column as day or timestamp. 'auto' infers from first row.",
    )
    p.add_argument("--series", default="EQ", help="Series to store in ref_symbol (default: EQ)")
    p.add_argument("--vendor", default="dataset", help="Vendor label used in source_file_uri")
    p.add_argument(
        "--dry-run", action="store_true", help="Parse/aggregate only; do not write to DB"
    )
    p.add_argument(
        "--limit", "-n", type=int, default=None,
        help="Limit number of files to process (useful for testing batches)"
    )
    args = p.parse_args()

    coro = ingest_path(
        Path(args.path),
        timeframe=args.timeframe,
        series=args.series,
        vendor=args.vendor,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    if sys.platform == "win32":
        selector_loop_cls = getattr(asyncio, "SelectorEventLoop", None)
        if selector_loop_cls is None:
            raise RuntimeError("asyncio.SelectorEventLoop is not available on this platform")

        runner_cls = getattr(asyncio, "Runner", None)
        if runner_cls is not None:
            with asyncio.Runner(loop_factory=selector_loop_cls) as runner:
                totals = runner.run(coro)
        else:
            loop = selector_loop_cls()
            try:
                asyncio.set_event_loop(loop)
                totals = loop.run_until_complete(coro)
            finally:
                try:
                    loop.close()
                finally:
                    asyncio.set_event_loop(None)
    else:
        totals = asyncio.run(coro)

    print("\nIngest complete")
    print(f"  files:   {totals['files']}")
    print(f"  symbols: {totals['symbols']}")
    print(f"  days:    {totals['days']}")
    print(f"  rows:    {totals['rows']} (raw input rows)")
    if args.dry_run:
        print("  mode:    DRY RUN (no DB writes)")


if __name__ == "__main__":
    main()
