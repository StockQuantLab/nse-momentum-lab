#!/usr/bin/env python3
"""Run momentum scans across a date range for a limited set of symbols.

Why this exists
- With a small sample universe (e.g. 10 stocks), you still need to validate *full-history*
  behavior (coverage + scan pass rates) before ingesting 1,800+ symbols.
- This script populates `scan_run` + `scan_result` for every trading date present in
  `md_ohlcv_adj` for the selected symbols and date range.

Example
  doppler run -- uv run python scripts/run_scan_range_limited.py \
    --start 2015-04-01 --end 2025-03-31 --symbols RELIANCE,TCS,INFY

Notes
- Idempotent: re-running the same range will reuse the existing ScanRun unless `--force` is set.
- Keeps everything deterministic; no synthetic data generation.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import MdOhlcvAdj, RefSymbol
from nse_momentum_lab.services.scan.worker import ScanWorker


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_symbols_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    parts = [p.strip().upper() for p in value.split(",") if p.strip()]
    return parts or None


@dataclass
class RunSummary:
    scanned_dates: int
    succeeded: int
    failed: int
    total_passed: int


_PREFIX_RE = re.compile(r"^\d+_")


def _base_symbol(symbol: str) -> str:
    return _PREFIX_RE.sub("", symbol.upper())


async def _resolve_symbols(*, series: str, symbols: list[str] | None) -> list[str]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        q = select(RefSymbol.symbol).where(RefSymbol.status == "ACTIVE", RefSymbol.series == series)
        rows = (await session.execute(q)).all()
        active_symbols = sorted({r[0].upper() for r in rows if r[0]})

        if not symbols:
            return active_symbols

        # Resolve user-provided tokens against either full symbols (exact) or base symbols
        # (e.g. RELIANCE -> 0001_RELIANCE). Fail fast if ambiguous.
        by_base: dict[str, list[str]] = {}
        active_set = set(active_symbols)
        for full_symbol in active_symbols:
            by_base.setdefault(_base_symbol(full_symbol), []).append(full_symbol)

        resolved: list[str] = []
        for token in symbols:
            tok = token.upper()
            if tok in active_set:
                resolved.append(tok)
                continue

            candidates = by_base.get(tok, [])
            if len(candidates) == 1:
                resolved.append(candidates[0])
                continue
            if len(candidates) > 1:
                raise SystemExit(f"Ambiguous symbol '{token}' matches: {candidates[:5]}")

        if not resolved:
            raise SystemExit(
                "No ACTIVE symbols matched the provided filter. "
                "Tip: run without --symbols to see what is available."
            )
        return sorted(set(resolved))


async def _list_trading_dates(
    *, series: str, symbols: list[str], start: date, end: date
) -> list[date]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        sym_rows = (
            await session.execute(
                select(RefSymbol.symbol_id).where(
                    RefSymbol.status == "ACTIVE",
                    RefSymbol.series == series,
                    RefSymbol.symbol.in_(symbols),
                )
            )
        ).all()
        symbol_ids = [r[0] for r in sym_rows]
        if not symbol_ids:
            return []

        q = (
            select(MdOhlcvAdj.trading_date)
            .where(
                MdOhlcvAdj.symbol_id.in_(symbol_ids),
                MdOhlcvAdj.trading_date >= start,
                MdOhlcvAdj.trading_date <= end,
            )
            .distinct()
            .order_by(MdOhlcvAdj.trading_date)
        )
        rows = (await session.execute(q)).all()
        return [r[0] for r in rows if r[0] is not None]


async def run_range(
    *,
    start: date,
    end: date,
    symbols: list[str] | None,
    series: str,
    force: bool,
    max_dates: int | None,
    progress_every: int,
    dry_run: bool,
) -> RunSummary:
    resolved_symbols = await _resolve_symbols(series=series, symbols=symbols)
    if not resolved_symbols:
        raise SystemExit("No ACTIVE symbols matched the provided filter")

    trading_dates = await _list_trading_dates(
        series=series, symbols=resolved_symbols, start=start, end=end
    )
    if not trading_dates:
        raise SystemExit(
            "No trading dates found in md_ohlcv_adj for the selected symbols/date range"
        )

    if max_dates is not None:
        trading_dates = trading_dates[:max_dates]

    print("\nScan range (limited universe)")
    print(f"  series:  {series}")
    print(f"  symbols: {len(resolved_symbols)}")
    print(f"  dates:   {len(trading_dates)}")
    print(f"  start:   {trading_dates[0].isoformat()}")
    print(f"  end:     {trading_dates[-1].isoformat()}")
    if dry_run:
        print("  mode:    DRY RUN (no scans executed)")
        return RunSummary(scanned_dates=len(trading_dates), succeeded=0, failed=0, total_passed=0)

    worker = ScanWorker(symbols=resolved_symbols)

    succeeded = 0
    failed = 0
    total_passed = 0

    for idx, d in enumerate(trading_dates, 1):
        try:
            result = await worker.run(d, force=force)
            if result.status == "SUCCEEDED":
                succeeded += 1
                total_passed += int(result.candidates_found)
            else:
                failed += 1
        except Exception as exc:
            failed += 1
            print(f"[{idx}/{len(trading_dates)}] {d.isoformat()} FAILED: {exc}")

        if progress_every > 0 and (idx % progress_every == 0 or idx == len(trading_dates)):
            print(
                f"[{idx}/{len(trading_dates)}] up to {d.isoformat()} :: "
                f"ok={succeeded}, failed={failed}, passed_total={total_passed}"
            )

    return RunSummary(
        scanned_dates=len(trading_dates),
        succeeded=succeeded,
        failed=failed,
        total_passed=total_passed,
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Run 4P_2LYNCH scans across a date range for limited symbols"
    )
    p.add_argument("--start", required=True, type=str, help="Start date YYYY-MM-DD")
    p.add_argument("--end", required=True, type=str, help="End date YYYY-MM-DD")
    p.add_argument("--symbols", default=None, type=str, help="CSV of symbols (e.g. RELIANCE,TCS)")
    p.add_argument("--series", default="EQ", type=str, help="Series (default: EQ)")
    p.add_argument(
        "--force", action="store_true", help="Recompute scans even if ScanRun already exists"
    )
    p.add_argument(
        "--max-dates", default=None, type=int, help="Limit the number of dates (debugging)"
    )
    p.add_argument(
        "--progress-every", default=25, type=int, help="Progress print frequency (default: 25)"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="List what would be scanned without executing"
    )
    args = p.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if end < start:
        raise SystemExit("--end must be >= --start")

    symbols = _parse_symbols_csv(args.symbols)

    coro = run_range(
        start=start,
        end=end,
        symbols=symbols,
        series=args.series,
        force=args.force,
        max_dates=args.max_dates,
        progress_every=args.progress_every,
        dry_run=args.dry_run,
    )

    if sys.platform == "win32":
        selector_loop_cls = getattr(asyncio, "SelectorEventLoop", None)
        if selector_loop_cls is None:
            raise RuntimeError("asyncio.SelectorEventLoop is not available on this platform")

        runner_cls = getattr(asyncio, "Runner", None)
        if runner_cls is not None:
            with asyncio.Runner(loop_factory=selector_loop_cls) as runner:
                summary = runner.run(coro)
        else:
            loop = selector_loop_cls()
            try:
                asyncio.set_event_loop(loop)
                summary = loop.run_until_complete(coro)
            finally:
                try:
                    loop.close()
                finally:
                    asyncio.set_event_loop(None)
    else:
        summary = asyncio.run(coro)

    print("\nDone")
    print(f"  dates:        {summary.scanned_dates}")
    print(f"  succeeded:    {summary.succeeded}")
    print(f"  failed:       {summary.failed}")
    print(f"  passed_total: {summary.total_passed}")


if __name__ == "__main__":
    main()
