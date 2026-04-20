#!/usr/bin/env python
"""Compare paper feed audit bars against the DuckDB market data lake.

Usage:
    doppler run -- uv run python scripts/paper_feed_audit_compare.py --session-id <ID>
    doppler run -- uv run python scripts/paper_feed_audit_compare.py --session-id <ID> --date 2026-04-18

Exit code 0 = all bars match within tolerance. Exit code 1 = mismatches found.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

try:
    from rich.console import Console
    from rich.table import Table

    _RICH = True
except ImportError:
    _RICH = False

PROJECT_ROOT = Path(__file__).parent.parent
PAPER_DB_DEFAULT = PROJECT_ROOT / "data" / "paper.duckdb"
PARQUET_5MIN_DIR = PROJECT_ROOT / "data" / "parquet" / "5min"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--session-id", required=True, help="Paper session ID to audit")
    p.add_argument(
        "--date",
        dest="trade_date",
        default=None,
        help="Filter to a specific trade date (YYYY-MM-DD)",
    )
    p.add_argument(
        "--tolerance-price",
        type=float,
        default=0.01,
        help="Price tolerance in %% (default 0.01 = 1bp)",
    )
    p.add_argument(
        "--tolerance-volume",
        type=float,
        default=50.0,
        help="Volume tolerance in %% (default 50%%)",
    )
    p.add_argument("--paper-db", default=str(PAPER_DB_DEFAULT), help="Path to paper.duckdb")
    return p.parse_args()


def _pct_diff(actual: float, expected: float) -> float:
    return abs(actual - expected) / max(abs(expected), 1.0) * 100.0


def main() -> int:
    args = _parse_args()

    paper_conn = duckdb.connect(args.paper_db, read_only=True)
    if args.trade_date:
        rows = paper_conn.execute(
            "SELECT symbol, bar_end, open, high, low, close, volume "
            "FROM paper_feed_audit WHERE session_id = ? AND trade_date = ? "
            "ORDER BY symbol, bar_end",
            [args.session_id, args.trade_date],
        ).fetchall()
    else:
        rows = paper_conn.execute(
            "SELECT symbol, bar_end, open, high, low, close, volume "
            "FROM paper_feed_audit WHERE session_id = ? "
            "ORDER BY symbol, bar_end",
            [args.session_id],
        ).fetchall()
    paper_conn.close()

    if not rows:
        print(f"No feed audit rows found for session {args.session_id!r}")
        return 0

    duck = duckdb.connect(":memory:")

    matched = 0
    mismatched = 0
    missing = 0
    issues: list[tuple[str, ...]] = []

    fields = ("open", "high", "low", "close", "volume")
    tolerances = (
        args.tolerance_price,
        args.tolerance_price,
        args.tolerance_price,
        args.tolerance_price,
        args.tolerance_volume,
    )

    for symbol, bar_end, o, h, lo, c, vol in rows:
        parquet_glob = (PARQUET_5MIN_DIR / symbol / "*.parquet").as_posix()
        try:
            mrow = duck.execute(
                f"SELECT open, high, low, close, volume "
                f"FROM read_parquet('{parquet_glob}') WHERE timestamp = ?",
                [bar_end],
            ).fetchone()
        except duckdb.Error:
            mrow = None

        if mrow is None:
            missing += 1
            issues.append((symbol, str(bar_end), "ALL", "N/A", "N/A", "N/A", "MISSING"))
            continue

        m_open, m_high, m_low, m_close, m_vol = mrow
        expected = (o, h, lo, c, vol)
        actual = (m_open, m_high, m_low, m_close, m_vol)

        bar_ok = True
        for fname, exp_val, act_val, tol in zip(fields, expected, actual, tolerances, strict=True):
            diff = _pct_diff(act_val, exp_val)
            if diff > tol:
                bar_ok = False
                issues.append(
                    (
                        symbol,
                        str(bar_end),
                        fname,
                        f"{exp_val:.4f}",
                        f"{act_val:.4f}",
                        f"{diff:.4f}",
                        "MISMATCH",
                    )
                )

        if bar_ok:
            matched += 1
        else:
            mismatched += 1

    duck.close()

    columns = ("SYMBOL", "BAR_END", "FIELD", "EXPECTED", "ACTUAL", "DIFF%", "STATUS")

    if _RICH:
        console = Console()
        if issues:
            tbl = Table(title="Feed Audit Mismatches / Missing")
            for col in columns:
                tbl.add_column(col)
            for row in issues:
                style = "red" if row[-1] == "MISMATCH" else "yellow"
                tbl.add_row(*row, style=style)
            console.print(tbl)
        console.print(
            f"\n[green]Matched:[/green] {matched}  "
            f"[red]Mismatched:[/red] {mismatched}  "
            f"[yellow]Missing:[/yellow] {missing}"
        )
    else:
        if issues:
            hdr = (
                f"{'SYMBOL':<20} {'BAR_END':<26} {'FIELD':<8}"
                f" {'EXPECTED':>12} {'ACTUAL':>12} {'DIFF%':>8} STATUS"
            )
            print(hdr)
            print("-" * len(hdr))
            for row in issues:
                print(
                    f"{row[0]:<20} {row[1]:<26} {row[2]:<8}"
                    f" {row[3]:>12} {row[4]:>12} {row[5]:>8} {row[6]}"
                )
        print(f"\nMatched: {matched}  Mismatched: {mismatched}  Missing: {missing}")

    return 1 if (mismatched > 0 or missing > 0) else 0


if __name__ == "__main__":
    sys.exit(main())
