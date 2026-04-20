#!/usr/bin/env python
"""Check parity between paper trading decisions and backtest signals for the same date.

Usage:
    doppler run -- uv run python scripts/parity_check.py --session-id <ID> --trade-date 2026-04-18
    doppler run -- uv run python scripts/parity_check.py \\
        --session-id <ID> --trade-date 2026-04-18 --exp-id 1716b78c208a90f3

Exit code 0 = no gross divergences. Exit code 1 = divergences found.
"""

from __future__ import annotations

import argparse
import json
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
BACKTEST_DB_DEFAULT = PROJECT_ROOT / "data" / "backtest.duckdb"

_COLUMNS = ("SYMBOL", "PAPER-DIR", "BT-DIR", "PAPER-PRICE", "BT-PRICE", "DIFF%", "STATUS")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--session-id", required=True, help="Paper session ID")
    p.add_argument("--trade-date", required=True, help="Trade date (YYYY-MM-DD)")
    p.add_argument("--exp-id", default=None, help="Pin to a specific backtest experiment ID")
    p.add_argument("--paper-db", default=str(PAPER_DB_DEFAULT), help="Path to paper.duckdb")
    p.add_argument(
        "--backtest-db", default=str(BACKTEST_DB_DEFAULT), help="Path to backtest.duckdb"
    )
    p.add_argument(
        "--price-tolerance",
        type=float,
        default=0.5,
        help="Price tolerance %% (default 0.5). Errors flagged at 5× tolerance.",
    )
    return p.parse_args()


def _extract_entry_price(metadata_json: str | None) -> float | None:
    if not metadata_json:
        return None
    try:
        d = json.loads(metadata_json)
        for key in ("entry_price", "trigger_price", "planned_entry"):
            val = d.get(key)
            if val is not None:
                return float(val)
        return None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def _extract_direction(metadata_json: str | None) -> str:
    if not metadata_json:
        return "LONG"
    try:
        d = json.loads(metadata_json)
        return str(d.get("direction", "LONG")).upper()
    except (ValueError, TypeError, json.JSONDecodeError):
        return "LONG"


def _print_results(rows_out: list[tuple[str, ...]], session_id: str, trade_date: str) -> None:
    if _RICH:
        console = Console()
        tbl = Table(title=f"Parity Check — session={session_id}  date={trade_date}")
        for col in _COLUMNS:
            tbl.add_column(col)
        for row in rows_out:
            status = row[-1]
            if status == "OK":
                style = "green"
            elif status in ("PAPER-ONLY", "BT-ONLY"):
                style = "yellow"
            elif status in ("DIR-MISMATCH", "ERROR"):
                style = "red"
            elif status == "WARN":
                style = "dark_orange"
            else:
                style = ""
            tbl.add_row(*row, style=style)
        console.print(tbl)
    else:
        hdr = "  ".join(f"{c:<15}" for c in _COLUMNS)
        print(hdr)
        print("-" * len(hdr))
        for row in rows_out:
            print("  ".join(f"{v:<15}" for v in row))


def main() -> int:
    args = _parse_args()

    paper_conn = duckdb.connect(args.paper_db, read_only=True)
    paper_rows = paper_conn.execute(
        "SELECT symbol, state, metadata_json FROM paper_signals WHERE session_id = ?",
        [args.session_id],
    ).fetchall()
    paper_conn.close()

    paper_map: dict[str, tuple[str, float | None]] = {}
    for symbol, _state, meta in paper_rows:
        paper_map[symbol] = (_extract_direction(meta), _extract_entry_price(meta))

    bt_conn = duckdb.connect(args.backtest_db, read_only=True)
    if args.exp_id:
        bt_rows = bt_conn.execute(
            "SELECT symbol, entry_price, direction FROM bt_trade "
            "WHERE entry_date = ? AND exp_id = ?",
            [args.trade_date, args.exp_id],
        ).fetchall()
    else:
        bt_rows = bt_conn.execute(
            "SELECT symbol, entry_price, direction FROM bt_trade WHERE entry_date = ?",
            [args.trade_date],
        ).fetchall()
    bt_conn.close()

    bt_map: dict[str, tuple[float, str]] = {
        sym: (float(ep or 0), str(d or "LONG").upper()) for sym, ep, d in bt_rows
    }

    matched = 0
    paper_only = 0
    bt_only = 0
    price_divergence = 0
    rows_out: list[tuple[str, ...]] = []

    for sym in sorted(set(paper_map) | set(bt_map)):
        in_paper = sym in paper_map
        in_bt = sym in bt_map

        if in_paper and not in_bt:
            paper_only += 1
            p_dir, p_price = paper_map[sym]
            rows_out.append(
                (sym, p_dir, "N/A", f"{p_price:.2f}" if p_price else "N/A", "N/A", "N/A", "PAPER-ONLY")
            )
            continue

        if in_bt and not in_paper:
            bt_only += 1
            b_price, b_dir = bt_map[sym]
            rows_out.append((sym, "N/A", b_dir, "N/A", f"{b_price:.2f}", "N/A", "BT-ONLY"))
            continue

        p_dir, p_price = paper_map[sym]
        b_price, b_dir = bt_map[sym]

        if p_price is not None and b_price:
            diff_pct = abs(p_price - b_price) / max(b_price, 0.01) * 100.0
        else:
            diff_pct = 0.0

        p_price_str = f"{p_price:.2f}" if p_price else "N/A"
        b_price_str = f"{b_price:.2f}"
        diff_str = f"{diff_pct:.2f}%"

        if p_dir != b_dir:
            status = "DIR-MISMATCH"
            price_divergence += 1
        elif diff_pct > args.price_tolerance * 5:
            status = "ERROR"
            price_divergence += 1
        elif diff_pct > args.price_tolerance:
            status = "WARN"
        else:
            status = "OK"
            matched += 1

        rows_out.append((sym, p_dir, b_dir, p_price_str, b_price_str, diff_str, status))

    _print_results(rows_out, args.session_id, args.trade_date)

    if _RICH:
        console = Console()
        console.print(
            f"\n[green]Matched: {matched}[/green]  "
            f"[yellow]Paper-only: {paper_only}  BT-only: {bt_only}[/yellow]  "
            f"[red]Errors: {price_divergence}[/red]"
        )
        if price_divergence == 0:
            console.print("[bold green]PASS[/bold green]")
        else:
            console.print("[bold red]FAIL[/bold red]")
    else:
        print(
            f"\nMatched: {matched}  Paper-only: {paper_only}  "
            f"BT-only: {bt_only}  Errors: {price_divergence}"
        )
        print("PASS" if price_divergence == 0 else "FAIL")

    return 1 if price_divergence > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
