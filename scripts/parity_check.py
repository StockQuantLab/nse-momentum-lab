#!/usr/bin/env python
"""Check parity between paper trading decisions and backtest signals for the same date.

Usage:
    doppler run -- uv run python scripts/parity_check.py --session-id <ID> --trade-date 2026-04-18
    doppler run -- uv run python scripts/parity_check.py \\
        --session-id <ID> --trade-date 2026-04-18 --exp-id 1716b78c208a90f3

    # Trades mode: compare closed positions exit prices and reasons vs bt_trade
    doppler run -- uv run python scripts/parity_check.py \\
        --session-id <ID> --mode trades

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

_COLUMNS_SIGNALS = ("SYMBOL", "PAPER-DIR", "BT-DIR", "PAPER-PRICE", "BT-PRICE", "DIFF%", "STATUS")
_COLUMNS_TRADES = (
    "SYMBOL", "DATE", "DIR", "ENTRY△%", "EXIT△%", "EXIT-PP", "EXIT-BT",
    "REASON-PP", "REASON-BT", "STATUS",
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--session-id", required=True, help="Paper session ID")
    p.add_argument("--trade-date", default=None, help="Trade date (YYYY-MM-DD); signals mode only")
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
    p.add_argument(
        "--mode",
        choices=["signals", "trades"],
        default="signals",
        help="signals (default): compare entry prices from paper_signals vs bt_trade. "
             "trades: compare closed paper_positions entry+exit prices and reasons vs bt_trade.",
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


def _print_table(
    rows_out: list[tuple[str, ...]],
    columns: tuple[str, ...],
    title: str,
) -> None:
    if _RICH:
        console = Console()
        tbl = Table(title=title)
        for col in columns:
            tbl.add_column(col)
        for row in rows_out:
            status = row[-1]
            if status == "OK":
                style = "green"
            elif status in ("PAPER-ONLY", "BT-ONLY"):
                style = "yellow"
            elif status in ("DIR-MISMATCH", "ERROR", "EXIT-MISMATCH"):
                style = "red"
            elif status in ("WARN", "REASON-MISMATCH"):
                style = "dark_orange"
            else:
                style = ""
            tbl.add_row(*row, style=style)
        console.print(tbl)
    else:
        hdr = "  ".join(f"{c:<15}" for c in columns)
        print(hdr)
        print("-" * len(hdr))
        for row in rows_out:
            print("  ".join(f"{v:<15}" for v in row))


def _print_summary(
    matched: int,
    paper_only: int,
    bt_only: int,
    divergences: int,
    extra: str = "",
) -> None:
    if _RICH:
        console = Console()
        console.print(
            f"\n[green]Matched: {matched}[/green]  "
            f"[yellow]Paper-only: {paper_only}  BT-only: {bt_only}[/yellow]  "
            f"[red]Divergences: {divergences}[/red]"
            + (f"  {extra}" if extra else "")
        )
        console.print("[bold green]PASS[/bold green]" if divergences == 0 else "[bold red]FAIL[/bold red]")
    else:
        print(
            f"\nMatched: {matched}  Paper-only: {paper_only}  "
            f"BT-only: {bt_only}  Divergences: {divergences}"
            + (f"  {extra}" if extra else "")
        )
        print("PASS" if divergences == 0 else "FAIL")


def _run_signals_mode(args: argparse.Namespace) -> int:
    if not args.trade_date:
        print("ERROR: --trade-date is required for signals mode", file=sys.stderr)
        return 1

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

    _print_table(rows_out, _COLUMNS_SIGNALS, f"Signals Parity — session={args.session_id}  date={args.trade_date}")
    _print_summary(matched, paper_only, bt_only, price_divergence)
    return 1 if price_divergence > 0 else 0


def _run_trades_mode(args: argparse.Namespace) -> int:
    """Compare closed paper_positions entry+exit prices and reasons vs bt_trade."""
    paper_conn = duckdb.connect(args.paper_db, read_only=True)
    paper_rows = paper_conn.execute(
        """
        SELECT
            symbol,
            direction,
            CAST(date_trunc('day', opened_at AT TIME ZONE 'Asia/Kolkata') AS DATE) AS entry_date,
            avg_entry,
            avg_exit,
            json_extract_string(metadata_json, '$.exit_reason') AS exit_reason
        FROM paper_positions
        WHERE session_id = ? AND closed_at IS NOT NULL
        ORDER BY entry_date, symbol
        """,
        [args.session_id],
    ).fetchall()
    paper_conn.close()

    # Build paper map keyed by (symbol, entry_date_str)
    paper_map: dict[tuple[str, str], tuple[str, float, float | None, str | None]] = {}
    for sym, direction, entry_date, avg_entry, avg_exit, exit_reason in paper_rows:
        key = (sym, str(entry_date))
        paper_map[key] = (
            str(direction or "LONG").upper(),
            float(avg_entry or 0),
            float(avg_exit) if avg_exit is not None else None,
            str(exit_reason) if exit_reason else None,
        )

    bt_conn = duckdb.connect(args.backtest_db, read_only=True)
    if args.exp_id:
        bt_where = "exp_id = ?"
        params_bt: list[str] = [args.exp_id]
    else:
        bt_where = "1=1"
        params_bt = []

    # Pull bt_trade for all (symbol, entry_date) combos present in paper
    if paper_map:
        symbols = list({k[0] for k in paper_map})
        dates = list({k[1] for k in paper_map})
        placeholders_sym = ", ".join("?" * len(symbols))
        placeholders_dt = ", ".join("?" * len(dates))
        bt_query = (
            f"SELECT symbol, CAST(entry_date AS VARCHAR), entry_price, exit_price, exit_reason "
            f"FROM bt_trade "
            f"WHERE ({bt_where}) "
            f"  AND symbol IN ({placeholders_sym}) "
            f"  AND CAST(entry_date AS VARCHAR) IN ({placeholders_dt})"
        )
        bt_rows = bt_conn.execute(bt_query, params_bt + symbols + dates).fetchall()
    else:
        bt_rows = []
    bt_conn.close()

    bt_map: dict[tuple[str, str], tuple[float, float | None, str | None]] = {}
    for sym, entry_date_str, entry_price, exit_price, exit_reason in bt_rows:
        key = (sym, str(entry_date_str))
        bt_map[key] = (
            float(entry_price or 0),
            float(exit_price) if exit_price is not None else None,
            str(exit_reason) if exit_reason else None,
        )

    matched = 0
    paper_only = 0
    bt_only = 0
    divergences = 0
    exit_mismatches = 0
    reason_mismatches = 0
    rows_out: list[tuple[str, ...]] = []

    for key in sorted(set(paper_map) | set(bt_map)):
        sym, entry_date_str = key
        in_paper = key in paper_map
        in_bt = key in bt_map

        if in_paper and not in_bt:
            paper_only += 1
            p_dir, p_entry, p_exit, p_reason = paper_map[key]
            rows_out.append((
                sym, entry_date_str, p_dir,
                "N/A", "N/A",
                f"{p_exit:.4f}" if p_exit is not None else "N/A",
                "N/A",
                p_reason or "N/A", "N/A",
                "PAPER-ONLY",
            ))
            continue

        if in_bt and not in_paper:
            bt_only += 1
            b_entry, b_exit, b_reason = bt_map[key]
            rows_out.append((
                sym, entry_date_str, "N/A",
                "N/A", "N/A",
                "N/A",
                f"{b_exit:.4f}" if b_exit is not None else "N/A",
                "N/A", b_reason or "N/A",
                "BT-ONLY",
            ))
            continue

        p_dir, p_entry, p_exit, p_reason = paper_map[key]
        b_entry, b_exit, b_reason = bt_map[key]

        entry_diff_pct = abs(p_entry - b_entry) / max(b_entry, 0.01) * 100.0 if b_entry else 0.0
        exit_diff_pct: float | None = None
        if p_exit is not None and b_exit is not None and b_exit != 0:
            exit_diff_pct = abs(p_exit - b_exit) / max(b_exit, 0.01) * 100.0

        entry_diff_str = f"{entry_diff_pct:.2f}%"
        exit_diff_str = f"{exit_diff_pct:.2f}%" if exit_diff_pct is not None else "N/A"
        p_exit_str = f"{p_exit:.4f}" if p_exit is not None else "N/A"
        b_exit_str = f"{b_exit:.4f}" if b_exit is not None else "N/A"

        has_entry_error = entry_diff_pct > args.price_tolerance * 5
        has_exit_error = exit_diff_pct is not None and exit_diff_pct > args.price_tolerance * 5
        has_reason_mismatch = bool(p_reason and b_reason and p_reason != b_reason)

        if has_entry_error or has_exit_error:
            status = "EXIT-MISMATCH" if has_exit_error else "ERROR"
            divergences += 1
            if has_exit_error:
                exit_mismatches += 1
        elif has_reason_mismatch:
            status = "REASON-MISMATCH"
            reason_mismatches += 1
        elif entry_diff_pct > args.price_tolerance or (exit_diff_pct and exit_diff_pct > args.price_tolerance):
            status = "WARN"
            matched += 1
        else:
            status = "OK"
            matched += 1

        rows_out.append((
            sym, entry_date_str, p_dir,
            entry_diff_str, exit_diff_str,
            p_exit_str, b_exit_str,
            p_reason or "N/A", b_reason or "N/A",
            status,
        ))

    _print_table(rows_out, _COLUMNS_TRADES, f"Trades Parity — session={args.session_id}")
    extra = f"Exit-mismatches: {exit_mismatches}  Reason-mismatches: {reason_mismatches}"
    _print_summary(matched, paper_only, bt_only, divergences, extra=extra)
    return 1 if divergences > 0 else 0


def main() -> int:
    args = _parse_args()
    if args.mode == "trades":
        return _run_trades_mode(args)
    return _run_signals_mode(args)


if __name__ == "__main__":
    sys.exit(main())
