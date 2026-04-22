#!/usr/bin/env python
"""Prune old backtest experiments from backtest.duckdb.

Usage:
    doppler run -- uv run python scripts/prune_backtest_history.py              # dry-run: list all
    doppler run -- uv run python scripts/prune_backtest_history.py --dry-run    # explicit dry-run
    doppler run -- uv run python scripts/prune_backtest_history.py --delete EXP_ID1 EXP_ID2
    doppler run -- uv run python scripts/prune_backtest_history.py --older-than 2025-01-01

Canonical experiment IDs are NEVER deleted (hard-coded protection).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import duckdb

try:
    from rich.console import Console
    from rich.table import Table

    _RICH = True
except ImportError:
    _RICH = False

PROJECT_ROOT = Path(__file__).parent.parent
BACKTEST_DB_DEFAULT = PROJECT_ROOT / "data" / "backtest.duckdb"

# Full-preset canonical runs — never delete.
CANONICAL_EXP_IDS: frozenset[str] = frozenset(
    {
        "d245816e1d89e196",  # breakout 4%
        "f5bf9a6836901550",  # breakout 2%
        "f4a125fce62ddb24",  # breakdown 4%
        "be7958b0f79c3c1c",  # breakdown 2%
    }
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="List experiments without deleting (implicit when no action flag is given)",
    )
    p.add_argument(
        "--delete",
        nargs="+",
        metavar="EXP_ID",
        help="One or more experiment IDs to delete",
    )
    p.add_argument(
        "--older-than",
        metavar="DATE",
        help="Delete experiments created before this date (YYYY-MM-DD)",
    )
    p.add_argument(
        "--yes",
        action="store_true",
        default=False,
        help="Skip confirmation prompt",
    )
    p.add_argument(
        "--backtest-db",
        default=str(BACKTEST_DB_DEFAULT),
        help="Path to backtest.duckdb",
    )
    return p.parse_args()


def _list_experiments(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    return conn.execute(
        "SELECT exp_id, strategy_name, start_date, end_date, created_at "
        "FROM bt_experiment ORDER BY created_at DESC"
    ).fetchall()


def _print_experiments(experiments: list[tuple], to_delete: set[str]) -> None:
    columns = ("EXP_ID", "STRATEGY", "START", "END", "CREATED_AT", "FLAGS")
    if _RICH:
        console = Console()
        tbl = Table(title="Backtest Experiments")
        for col in columns:
            tbl.add_column(col)
        for exp_id, strategy, start, end, created_at in experiments:
            flags: list[str] = []
            if exp_id in CANONICAL_EXP_IDS:
                flags.append("[PROTECTED]")
            if exp_id in to_delete:
                flags.append("[WILL DELETE]")
            if exp_id in to_delete and exp_id not in CANONICAL_EXP_IDS:
                style = "red"
            elif exp_id in CANONICAL_EXP_IDS:
                style = "yellow"
            else:
                style = ""
            tbl.add_row(
                exp_id,
                strategy or "",
                str(start),
                str(end),
                str(created_at),
                " ".join(flags),
                style=style,
            )
        console.print(tbl)
    else:
        hdr = (
            f"{'EXP_ID':<20} {'STRATEGY':<25} {'START':<12}"
            f" {'END':<12} {'CREATED_AT':<26} FLAGS"
        )
        print(hdr)
        print("-" * len(hdr))
        for exp_id, strategy, start, end, created_at in experiments:
            flags = []
            if exp_id in CANONICAL_EXP_IDS:
                flags.append("[PROTECTED]")
            if exp_id in to_delete:
                flags.append("[WILL DELETE]")
            print(
                f"{exp_id:<20} {(strategy or ''):<25} {start!s:<12}"
                f" {end!s:<12} {created_at!s:<26} {' '.join(flags)}"
            )


def _delete_experiment(conn: duckdb.DuckDBPyConnection, exp_id: str) -> int:
    """Delete one experiment and its trades. Returns count of deleted trade rows."""
    result = conn.execute(
        "SELECT COUNT(*) FROM bt_trade WHERE exp_id = ?", [exp_id]
    ).fetchone()
    trade_count: int = result[0] if result else 0
    conn.execute("DELETE FROM bt_trade WHERE exp_id = ?", [exp_id])
    conn.execute("DELETE FROM bt_experiment WHERE exp_id = ?", [exp_id])
    return trade_count


def main() -> int:
    args = _parse_args()

    is_dry_run: bool = args.dry_run or (args.delete is None and args.older_than is None)
    conn = duckdb.connect(args.backtest_db, read_only=is_dry_run)
    experiments = _list_experiments(conn)
    known_ids = {e[0] for e in experiments}

    to_delete: set[str] = set()

    if args.delete:
        for eid in args.delete:
            if eid in CANONICAL_EXP_IDS:
                print(
                    f"WARNING: {eid} is a canonical experiment — skipping.",
                    file=sys.stderr,
                )
            elif eid not in known_ids:
                print(f"WARNING: {eid} not found in backtest.duckdb — skipping.", file=sys.stderr)
            else:
                to_delete.add(eid)

    if args.older_than:
        cutoff = date.fromisoformat(args.older_than)
        for exp_id, _strategy, _start, _end, created_at in experiments:
            if exp_id in CANONICAL_EXP_IDS:
                continue
            if hasattr(created_at, "date"):
                created_date: date = created_at.date()
            else:
                created_date = date.fromisoformat(str(created_at)[:10])
            if created_date < cutoff:
                to_delete.add(exp_id)

    _print_experiments(experiments, to_delete)

    if is_dry_run:
        if to_delete:
            print(f"\nDry-run: {len(to_delete)} experiment(s) would be deleted.")
        else:
            print(
                f"\n{len(experiments)} experiment(s) listed. "
                "Use --delete or --older-than to prune."
            )
        conn.close()
        return 0

    if not to_delete:
        print("Nothing to delete.")
        conn.close()
        return 0

    if not args.yes:
        answer = input(f"\nDelete {len(to_delete)} experiment(s)? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            conn.close()
            return 0

    total_trades = 0
    total_exps = 0
    for eid in sorted(to_delete):
        trades = _delete_experiment(conn, eid)
        total_trades += trades
        total_exps += 1
        print(f"Deleted {eid}: {trades} trade rows removed")

    print(f"\nDeleted {total_exps} experiment(s), {total_trades} trade rows total.")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
