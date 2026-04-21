"""CLI command to prune backtest experiments from DuckDB and sync the dashboard replica.

Usage examples
--------------
# Preview what would be deleted (dry-run, keep only 4 canonical IDs):
nseml-backtest-cleanup --keep-only abc123 def456 ghi789 jkl012 --dry-run

# Delete all experiments EXCEPT the listed IDs, then sync replica:
nseml-backtest-cleanup --keep-only abc123 def456 ghi789 jkl012

# Delete specific experiments by ID:
nseml-backtest-cleanup --delete abc123 def456

# List all experiments currently in the DB:
nseml-backtest-cleanup --list
"""

from __future__ import annotations

import argparse
import sys

from nse_momentum_lab.db.market_db import get_backtest_db, get_backtest_replica_sync

# Tables that reference bt_experiment via exp_id (delete children first).
_CHILD_TABLES = [
    "bt_execution_diagnostic",
    "bt_trade",
    "bt_yearly_metric",
]
_PARENT_TABLE = "bt_experiment"


def _list_experiments(con) -> list[tuple[str, str, str]]:
    """Return [(exp_id, strategy_name, created_at), ...]  newest first."""
    return con.execute(
        "SELECT exp_id, strategy_name, created_at FROM bt_experiment ORDER BY created_at DESC"
    ).fetchall()


def _delete_experiments(con, exp_ids: list[str]) -> dict[str, int]:
    """Delete experiments and return row counts deleted per table."""
    counts: dict[str, int] = {}
    placeholders = ", ".join("?" * len(exp_ids))
    for table in _CHILD_TABLES:
        result = con.execute(f"DELETE FROM {table} WHERE exp_id IN ({placeholders})", exp_ids)
        counts[table] = result.fetchone()[0] if result else 0
    result = con.execute(f"DELETE FROM {_PARENT_TABLE} WHERE exp_id IN ({placeholders})", exp_ids)
    counts[_PARENT_TABLE] = result.fetchone()[0] if result else 0
    return counts


def _sync_replica(con) -> None:
    """Force-sync the backtest replica so the dashboard sees the updated data."""
    sync = get_backtest_replica_sync()
    sync.mark_dirty()
    sync.force_sync(source_conn=con)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prune backtest experiments from DuckDB and refresh the dashboard replica.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--list",
        action="store_true",
        help="List all experiments in the DB and exit.",
    )
    mode.add_argument(
        "--keep-only",
        nargs="+",
        metavar="EXP_ID",
        help="Delete every experiment NOT in this list.",
    )
    mode.add_argument(
        "--delete",
        nargs="+",
        metavar="EXP_ID",
        help="Delete only these specific experiment IDs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting.",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip the dashboard replica sync after deletion.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    db = get_backtest_db(read_only=args.list or args.dry_run)
    con = db.con

    all_experiments = _list_experiments(con)
    all_ids = {row[0] for row in all_experiments}

    # --- LIST mode ----------------------------------------------------------
    if args.list:
        if not all_experiments:
            print("No experiments found.")
            return
        print(f"{'exp_id':<20}  {'strategy':<20}  created_at")
        print("-" * 70)
        for exp_id, strategy, created_at in all_experiments:
            print(f"{exp_id:<20}  {strategy:<20}  {created_at}")
        return

    # --- Determine which IDs to delete --------------------------------------
    if args.keep_only:
        keep = set(args.keep_only)
        unknown_keep = keep - all_ids
        if unknown_keep:
            print(f"[WARN] These --keep-only IDs are not in the DB: {sorted(unknown_keep)}")
        to_delete = sorted(all_ids - keep)
    elif args.delete:
        to_delete = sorted(set(args.delete) & all_ids)
        unknown_del = set(args.delete) - all_ids
        if unknown_del:
            print(f"[WARN] These --delete IDs are not in the DB: {sorted(unknown_del)}")
    else:
        print("Nothing to do. Use --list, --keep-only, or --delete.")
        sys.exit(0)

    if not to_delete:
        print("Nothing to delete.")
        return

    # --- Preview ------------------------------------------------------------
    print(f"\nExperiments to delete ({len(to_delete)}):")
    exp_map = {row[0]: (row[1], row[2]) for row in all_experiments}
    for exp_id in to_delete:
        strategy, created_at = exp_map.get(exp_id, ("?", "?"))
        print(f"  {exp_id}  {strategy:<20}  {created_at}")

    if args.keep_only:
        keep_list = sorted(set(args.keep_only) & all_ids)
        print(f"\nExperiments to keep ({len(keep_list)}):")
        for exp_id in keep_list:
            strategy, created_at = exp_map.get(exp_id, ("?", "?"))
            print(f"  {exp_id}  {strategy:<20}  {created_at}")

    if args.dry_run:
        print("\n[DRY-RUN] No changes made.")
        return

    # --- Confirm & delete ---------------------------------------------------
    if sys.stdin.isatty():
        answer = input(f"\nDelete {len(to_delete)} experiment(s)? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            print("Aborted.")
            return

    # Re-open as read-write for the actual delete.
    if args.list or args.dry_run:
        db = get_backtest_db(read_only=False)
        con = db.con

    counts = _delete_experiments(con, to_delete)
    con.execute("CHECKPOINT")

    print("\nDeleted:")
    for table, n in counts.items():
        print(f"  {table}: {n} rows")

    # --- Replica sync -------------------------------------------------------
    if not args.no_sync:
        print("\nSyncing dashboard replica …")
        _sync_replica(con)
        print("Replica synced. Dashboard will see updated experiments.")
    else:
        print("\n[INFO] Skipped replica sync (--no-sync).")


if __name__ == "__main__":
    main()
