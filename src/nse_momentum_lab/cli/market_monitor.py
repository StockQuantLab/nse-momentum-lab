#!/usr/bin/env python3
"""Build and inspect the NSE Market Monitor materialization.

Usage:
    doppler run -- uv run nseml-market-monitor
    doppler run -- uv run nseml-market-monitor --force --allow-full-rebuild
    doppler run -- uv run nseml-market-monitor --status
    doppler run -- uv run nseml-market-monitor --incremental
    doppler run -- uv run nseml-market-monitor --since 2025-03-10
"""

from __future__ import annotations

import argparse
from datetime import date

from nse_momentum_lab.cli.rebuild_guards import require_full_rebuild_ack
from nse_momentum_lab.db.market_db import get_market_db


def _print_latest_snapshot(db) -> None:
    latest = db.get_market_monitor_latest()
    if latest.is_empty():
        print("  [WARN] market_monitor_daily is empty")
        return

    row = latest.to_dicts()[0]
    print("  Latest snapshot:")
    print(f"    date: {row.get('trading_date')}")
    print(f"    primary_regime: {row.get('primary_regime')}")
    print(f"    tactical_regime: {row.get('tactical_regime')}")
    print(f"    posture: {row.get('posture_label')}")
    print(f"    aggression_score: {row.get('aggression_score')}")
    print(f"    universe_size: {row.get('universe_size')}")
    print(f"    t2108_equivalent_pct: {row.get('t2108_equivalent_pct')}")
    print(f"    ratio_10d: {row.get('ratio_10d')}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build or inspect the NSE Market Monitor")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild of the monitor table (requires --allow-full-rebuild)",
    )
    parser.add_argument(
        "--allow-full-rebuild",
        action="store_true",
        help="Acknowledge a destructive full monitor rebuild when used with --force.",
    )
    parser.add_argument("--status", action="store_true", help="Show status without rebuilding")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incrementally update from the latest existing date instead of full rebuild.",
    )
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        help="Incrementally rebuild from this date (YYYY-MM-DD), inclusive.",
    )
    args = parser.parse_args()
    since_date = getattr(args, "since", None)

    print("=" * 60)
    print("NSE Market Monitor")
    print("=" * 60)

    if args.status:
        db = get_market_db(read_only=True)
        status = db.get_status()
        tables = status.get("tables", {})
        print("Status:")
        print(f"  data_source: {status.get('data_source')}")
        print(f"  dataset_hash: {status.get('dataset_hash')}")
        print(f"  feat_daily_core: {tables.get('feat_daily_core', 0):,} rows")
        print(f"  market_monitor_daily: {tables.get('market_monitor_daily', 0):,} rows")
        _print_latest_snapshot(db)
        return 0

    if not args.incremental and not since_date:
        require_full_rebuild_ack(
            parser,
            force=args.force,
            allow_full_rebuild=args.allow_full_rebuild,
            operation="nseml-market-monitor",
            incremental_hint="nseml-market-monitor --incremental or --since YYYY-MM-DD",
        )

    db = get_market_db()

    if args.incremental or since_date:
        if since_date:
            print(f"Incremental update from {since_date.isoformat()}...")
        else:
            print("Incremental update from the latest existing date...")
        monitor_rows = db.build_market_monitor_incremental(since_date=since_date, force=args.force)
        print(f"  [OK] market_monitor_daily: {monitor_rows:,} rows")
    else:
        print("Building feat_daily_core if needed...")
        core_rows = db.build_feat_daily_core(force=args.force)
        print(f"  [OK] feat_daily_core: {core_rows:,} rows")

        print("Building market_monitor_daily...")
        monitor_rows = db.build_market_monitor_table(force=args.force)
        print(f"  [OK] market_monitor_daily: {monitor_rows:,} rows")

    _print_latest_snapshot(db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
