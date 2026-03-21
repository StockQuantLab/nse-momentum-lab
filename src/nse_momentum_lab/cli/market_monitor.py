#!/usr/bin/env python3
"""Build and inspect the NSE Market Monitor materialization.

Usage:
    doppler run -- uv run nseml-market-monitor
    doppler run -- uv run nseml-market-monitor --force
    doppler run -- uv run nseml-market-monitor --status
"""

from __future__ import annotations

import argparse

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
    parser.add_argument("--force", action="store_true", help="Force rebuild of the monitor table")
    parser.add_argument("--status", action="store_true", help="Show status without rebuilding")
    args = parser.parse_args()

    db = get_market_db(read_only=args.status)

    print("=" * 60)
    print("NSE Market Monitor")
    print("=" * 60)

    if args.status:
        status = db.get_status()
        tables = status.get("tables", {})
        print("Status:")
        print(f"  data_source: {status.get('data_source')}")
        print(f"  dataset_hash: {status.get('dataset_hash')}")
        print(f"  feat_daily_core: {tables.get('feat_daily_core', 0):,} rows")
        print(f"  market_monitor_daily: {tables.get('market_monitor_daily', 0):,} rows")
        _print_latest_snapshot(db)
        return 0

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
