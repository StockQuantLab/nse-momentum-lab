#!/usr/bin/env python3
"""
Build feature materialized tables in DuckDB.

Usage:
    nseml-build-features                      # Build all feature sets
    nseml-build-features --force --allow-full-rebuild  # Explicit full rebuild
    nseml-build-features --since 2026-03-23   # Incremental rebuild from a date
    nseml-build-features --feature-set daily_core  # Build specific feature set
    nseml-build-features --status             # Show feature registry status
    nseml-build-features --legacy             # Use legacy monolithic feat_daily
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from nse_momentum_lab.cli.rebuild_guards import require_full_rebuild_ack
from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.features import get_feature_registry

logger = logging.getLogger(__name__)


# Feature set aliases
FEATURE_SET_ALIASES = {
    "daily": "feat_daily_core",
    "daily_core": "feat_daily_core",
    "intraday": "feat_intraday_core",
    "intraday_core": "feat_intraday_core",
    "event": "feat_event_core",
    "event_core": "feat_event_core",
    "2lynch": "feat_2lynch_derived",
    "lynch": "feat_2lynch_derived",
    "derived": "feat_2lynch_derived",
    "all": None,  # Special case - build all
}


def show_status(db) -> None:
    """Show feature registry and materialization status."""
    print("\n" + "=" * 60)
    print("Feature Registry Status")
    print("=" * 60)

    registry = get_feature_registry()

    # Group by layer
    by_layer: dict[str, list] = {
        "core": [],
        "event": [],
        "derived": [],
    }

    for feat_def in registry.list_all():
        if feat_def.layer in by_layer:
            by_layer[feat_def.layer].append(feat_def)

    for layer, features in by_layer.items():
        if not features:
            continue
        print(f"\n{layer.upper()} FEATURES:")
        for feat in features:
            # Check if materialized
            try:
                state = db.con.execute(
                    f"SELECT query_version, row_count FROM bt_materialization_state "
                    f"WHERE table_name = '{feat.name}'"
                ).fetchone()
                if state:
                    version, count = state
                    status_icon = "OK" if version == feat.version else "WARN"
                    print(f"  [{status_icon}] {feat.name:30s} v{feat.version} ({count:,} rows)")
                else:
                    print(f"  [ ] {feat.name:30s} v{feat.version} (not built)")
            except Exception:
                print(f"  [?] {feat.name:30s} v{feat.version} (error checking)")

    print("\n" + "=" * 60)


def show_feature_list() -> None:
    """Show available feature sets."""
    print("\n" + "=" * 60)
    print("Available Feature Sets")
    print("=" * 60)

    print("\nCore Features (strategy-agnostic):")
    print("  daily_core, daily       - Daily OHLCV + returns, volatility, trend, liquidity")
    print("  intraday_core, intraday - Intraday opening ranges, breakout times, FEE windows")
    print("\nEvent Features:")
    print("  event_core, event       - Earnings, corporate actions, post-event drift")
    print("\nDerived Features (strategy-specific):")
    print("  2lynch, lynch, derived  - 2LYNCH strategy filters and young breakout counters")
    print("\nUse 'all' to build everything, or specify individual feature sets.")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Build DuckDB feature tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild (requires --allow-full-rebuild)",
    )
    parser.add_argument(
        "--allow-full-rebuild",
        action="store_true",
        help="Acknowledge a destructive full rebuild when used with --force.",
    )
    parser.add_argument(
        "--feature-set",
        "-f",
        dest="feature_set",
        help="Specific feature set to build (daily_core, intraday_core, event_core, 2lynch, all)",
    )
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        help="Incrementally rebuild from this date (YYYY-MM-DD), inclusive.",
    )
    parser.add_argument("--status", "-s", action="store_true", help="Show feature registry status")
    parser.add_argument("--list", "-l", action="store_true", help="List available feature sets")
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Use legacy monolithic feat_daily (not recommended)",
    )

    args = parser.parse_args()
    since_date = getattr(args, "since", None)

    # Handle --list
    if args.list:
        show_feature_list()
        return

    feature_set = args.feature_set.lower() if args.feature_set else None
    resolved_feature_set = (
        FEATURE_SET_ALIASES.get(feature_set, feature_set) if feature_set else None
    )
    full_rebuild_requested = args.legacy or feature_set is None or resolved_feature_set == "all"
    incremental_mode = since_date is not None

    if not args.status and not incremental_mode and full_rebuild_requested:
        require_full_rebuild_ack(
            parser,
            force=args.force,
            allow_full_rebuild=args.allow_full_rebuild,
            operation="nseml-build-features",
            incremental_hint="nseml-build-features without --force",
        )

    db = get_market_db()

    # Handle --status
    if args.status:
        show_status(db)
        return

    print("Building DuckDB feature tables...\n")

    if args.legacy:
        print("Using legacy monolithic feat_daily...")
        db.build_feat_daily_table(force=args.force)
    elif args.feature_set:
        # Build specific feature set(s)
        feature_set = resolved_feature_set

        if feature_set == "all" or feature_set is None:
            if incremental_mode:
                print(f"Incrementally building all feature sets since {since_date.isoformat()}...")
                db._build_modular_features(force=args.force, since_date=since_date)
                db.build_market_monitor_incremental(since_date=since_date, force=args.force)
            else:
                print("Building all feature sets...")
                db._build_modular_features(force=args.force)
        else:
            print(f"Building {feature_set}...")
            builders = {
                "feat_daily_core": lambda: db.build_feat_daily_core(
                    force=args.force, since_date=since_date
                ),
                "feat_intraday_core": lambda: db.build_feat_intraday_core(
                    force=args.force, since_date=since_date
                ),
                "feat_event_core": lambda: db.build_feat_event_core(
                    force=args.force, since_date=since_date
                ),
                "feat_2lynch_derived": lambda: db.build_2lynch_derived(
                    force=args.force, since_date=since_date
                ),
            }
            builder = builders.get(feature_set)
            if builder is None:
                print(f"Unknown feature set: {args.feature_set}")
                show_feature_list()
                return 1

            rows = builder()
            print(f"[OK] Built {feature_set}: {rows:,} rows")
    else:
        # Default: build all with modular approach
        if incremental_mode:
            print(f"Incrementally building all feature sets since {since_date.isoformat()}...")
            db._build_modular_features(force=args.force, since_date=since_date)
            db.build_market_monitor_incremental(since_date=since_date, force=args.force)
        else:
            print("Building all feature sets (modular approach)...")
            db._build_modular_features(force=args.force)

    print("\n" + "=" * 60)
    print("Status:")
    status = db.get_status()
    for key, value in status.items():
        if key == "tables":
            for t, cnt in value.items():
                if cnt > 0:
                    print(f"  {t}: {cnt:,} rows")
        elif key != "features":
            print(f"  {key}: {value}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
