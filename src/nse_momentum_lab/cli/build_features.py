#!/usr/bin/env python3
"""
Build feature materialized tables in DuckDB.

Usage:
    nseml-build-features                      # Build all feature sets
    nseml-build-features --force --allow-full-rebuild  # Explicit full rebuild
    nseml-build-features --since 2026-03-23   # Incremental rebuild from a date
    nseml-build-features --feature-set daily_core  # Build specific feature set
    nseml-build-features --missing            # Smart: check ALL tables, upsert gaps
    nseml-build-features --status             # Show feature registry status
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date

from nse_momentum_lab.cli.rebuild_guards import require_full_rebuild_ack
from nse_momentum_lab.db.market_db import close_market_db, get_market_db
from nse_momentum_lab.features import get_feature_registry

# Configure logging so feature-module logger.info() calls are visible
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Tables to check for --missing, in dependency order
FEATURE_TABLES = ["feat_daily_core", "feat_intraday_core", "feat_2lynch_derived"]

# Source view each table depends on (for existence check)
TABLE_SOURCE_VIEW = {
    "feat_daily_core": "v_daily",
    "feat_intraday_core": "v_daily",  # needs both v_daily + v_5min, but symbols come from v_daily
    "feat_2lynch_derived": "v_daily",
}


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


def _detect_missing_per_table(db) -> dict[str, list[str]]:
    """Check each feature table for missing symbols. Returns {table: [symbols]}."""
    gaps: dict[str, list[str]] = {}
    for table in FEATURE_TABLES:
        source = TABLE_SOURCE_VIEW[table]
        try:
            db.con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            rows = db.con.execute(
                f"SELECT DISTINCT symbol FROM {source} "
                f"WHERE symbol NOT IN (SELECT DISTINCT symbol FROM {table}) "
                f"ORDER BY symbol"
            ).fetchall()
            missing = [r[0] for r in rows]
        except Exception:
            # Table doesn't exist — all source symbols are missing
            rows = db.con.execute(
                f"SELECT DISTINCT symbol FROM {source} ORDER BY symbol"
            ).fetchall()
            missing = [r[0] for r in rows]
        if missing:
            gaps[table] = missing
    return gaps


def _timed(label: str, fn):
    """Run fn(), print elapsed time, return result."""
    t0 = time.monotonic()
    result = fn()
    elapsed = time.monotonic() - t0
    if result is not None and isinstance(result, (int, float)):
        print(f"  [{elapsed:6.1f}s] {label}: {result:,} rows")
    else:
        print(f"  [{elapsed:6.1f}s] {label}: done")
    return result


def _print_source_summary(db) -> None:
    """Print a quick summary of source data before building."""
    try:
        row = db.con.execute("""
            SELECT COUNT(*)::BIGINT AS rows,
                   COUNT(DISTINCT symbol)::BIGINT AS symbols,
                   MIN(date)::VARCHAR AS min_date,
                   MAX(date)::VARCHAR AS max_date
            FROM v_daily
        """).fetchone()
        if row:
            print(f"  Source: {row[1]:,} symbols, {row[0]:,} daily rows ({row[2]} to {row[3]})")
    except Exception:
        pass


def _print_startup_banner(
    *,
    force: bool,
    allow_full_rebuild: bool,
    feature_set: str | None,
    since_date: date | None,
    legacy: bool,
    symbols: list[str] | None,
    missing: bool,
    year_start: int | None,
    year_end: int | None,
    db=None,
) -> None:
    """Print a concise startup banner before any long-running DuckDB work."""
    print("=" * 60, flush=True)
    print("Feature Build Starting", flush=True)
    print("=" * 60, flush=True)
    print(
        "Mode: "
        + (
            "legacy"
            if legacy
            else "missing"
            if missing
            else "symbol-level"
            if symbols is not None
            else "incremental"
            if since_date is not None
            else "feature-set"
            if feature_set
            else "full"
        ),
        flush=True,
    )
    if feature_set:
        print(f"Feature set: {feature_set}", flush=True)
    if since_date is not None:
        print(f"Since: {since_date.isoformat()}", flush=True)
    if symbols is not None:
        print(f"Symbols: {len(symbols)}", flush=True)
    if year_start is not None or year_end is not None:
        print(
            "Year bounds: "
            f"{year_start if year_start is not None else 'min'}"
            f"..{year_end if year_end is not None else 'max'}",
            flush=True,
        )
    print(f"Force: {force}", flush=True)
    print(f"Allow full rebuild: {allow_full_rebuild}", flush=True)
    print(
        "DuckDB env: "
        f"memory_limit={os.getenv('DUCKDB_MEMORY_LIMIT', 'default')}, "
        f"max_temp_directory_size={os.getenv('DUCKDB_MAX_TEMP_DIRECTORY_SIZE', 'default')}, "
        f"threads={os.getenv('DUCKDB_THREADS', 'default')}",
        flush=True,
    )
    print("=" * 60, flush=True)
    if db is not None:
        try:
            row = db.con.execute("""
                SELECT COUNT(*)::BIGINT AS rows,
                       COUNT(DISTINCT symbol)::BIGINT AS symbols
                FROM v_5min
            """).fetchone()
            if row:
                print(f"  Source: {row[1]:,} symbols, {row[0]:,} 5min rows")
        except Exception:
            pass


def _run_smart_missing(db) -> int:
    """Smart --missing: check each table independently, upsert only what's needed."""
    from nse_momentum_lab.features import create_legacy_feat_daily_view

    print("=" * 60)
    print("Smart Missing Symbol Detection")
    print("=" * 60)
    _print_source_summary(db)
    print()

    print("Scanning feature tables for gaps...")
    t_start = time.monotonic()
    gaps = _detect_missing_per_table(db)

    if not gaps:
        print("All feature tables are complete — no missing symbols.")
        return 0

    tables_with_gaps = len(gaps)
    total_missing = sum(len(s) for s in gaps.values())
    print(f"Found {total_missing} missing symbol(s) across {tables_with_gaps} table(s):\n")
    for table, syms in gaps.items():
        print(f"  {table}: {len(syms)} missing")

    # Build each table's missing symbols independently
    builders = {
        "feat_daily_core": lambda syms: db.build_feat_daily_core(symbols=syms),
        "feat_intraday_core": lambda syms: db.build_feat_intraday_core(symbols=syms),
        "feat_2lynch_derived": lambda syms: db.build_2lynch_derived(symbols=syms),
    }

    print()
    step = 0
    for table in FEATURE_TABLES:
        if table not in gaps:
            continue
        step += 1
        syms = gaps[table]
        builder = builders[table]
        print(f"[{step}/{tables_with_gaps}] {table} — {len(syms)} symbols...")
        _timed(f"{table}", lambda b=builder, s=syms: b(s))
        print()

    create_legacy_feat_daily_view(db.con)
    total = time.monotonic() - t_start
    print("=" * 60)
    print(f"Done in {total:.1f}s.")
    print("=" * 60)
    return 0


def _run_symbol_rebuild(db, symbols: list[str]) -> int:
    """Rebuild all feature tables for explicit symbol list."""
    from nse_momentum_lab.features import create_legacy_feat_daily_view

    print("=" * 60)
    print(f"Symbol-Level Feature Rebuild — {len(symbols)} symbol(s)")
    print("=" * 60)
    if len(symbols) <= 10:
        print(f"  Symbols: {', '.join(symbols)}")
    else:
        print(f"  Symbols: {', '.join(symbols[:5])} ... +{len(symbols) - 5} more")
    print()

    t_start = time.monotonic()
    print("[1/3] feat_daily_core...")
    _timed("feat_daily_core", lambda: db.build_feat_daily_core(symbols=symbols))
    print()
    print("[2/3] feat_intraday_core...")
    _timed("feat_intraday_core", lambda: db.build_feat_intraday_core(symbols=symbols))
    print()
    print("[3/3] feat_2lynch_derived...")
    _timed("feat_2lynch_derived", lambda: db.build_2lynch_derived(symbols=symbols))

    create_legacy_feat_daily_view(db.con)
    total = time.monotonic() - t_start
    print(f"\n{'=' * 60}")
    print(f"Done in {total:.1f}s.")
    print("=" * 60)
    return 0


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
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to rebuild features for (e.g. RELIANCE,TCS,INFY).",
    )
    parser.add_argument(
        "--symbols-file",
        type=str,
        default=None,
        dest="symbols_file",
        help="Path to a text file with one symbol per line.",
    )
    parser.add_argument(
        "--missing",
        action="store_true",
        help="Smart: check ALL feature tables independently, upsert only what's missing.",
    )
    parser.add_argument(
        "--year-start",
        type=int,
        default=None,
        help="Lower bound year for feat_intraday_core batched rebuilds.",
    )
    parser.add_argument(
        "--year-end",
        type=int,
        default=None,
        help="Upper bound year for feat_intraday_core batched rebuilds.",
    )

    args = parser.parse_args()
    since_date = getattr(args, "since", None)
    year_start = getattr(args, "year_start", None)
    year_end = getattr(args, "year_end", None)
    symbols: list[str] | None = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    elif args.symbols_file:
        from pathlib import Path

        p = Path(args.symbols_file)
        if not p.exists():
            print(f"ERROR: symbols file not found: {p}")
            return 1
        symbols = [line.strip().upper() for line in p.read_text().splitlines() if line.strip()]

    feature_set_arg = args.feature_set.lower() if args.feature_set else None
    if (year_start is not None or year_end is not None) and feature_set_arg not in {
        "intraday",
        "intraday_core",
    }:
        print("--year-start/--year-end can only be used with --feature-set intraday_core")
        return 1
    if year_start is not None and year_end is not None and year_start > year_end:
        print("--year-start must be <= --year-end")
        return 1

    # Handle --list
    if args.list:
        show_feature_list()
        return

    _print_startup_banner(
        force=args.force,
        allow_full_rebuild=args.allow_full_rebuild,
        feature_set=args.feature_set,
        since_date=since_date,
        legacy=args.legacy,
        symbols=symbols,
        missing=args.missing,
        year_start=year_start,
        year_end=year_end,
    )

    db = None
    try:
        # --missing: smart detection — check EACH feature table independently
        if args.missing:
            db = get_market_db()
            return _run_smart_missing(db)

        # Explicit --symbols / --symbols-file: rebuild all tables for those symbols
        if symbols is not None:
            if not symbols:
                print("No symbols to rebuild.")
                return 0
            db = get_market_db()
            return _run_symbol_rebuild(db, symbols)

        feature_set = feature_set_arg
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
            return 0

        t_start = time.monotonic()
        print("=" * 60)
        print("Building DuckDB feature tables")
        print("=" * 60)
        _print_source_summary(db)
        print()

        if args.legacy:
            print("Using legacy monolithic feat_daily...")
            _timed("feat_daily (legacy)", lambda: db.build_feat_daily_table(force=args.force))
        elif args.feature_set:
            # Build specific feature set(s)
            feature_set = resolved_feature_set

            if feature_set == "all" or feature_set is None:
                if incremental_mode:
                    print(
                        f"Incrementally building all feature sets since {since_date.isoformat()}..."
                    )
                    _timed(
                        "modular features",
                        lambda: db._build_modular_features(force=args.force, since_date=since_date),
                    )
                    _timed(
                        "market monitor",
                        lambda: db.build_market_monitor_incremental(
                            since_date=since_date, force=args.force
                        ),
                    )
                else:
                    print("Building all feature sets...")
                    _timed("modular features", lambda: db._build_modular_features(force=args.force))
            else:
                print(f"Building {feature_set}...")
                builders = {
                    "feat_daily_core": lambda: db.build_feat_daily_core(
                        force=args.force, since_date=since_date
                    ),
                    "feat_intraday_core": lambda: db.build_feat_intraday_core(
                        force=args.force,
                        since_date=since_date,
                        **(
                            {"year_start": year_start, "year_end": year_end}
                            if year_start is not None or year_end is not None
                            else {}
                        ),
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

                _timed(feature_set, builder)
        else:
            # Default: build all with modular approach
            if incremental_mode:
                print(f"Incrementally building all feature sets since {since_date.isoformat()}...")
                _timed(
                    "modular features",
                    lambda: db._build_modular_features(force=args.force, since_date=since_date),
                )
                _timed(
                    "market monitor",
                    lambda: db.build_market_monitor_incremental(
                        since_date=since_date, force=args.force
                    ),
                )
            else:
                print("Building all feature sets (modular approach)...")
                _timed("modular features", lambda: db._build_modular_features(force=args.force))

        total = time.monotonic() - t_start
        print(f"\nTotal build time: {total:.1f}s")
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
    finally:
        if db is not None:
            close_market_db()


if __name__ == "__main__":
    raise SystemExit(main())
