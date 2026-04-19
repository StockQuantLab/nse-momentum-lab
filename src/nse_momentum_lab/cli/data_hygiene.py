"""Data Hygiene CLI -- detect, scan, and purge dead symbols + DQ issues.

Entry point: ``nseml-hygiene``

Usage::

    # Preview dead symbols, row counts, disk usage
    doppler run -- uv run nseml-hygiene --dry-run

    # Print dead symbol names only (for piping)
    doppler run -- uv run nseml-hygiene --list-dead

    # Execute purge (requires --confirm)
    doppler run -- uv run nseml-hygiene --purge --confirm

    # Quick data quality report (coverage, gaps, anomalies)
    doppler run -- uv run nseml-hygiene --report

    # Fast DQ refresh (coverage scan only, persist to DQ table)
    doppler run -- uv run nseml-hygiene --refresh

    # Full DQ refresh (all 11 scans, persist to DQ table)
    doppler run -- uv run nseml-hygiene --refresh --full

    # Trade-date readiness gate (exit 0 if clean, 1 if issues)
    doppler run -- uv run nseml-hygiene --date 2026-04-10

    # Bounded scan for a date window
    doppler run -- uv run nseml-hygiene --refresh --window-start 2025-01-01 --window-end 2025-03-31

    # Query stored active issues
    doppler run -- uv run nseml-hygiene
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from nse_momentum_lab.db.market_db import DUCKDB_FILE, PARQUET_DIR, get_market_db
from nse_momentum_lab.services.dq_scanner import run_fast_scan, run_full_scan
from nse_momentum_lab.services.kite.tradeable import (
    get_dead_symbol_stats,
    get_dead_symbols,
    get_duckdb_dead_row_counts,
    get_tradeable_symbols,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = PROJECT_ROOT / "data" / "raw" / "kite" / "reports"

# DuckDB tables to purge (symbol-scoped only; bt_* tables kept for historical analysis)
# Base tables only -- views (e.g. feat_daily) are derived from these and can't be DELETE'd directly
DUCKDB_PURGE_TABLES = [
    "feat_daily_core",
    "feat_intraday_core",
    "feat_2lynch_derived",
    "feat_event_core",
]


def _save_audit_trail(dead_symbols: set[str], report: dict[str, Any]) -> Path:
    """Snapshot dead symbol list + stats before any mutation."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"hygiene_{ts}.json"
    with path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    logger.info("Audit trail saved to %s", path)
    return path


def _format_size(size_bytes: int) -> str:
    if size_bytes >= 1 << 30:
        return f"{size_bytes / (1 << 30):.1f} GB"
    if size_bytes >= 1 << 20:
        return f"{size_bytes / (1 << 20):.1f} MB"
    return f"{size_bytes / 1e3:.0f} KB"


def _detect_dead() -> set[str]:
    """Detect dead symbols across both daily and 5min parquet dirs."""
    tradeable = get_tradeable_symbols()
    if not tradeable:
        raise SystemExit(
            "Could not load tradeable symbols. "
            "Instrument master may be missing or too small. "
            "Refresh with: kite.refresh_instruments()"
        )

    daily_dir = PARQUET_DIR / "daily"
    five_min_dir = PARQUET_DIR / "5min"
    dead_daily = get_dead_symbols(daily_dir, tradeable)
    dead_5min = get_dead_symbols(five_min_dir, tradeable, layout="5min")
    return dead_daily | dead_5min


def run_dry_run() -> int:
    """Preview dead symbols without mutating anything. Returns exit code."""
    dead_all = _detect_dead()
    if not dead_all:
        print("No dead symbols found. Data is clean!")
        return 0

    daily_dir = PARQUET_DIR / "daily"
    five_min_dir = PARQUET_DIR / "5min"
    dead_daily = get_dead_symbols(daily_dir, get_tradeable_symbols())
    dead_5min = get_dead_symbols(five_min_dir, get_tradeable_symbols(), layout="5min")

    print(f"\n{'=' * 70}")
    print("DATA HYGIENE -- DRY RUN")
    print(f"{'=' * 70}")
    print(f"Dead symbols: {len(dead_all):,}")
    print()

    # Parquet stats
    print(f"{'-' * 70}")
    print("PARQUET FILES")
    print(f"{'-' * 70}")
    print(f"  Daily dirs to remove: {len(dead_daily)}")
    print(f"  5-min dirs to remove: {len(dead_5min)}")

    daily_stats = get_dead_symbol_stats(daily_dir, dead_daily)
    five_min_stats = get_dead_symbol_stats(five_min_dir, dead_5min)

    daily_bytes = sum(s["dir_size_bytes"] for s in daily_stats)
    five_min_bytes = sum(s["dir_size_bytes"] for s in five_min_stats)

    print(f"  Daily space to free:  {_format_size(daily_bytes)}")
    print(f"  5-min space to free: {_format_size(five_min_bytes)}")
    print()

    # Top dead symbols by row count
    if daily_stats:
        print("  Top 10 dead daily symbols by rows:")
        for s in daily_stats[:10]:
            print(f"    {s['symbol']:20s} {s['row_count']:>8,d} rows  last: {s['last_date']}")
        print()

    # DuckDB stats
    print(f"{'-' * 70}")
    print("DUCKDB TABLES")
    print(f"{'-' * 70}")

    db_path = DUCKDB_FILE
    if db_path.exists():
        duckdb_counts = get_duckdb_dead_row_counts(db_path, dead_all, DUCKDB_PURGE_TABLES)
        total_rows = sum(duckdb_counts.values())
        print(f"  Tables to purge: {', '.join(DUCKDB_PURGE_TABLES)}")
        print(f"  Total dead rows:   {total_rows:,}")
        for table, count in duckdb_counts.items():
            if count > 0:
                print(f"    {table:30s} {count:>10,} rows")
    else:
        print(f"  DuckDB file not found: {db_path}")
        duckdb_counts = {}

    print()
    print("Run with --purge --confirm to execute.")
    return 0


def run_list_dead() -> int:
    """Print dead symbol names only (one per line, for piping)."""
    dead_all = _detect_dead()
    for symbol in sorted(dead_all):
        print(symbol)
    return 0


def run_purge(*, confirm: bool) -> int:
    """Execute purge: DuckDB first, then parquet. Returns exit code."""
    if not confirm:
        print("ERROR: --purge requires --confirm flag for safety.")
        print("Usage: nseml-hygiene --purge --confirm")
        return 1

    dead_all = _detect_dead()
    if not dead_all:
        print("No dead symbols found. Nothing to do.")
        return 0

    tradeable = get_tradeable_symbols()
    daily_dir = PARQUET_DIR / "daily"
    five_min_dir = PARQUET_DIR / "5min"
    dead_daily = get_dead_symbols(daily_dir, tradeable)
    dead_5min = get_dead_symbols(five_min_dir, tradeable, layout="5min")

    # Collect stats before mutation
    daily_stats = get_dead_symbol_stats(daily_dir, dead_daily)
    five_min_stats = get_dead_symbol_stats(five_min_dir, dead_5min)
    db_path = DUCKDB_FILE
    duckdb_counts = (
        get_duckdb_dead_row_counts(db_path, dead_all, DUCKDB_PURGE_TABLES)
        if db_path.exists()
        else {}
    )

    daily_bytes = sum(s["dir_size_bytes"] for s in daily_stats)
    five_min_bytes = sum(s["dir_size_bytes"] for s in five_min_stats)

    # Save audit trail BEFORE any mutation
    audit = {
        "timestamp": datetime.now(UTC).isoformat(),
        "tradeable_count": len(tradeable),
        "dead_count": len(dead_all),
        "dead_daily_count": len(dead_daily),
        "dead_5min_count": len(dead_5min),
        "dead_symbols": sorted(dead_all),
        "daily_bytes_freed": daily_bytes,
        "five_min_bytes_freed": five_min_bytes,
        "duckdb_rows_purged": duckdb_counts,
        "duckdb_tables": DUCKDB_PURGE_TABLES,
    }
    audit_path = _save_audit_trail(dead_all, audit)

    print(f"\n{'=' * 70}")
    print("DATA HYGIENE -- PURGE")
    print(f"{'=' * 70}")
    print(f"Audit trail: {audit_path}")

    # Step 1: Purge DuckDB tables FIRST (prevents broken views)
    print()
    print("[1/3] Purging DuckDB tables...")
    if db_path.exists() and duckdb_counts:
        total_to_purge = sum(duckdb_counts.values())
        if total_to_purge == 0:
            print("  No dead rows found in DuckDB tables.")
        else:
            import duckdb

            con = duckdb.connect(str(db_path))
            symbol_list = sorted(dead_all)
            placeholders = ",".join(["?"] * len(symbol_list))

            try:
                con.execute("BEGIN TRANSACTION")
                for table in DUCKDB_PURGE_TABLES:
                    count = duckdb_counts.get(table, 0)
                    if count > 0:
                        con.execute(
                            f"DELETE FROM {table} WHERE symbol IN ({placeholders})",
                            symbol_list,
                        )
                        print(f"  {table}: purged {count:,} rows")
                con.execute("COMMIT")
                print("  DuckDB purge committed successfully.")
            except Exception:
                con.execute("ROLLBACK")
                print("  ERROR: DuckDB purge failed, rolled back!")
                logger.exception("DuckDB purge failed")
                con.close()
                return 1
            finally:
                con.close()
    else:
        if not db_path.exists():
            print(f"  DuckDB file not found: {db_path} -- skipping")
        else:
            print("  No dead rows to purge in DuckDB.")

    # Step 2: Delete parquet directories
    print()
    print("[2/3] Removing parquet directories...")
    dirs_removed = 0
    bytes_freed = 0

    for symbol in sorted(dead_all):
        for subdir in [daily_dir, five_min_dir]:
            sym_dir = subdir / symbol
            if sym_dir.exists():
                dir_size = sum(f.stat().st_size for f in sym_dir.rglob("*") if f.is_file())
                try:
                    shutil.rmtree(sym_dir)
                    bytes_freed += dir_size
                    dirs_removed += 1
                except OSError as e:
                    print(f"  WARNING: Could not remove {sym_dir}: {e}")

    print(f"  Removed {dirs_removed} directories ({_format_size(bytes_freed)} freed)")

    # Step 3: Summary
    total_duckdb_rows = sum(duckdb_counts.values()) if duckdb_counts else 0
    print()
    print(f"{'=' * 70}")
    print("PURGE COMPLETE")
    print(f"  Parquet dirs removed: {dirs_removed}")
    print(f"  Space freed:       {_format_size(bytes_freed)}")
    print(f"  DuckDB rows purged: {total_duckdb_rows:,}")
    print(f"  Audit trail:       {audit_path}")
    print()
    print("Next steps:")
    print("  1. Rebuild feat_daily:   doppler run -- uv run nseml-build-features")
    print("  2. Verify backtest:      doppler run -- uv run nseml-backtest --list-strategies")
    print(f"{'=' * 70}")

    return 0


def run_report() -> int:
    """Quick data quality report: coverage, gaps, freshness, anomalies."""
    import time as _time

    import duckdb

    t0 = _time.monotonic()
    db_path = DUCKDB_FILE
    if not db_path.exists():
        print("ERROR: DuckDB file not found. Run ingestion first.")
        return 1

    con = duckdb.connect(str(db_path), read_only=True)

    # Register parquet views
    parquet_dir = PARQUET_DIR.resolve()
    daily_glob = str(parquet_dir / "daily" / "*" / "*.parquet").replace("\\", "/")
    fivemin_glob = str(parquet_dir / "5min" / "*" / "*.parquet").replace("\\", "/")

    has_daily = False
    has_5min = False
    try:
        con.execute(
            f"CREATE TEMP VIEW v_daily AS SELECT * FROM read_parquet('{daily_glob}', "
            f"hive_partitioning=false, union_by_name=true)"
        )
        has_daily = True
    except Exception:
        pass
    try:
        con.execute(
            f"CREATE TEMP VIEW v_5min AS SELECT * FROM read_parquet('{fivemin_glob}', "
            f"hive_partitioning=false, union_by_name=true)"
        )
        has_5min = True
    except Exception:
        pass

    print(f"\n{'=' * 70}")
    print("DATA QUALITY REPORT")
    print(f"{'=' * 70}")

    # --- 1. Symbol & Row Counts ---
    print(f"\n{'-' * 70}")
    print("COVERAGE")
    print(f"{'-' * 70}")

    if has_daily:
        r = con.execute(
            "SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR "
            "FROM v_daily"
        ).fetchone()
        if r is not None:
            print(f"  Daily:  {r[0]:>6,} symbols  {r[1]:>12,} rows  {r[2]} to {r[3]}")

    if has_5min:
        r = con.execute(
            "SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR "
            "FROM v_5min"
        ).fetchone()
        if r is not None:
            print(f"  5-Min:  {r[0]:>6,} symbols  {r[1]:>12,} rows  {r[2]} to {r[3]}")

    # --- 2. Feature Table Health ---
    print(f"\n{'-' * 70}")
    print("FEATURE TABLES")
    print(f"{'-' * 70}")

    for table in [
        "feat_daily_core",
        "feat_intraday_core",
        "feat_event_core",
        "feat_2lynch_derived",
    ]:
        try:
            r = con.execute(f"SELECT COUNT(DISTINCT symbol), COUNT(*) FROM {table}").fetchone()
            if r is None:
                continue
            # Check for missing symbols vs v_daily
            if has_daily:
                m = con.execute(
                    f"SELECT COUNT(DISTINCT symbol) FROM v_daily "
                    f"WHERE symbol NOT IN (SELECT DISTINCT symbol FROM {table})"
                ).fetchone()
                missing = m[0] if m else 0
                status = "OK" if missing == 0 else f"MISSING {missing}"
            else:
                status = "?"
            print(f"  {table:30s} {r[0]:>6,} symbols  {r[1]:>10,} rows  [{status}]")
        except Exception:
            print(f"  {table:30s} NOT BUILT")

    # --- 3. Freshness ---
    if has_daily:
        print(f"\n{'-' * 70}")
        print("FRESHNESS")
        print(f"{'-' * 70}")

        rows = con.execute("""
            WITH last_dates AS (
                SELECT symbol, MAX(date) AS last_date FROM v_daily GROUP BY symbol
            )
            SELECT
                CASE
                    WHEN CURRENT_DATE - last_date <= 7 THEN 'Fresh (<7d)'
                    WHEN CURRENT_DATE - last_date <= 30 THEN 'Recent (7-30d)'
                    WHEN CURRENT_DATE - last_date <= 90 THEN 'Stale (30-90d)'
                    ELSE 'Very Stale (>90d)'
                END AS bucket,
                COUNT(*) AS cnt
            FROM last_dates GROUP BY bucket ORDER BY MIN(CURRENT_DATE - last_date)
        """).fetchall()
        for bucket, cnt in rows:
            print(f"  {bucket:25s} {cnt:>6,} symbols")

    # --- 4. Top Gaps ---
    if has_daily:
        print(f"\n{'-' * 70}")
        print("TOP GAPS (> 5 calendar days)")
        print(f"{'-' * 70}")

        gaps = con.execute("""
            WITH symbol_dates AS (
                SELECT symbol, date,
                       LAG(date) OVER (PARTITION BY symbol ORDER BY date) AS prev_date
                FROM v_daily
            )
            SELECT symbol, prev_date::VARCHAR, date::VARCHAR,
                   DATEDIFF('day', prev_date, date) AS gap_days
            FROM symbol_dates
            WHERE prev_date IS NOT NULL AND DATEDIFF('day', prev_date, date) > 5
            ORDER BY gap_days DESC LIMIT 10
        """).fetchall()
        if gaps:
            for sym, start, end, days in gaps:
                print(f"  {sym:15s} {start} to {end}  ({days} days)")
        else:
            print("  No gaps > 5 calendar days found.")

    # --- 5. Anomalies ---
    if has_daily:
        print(f"\n{'-' * 70}")
        print("ANOMALIES")
        print(f"{'-' * 70}")

        anomalies = con.execute("""
            WITH priced AS (
                SELECT symbol, date, open, high, low, close, volume,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
                FROM v_daily
            )
            SELECT issue, COUNT(*) AS cnt FROM (
                SELECT 'OHLC Invalid (H<L)' AS issue FROM priced WHERE high < low
                UNION ALL
                SELECT 'Zero Volume' FROM priced WHERE volume = 0
                UNION ALL
                SELECT 'Extreme Move (>30%)' FROM priced
                WHERE prev_close IS NOT NULL AND ABS(close/NULLIF(prev_close,0)-1) > 0.30
                UNION ALL
                SELECT 'Zero/Negative Price' FROM priced WHERE close <= 0
            ) GROUP BY issue ORDER BY cnt DESC
        """).fetchall()
        if anomalies:
            for issue, cnt in anomalies:
                print(f"  {issue:30s} {cnt:>8,}")
        else:
            print("  No anomalies detected.")

    con.close()
    elapsed = _time.monotonic() - t0
    print(f"\n{'=' * 70}")
    print(f"Report completed in {elapsed:.1f}s")
    print(f"{'=' * 70}")
    return 0


def run_dq_refresh(
    *,
    full: bool = False,
    window_start: date | None = None,
    window_end: date | None = None,
) -> int:
    """Run DQ scans and persist results to the DQ issue table."""
    db = get_market_db()
    scan_fn = run_full_scan if full else run_fast_scan
    mode_label = "full" if full else "fast"

    print(f"Running {mode_label} DQ scan...")
    results = scan_fn(db.con, window_start=window_start, window_end=window_end)

    total_flagged = 0
    critical_count = 0
    for result in results:
        if result.symbols:
            db.upsert_data_quality_issues(
                symbols=result.symbols,
                issue_code=result.issue_code,
                details=result.details,
                severity=result.severity,
            )
            total_flagged += len(result.symbols)
            if result.severity == "CRITICAL":
                critical_count += len(result.symbols)
            print(
                f"  {result.issue_code:30s} {result.severity:10s} {len(result.symbols):>6,} symbols"
            )
        # Deactivate issues no longer detected by this scan
        deactivated = db.deactivate_data_quality_issue(
            issue_code=result.issue_code,
            keep_symbols=result.symbols if result.symbols else None,
        )
        if deactivated > 0:
            logger.info("Deactivated %d stale %s issues", deactivated, result.issue_code)

    print(f"\nDQ {mode_label} scan complete: {len(results)} checks, {total_flagged} issues flagged")
    if critical_count:
        print(f"  WARNING: {critical_count} CRITICAL issues found")
    return 0


def run_dq_date_gate(target_date: date) -> int:
    """Trade-date readiness gate: full scan for a specific date, exit 0/1."""
    db = get_market_db()
    results = run_full_scan(
        db.con,
        window_start=target_date,
        window_end=target_date,
    )

    print(f"\nTrade-date readiness gate: {target_date}")
    print(f"{'=' * 50}")

    has_critical = False
    total_issues = 0
    for result in results:
        if not result.symbols:
            continue
        total_issues += len(result.symbols)
        marker = "[CRITICAL]" if result.severity == "CRITICAL" else "[WARN]"
        if result.severity == "CRITICAL":
            has_critical = True
        print(f"  {marker} {result.issue_code}: {len(result.symbols)} symbols")
        for sym in result.symbols[:10]:
            print(f"    {sym}")
        if len(result.symbols) > 10:
            print(f"    ... and {len(result.symbols) - 10} more")

        db.upsert_data_quality_issues(
            symbols=result.symbols,
            issue_code=result.issue_code,
            details=result.details,
            severity=result.severity,
        )
        db.deactivate_data_quality_issue(
            issue_code=result.issue_code,
            keep_symbols=result.symbols,
        )

    if has_critical:
        print(f"\n[FAIL] {target_date} has critical DQ issues ({total_issues} total)")
        return 1
    if total_issues > 0:
        print(f"\n[WARN] {target_date} has warnings but no critical issues")
        return 0
    print(f"\n[OK] {target_date} passed all DQ checks")
    return 0


def run_query_issues(
    *,
    issue_code: str | None = None,
    severity: str | None = None,
) -> int:
    """Query and display stored active DQ issues."""
    db = get_market_db()
    issues = db.query_active_dq_issues(issue_code=issue_code, severity=severity)

    if issues.is_empty():
        print("No active DQ issues found.")
        return 0

    print(f"\n{'=' * 70}")
    print("ACTIVE DATA QUALITY ISSUES")
    print(f"{'=' * 70}")
    print(f"  Total: {len(issues)} issues\n")

    # Group by issue_code for summary
    summary = (
        issues.group_by("issue_code")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("severity").first().alias("severity"),
            ]
        )
        .sort("severity", "count", descending=[False, True])
    )

    print(f"  {'Issue Code':35s} {'Severity':10s} {'Count':>8s}")
    print(f"  {'-' * 55}")
    for row in summary.iter_rows(named=True):
        print(f"  {row['issue_code']:35s} {row['severity']:10s} {row['count']:>8,}")

    # Show per-symbol details if not too many
    if len(issues) <= 50:
        print(f"\n  {'Symbol':15s} {'Issue':30s} {'Severity':10s} Details")
        print(f"  {'-' * 80}")
        for row in issues.sort("severity", "symbol").iter_rows(named=True):
            details = row["details"][:40] if row["details"] else ""
            print(f"  {row['symbol']:15s} {row['issue_code']:30s} {row['severity']:10s} {details}")

    return 0


def run_acknowledge(
    issue_code: str | None = None,
    symbols: list[str] | None = None,
) -> int:
    """Mark active DQ issues as acknowledged (operator-reviewed)."""
    db = get_market_db()
    count = db.acknowledge_data_quality_issues(issue_code=issue_code, symbols=symbols)
    if count == 0:
        print("No unacknowledged issues matched the filter.")
        return 0
    filter_desc = []
    if issue_code:
        filter_desc.append(f"issue_code={issue_code}")
    if symbols:
        filter_desc.append(f"symbols={','.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
    filter_str = f" ({', '.join(filter_desc)})" if filter_desc else ""
    print(f"Acknowledged {count} DQ issue(s){filter_str}.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Data Hygiene -- detect dead symbols, purge, DQ scans, or run DQ report",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", help="Preview dead symbols (default)")
    group.add_argument(
        "--list-dead", action="store_true", help="Print dead symbol names only (for piping)"
    )
    group.add_argument(
        "--purge", action="store_true", help="Delete dead parquet dirs + purge DuckDB rows"
    )
    group.add_argument(
        "--report",
        action="store_true",
        help="Quick data quality report (coverage, gaps, anomalies)",
    )
    group.add_argument(
        "--refresh",
        action="store_true",
        help="Run DQ scans and persist results to issue table",
    )
    group.add_argument(
        "--date",
        type=str,
        help="Trade-date readiness gate (YYYY-MM-DD). Full scan, exit 0/1.",
    )
    group.add_argument(
        "--acknowledge",
        action="store_true",
        help="Mark active DQ issues as acknowledged (operator-reviewed)",
    )
    parser.add_argument(
        "--confirm", action="store_true", help="Required with --purge (safety gate)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="With --refresh: run all 11 scans (default: fast/coverage only)",
    )
    parser.add_argument(
        "--issue-code",
        type=str,
        help="Filter by issue code (used with --acknowledge or default query)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated symbols to acknowledge (used with --acknowledge)",
    )
    parser.add_argument(
        "--window-start",
        type=str,
        help="Scan window start date (YYYY-MM-DD, used with --refresh)",
    )
    parser.add_argument(
        "--window-end",
        type=str,
        help="Scan window end date (YYYY-MM-DD, used with --refresh)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s %(name)s: %(message)s")

    if args.date:
        target_date = date.fromisoformat(args.date)
        sys.exit(run_dq_date_gate(target_date))
    elif args.refresh:
        window_start = date.fromisoformat(args.window_start) if args.window_start else None
        window_end = date.fromisoformat(args.window_end) if args.window_end else None
        sys.exit(run_dq_refresh(full=args.full, window_start=window_start, window_end=window_end))
    elif args.report:
        sys.exit(run_report())
    elif args.acknowledge:
        symbols = (
            [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
            if args.symbols
            else None
        )
        sys.exit(run_acknowledge(issue_code=args.issue_code, symbols=symbols))
    elif args.purge:
        sys.exit(run_purge(confirm=args.confirm))
    elif args.list_dead:
        sys.exit(run_list_dead())
    else:
        # Default: query stored active issues
        sys.exit(run_query_issues())


if __name__ == "__main__":
    main()
