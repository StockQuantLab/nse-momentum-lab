from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.kite.parquet_repair import scan_5min_timestamp_alignment
from nse_momentum_lab.services.kite.scheduler import get_kite_scheduler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_ROOT / "data" / "raw" / "kite" / "reports"


@dataclass(slots=True)
class DatasetIssue:
    check: str
    message: str
    severity: str
    value: int
    status: str


def _build_issue(check: str, message: str, severity: str, value: int) -> DatasetIssue:
    return DatasetIssue(
        check=check,
        message=message,
        severity=severity,
        value=value,
        status="PASS" if value == 0 else "FAIL",
    )


def _business_days(start_date: date, end_date: date) -> int:
    days = 0
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            days += 1
        current = date.fromordinal(current.toordinal() + 1)
    return days


def _write_symbol_csv(path: Path, symbols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol"])
        for symbol in symbols:
            writer.writerow([symbol])


def _query_scalar(con, sql: str, params: list[Any]) -> int:
    row = con.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def _query_dataset_report(
    *,
    dataset: str,
    start_date: date,
    end_date: date,
    expected_symbols: list[str],
    include_duplicates: bool,
) -> dict[str, Any]:
    db = get_market_db(read_only=True)
    con = db.con
    view_name = "v_daily" if dataset == "daily" else "v_5min"

    base_where = "date BETWEEN ? AND ?"
    params = [start_date, end_date]
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            COUNT(DISTINCT date) AS days_covered
        FROM {view_name}
        WHERE {base_where}
        """,
        params,
    ).fetchone()
    rows = int(row[0] or 0) if row else 0
    symbol_count = int(row[1] or 0) if row else 0
    min_date = row[2] if row else None
    max_date = row[3] if row else None
    days_covered = int(row[4] or 0) if row else 0

    null_rows = _query_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM {view_name}
        WHERE {base_where}
          AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL)
        """,
        params,
    )
    ohlc_invalid_rows = _query_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM {view_name}
        WHERE {base_where}
          AND (
            high < low
            OR high < GREATEST(open, close)
            OR low > LEAST(open, close)
            OR volume < 0
          )
        """,
        params,
    )
    non_positive_price_rows = _query_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM {view_name}
        WHERE {base_where}
          AND (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0)
        """,
        params,
    )
    negative_volume_rows = _query_scalar(
        con,
        f"SELECT COUNT(*) FROM {view_name} WHERE {base_where} AND volume < 0",
        params,
    )

    duplicate_groups = 0
    duplicate_check_error = None
    duplicate_check_mode = "symbol_date" if dataset == "daily" else "exact_row"
    if include_duplicates:
        try:
            if dataset == "daily":
                duplicate_groups = _query_scalar(
                    con,
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT symbol, date
                        FROM {view_name}
                        WHERE {base_where}
                        GROUP BY 1, 2
                        HAVING COUNT(*) > 1
                    )
                    """,
                    params,
                )
            else:
                duplicate_groups = _query_scalar(
                    con,
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT symbol, date, candle_time, open, high, low, close, volume
                        FROM {view_name}
                        WHERE {base_where}
                        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
                        HAVING COUNT(*) > 1
                    )
                    """,
                    params,
                )
        except Exception as exc:
            duplicate_check_error = str(exc)
            duplicate_groups = -1
    else:
        duplicate_check_mode = "skipped"

    found_symbols = con.execute(
        f"SELECT DISTINCT symbol FROM {view_name} WHERE {base_where} ORDER BY symbol",
        params,
    ).fetchall()
    found_symbol_set = {str(row[0]).strip().upper() for row in found_symbols if row[0]}
    expected_symbol_set = {symbol.strip().upper() for symbol in expected_symbols if symbol.strip()}
    missing_symbols = sorted(expected_symbol_set - found_symbol_set)

    issues = [
        _build_issue("null_rows", "Rows with NULL OHLCV values", "ERROR", null_rows),
        _build_issue(
            "ohlc_invalid_rows",
            "Rows violating OHLC constraints or negative volume",
            "ERROR",
            ohlc_invalid_rows,
        ),
        _build_issue(
            "non_positive_price_rows",
            "Rows with non-positive OHLC prices",
            "ERROR",
            non_positive_price_rows,
        ),
        _build_issue(
            "negative_volume_rows",
            "Rows with negative volume",
            "ERROR",
            negative_volume_rows,
        ),
    ]
    if include_duplicates:
        issues.append(
            _build_issue(
                "duplicate_groups",
                f"Duplicate groups using mode '{duplicate_check_mode}'",
                "WARNING",
                max(duplicate_groups, 0),
            )
        )
    issues.append(
        _build_issue(
            "missing_symbols",
            "Expected symbols without data in selected range",
            "WARNING",
            len(missing_symbols),
        )
    )

    status = "PASS"
    if any(issue.status == "FAIL" and issue.severity == "ERROR" for issue in issues):
        status = "FAIL"
    elif any(issue.status == "FAIL" for issue in issues):
        status = "WARN"

    return {
        "dataset": dataset,
        "status": status,
        "rows": rows,
        "symbols": symbol_count,
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None,
        "days_covered": days_covered,
        "expected_symbols": len(expected_symbol_set),
        "expected_business_days": _business_days(start_date, end_date),
        "coverage_ratio": round(days_covered / max(_business_days(start_date, end_date), 1), 3),
        "null_rows": null_rows,
        "ohlc_invalid_rows": ohlc_invalid_rows,
        "non_positive_price_rows": non_positive_price_rows,
        "negative_volume_rows": negative_volume_rows,
        "duplicate_groups": max(duplicate_groups, 0),
        "duplicate_check_mode": duplicate_check_mode,
        "duplicate_check_error": duplicate_check_error,
        "missing_symbols_count": len(missing_symbols),
        "missing_symbols_sample": missing_symbols[:50],
        "issues": [asdict(issue) for issue in issues],
        "_missing_symbols": missing_symbols,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local Kite parquet data-quality report")
    parser.add_argument("--start-date", required=True, help="Range start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Range end date (YYYY-MM-DD)")
    parser.add_argument(
        "--no-duplicates",
        action="store_true",
        help="Skip duplicate-group scans for a faster report",
    )
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if start_date > end_date:
        raise SystemExit("--start-date must be on or before --end-date")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    expected_symbols = get_kite_scheduler().get_symbols_from_local_parquet()
    include_duplicates = not args.no_duplicates
    reports = [
        _query_dataset_report(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            expected_symbols=expected_symbols,
            include_duplicates=include_duplicates,
        )
        for dataset in ("daily", "5min")
    ]
    timestamp_issues = scan_5min_timestamp_alignment(PROJECT_ROOT / "data" / "parquet" / "5min")
    timestamp_issue_count = len(timestamp_issues)
    timestamp_issue_sample = [issue.as_dict() for issue in timestamp_issues[:50]]

    flattened_issues: list[dict[str, Any]] = []
    overall_status = "PASS"
    for report in reports:
        if report["status"] == "FAIL":
            overall_status = "FAIL"
        elif report["status"] == "WARN" and overall_status == "PASS":
            overall_status = "WARN"
        for issue in report["issues"]:
            flattened_issues.append(
                {
                    "dataset": report["dataset"],
                    **issue,
                }
            )

    timestamp_issue = _build_issue(
        "five_min_timestamp_alignment",
        "5-minute parquet files whose first candle_time is not 09:15 IST",
        "ERROR",
        timestamp_issue_count,
    )
    flattened_issues.append({"dataset": "lake", **asdict(timestamp_issue)})
    if timestamp_issue.status == "FAIL":
        overall_status = "FAIL"

    summary = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "include_duplicates": include_duplicates,
        "expected_universe_symbols": len(expected_symbols),
        "overall_status": overall_status,
        "timestamp_alignment": {
            "issue_count": timestamp_issue_count,
            "issue_sample": timestamp_issue_sample,
        },
        "issues": flattened_issues,
        "reports": [
            {key: value for key, value in report.items() if not key.startswith("_")}
            for report in reports
        ],
    }

    latest_path = REPORTS_DIR / "dq_summary_latest.json"
    snapshot_path = REPORTS_DIR / f"dq_summary_{start_date.isoformat()}_{end_date.isoformat()}.json"
    latest_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    snapshot_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    for report in reports:
        dataset = report["dataset"]
        missing_symbols = report.pop("_missing_symbols")
        missing_path = REPORTS_DIR / f"missing_symbols_{dataset}_{start_date}_{end_date}.csv"
        _write_symbol_csv(missing_path, missing_symbols)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
