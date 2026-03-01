"""Data quality validation for OHLCV data ingestion.

This module provides comprehensive data quality checks to ensure
ingested data meets quality standards before being used for backtesting.

Quality Checks:
1. Missing data detection (nulls, gaps)
2. Price anomalies (outliers, unphysical values)
3. Volume anomalies (zero volume, extreme spikes)
4. OHLC consistency (high >= low, high >= open/close, etc.)
5. Data continuity (trading day gaps)
6. Corporate action detection (sudden price jumps)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class QualityIssueSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class QualityIssueType(StrEnum):
    MISSING_DATA = "MISSING_DATA"
    PRICE_ANOMALY = "PRICE_ANOMALY"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"
    OHLC_INVALID = "OHLC_INVALID"
    DATE_GAP = "DATE_GAP"
    CORPORATE_ACTION = "CORPORATE_ACTION"
    DUPLICATE_DATE = "DUPLICATE_DATE"
    NEGATIVE_VALUE = "NEGATIVE_VALUE"
    ZERO_PRICE = "ZERO_PRICE"
    EXTREME_MOVE = "EXTREME_MOVE"


@dataclass
class QualityIssue:
    issue_type: QualityIssueType
    severity: QualityIssueSeverity
    symbol: str
    trading_date: date | None
    description: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    symbol: str
    total_rows: int
    valid_rows: int
    issues: list[QualityIssue]
    passed: bool
    quality_score: float

    @property
    def critical_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == QualityIssueSeverity.CRITICAL)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == QualityIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == QualityIssueSeverity.WARNING)


@dataclass
class DataQualityConfig:
    max_price_change_pct: float = 0.30
    max_volume_spike_mult: float = 20.0
    min_price: float = 0.01
    max_price: float = 100000.0
    max_volume: int = 10_000_000_000
    allowed_gap_days: int = 5
    extreme_move_threshold: float = 0.50
    require_all_ohlc: bool = True


class DataQualityValidator:
    def __init__(self, config: DataQualityConfig | None = None) -> None:
        self.config = config or DataQualityConfig()

    def validate_symbol_data(
        self,
        symbol: str,
        rows: list[dict[str, Any]],
        trading_dates: set[date] | None = None,
    ) -> QualityReport:
        issues: list[QualityIssue] = []

        if not rows:
            return QualityReport(
                symbol=symbol,
                total_rows=0,
                valid_rows=0,
                issues=[
                    QualityIssue(
                        issue_type=QualityIssueType.MISSING_DATA,
                        severity=QualityIssueSeverity.CRITICAL,
                        symbol=symbol,
                        trading_date=None,
                        description="No data rows provided",
                    )
                ],
                passed=False,
                quality_score=0.0,
            )

        rows_by_date = {}
        for row in rows:
            row_date = row.get("trading_date")
            if row_date is None:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.MISSING_DATA,
                        severity=QualityIssueSeverity.ERROR,
                        symbol=symbol,
                        trading_date=None,
                        description="Row missing trading_date",
                        details=row,
                    )
                )
                continue
            if row_date in rows_by_date:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.DUPLICATE_DATE,
                        severity=QualityIssueSeverity.WARNING,
                        symbol=symbol,
                        trading_date=row_date,
                        description=f"Duplicate date: {row_date}",
                    )
                )
            rows_by_date[row_date] = row

        sorted_dates = sorted(rows_by_date.keys())
        for row_date in sorted_dates:
            row = rows_by_date[row_date]
            row_issues = self._validate_row(symbol, row_date, row)
            issues.extend(row_issues)

        if len(sorted_dates) > 1:
            gap_issues = self._check_date_gaps(symbol, sorted_dates)
            issues.extend(gap_issues)

        if len(sorted_dates) > 5:
            anomaly_issues = self._detect_anomalies(symbol, rows_by_date, sorted_dates)
            issues.extend(anomaly_issues)

        if trading_dates:
            continuity_issues = self._check_trading_continuity(symbol, sorted_dates, trading_dates)
            issues.extend(continuity_issues)

        critical_count = sum(1 for i in issues if i.severity == QualityIssueSeverity.CRITICAL)
        error_count = sum(1 for i in issues if i.severity == QualityIssueSeverity.ERROR)

        valid_rows = len(sorted_dates) - error_count
        quality_score = valid_rows / len(rows) if rows else 0.0

        passed = critical_count == 0 and error_count == 0

        return QualityReport(
            symbol=symbol,
            total_rows=len(rows),
            valid_rows=max(0, valid_rows),
            issues=issues,
            passed=passed,
            quality_score=quality_score,
        )

    def _validate_row(
        self,
        symbol: str,
        row_date: date,
        row: dict[str, Any],
    ) -> list[QualityIssue]:
        issues: list[QualityIssue] = []

        open_price = row.get("open")
        high_price = row.get("high")
        low_price = row.get("low")
        close_price = row.get("close")
        volume = row.get("volume")

        prices = [open_price, high_price, low_price, close_price]
        if self.config.require_all_ohlc and any(p is None for p in prices):
            issues.append(
                QualityIssue(
                    issue_type=QualityIssueType.MISSING_DATA,
                    severity=QualityIssueSeverity.ERROR,
                    symbol=symbol,
                    trading_date=row_date,
                    description="Missing OHLC values",
                    details={
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                    },
                )
            )

        for name, price in [
            ("open", open_price),
            ("high", high_price),
            ("low", low_price),
            ("close", close_price),
        ]:
            if price is not None:
                if price < self.config.min_price:
                    issues.append(
                        QualityIssue(
                            issue_type=QualityIssueType.ZERO_PRICE,
                            severity=QualityIssueSeverity.ERROR,
                            symbol=symbol,
                            trading_date=row_date,
                            description=f"{name} price {price} below minimum {self.config.min_price}",
                            details={name: price},
                        )
                    )
                if price > self.config.max_price:
                    issues.append(
                        QualityIssue(
                            issue_type=QualityIssueType.PRICE_ANOMALY,
                            severity=QualityIssueSeverity.WARNING,
                            symbol=symbol,
                            trading_date=row_date,
                            description=f"{name} price {price} above maximum {self.config.max_price}",
                            details={name: price},
                        )
                    )

        if all(
            isinstance(p, int | float) for p in [open_price, high_price, low_price, close_price]
        ):
            open_px = float(open_price)
            high_px = float(high_price)
            low_px = float(low_price)
            close_px = float(close_price)

            if high_px < low_px:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.OHLC_INVALID,
                        severity=QualityIssueSeverity.ERROR,
                        symbol=symbol,
                        trading_date=row_date,
                        description=f"High ({high_px}) < Low ({low_px})",
                        details={"high": high_px, "low": low_px},
                    )
                )

            max_oc = max(open_px, close_px)
            min_oc = min(open_px, close_px)

            if high_px < max_oc:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.OHLC_INVALID,
                        severity=QualityIssueSeverity.ERROR,
                        symbol=symbol,
                        trading_date=row_date,
                        description=f"High ({high_px}) < max(open, close) ({max_oc})",
                        details={"high": high_px, "open": open_px, "close": close_px},
                    )
                )

            if low_px > min_oc:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.OHLC_INVALID,
                        severity=QualityIssueSeverity.ERROR,
                        symbol=symbol,
                        trading_date=row_date,
                        description=f"Low ({low_px}) > min(open, close) ({min_oc})",
                        details={"low": low_px, "open": open_px, "close": close_px},
                    )
                )

        if volume is not None:
            if volume < 0:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.NEGATIVE_VALUE,
                        severity=QualityIssueSeverity.ERROR,
                        symbol=symbol,
                        trading_date=row_date,
                        description=f"Negative volume: {volume}",
                        details={"volume": volume},
                    )
                )
            elif volume == 0:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.VOLUME_ANOMALY,
                        severity=QualityIssueSeverity.INFO,
                        symbol=symbol,
                        trading_date=row_date,
                        description="Zero volume",
                        details={"volume": volume},
                    )
                )
            elif volume > self.config.max_volume:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.VOLUME_ANOMALY,
                        severity=QualityIssueSeverity.WARNING,
                        symbol=symbol,
                        trading_date=row_date,
                        description=f"Volume {volume} exceeds maximum {self.config.max_volume}",
                        details={"volume": volume},
                    )
                )

        return issues

    def _check_date_gaps(
        self,
        symbol: str,
        sorted_dates: list[date],
    ) -> list[QualityIssue]:
        issues: list[QualityIssue] = []

        for i in range(1, len(sorted_dates)):
            prev_date = sorted_dates[i - 1]
            curr_date = sorted_dates[i]
            gap_days = (curr_date - prev_date).days - 1

            if gap_days > self.config.allowed_gap_days:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.DATE_GAP,
                        severity=QualityIssueSeverity.WARNING,
                        symbol=symbol,
                        trading_date=curr_date,
                        description=f"Gap of {gap_days} days between {prev_date} and {curr_date}",
                        details={"gap_days": gap_days, "prev_date": prev_date.isoformat()},
                    )
                )

        return issues

    def _check_trading_continuity(
        self,
        symbol: str,
        sorted_dates: list[date],
        trading_dates: set[date],
    ) -> list[QualityIssue]:
        issues: list[QualityIssue] = []

        expected_dates = set(sorted_dates) & trading_dates
        missing_dates = expected_dates - set(sorted_dates)

        if missing_dates:
            missing_count = len(missing_dates)
            sample_missing = sorted(missing_dates)[:5]

            issues.append(
                QualityIssue(
                    issue_type=QualityIssueType.MISSING_DATA,
                    severity=QualityIssueSeverity.WARNING
                    if missing_count < 5
                    else QualityIssueSeverity.ERROR,
                    symbol=symbol,
                    trading_date=sample_missing[0] if sample_missing else None,
                    description=f"Missing {missing_count} trading dates",
                    details={"sample_missing": [d.isoformat() for d in sample_missing]},
                )
            )

        return issues

    def _detect_anomalies(
        self,
        symbol: str,
        rows_by_date: dict[date, dict[str, Any]],
        sorted_dates: list[date],
    ) -> list[QualityIssue]:
        issues: list[QualityIssue] = []

        closes = []
        volumes = []
        dates = []

        for d in sorted_dates:
            row = rows_by_date[d]
            close = row.get("close")
            volume = row.get("volume")
            if close is not None and close > 0:
                closes.append(close)
                volumes.append(volume or 0)
                dates.append(d)

        if len(closes) < 5:
            return issues

        closes_arr = np.array(closes)
        volumes_arr = np.array(volumes)

        returns = np.diff(closes_arr) / closes_arr[:-1]
        for i, ret in enumerate(returns):
            if abs(ret) > self.config.extreme_move_threshold:
                issues.append(
                    QualityIssue(
                        issue_type=QualityIssueType.EXTREME_MOVE,
                        severity=QualityIssueSeverity.WARNING,
                        symbol=symbol,
                        trading_date=dates[i + 1],
                        description=f"Extreme price move: {ret * 100:.1f}%",
                        details={
                            "return": ret,
                            "prev_close": closes_arr[i],
                            "close": closes_arr[i + 1],
                        },
                    )
                )

        if len(volumes_arr) > 10:
            median_vol = np.median(volumes_arr)
            if median_vol > 0:
                for i, vol in enumerate(volumes_arr):
                    if vol > median_vol * self.config.max_volume_spike_mult:
                        issues.append(
                            QualityIssue(
                                issue_type=QualityIssueType.VOLUME_ANOMALY,
                                severity=QualityIssueSeverity.INFO,
                                symbol=symbol,
                                trading_date=dates[i],
                                description=f"Volume spike: {vol:.0f} vs median {median_vol:.0f}",
                                details={"volume": vol, "median_volume": median_vol},
                            )
                        )

        return issues

    def generate_summary_report(
        self,
        reports: list[QualityReport],
    ) -> dict[str, Any]:
        if not reports:
            return {"total_symbols": 0, "passed": 0, "failed": 0, "issues": {}}

        passed = sum(1 for r in reports if r.passed)
        failed = len(reports) - passed

        issue_counts: dict[str, int] = {}
        for report in reports:
            for issue in report.issues:
                key = f"{issue.issue_type.value}:{issue.severity.value}"
                issue_counts[key] = issue_counts.get(key, 0) + 1

        avg_quality_score = sum(r.quality_score for r in reports) / len(reports)

        return {
            "total_symbols": len(reports),
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / len(reports),
            "avg_quality_score": avg_quality_score,
            "total_issues": sum(len(r.issues) for r in reports),
            "critical_issues": sum(r.critical_count for r in reports),
            "error_issues": sum(r.error_count for r in reports),
            "warning_issues": sum(r.warning_count for r in reports),
            "issue_breakdown": issue_counts,
        }


def validate_ingestion_batch(
    data: dict[str, list[dict[str, Any]]],
    trading_dates: set[date] | None = None,
    config: DataQualityConfig | None = None,
) -> tuple[list[QualityReport], dict[str, Any]]:
    validator = DataQualityValidator(config)
    reports: list[QualityReport] = []

    for symbol, rows in data.items():
        report = validator.validate_symbol_data(symbol, rows, trading_dates)
        reports.append(report)

    summary = validator.generate_summary_report(reports)

    return reports, summary
