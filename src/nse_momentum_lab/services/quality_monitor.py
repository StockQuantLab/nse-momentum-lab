"""Automated Data Quality Monitoring and Alerts.

This module provides automated daily data quality checks:
- Null/missing data detection
- OHLC constraint violations
- Gap detection (missing trading days)
- Anomaly detection (extreme moves, volume spikes)
- Stale data alerts

Usage:
    # Run daily quality check
    doppler run -- uv run python -m nse_momentum_lab.services.quality_monitor

    # Run as part of pipeline
    doppler run -- uv run python -m nse_momentum_lab.services.quality_monitor --date 2024-01-15
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import func, or_, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    JobRun,
    MdOhlcvAdj,
    RefSymbol,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class QualityAlert:
    alert_type: str
    severity: str
    message: str
    affected_symbols: list[str] = field(default_factory=list)
    count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    check_date: date
    checks_run: int = 0
    alerts: list[QualityAlert] = field(default_factory=list)
    summary: dict[str, int] = field(default_factory=dict)

    def has_critical(self) -> bool:
        return any(a.severity == "CRITICAL" for a in self.alerts)

    def has_warnings(self) -> bool:
        return any(a.severity == "WARNING" for a in self.alerts)


class DataQualityMonitor:
    def __init__(self, check_date: date | None = None) -> None:
        self.check_date = check_date or date.today()
        self._sessionmaker = get_sessionmaker()

    async def run_all_checks(self) -> QualityReport:
        """Run all data quality checks."""
        report = QualityReport(check_date=self.check_date)

        logger.info(f"Running data quality checks for {self.check_date}")

        await self._check_null_prices(report)
        await self._check_ohlc_constraints(report)
        await self._check_gaps(report)
        await self._check_extreme_moves(report)
        await self._check_volume_anomalies(report)
        await self._check_stale_data(report)
        await self._check_symbol_coverage(report)

        report.checks_run = 8
        report.summary = {
            "critical": sum(1 for a in report.alerts if a.severity == "CRITICAL"),
            "warnings": sum(1 for a in report.alerts if a.severity == "WARNING"),
            "info": sum(1 for a in report.alerts if a.severity == "INFO"),
        }

        logger.info(
            f"Quality check complete: {report.summary['critical']} critical, "
            f"{report.summary['warnings']} warnings"
        )

        return report

    async def _check_null_prices(self, report: QualityReport) -> None:
        """Check for null/missing prices in adjusted data."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(MdOhlcvAdj.symbol_id, func.count())
                .where(
                    or_(
                        MdOhlcvAdj.open_adj.is_(None),
                        MdOhlcvAdj.high_adj.is_(None),
                        MdOhlcvAdj.low_adj.is_(None),
                        MdOhlcvAdj.close_adj.is_(None),
                    )
                )
                .where(MdOhlcvAdj.trading_date == self.check_date)
                .group_by(MdOhlcvAdj.symbol_id)
            )
            rows = result.all()

            if rows:
                symbol_ids = [r[0] for r in rows]
                sym_result = await session.execute(
                    select(RefSymbol.symbol).where(RefSymbol.symbol_id.in_(symbol_ids))
                )
                symbols = [s[0] for s in sym_result.all()]

                report.alerts.append(
                    QualityAlert(
                        alert_type="NULL_PRICES",
                        severity="CRITICAL" if len(symbols) > 10 else "WARNING",
                        message=f"{len(symbols)} symbols have null prices on {self.check_date}",
                        affected_symbols=symbols[:20],
                        count=len(symbols),
                        metadata={"date": self.check_date.isoformat()},
                    )
                )

    async def _check_ohlc_constraints(self, report: QualityReport) -> None:
        """Check OHLC price constraints."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(MdOhlcvAdj.symbol_id, func.count())
                .where(
                    or_(
                        MdOhlcvAdj.low_adj > func.least(MdOhlcvAdj.open_adj, MdOhlcvAdj.close_adj),
                        MdOhlcvAdj.high_adj
                        < func.greatest(MdOhlcvAdj.open_adj, MdOhlcvAdj.close_adj),
                    )
                )
                .where(MdOhlcvAdj.trading_date == self.check_date)
                .group_by(MdOhlcvAdj.symbol_id)
            )
            rows = result.all()

            if rows:
                symbol_ids = [r[0] for r in rows]
                sym_result = await session.execute(
                    select(RefSymbol.symbol).where(RefSymbol.symbol_id.in_(symbol_ids))
                )
                symbols = [s[0] for s in sym_result.all()]

                report.alerts.append(
                    QualityAlert(
                        alert_type="OHLC_VIOLATION",
                        severity="CRITICAL",
                        message=f"{len(symbols)} symbols have invalid OHLC on {self.check_date}",
                        affected_symbols=symbols[:10],
                        count=len(symbols),
                    )
                )

    async def _check_gaps(self, report: QualityReport) -> None:
        """Check for gaps in trading days."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(func.count(MdOhlcvAdj.trading_date.distinct())).where(
                    MdOhlcvAdj.trading_date <= self.check_date
                )
            )
            total_days = result.scalar() or 0

            expected_days = (self.check_date - date(2020, 1, 1)).days // 7 * 5
            if total_days < expected_days * 0.7:
                report.alerts.append(
                    QualityAlert(
                        alert_type="DATA_GAP",
                        severity="WARNING",
                        message=f"Only {total_days} trading days found, expected ~{expected_days}",
                        count=expected_days - total_days,
                    )
                )

    async def _check_extreme_moves(self, report: QualityReport) -> None:
        """Check for extreme price movements (>20%)."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(MdOhlcvAdj.symbol_id, MdOhlcvAdj.close_adj, MdOhlcvAdj.trading_date)
                .where(MdOhlcvAdj.trading_date == self.check_date)
                .where(MdOhlcvAdj.close_adj.isnot(None))
            )
            current = {r[0]: (r[1], r[2]) for r in result.all()}

            if not current:
                return

            result = await session.execute(
                select(MdOhlcvAdj.symbol_id, MdOhlcvAdj.close_adj)
                .where(
                    MdOhlcvAdj.symbol_id.in_(current.keys()),
                    MdOhlcvAdj.trading_date < self.check_date,
                )
                .order_by(MdOhlcvAdj.symbol_id, MdOhlcvAdj.trading_date.desc())
            )

            prev = {}
            for r in result.all():
                if r[0] not in prev:
                    prev[r[0]] = r[1]

            extreme = []
            for sym_id, (close, _) in current.items():
                if sym_id in prev and prev[sym_id] and close:
                    move = abs(close - prev[sym_id]) / prev[sym_id]
                    if move > 0.20:
                        extreme.append((sym_id, move * 100))

            if extreme:
                symbol_ids = [e[0] for e in extreme[:10]]
                sym_result = await session.execute(
                    select(RefSymbol.symbol).where(RefSymbol.symbol_id.in_(symbol_ids))
                )
                symbols = [s[0] for s in sym_result.all()]

                report.alerts.append(
                    QualityAlert(
                        alert_type="EXTREME_MOVE",
                        severity="WARNING",
                        message=f"{len(extreme)} symbols moved >20% on {self.check_date}",
                        affected_symbols=symbols,
                        count=len(extreme),
                    )
                )

    async def _check_volume_anomalies(self, report: QualityReport) -> None:
        """Check for volume spikes (>10x average)."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(MdOhlcvAdj.symbol_id, MdOhlcvAdj.volume)
                .where(MdOhlcvAdj.trading_date == self.check_date)
                .where(MdOhlcvAdj.volume.isnot(None))
            )
            current = {r[0]: r[1] for r in result.all() if r[1]}

            if not current:
                return

            avg_result = await session.execute(
                select(MdOhlcvAdj.symbol_id, func.avg(MdOhlcvAdj.volume))
                .where(
                    MdOhlcvAdj.symbol_id.in_(current.keys()),
                    MdOhlcvAdj.trading_date < self.check_date,
                    MdOhlcvAdj.trading_date >= self.check_date - timedelta(days=60),
                )
                .group_by(MdOhlcvAdj.symbol_id)
            )
            averages = dict(avg_result.all())

            spikes = []
            for sym_id, vol in current.items():
                if averages.get(sym_id):
                    if vol > averages[sym_id] * 10:
                        spikes.append(sym_id)

            if spikes:
                symbol_ids = spikes[:10]
                sym_result = await session.execute(
                    select(RefSymbol.symbol).where(RefSymbol.symbol_id.in_(symbol_ids))
                )
                symbols = [s[0] for s in sym_result.all()]

                report.alerts.append(
                    QualityAlert(
                        alert_type="VOLUME_SPIKE",
                        severity="INFO",
                        message=f"{len(spikes)} symbols have 10x average volume on {self.check_date}",
                        affected_symbols=symbols,
                        count=len(spikes),
                    )
                )

    async def _check_stale_data(self, report: QualityReport) -> None:
        """Check if data for the date exists."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(func.count(MdOhlcvAdj.symbol_id)).where(
                    MdOhlcvAdj.trading_date == self.check_date
                )
            )
            count = result.scalar() or 0

            if count == 0:
                report.alerts.append(
                    QualityAlert(
                        alert_type="NO_DATA",
                        severity="CRITICAL",
                        message=f"No data found for {self.check_date}",
                        count=0,
                    )
                )
            elif count < 500:
                report.alerts.append(
                    QualityAlert(
                        alert_type="LOW_COVERAGE",
                        severity="WARNING",
                        message=f"Only {count} symbols for {self.check_date} (expected ~2000)",
                        count=count,
                    )
                )

    async def _check_symbol_coverage(self, report: QualityReport) -> None:
        """Check overall symbol coverage."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            total = await session.execute(
                select(func.count(RefSymbol.symbol_id)).where(RefSymbol.status == "ACTIVE")
            )
            total_symbols = total.scalar() or 0

            result = await session.execute(
                select(func.count(MdOhlcvAdj.symbol_id.distinct())).where(
                    MdOhlcvAdj.trading_date == self.check_date
                )
            )
            covered = result.scalar() or 0

            if total_symbols > 0:
                coverage = covered / total_symbols
                if coverage < 0.5:
                    report.alerts.append(
                        QualityAlert(
                            alert_type="LOW_COVERAGE",
                            severity="CRITICAL",
                            message=f"Only {coverage * 100:.1f}% symbol coverage ({covered}/{total_symbols})",
                            count=covered,
                            metadata={"total": total_symbols, "covered": covered},
                        )
                    )
                elif coverage < 0.8:
                    report.alerts.append(
                        QualityAlert(
                            alert_type="LOW_COVERAGE",
                            severity="WARNING",
                            message=f"{coverage * 100:.1f}% symbol coverage ({covered}/{total_symbols})",
                            count=covered,
                        )
                    )

    async def store_alerts(self, report: QualityReport) -> None:
        """Store alerts in the database."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            for alert in report.alerts:
                job = JobRun(
                    job_name=f"quality_{alert.alert_type}",
                    job_kind="QUALITY_ALERT",
                    asof_date=self.check_date,
                    status="FAILED" if alert.severity == "CRITICAL" else "RUNNING",
                    metrics_json={
                        "alert_type": alert.alert_type,
                        "severity": alert.severity,
                        "message": alert.message,
                        "affected_count": alert.count,
                        "symbols": alert.affected_symbols,
                    },
                    error_json=alert.metadata if alert.severity == "CRITICAL" else {},
                )
                session.add(job)

            await session.commit()


def format_report(report: QualityReport) -> str:
    """Format the quality report as a string."""
    lines = [
        f"\n{'=' * 60}",
        f"DATA QUALITY REPORT - {report.check_date}",
        f"{'=' * 60}",
        f"Checks Run: {report.checks_run}",
        f"Alerts: {report.summary.get('critical', 0)} critical, "
        f"{report.summary.get('warnings', 0)} warnings, "
        f"{report.summary.get('info', 0)} info",
        f"{'=' * 60}",
    ]

    for alert in report.alerts:
        icon = (
            "🔴" if alert.severity == "CRITICAL" else "🟡" if alert.severity == "WARNING" else "🔵"
        )
        lines.append(f"\n{icon} [{alert.severity}] {alert.alert_type}")
        lines.append(f"   {alert.message}")
        if alert.affected_symbols:
            lines.append(f"   Symbols: {', '.join(alert.affected_symbols[:5])}")

    return "\n".join(lines)


async def main_async(check_date: date | None):
    monitor = DataQualityMonitor(check_date)
    report = await monitor.run_all_checks()

    print(format_report(report))

    if report.has_critical():
        print("\n⚠️ CRITICAL ISSUES DETECTED - Action required!")
        await monitor.store_alerts(report)
        sys.exit(1)
    elif report.has_warnings():
        print("\n⚠️ Warnings detected - Review recommended")
        await monitor.store_alerts(report)
    else:
        print("\n✅ All checks passed!")
        await monitor.store_alerts(report)


def main():
    parser = argparse.ArgumentParser(description="Data Quality Monitor")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to check (YYYY-MM-DD). Defaults to today.",
    )
    args = parser.parse_args()

    check_date = None
    if args.date:
        check_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    asyncio.run(main_async(check_date), loop_factory=asyncio.SelectorEventLoop)


if __name__ == "__main__":
    main()
