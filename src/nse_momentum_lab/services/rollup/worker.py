from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db.models import (
    BtTrade,
    ExpRun,
    RptBtDaily,
    RptBtFailureDaily,
    RptScanDaily,
    ScanDefinition,
    ScanResult,
    ScanRun,
)

logger = logging.getLogger(__name__)


@dataclass
class RollupResult:
    asof_date: date
    scan_rollup: dict[str, Any] | None
    bt_rollup: list[dict[str, Any]] | None
    failure_rollup: list[dict[str, Any]] | None


class DailyRollupWorker:
    def __init__(self, scan_def_id: int | None = None) -> None:
        self._scan_def_id = scan_def_id

    async def run(self, asof_date: date) -> RollupResult:
        from nse_momentum_lab.db import get_sessionmaker

        sessionmaker = get_sessionmaker()

        async with sessionmaker() as session:
            scan_def_id = self._scan_def_id or await self._get_default_scan_def_id(session)
            scan_result = await self._compute_scan_rollup(session, asof_date, scan_def_id)
            bt_result = await self._compute_bt_rollup(session, asof_date)
            failure_result = await self._compute_failure_rollup(session, asof_date)

            return RollupResult(
                asof_date=asof_date,
                scan_rollup=scan_result,
                bt_rollup=bt_result,
                failure_rollup=failure_result,
            )

    async def _get_default_scan_def_id(self, session: AsyncSession) -> int:
        result = await session.execute(
            select(ScanDefinition.scan_def_id).order_by(ScanDefinition.created_at.desc()).limit(1)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise ValueError("No scan definitions found. Please run a scan first.")
        return row

    async def _compute_scan_rollup(
        self, session: AsyncSession, asof_date: date, scan_def_id: int
    ) -> dict[str, Any] | None:
        result = await session.execute(
            select(
                func.count(ScanResult.symbol_id).label("total_universe"),
                func.sum(case((ScanResult.passed.is_(True), 1), else_=0)).label("passed"),
            )
            .select_from(ScanResult)
            .where(
                ScanResult.scan_run_id.in_(
                    select(ScanRun.scan_run_id).where(
                        ScanRun.scan_def_id == scan_def_id,
                        ScanRun.asof_date == asof_date,
                    )
                )
            )
        )
        row = result.one()

        total = row.total_universe or 0
        passed = row.passed or 0

        by_fail_reason = {}
        fail_result = await session.execute(
            select(ScanResult.reason_json)
            .select_from(ScanResult)
            .where(
                ScanResult.scan_run_id.in_(
                    select(ScanRun.scan_run_id).where(
                        ScanRun.scan_def_id == scan_def_id,
                        ScanRun.asof_date == asof_date,
                    )
                ),
                ~ScanResult.passed,
            )
        )
        for row in fail_result.all():
            checks = row[0].get("checks", [])
            for check in checks:
                if not check.get("passed"):
                    letter = check.get("letter", "UNKNOWN")
                    by_fail_reason[letter] = by_fail_reason.get(letter, 0) + 1

        run_result = await session.execute(
            select(ScanRun).where(
                ScanRun.scan_def_id == scan_def_id,
                ScanRun.asof_date == asof_date,
            )
        )
        scan_run = run_result.scalar_one_or_none()
        dataset_hash = scan_run.dataset_hash if scan_run else ""

        rollup = RptScanDaily(
            asof_date=asof_date,
            scan_def_id=scan_def_id,
            dataset_hash=dataset_hash,
            total_universe=total,
            passed_base_4p=passed,
            passed_2lynch=passed,
            passed_final=passed,
            by_fail_reason=by_fail_reason,
            by_liquidity_bucket={},
        )
        session.add(rollup)
        await session.commit()

        return {
            "total_universe": total,
            "passed": passed,
            "by_fail_reason": by_fail_reason,
        }

    async def _compute_bt_rollup(
        self, session: AsyncSession, asof_date: date
    ) -> list[dict[str, Any]] | None:
        result = await session.execute(
            select(
                BtTrade.entry_mode,
                func.count(BtTrade.trade_id).label("entries"),
                func.sum(case((BtTrade.pnl > 0, 1), else_=0)).label("wins"),
                func.avg(BtTrade.pnl_r).label("avg_r"),
            )
            .select_from(BtTrade)
            .where(BtTrade.entry_date == asof_date)
            .group_by(BtTrade.entry_mode)
        )
        rows = result.all()

        strategy_names_by_mode: dict[str, str] = {}

        for row in rows:
            entry_mode = row.entry_mode
            if entry_mode and entry_mode not in strategy_names_by_mode:
                exp_result = await session.execute(
                    select(ExpRun.strategy_name)
                    .where(ExpRun.trades.any(BtTrade.entry_mode == entry_mode))
                    .limit(1)
                )
                strategy_name = exp_result.scalar_one_or_none() or "UNKNOWN"
                strategy_names_by_mode[entry_mode] = strategy_name

        for row in rows:
            entry_mode = row.entry_mode
            entries = row.entries or 0
            wins = row.wins or 0
            avg_r = row.avg_r or 0

            win_rate = wins / entries if entries > 0 else 0
            strategy_name = strategy_names_by_mode.get(entry_mode, "UNKNOWN")

            rollup = RptBtDaily(
                asof_date=asof_date,
                strategy_name=strategy_name,
                dataset_hash="",
                entry_mode=entry_mode,
                signals=0,
                entries=entries,
                exits=entries,
                wins=wins,
                losses=entries - wins,
                win_rate=win_rate,
                avg_r=avg_r,
                profit_factor=1.0,
                max_dd=0.0,
            )
            session.add(rollup)

        await session.commit()

        return [
            {
                "entry_mode": row.entry_mode,
                "entries": row.entries or 0,
                "wins": row.wins or 0,
                "win_rate": (row.wins or 0) / (row.entries or 1),
                "avg_r": row.avg_r or 0,
            }
            for row in rows
        ]

    async def _compute_failure_rollup(
        self, session: AsyncSession, asof_date: date
    ) -> list[dict[str, Any]] | None:
        result = await session.execute(
            select(
                BtTrade.entry_mode,
                BtTrade.exit_reason,
                func.count(BtTrade.trade_id).label("count"),
                func.avg(BtTrade.pnl_r).label("avg_r"),
            )
            .select_from(BtTrade)
            .where(
                BtTrade.entry_date == asof_date,
                BtTrade.pnl < 0,
            )
            .group_by(BtTrade.entry_mode, BtTrade.exit_reason)
        )
        rows = result.all()

        strategy_names_by_mode: dict[str, str] = {}

        for row in rows:
            entry_mode = row.entry_mode
            if entry_mode and entry_mode not in strategy_names_by_mode:
                exp_result = await session.execute(
                    select(ExpRun.strategy_name)
                    .where(ExpRun.trades.any(BtTrade.entry_mode == entry_mode))
                    .limit(1)
                )
                strategy_name = exp_result.scalar_one_or_none() or "UNKNOWN"
                strategy_names_by_mode[entry_mode] = strategy_name

        for row in rows:
            if row.exit_reason is None:
                continue

            strategy_name = strategy_names_by_mode.get(row.entry_mode, "UNKNOWN")

            rollup = RptBtFailureDaily(
                asof_date=asof_date,
                strategy_name=strategy_name,
                dataset_hash="",
                entry_mode=row.entry_mode,
                exit_reason=row.exit_reason,
                count=row.count or 0,
                avg_r=row.avg_r,
                median_r=row.avg_r,
            )
            session.add(rollup)

        await session.commit()

        return [
            {
                "entry_mode": row.entry_mode,
                "exit_reason": row.exit_reason,
                "count": row.count or 0,
                "avg_r": row.avg_r or 0,
            }
            for row in rows
        ]


async def run_daily_rollup(asof_date: date, scan_def_id: int | None = None) -> RollupResult:
    worker = DailyRollupWorker(scan_def_id=scan_def_id)
    return await worker.run(asof_date)
