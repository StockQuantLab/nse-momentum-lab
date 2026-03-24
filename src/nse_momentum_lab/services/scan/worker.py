from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import date

from sqlalchemy import case, delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db.models import (
    ScanDefinition,
    ScanResult,
    ScanRun,
)
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy
from nse_momentum_lab.services.dataset import (
    DatasetManifestRepository,
    build_code_hash,
    build_manifest_payload_from_snapshot,
)
from nse_momentum_lab.services.scan.features import FeatureEngine, PriceData
from nse_momentum_lab.services.scan.rules import ScanConfig, ScanRuleEngine

logger = logging.getLogger(__name__)


@dataclass
class ScanWorkerResult:
    scan_run_id: int
    status: str
    candidates_found: int = 0
    total_universe: int = 0
    filter_breakdown: dict[str, dict[str, int] | None] | None = None


class ScanWorker:
    DATASET_KIND = "duckdb_market_daily"

    def __init__(
        self,
        scan_def_id: int | None = None,
        config: ScanConfig | None = None,
        symbols: list[str] | None = None,
        strategy_name: str = "thresholdbreakout",
    ) -> None:
        self.scan_def_id = scan_def_id
        self.config = config or ScanConfig()
        self.symbols = [s.strip().upper() for s in symbols] if symbols else None
        # Registry-aware: currently only the canonical breakout family is supported for live scanning.
        # Multi-strategy live scanning is tracked in Phase 1 of the platform plan.
        self.strategy_name = strategy_name
        self._feature_engine = FeatureEngine()
        self._rule_engine = ScanRuleEngine(self.config)

    async def run(self, asof_date: date, *, force: bool = False) -> ScanWorkerResult:
        from nse_momentum_lab.db import get_sessionmaker

        sessionmaker = get_sessionmaker()

        async with sessionmaker() as session:
            scan_def_id = self.scan_def_id or await self._get_or_create_scan_def(session)

            dataset_hash = await self._compute_dataset_hash(session, asof_date)
            await self._upsert_dataset_manifest(session)

            # Idempotency: ScanRun has a unique constraint on (scan_def_id, asof_date, dataset_hash).
            # Reuse existing run to avoid unique-constraint failures during date-range scans.
            existing_result = await session.execute(
                select(ScanRun).where(
                    ScanRun.scan_def_id == scan_def_id,
                    ScanRun.asof_date == asof_date,
                    ScanRun.dataset_hash == dataset_hash,
                )
            )
            scan_run = existing_result.scalar_one_or_none()
            if scan_run is not None:
                if scan_run.status == "SUCCEEDED" and not force:
                    # Best-effort counts from existing results.
                    passed_result = await session.execute(
                        select(func.sum(case((ScanResult.passed.is_(True), 1), else_=0))).where(
                            ScanResult.scan_run_id == scan_run.scan_run_id
                        )
                    )
                    passed_count = passed_result.scalar() or 0
                    return ScanWorkerResult(
                        scan_run_id=scan_run.scan_run_id,
                        status="SUCCEEDED",
                        candidates_found=int(passed_count),
                        total_universe=len(self.symbols) if self.symbols else 0,
                    )

                if scan_run.status == "RUNNING" and not force:
                    return ScanWorkerResult(
                        scan_run_id=scan_run.scan_run_id,
                        status="RUNNING",
                        candidates_found=0,
                        total_universe=len(self.symbols) if self.symbols else 0,
                    )

                # Force (or retry) by clearing prior results.
                await session.execute(
                    delete(ScanResult).where(ScanResult.scan_run_id == scan_run.scan_run_id)
                )
                scan_run.status = "RUNNING"
                scan_run.started_at = func.now()
                scan_run.finished_at = None
            else:
                scan_run = ScanRun(
                    scan_def_id=scan_def_id,
                    asof_date=asof_date,
                    dataset_hash=dataset_hash,
                    status="RUNNING",
                    started_at=func.now(),
                )
                session.add(scan_run)
                try:
                    await session.flush()
                except IntegrityError:
                    await session.rollback()
                    existing_after_conflict = await session.execute(
                        select(ScanRun).where(
                            ScanRun.scan_def_id == scan_def_id,
                            ScanRun.asof_date == asof_date,
                            ScanRun.dataset_hash == dataset_hash,
                        )
                    )
                    scan_run = existing_after_conflict.scalar_one_or_none()
                    if scan_run is None:
                        raise

                    if scan_run.status == "SUCCEEDED" and not force:
                        passed_result = await session.execute(
                            select(func.sum(case((ScanResult.passed.is_(True), 1), else_=0))).where(
                                ScanResult.scan_run_id == scan_run.scan_run_id
                            )
                        )
                        passed_count = passed_result.scalar() or 0
                        return ScanWorkerResult(
                            scan_run_id=scan_run.scan_run_id,
                            status="SUCCEEDED",
                            candidates_found=int(passed_count),
                            total_universe=len(self.symbols) if self.symbols else 0,
                        )

                    if scan_run.status == "RUNNING" and not force:
                        return ScanWorkerResult(
                            scan_run_id=scan_run.scan_run_id,
                            status="RUNNING",
                            candidates_found=0,
                            total_universe=len(self.symbols) if self.symbols else 0,
                        )

                    await session.execute(
                        delete(ScanResult).where(ScanResult.scan_run_id == scan_run.scan_run_id)
                    )
                    scan_run.status = "RUNNING"
                    scan_run.started_at = func.now()
                    scan_run.finished_at = None

            try:
                universe = await self._load_universe(session)
                results: list[ScanResult] = []

                for symbol_id, symbol in universe:
                    candidates = await self._scan_symbol(
                        session,
                        scan_run.scan_run_id,
                        symbol_id,
                        symbol,
                        asof_date,
                    )
                    if candidates:
                        session.add_all(candidates)
                        results.extend(candidates)

                passed_count = sum(1 for r in results if r.passed)

                scan_run.status = "SUCCEEDED"
                scan_run.finished_at = func.now()

                await session.commit()

                return ScanWorkerResult(
                    scan_run_id=scan_run.scan_run_id,
                    status="SUCCEEDED",
                    candidates_found=passed_count,
                    total_universe=len(universe),
                )

            except Exception as e:
                logger.error(f"Scan failed: {e}")
                scan_run.status = "FAILED"
                scan_run.finished_at = func.now()
                await session.commit()
                raise

    async def _get_or_create_scan_def(self, session: AsyncSession) -> int:
        config_json = {
            "breakout_threshold": self.config.breakout_threshold,
            "close_pos_threshold": self.config.close_pos_threshold,
            "nr_percentile": self.config.nr_percentile,
            "min_r2_l": self.config.min_r2_l,
            "max_down_days_l": self.config.max_down_days_l,
            "atr_compress_ratio": self.config.atr_compress_ratio,
            "range_percentile": self.config.range_percentile,
            "range_ref_window": self.config.range_ref_window,
            "vol_dryup_ratio": self.config.vol_dryup_ratio,
            "lookback_high": self.config.lookback_high,
            "lookback_y": self.config.lookback_y,
            "lookback_l": self.config.lookback_l,
            "lookback_c": self.config.lookback_c,
        }

        strategy_def = resolve_strategy(self.strategy_name)
        scan_def_name = strategy_def.name
        scan_def_version = strategy_def.version

        if strategy_def.family not in ("threshold_breakout",):
            logger.warning(
                "ScanWorker: strategy '%s' uses FeatureEngine/ScanRuleEngine (threshold breakout path). "
                "DuckDB-based routing for '%s' is a Phase 1 enhancement.",
                self.strategy_name,
                strategy_def.family,
            )

        result = await session.execute(
            select(ScanDefinition).where(
                ScanDefinition.name == scan_def_name,
                ScanDefinition.version == scan_def_version,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            return existing.scan_def_id

        scan_def = ScanDefinition(
            name=scan_def_name,
            version=scan_def_version,
            config_json=config_json,
        )
        session.add(scan_def)
        await session.flush()
        return scan_def.scan_def_id

    async def _compute_dataset_hash(self, session, asof_date: date) -> str:
        from nse_momentum_lab.db.market_db import get_market_db

        db = get_market_db()
        snapshot = db.get_dataset_snapshot()
        daily = snapshot.get("daily", {})
        hash_input = (
            f"{asof_date.isoformat()}:"
            f"{snapshot.get('dataset_hash', '')}:"
            f"{daily.get('max_date')}:"
            f"{daily.get('rows')}"
        )
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    async def _upsert_dataset_manifest(self, session: AsyncSession) -> None:
        from nse_momentum_lab.db.market_db import get_market_db

        db = get_market_db()
        snapshot = db.get_dataset_snapshot()
        payload = build_manifest_payload_from_snapshot(
            dataset_kind=self.DATASET_KIND,
            snapshot=snapshot,
            code_hash=build_code_hash(
                "scan_worker",
                {
                    "breakout_threshold": self.config.breakout_threshold,
                    "lookback_high": self.config.lookback_high,
                    "lookback_l": self.config.lookback_l,
                    "lookback_c": self.config.lookback_c,
                },
            ),
            params_hash="default",
        )
        repo = DatasetManifestRepository()
        await repo.upsert(session, payload)

    async def _load_universe(self, session) -> list[tuple[int, str]]:
        from nse_momentum_lab.db.market_db import get_market_db

        db = get_market_db()
        symbols = db.get_available_symbols()
        return [(i + 1, s) for i, s in enumerate(symbols)]

    async def _scan_symbol(
        self,
        session,
        scan_run_id: int,
        symbol_id: int,
        symbol: str,
        asof_date: date,
    ) -> list[ScanResult]:
        from nse_momentum_lab.db.market_db import get_market_db

        db = get_market_db()
        end_date = asof_date.isoformat()
        start_date = (date(asof_date.year - 2, asof_date.month, asof_date.day)).isoformat()

        df = db.query_daily(symbol, start_date, end_date)

        if df.is_empty() or len(df) < 65:
            return []

        df = df.sort("date")
        rows = df.to_dicts()

        prices = [
            PriceData(
                trading_date=r["date"],
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=r["volume"],
                value_traded=r.get("value_traded"),
            )
            for r in rows
        ]

        features = self._feature_engine.compute_all(symbol_id, prices)

        candidates = self._rule_engine.run_scan(symbol_id, symbol, features, asof_date)

        scan_results = []
        for candidate in candidates:
            # Convert reason_json to dict to ensure JSON serializability
            reason_dict = candidate.reason_json if isinstance(candidate.reason_json, dict) else {}

            sr = ScanResult(
                scan_run_id=scan_run_id,
                symbol_id=candidate.symbol_id,
                asof_date=asof_date,
                score=candidate.score,
                passed=candidate.passed,
                reason_json=reason_dict,
            )
            scan_results.append(sr)

        return scan_results


if __name__ == "__main__":
    import sys
    from datetime import datetime

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) != 2:
        print("Usage: python -m nse_momentum_lab.services.scan.worker <YYYY-MM-DD>")
        sys.exit(1)

    asof_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    worker = ScanWorker()
    result = asyncio.run(
        worker.run(asof_date),
        loop_factory=asyncio.SelectorEventLoop,
    )
    print(
        f"Scan run {result.scan_run_id}: {result.status}, {result.candidates_found}/{result.total_universe} passed"
    )
