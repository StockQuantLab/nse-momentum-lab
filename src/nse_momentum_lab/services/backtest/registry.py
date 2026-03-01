from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db.models import (
    BtTrade,
    ExpMetric,
    ExpRun,
)

logger = logging.getLogger(__name__)


@dataclass
class ExperimentResult:
    exp_run_id: int
    exp_hash: str
    status: str
    metrics: dict[str, float]
    trades: list[dict[str, Any]]


class ExperimentRegistry:
    def __init__(self) -> None:
        pass

    async def register_and_run(
        self,
        strategy_name: str,
        params: dict[str, Any],
        signals: list[tuple[date, int, str, float, dict]],
        price_data: dict[int, dict[date, dict[str, float]]],
        value_traded_inr: dict[int, float],
        code_sha: str,
        dataset_hash: str,
    ) -> ExperimentResult:
        from nse_momentum_lab.db import get_sessionmaker

        strategy_hash = self._compute_strategy_hash(strategy_name, params)
        exp_hash = self._compute_exp_hash(strategy_hash, dataset_hash)

        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            exp_run = ExpRun(
                exp_hash=exp_hash,
                strategy_name=strategy_name,
                strategy_hash=strategy_hash,
                dataset_hash=dataset_hash,
                params_json=params,
                code_sha=code_sha,
                status="RUNNING",
            )
            session.add(exp_run)
            await session.flush()

            try:
                result_open = self._run_and_store(
                    session,
                    strategy_name,
                    "open",
                    signals,
                    price_data,
                    value_traded_inr,
                    exp_run.exp_run_id,
                )
                result_close = self._run_and_store(
                    session,
                    strategy_name,
                    "close",
                    signals,
                    price_data,
                    value_traded_inr,
                    exp_run.exp_run_id,
                )

                avg_sharpe = (result_open["sharpe_ratio"] + result_close["sharpe_ratio"]) / 2
                avg_return = (result_open["total_return"] + result_close["total_return"]) / 2

                exp_run.status = "SUCCEEDED"
                exp_run.finished_at = func.now()

                await session.commit()

                return ExperimentResult(
                    exp_run_id=exp_run.exp_run_id,
                    exp_hash=exp_hash,
                    status="SUCCEEDED",
                    metrics={
                        "sharpe_ratio": avg_sharpe,
                        "total_return": avg_return,
                        "sharpe_open": result_open["sharpe_ratio"],
                        "sharpe_close": result_close["sharpe_ratio"],
                    },
                    trades=[],
                )

            except Exception as e:
                logger.error(f"Experiment failed: {e}")
                exp_run.status = "FAILED"
                exp_run.finished_at = func.now()
                await session.commit()
                raise

    def _run_and_store(
        self,
        session: AsyncSession,
        strategy_name: str,
        entry_mode: str,
        signals: list[tuple[date, int, str, float, dict]],
        price_data: dict[int, dict[date, dict[str, float]]],
        value_traded_inr: dict[int, float],
        exp_run_id: int,
    ) -> dict[str, float]:
        from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTEngine

        engine = VectorBTEngine()
        result = engine.run_backtest(
            f"{strategy_name}_{entry_mode}",
            entry_mode,
            signals,
            price_data,
            value_traded_inr,
        )

        for trade in result.trades:
            bt_trade = BtTrade(
                exp_run_id=exp_run_id,
                symbol_id=trade.symbol_id,
                entry_date=trade.entry_date,
                entry_price=trade.entry_price,
                entry_mode=trade.entry_mode,
                qty=trade.qty,
                initial_stop=trade.initial_stop,
                exit_date=trade.exit_date,
                exit_price=trade.exit_price,
                pnl=trade.pnl,
                pnl_r=trade.pnl_r,
                fees=trade.fees,
                slippage_bps=trade.slippage_bps,
                mfe_r=trade.mfe_r,
                mae_r=trade.mae_r,
                exit_reason=trade.exit_reason.value if trade.exit_reason else None,
                exit_rule_version=trade.exit_rule_version,
                reason_json={},
            )
            session.add(bt_trade)

        exp_metric = ExpMetric(
            exp_run_id=exp_run_id,
            metric_name=f"sharpe_{entry_mode}",
            metric_value=result.sharpe_ratio,
        )
        session.add(exp_metric)

        exp_metric2 = ExpMetric(
            exp_run_id=exp_run_id,
            metric_name=f"total_return_{entry_mode}",
            metric_value=result.total_return,
        )
        session.add(exp_metric2)

        return {
            "sharpe_ratio": result.sharpe_ratio,
            "total_return": result.total_return,
            "win_rate": result.win_rate,
            "max_drawdown": result.max_drawdown,
        }

    def _compute_strategy_hash(self, strategy_name: str, params: dict[str, Any]) -> str:
        content = json.dumps({"name": strategy_name, "params": params}, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _compute_exp_hash(self, strategy_hash: str, dataset_hash: str) -> str:
        content = f"{strategy_hash}:{dataset_hash}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def get_experiment(self, exp_hash: str) -> dict[str, Any] | None:
        from nse_momentum_lab.db import get_sessionmaker

        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(select(ExpRun).where(ExpRun.exp_hash == exp_hash))
            exp_run = result.scalar_one_or_none()

            if not exp_run:
                return None

            return {
                "exp_hash": exp_run.exp_hash,
                "strategy_name": exp_run.strategy_name,
                "strategy_hash": exp_run.strategy_hash,
                "dataset_hash": exp_run.dataset_hash,
                "params": exp_run.params_json,
                "status": exp_run.status,
                "started_at": exp_run.started_at,
                "finished_at": exp_run.finished_at,
            }
