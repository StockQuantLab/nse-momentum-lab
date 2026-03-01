#!/usr/bin/env python3
"""Backtest worker - runs vectorbt backtests on scan results.

Usage:
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.worker \
        --scan-run-id 1 --strategy STRAT_4P_2LYNCH_v1

This worker:
1. Fetches scan results (passed candidates)
2. Builds price data from adjusted OHLCV
3. Runs vectorbt backtest
4. Stores results in exp_run, exp_metric, bt_trade tables
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    BtTrade,
    ExpMetric,
    ExpRun,
    MdOhlcvAdj,
    RefSymbol,
    ScanResult,
    ScanRun,
)
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_scan_results(
    session: AsyncSession, scan_run_id: int, limit: int | None = None
) -> list[ScanResult]:
    query = select(ScanResult).where(ScanResult.scan_run_id == scan_run_id)
    if limit:
        query = query.limit(limit)
    result = await session.execute(query)
    return result.scalars().all()


async def get_price_data(
    session: AsyncSession,
    symbol_ids: list[int],
    start_date: date,
    end_date: date,
) -> dict[int, dict[date, dict[str, float]]]:
    result = await session.execute(
        select(MdOhlcvAdj)
        .where(MdOhlcvAdj.symbol_id.in_(symbol_ids))
        .where(MdOhlcvAdj.trading_date >= start_date)
        .where(MdOhlcvAdj.trading_date <= end_date)
        .order_by(MdOhlcvAdj.trading_date)
    )
    rows = result.scalars().all()

    price_data: dict[int, dict[date, dict[str, float]]] = {}

    for row in rows:
        if row.symbol_id not in price_data:
            price_data[row.symbol_id] = {}

        price_data[row.symbol_id][row.trading_date] = {
            "open": float(row.open_adj) if row.open_adj else 0.0,
            "high": float(row.high_adj) if row.high_adj else 0.0,
            "low": float(row.low_adj) if row.low_adj else 0.0,
            "close": float(row.close_adj) if row.close_adj else 0.0,
            "close_adj": float(row.close_adj) if row.close_adj else 0.0,
            "open_adj": float(row.open_adj) if row.open_adj else 0.0,
            "high_adj": float(row.high_adj) if row.high_adj else 0.0,
            "low_adj": float(row.low_adj) if row.low_adj else 0.0,
            "volume": row.volume or 0,
            "value_traded": float(row.value_traded) if row.value_traded else 0.0,
        }

    return price_data


async def get_symbol_map(
    session: AsyncSession, symbol_ids: set[int] | None = None
) -> tuple[dict[int, str], dict[int, date]]:
    """Fetch symbol map and delisting dates.

    Returns:
        Tuple of (symbol_map, delisting_dates) where:
        - symbol_map: dict[symbol_id] -> symbol
        - delisting_dates: dict[symbol_id] -> delisting_date (or None if active)
    """
    query = select(RefSymbol)
    if symbol_ids:
        query = query.where(RefSymbol.symbol_id.in_(symbol_ids))
    result = await session.execute(query)
    rows = result.scalars().all()

    symbol_map = {s.symbol_id: s.symbol for s in rows}
    delisting_dates = {s.symbol_id: s.delisting_date for s in rows if s.delisting_date is not None}
    return symbol_map, delisting_dates


def compute_value_traded_inr(
    price_data: dict[int, dict[date, dict[str, float]]], window: int = 20
) -> dict[int, float]:
    value_traded_inr: dict[int, float] = {}

    for symbol_id, symbol_prices in price_data.items():
        dates = sorted(symbol_prices.keys())
        values = [
            symbol_prices[d].get("value_traded")
            for d in dates
            if symbol_prices[d].get("value_traded")
        ]

        if not values:
            continue

        tail = values[-window:] if len(values) >= window else values
        value_traded_inr[symbol_id] = float(sum(tail) / len(tail))

    return value_traded_inr


async def build_signals(
    scan_results: list[ScanResult],
    price_data: dict[int, dict[date, dict[str, float]]],
    symbol_map: dict[int, str],
) -> list[tuple[date, int, str, float, dict]]:
    signals = []

    for sr in scan_results:
        if not sr.passed:
            continue

        symbol_id = sr.symbol_id
        trading_date = sr.trading_date

        if symbol_id not in price_data:
            continue

        if trading_date not in price_data[symbol_id]:
            continue

        symbol_prices = price_data[symbol_id][trading_date]
        entry_price = symbol_prices.get("close_adj", symbol_prices.get("close", 100.0))
        initial_stop = symbol_prices.get("low_adj", symbol_prices.get("low", entry_price * 0.95))

        signals.append(
            (trading_date, symbol_id, symbol_map.get(symbol_id, str(symbol_id)), initial_stop, {})
        )

    return signals


async def store_trades(
    session: AsyncSession,
    exp_run_id: int,
    trades: list,
) -> int:
    stored_count = 0
    for trade in trades:
        try:
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
            stored_count += 1
        except Exception as e:
            logger.error(f"Failed to store trade {trade.symbol_id}: {e}")
            raise
    return stored_count


async def run_backtest_worker(
    scan_run_id: int,
    strategy_name: str = "STRAT_4P_2LYNCH_v1",
) -> dict[str, Any]:
    logger.info(f"Starting backtest worker for scan_run_id={scan_run_id}")

    sessionmaker = get_sessionmaker()
    results_summary: list[dict[str, Any]] = []

    async with sessionmaker() as session:
        async with session.begin():
            scan_query = await session.execute(
                select(ScanRun).where(ScanRun.scan_run_id == scan_run_id)
            )
            scan_run = scan_query.scalar_one_or_none()

            if not scan_run:
                raise ValueError(f"ScanRun {scan_run_id} not found")

            scan_results = await get_scan_results(session, scan_run_id, limit=10000)
            logger.info(f"Found {len(scan_results)} scan results")

            if not scan_results:
                return {"status": "no_results", "scan_run_id": scan_run_id}

            symbol_ids = list({sr.symbol_id for sr in scan_results})
            if not symbol_ids:
                return {"status": "no_symbols", "scan_run_id": scan_run_id}

            date_range = await session.execute(
                select(func.min(MdOhlcvAdj.trading_date), func.max(MdOhlcvAdj.trading_date)).where(
                    MdOhlcvAdj.symbol_id.in_(symbol_ids)
                )
            )
            date_row = date_range.first()
            if not date_row or not date_row[0] or not date_row[1]:
                return {"status": "no_date_range", "scan_run_id": scan_run_id}
            min_date, max_date = date_row[0], date_row[1]

            price_data = await get_price_data(session, symbol_ids, min_date, max_date)
            symbol_map, delisting_dates = await get_symbol_map(session, set(symbol_ids))

            signals = await build_signals(scan_results, price_data, symbol_map)
            logger.info(f"Built {len(signals)} signals")

            if not signals:
                logger.warning("No signals to backtest")
                return {"status": "no_signals", "scan_run_id": scan_run_id}

            value_traded_inr = compute_value_traded_inr(price_data)
            config = VectorBTConfig()
            engine = VectorBTEngine(config)

            logger.info("Running gap-up breakout backtest (entry at signal day's open)")

            try:
                result = engine.run_backtest(
                    strategy_name=f"{strategy_name}_gap_open",
                    signals=signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                    delisting_dates=delisting_dates if delisting_dates else None,
                )

                exp_run = ExpRun(
                    exp_hash=None,
                    strategy_name=result.strategy_name,
                    strategy_hash=None,
                    dataset_hash=None,
                    params_json={"entry_mode": "gap_open"},
                    code_sha="",
                    status="SUCCEEDED",
                )
                session.add(exp_run)
                await session.flush()

                trades_stored = await store_trades(session, exp_run.exp_run_id, result.trades)

                for metric_name, metric_value in [
                    ("sharpe_ratio", result.sharpe_ratio),
                    ("total_return", result.total_return),
                    ("max_drawdown", result.max_drawdown),
                    ("win_rate", result.win_rate),
                ]:
                    session.add(
                        ExpMetric(
                            exp_run_id=exp_run.exp_run_id,
                            metric_name=metric_name,
                            metric_value=metric_value,
                        )
                    )

                results_summary.append(
                    {
                        "entry_mode": "gap_open",
                        "trades": len(result.trades),
                        "trades_stored": trades_stored,
                        "return": result.total_return,
                        "sharpe": result.sharpe_ratio,
                    }
                )

                logger.info(
                    f"  gap_open: {len(result.trades)} trades, "
                    f"Return: {result.total_return * 100:.2f}%, "
                    f"Sharpe: {result.sharpe_ratio:.2f}"
                )

            except Exception as e:
                logger.error(f"Backtest failed: {e}")
                raise

    return {
        "status": "success",
        "scan_run_id": scan_run_id,
        "strategy_name": strategy_name,
        "signals": len(signals),
        "results": results_summary,
    }


def main():
    parser = argparse.ArgumentParser(description="Run vectorbt backtest on scan results")
    parser.add_argument("--scan-run-id", type=int, required=True, help="Scan run ID to backtest")
    parser.add_argument("--strategy", type=str, default="STRAT_4P_2LYNCH_v1", help="Strategy name")
    args = parser.parse_args()

    result = asyncio.run(
        run_backtest_worker(args.scan_run_id, args.strategy),
        loop_factory=asyncio.SelectorEventLoop,
    )

    print(f"\n{'=' * 60}")
    print("BACKTEST WORKER COMPLETE")
    print(f"{'=' * 60}")
    print(f"Status: {result['status']}")
    print(f"Strategy: {result.get('strategy_name', 'N/A')}")
    print(f"Signals: {result.get('signals', 0)}")


if __name__ == "__main__":
    main()
