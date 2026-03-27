from __future__ import annotations

import csv
import hashlib
import inspect
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from nse_momentum_lab.db.market_db import get_backtest_db, get_market_db
from nse_momentum_lab.db.models import MdOhlcvAdj, PaperPosition, RefSymbol
from nse_momentum_lab.db.paper import (
    create_or_update_paper_session,
    get_paper_feed_state,
    get_paper_session_summary,
    list_paper_session_signals,
    list_session_signals,
    reset_session_signal_queue,
    sync_paper_session_signals_from_signals,
    upsert_paper_feed_state,
    upsert_paper_session_signal,
    upsert_signal,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
)
from nse_momentum_lab.services.backtest.engine import PositionSide
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy
from nse_momentum_lab.services.kite.client import KiteConnectClient
from nse_momentum_lab.services.kite.ticker import (
    build_websocket_url,
    plan_subscription_batches,
)
from nse_momentum_lab.services.paper.candidate_builder import (
    apply_breakdown_selection_ranking,
    apply_breakout_selection_ranking,
    resolve_entry_cutoff_minutes,
    resolve_intraday_entries_bulk,
    resolve_same_day_r_ladder_start_r,
)
from nse_momentum_lab.services.paper.engine import PaperTrader, RiskConfig
from nse_momentum_lab.utils import ALL_FILTERS
from nse_momentum_lab.utils.time_utils import IST, nse_open_time

PaperMode = Literal["replay", "live"]
PROJECT_ROOT = Path(__file__).resolve().parents[4]
INSTRUMENT_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "kite" / "instruments" / "NSE.csv"
ACTIONABLE_BACKTEST_STATUSES = {"queued_for_execution", "executed", "not_executed_portfolio"}

logger = logging.getLogger(__name__)


def _utc_today() -> date:
    return datetime.now(UTC).date()


@dataclass(slots=True)
class PaperRuntimePlan:
    session_id: str
    strategy_name: str
    trade_date: date | None
    mode: PaperMode
    symbols: list[str] = field(default_factory=list)
    experiment_id: str | None = None
    notes: str | None = None
    strategy_params: dict[str, Any] = field(default_factory=dict)
    risk_config: dict[str, Any] = field(default_factory=dict)
    feed_mode: str = "full"
    feed_source: str = "kite"
    kite_api_key: str | None = None
    kite_access_token: str | None = None
    instrument_tokens: list[int] = field(default_factory=list)
    observe_only: bool = False


@dataclass(slots=True)
class PaperRuntimeSnapshot:
    session: dict[str, Any] | None
    feed_state: dict[str, Any] | None
    signals: list[dict[str, Any]]
    feed_plan: dict[str, Any]


class PaperRuntimeScaffold:
    """Session/bootstrap scaffolding for replay-day and live paper workflows."""

    def __init__(self, *, feed_batch_size: int = 3000) -> None:
        self.feed_batch_size = feed_batch_size
        self._traders: dict[str, PaperTrader] = {}
        self._last_risk_reset_date: dict[str, date] = {}

    @staticmethod
    def _public_strategy_params(plan: PaperRuntimePlan) -> dict[str, Any]:
        return {
            str(key): value
            for key, value in (plan.strategy_params or {}).items()
            if not str(key).startswith("_")
        }

    @staticmethod
    def _safe_json(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @staticmethod
    def _signal_state_from_queue_status(status: str) -> str:
        normalized = status.strip().lower()
        if normalized in ACTIONABLE_BACKTEST_STATUSES:
            return "NEW"
        return "ARCHIVED"

    @staticmethod
    def _signal_state_from_runtime_row(
        *,
        plan: PaperRuntimePlan,
        row: dict[str, Any],
    ) -> str:
        """Map a runtime row to an initial signal state.

        Live watchlist rows are not backtest execution diagnostics. They need to
        enter the live runtime as NEW so the websocket tick loop can promote them
        when intraday price action triggers. Backtest-derived rows keep the
        legacy status-to-state mapping.
        """

        if plan.mode == "live" and plan.strategy_params.get("_watchlist_mode"):
            return "NEW"
        return PaperRuntimeScaffold._signal_state_from_queue_status(str(row.get("status") or ""))

    @staticmethod
    def _build_signal_metadata(
        plan: PaperRuntimePlan,
        row: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "experiment_id": plan.experiment_id,
            "symbol": row.get("symbol"),
            "backtest_status": row.get("status"),
            "backtest_reason": row.get("reason"),
            "selection_score": row.get("selection_score"),
            "selection_rank": row.get("selection_rank"),
            "selection_components": PaperRuntimeScaffold._safe_json(
                row.get("selection_components_json")
            ),
            "filters": PaperRuntimeScaffold._safe_json(row.get("filters_json")),
            "entry_price": row.get("entry_price"),
            "entry_time": str(row.get("entry_time") or ""),
            "hold_quality_passed": row.get("hold_quality_passed"),
            "executed_exit_reason": row.get("executed_exit_reason"),
            "pnl_pct": row.get("pnl_pct"),
            "prev_close": row.get("prev_close"),
            "threshold": row.get("threshold"),
            "direction": row.get("direction"),
            "trigger_price": row.get("trigger_price"),
            "entry_cutoff_minutes": row.get("entry_cutoff_minutes"),
            "instrument_token": row.get("instrument_token"),
            "watch_state": row.get("watch_state"),
            "watch_reason": row.get("watch_reason"),
            "watch_filters_passed": row.get("filters_passed"),
        }

    @staticmethod
    def _strategy_hash_for_plan(plan: PaperRuntimePlan) -> str:
        if plan.experiment_id:
            return plan.experiment_id
        payload = json.dumps(
            {
                "strategy_name": plan.strategy_name,
                "trade_date": plan.trade_date.isoformat() if plan.trade_date else None,
                "strategy_params": PaperRuntimeScaffold._public_strategy_params(plan),
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _resolve_instrument_tokens(symbols: list[str]) -> list[int]:
        return list(PaperRuntimeScaffold._resolve_instrument_token_map(symbols).values())

    @staticmethod
    def _resolve_instrument_token_map(symbols: list[str]) -> dict[str, int]:
        if not symbols or not INSTRUMENT_CACHE_PATH.exists():
            return {}

        wanted = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        resolved: dict[str, int] = {}
        with INSTRUMENT_CACHE_PATH.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                tradingsymbol = str(row.get("tradingsymbol") or "").strip().upper()
                if tradingsymbol not in wanted:
                    continue
                try:
                    resolved[tradingsymbol] = int(str(row.get("instrument_token") or "").strip())
                except ValueError:
                    continue
        return resolved

    @staticmethod
    def _risk_config_from_dict(data: dict[str, Any] | None) -> RiskConfig:
        if not data:
            return RiskConfig()
        allowed = {
            "max_daily_loss_pct",
            "max_drawdown_pct",
            "max_positions",
            "max_position_size_pct",
            "kill_switch_threshold",
        }
        clean = {key: value for key, value in data.items() if key in allowed}
        return RiskConfig(**clean)

    @staticmethod
    def _coerce_date(value: Any) -> date | None:
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value.strip():
            return date.fromisoformat(value.strip())
        return None

    @classmethod
    def _normalize_runtime_signals(cls, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for signal in signals:
            row = dict(signal)
            row["asof_date"] = cls._coerce_date(signal.get("asof_date"))
            row["planned_entry_date"] = cls._coerce_date(signal.get("planned_entry_date"))
            normalized.append(row)
        return normalized

    @staticmethod
    def _backtest_params_from_plan(plan: PaperRuntimePlan) -> BacktestParams:
        base = asdict(BacktestParams(strategy=plan.strategy_name))
        for key, value in (plan.strategy_params or {}).items():
            if key in BacktestParams.__dataclass_fields__:
                base[key] = value
        base["strategy"] = plan.strategy_name
        return BacktestParams(**base)

    @staticmethod
    def _build_filter_snapshot(
        row: dict[str, Any],
        active_filter_cols: list[str],
        hold_quality_cols: list[str],
    ) -> dict[str, bool]:
        filter_snapshot = {
            col: bool(row.get(col, False)) for col in active_filter_cols if col in row
        }
        for col in hold_quality_cols:
            if col in row:
                filter_snapshot[col] = bool(row.get(col, False))
        return filter_snapshot

    async def _ensure_ref_symbols(
        self,
        db_session: Any,
        symbols: list[str],
    ) -> dict[str, RefSymbol]:
        wanted = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
        if not wanted:
            return {}

        result = await db_session.execute(select(RefSymbol).where(RefSymbol.symbol.in_(wanted)))
        existing = {row.symbol.strip().upper(): row for row in result.scalars().all() if row.symbol}
        missing = [symbol for symbol in wanted if symbol not in existing]
        if missing:
            for symbol in missing:
                db_session.add(RefSymbol(symbol=symbol, series="EQ", status="ACTIVE"))
            await db_session.flush()
            result = await db_session.execute(select(RefSymbol).where(RefSymbol.symbol.in_(wanted)))
            existing = {
                row.symbol.strip().upper(): row for row in result.scalars().all() if row.symbol
            }
        return existing

    async def _fetch_queue_from_runtime(
        self,
        db_session: Any,
        plan: PaperRuntimePlan,
    ) -> dict[str, Any]:
        if plan.trade_date is None or not plan.symbols:
            return {
                "rows": [],
                "queue_size": 0,
                "actionable_queue_size": 0,
                "symbols": [],
                "ref_symbols": {},
                "missing_symbols": [],
            }

        params = self._backtest_params_from_plan(plan)
        strategy = resolve_strategy(plan.strategy_name)

        # Use read-only market DuckDB — never backtest.duckdb.
        market_db = get_market_db(read_only=True)
        context_start = plan.trade_date - timedelta(days=21)
        query, params_tuple = strategy.build_candidate_query(
            params,
            list(plan.symbols),
            context_start,
            plan.trade_date,
        )
        query_result = market_db.con.execute(query, params_tuple)
        try:
            df_pl = query_result.pl()
        except Exception:
            df_pl = pl.from_arrow(query_result.arrow())
        if "trading_date" in df_pl.columns:
            df_pl = df_pl.filter(pl.col("trading_date") == pl.lit(plan.trade_date))

        if df_pl.is_empty():
            return {
                "rows": [],
                "queue_size": 0,
                "actionable_queue_size": 0,
                "symbols": [],
                "ref_symbols": {},
                "missing_symbols": [],
            }

        active_filter_cols = (
            strategy.entry_filter_columns
            or strategy.filter_columns
            or [f.value for f in ALL_FILTERS]
        )
        hold_quality_cols = strategy.hold_quality_filter_columns or []

        for col in active_filter_cols:
            if col in df_pl.columns:
                df_pl = df_pl.with_columns(pl.col(col).fill_null(False))
            else:
                df_pl = df_pl.with_columns(pl.lit(False).alias(col))
        for col in hold_quality_cols:
            if col in df_pl.columns:
                df_pl = df_pl.with_columns(pl.col(col).fill_null(False))
            else:
                df_pl = df_pl.with_columns(pl.lit(False).alias(col))

        filter_sum_expr = pl.col(active_filter_cols[0]).cast(int)
        for col in active_filter_cols[1:]:
            filter_sum_expr = filter_sum_expr + pl.col(col).cast(int)
        df_pl = df_pl.with_columns(filter_sum_expr.alias("filters_passed"))

        effective_min_filters = (
            strategy.min_filters_override
            if strategy.min_filters_override is not None
            else params.min_filters
        )
        df_filtered = df_pl.filter(pl.col("filters_passed") >= effective_min_filters)

        ranking_rejected = pl.DataFrame()
        if not df_filtered.is_empty():
            if strategy.direction == PositionSide.LONG and strategy.family == "threshold_breakout":
                df_filtered, ranking_rejected = apply_breakout_selection_ranking(
                    df_filtered, params
                )
            elif (
                strategy.direction == PositionSide.SHORT
                and strategy.family == "threshold_breakdown"
            ):
                df_filtered, ranking_rejected = apply_breakdown_selection_ranking(
                    df_filtered, params
                )
            else:
                df_filtered = df_filtered.with_columns(
                    pl.lit(None, dtype=pl.Float64).alias("selection_score"),
                    pl.lit(None, dtype=pl.Int64).alias("selection_rank"),
                    pl.lit(None, dtype=pl.Int64).alias("selection_budget"),
                    pl.lit(None, dtype=pl.Int64).alias("selection_c_strength"),
                    pl.lit(None, dtype=pl.Int64).alias("selection_y_score"),
                    pl.lit(None, dtype=pl.Int64).alias("selection_n_score"),
                    pl.lit(None, dtype=pl.Float64).alias("selection_r2_quality"),
                )

        queue_rows: list[dict[str, Any]] = []
        for row in ranking_rejected.iter_rows(named=True):
            filter_snapshot = self._build_filter_snapshot(
                row, active_filter_cols, hold_quality_cols
            )
            queue_rows.append(
                {
                    **row,
                    "signal_date": row.get("trading_date"),
                    "status": "skipped_rank_budget",
                    "reason": "below_daily_rank_budget",
                    "filters_json": filter_snapshot,
                    "hold_quality_passed": all(
                        bool(row.get(col, False)) for col in hold_quality_cols
                    )
                    if hold_quality_cols
                    else True,
                    "entry_price": None,
                    "entry_time": None,
                    "initial_stop": None,
                    "selection_components_json": {
                        "c_strength": int(row.get("selection_c_strength") or 0),
                        "y_score": int(row.get("selection_y_score") or 0),
                        "n_score": int(row.get("selection_n_score") or 0),
                        "r2_quality": float(row.get("selection_r2_quality") or 0.0),
                    },
                }
            )

        intraday_entry_by_signal: dict[tuple[str, date], dict[str, Any]] = {}
        if not df_filtered.is_empty() and str(params.entry_timeframe).lower() == "5min":
            intraday_entry_by_signal = resolve_intraday_entries_bulk(
                db_con=market_db.con,
                df_filtered=df_filtered,
                breakout_threshold=params.breakout_threshold,
                entry_cutoff_minutes=resolve_entry_cutoff_minutes(params, strategy),
                is_short=strategy.direction == PositionSide.SHORT,
                same_day_r_ladder=params.same_day_r_ladder,
                same_day_r_ladder_start_r=resolve_same_day_r_ladder_start_r(params, strategy),
                short_initial_stop_atr_cap_mult=(
                    params.short_initial_stop_atr_cap_mult
                    if strategy.direction == PositionSide.SHORT
                    else None
                ),
                short_same_day_take_profit_pct=(
                    params.short_same_day_take_profit_pct
                    if strategy.direction == PositionSide.SHORT
                    else None
                ),
            )

        for row in df_filtered.iter_rows(named=True):
            trading_date = self._coerce_date(row.get("trading_date"))
            symbol = str(row.get("symbol") or "").strip().upper()
            if trading_date is None or not symbol:
                continue

            filter_snapshot = self._build_filter_snapshot(
                row, active_filter_cols, hold_quality_cols
            )
            hold_quality_passed = (
                all(bool(row.get(col, False)) for col in hold_quality_cols)
                if hold_quality_cols
                else True
            )
            intraday_entry = intraday_entry_by_signal.get((symbol, trading_date))

            status = "queued_for_execution"
            reason = "eligible"
            entry_price = None
            initial_stop = None
            entry_time = None

            if str(params.entry_timeframe).lower() == "5min":
                if intraday_entry is None:
                    status = "skipped_no_intraday_entry"
                    reason = "no_5min_breakout_before_cutoff"
                else:
                    entry_price = float(intraday_entry["entry_price"])
                    initial_stop = float(intraday_entry["initial_stop"])
                    entry_time = intraday_entry.get("entry_time")
            else:
                raw_entry_price = row.get("open")
                if raw_entry_price not in (None, ""):
                    entry_price = float(raw_entry_price)
                if strategy.direction == PositionSide.SHORT:
                    raw_stop = row.get("high", row.get("prev_high"))
                else:
                    raw_stop = row.get("low", row.get("prev_low"))
                if raw_stop not in (None, ""):
                    initial_stop = float(raw_stop)

            queue_rows.append(
                {
                    **row,
                    "signal_date": trading_date,
                    "status": status,
                    "reason": reason,
                    "filters_json": filter_snapshot,
                    "hold_quality_passed": hold_quality_passed,
                    "entry_price": entry_price,
                    "entry_time": entry_time,
                    "initial_stop": initial_stop,
                    "selection_components_json": {
                        "c_strength": int(row.get("selection_c_strength") or 0),
                        "y_score": int(row.get("selection_y_score") or 0),
                        "n_score": int(row.get("selection_n_score") or 0),
                        "r2_quality": float(row.get("selection_r2_quality") or 0.0),
                    },
                }
            )

        queued_symbols = sorted(
            {
                str(row.get("symbol") or "").strip().upper()
                for row in queue_rows
                if str(row.get("symbol") or "").strip()
            }
        )
        ref_symbols = await self._ensure_ref_symbols(db_session, queued_symbols)
        missing_symbols = sorted(set(queued_symbols) - set(ref_symbols))
        actionable_symbols = sorted(
            {
                str(row.get("symbol") or "").strip().upper()
                for row in queue_rows
                if str(row.get("status") or "").strip().lower() in ACTIONABLE_BACKTEST_STATUSES
                and str(row.get("symbol") or "").strip().upper() in ref_symbols
            }
        )

        return {
            "rows": queue_rows,
            "queue_size": len(queue_rows),
            "actionable_queue_size": sum(
                1
                for row in queue_rows
                if str(row.get("status") or "").strip().lower() in ACTIONABLE_BACKTEST_STATUSES
                and str(row.get("symbol") or "").strip().upper() in ref_symbols
            ),
            "symbols": actionable_symbols,
            "ref_symbols": ref_symbols,
            "missing_symbols": missing_symbols,
        }

    async def _fetch_queue_from_watchlist(
        self,
        db_session: Any,
        plan: PaperRuntimePlan,
    ) -> dict[str, Any]:
        raw_rows = plan.strategy_params.get("_live_watchlist_rows", [])
        if plan.trade_date is None or not isinstance(raw_rows, list) or not raw_rows:
            return {
                "rows": [],
                "queue_size": 0,
                "actionable_queue_size": 0,
                "symbols": [],
                "ref_symbols": {},
                "missing_symbols": [],
            }

        symbols = sorted(
            {
                str(row.get("symbol") or "").strip().upper()
                for row in raw_rows
                if str(row.get("symbol") or "").strip()
            }
        )
        ref_symbols = await self._ensure_ref_symbols(db_session, symbols)
        token_by_symbol = self._resolve_instrument_token_map(symbols)
        params = self._backtest_params_from_plan(plan)
        strategy = resolve_strategy(plan.strategy_name)
        threshold = float(plan.strategy_params.get("breakout_threshold", 0.04))
        direction = "short" if "breakdown" in plan.strategy_name.lower() else "long"
        entry_cutoff_minutes = resolve_entry_cutoff_minutes(params, strategy)
        queue_rows: list[dict[str, Any]] = []

        for rank, raw_row in enumerate(raw_rows, start=1):
            symbol = str(raw_row.get("symbol") or "").strip().upper()
            if not symbol or symbol not in ref_symbols:
                continue
            prev_close = float(raw_row.get("last_close") or 0.0)
            atr_20 = float(raw_row.get("atr_20") or 0.0)
            if prev_close <= 0:
                continue
            if direction == "short":
                trigger_price = prev_close * (1 - threshold)
                initial_stop = prev_close + atr_20 if atr_20 > 0 else prev_close * 1.05
            else:
                trigger_price = prev_close * (1 + threshold)
                initial_stop = prev_close - atr_20 if atr_20 > 0 else prev_close * 0.95
            queue_rows.append(
                {
                    **raw_row,
                    "symbol": symbol,
                    "signal_date": plan.trade_date,
                    "status": "watching_intraday_trigger",
                    "reason": "prior_day_watchlist",
                    "entry_price": None,
                    "entry_time": None,
                    "initial_stop": float(initial_stop),
                    "selection_rank": rank,
                    "selection_score": float(raw_row.get("filters_passed") or 0.0),
                    "selection_components_json": {},
                    "filters_json": {},
                    "hold_quality_passed": True,
                    "prev_close": prev_close,
                    "threshold": threshold,
                    "direction": direction,
                    "trigger_price": float(trigger_price),
                    "entry_cutoff_minutes": int(entry_cutoff_minutes),
                    "instrument_token": token_by_symbol.get(symbol),
                    "watch_state": "WATCH",
                    "watch_reason": "prior_day_watchlist",
                }
            )

        missing_symbols = sorted(set(symbols) - set(ref_symbols))
        return {
            "rows": queue_rows,
            "queue_size": len(queue_rows),
            "actionable_queue_size": 0,
            "symbols": sorted(symbols),
            "ref_symbols": ref_symbols,
            "missing_symbols": missing_symbols,
        }

    async def _fetch_queue_from_experiment(
        self,
        db_session: Any,
        plan: PaperRuntimePlan,
    ) -> dict[str, Any]:
        if not plan.experiment_id or plan.trade_date is None:
            return {
                "rows": [],
                "queue_size": 0,
                "actionable_queue_size": 0,
                "symbols": [],
                "ref_symbols": {},
                "missing_symbols": [],
            }

        backtest_db = get_backtest_db(read_only=True)
        diagnostics_df = backtest_db.get_experiment_execution_diagnostics(plan.experiment_id)
        if diagnostics_df.is_empty():
            raise ValueError(f"Experiment {plan.experiment_id} has no execution diagnostics")

        rows = [
            row
            for row in diagnostics_df.to_dicts()
            if self._coerce_date(row.get("signal_date")) == plan.trade_date
        ]
        if plan.symbols:
            wanted = {symbol.upper() for symbol in plan.symbols}
            rows = [row for row in rows if str(row.get("symbol") or "").upper() in wanted]
        if not rows:
            raise ValueError(
                f"Experiment {plan.experiment_id} has no execution diagnostics for {plan.trade_date}"
            )

        symbols = sorted(
            {
                str(row.get("symbol") or "").strip().upper()
                for row in rows
                if str(row.get("symbol") or "").strip()
            }
        )
        ref_symbols = await self._ensure_ref_symbols(db_session, symbols)

        missing_symbols = sorted(set(symbols) - set(ref_symbols))
        if missing_symbols:
            logger.warning(
                "Skipping %d missing ref symbols for experiment %s: %s",
                len(missing_symbols),
                plan.experiment_id,
                ", ".join(missing_symbols[:10]),
            )

        return {
            "rows": rows,
            "queue_size": len(rows),
            "actionable_queue_size": sum(
                1
                for row in rows
                if str(row.get("status") or "ARCHIVED").strip().lower()
                in ACTIONABLE_BACKTEST_STATUSES
                and str(row.get("symbol") or "").strip().upper() in ref_symbols
            ),
            "symbols": sorted(ref_symbols),
            "ref_symbols": ref_symbols,
            "missing_symbols": missing_symbols,
        }

    async def _persist_queue_from_experiment(
        self,
        db_session: Any,
        plan: PaperRuntimePlan,
        queue: dict[str, Any],
    ) -> None:
        await reset_session_signal_queue(db_session, plan.session_id)

        ref_symbols = cast(dict[str, Any], queue.get("ref_symbols", {}))
        for row in cast(list[dict[str, Any]], queue.get("rows", [])):
            symbol = str(row.get("symbol") or "").strip().upper()
            ref_symbol = ref_symbols.get(symbol)
            if ref_symbol is None:
                continue

            metadata_json = self._build_signal_metadata(plan, row)
            signal_state = self._signal_state_from_runtime_row(plan=plan, row=row)
            signal_row = await upsert_signal(
                db_session,
                session_id=plan.session_id,
                symbol_id=ref_symbol.symbol_id,
                asof_date=plan.trade_date,
                strategy_hash=self._strategy_hash_for_plan(plan),
                state=signal_state,
                entry_mode="MARKET",
                planned_entry_date=plan.trade_date,
                initial_stop=float(row["initial_stop"])
                if row.get("initial_stop") is not None
                else None,
                metadata_json=metadata_json,
            )
            await upsert_paper_session_signal(
                db_session,
                session_id=plan.session_id,
                signal_id=signal_row.signal_id,
                symbol_id=ref_symbol.symbol_id,
                asof_date=plan.trade_date,
                decision_status=str(row.get("status") or "UNKNOWN"),
                rank=int(row["selection_rank"])
                if row.get("selection_rank") not in (None, "")
                else None,
                selection_score=(
                    float(row["selection_score"])
                    if row.get("selection_score") not in (None, "")
                    else None
                ),
                decision_reason=str(row.get("reason") or "") or None,
                metadata_json=metadata_json,
            )

    async def _load_eod_prices(
        self,
        db_session: Any,
        signals: list[dict[str, Any]],
    ) -> dict[int, dict[date, dict[str, float]]]:
        symbol_ids = sorted({int(signal["symbol_id"]) for signal in signals})
        price_dates = sorted(
            {
                date.fromisoformat(str(signal["planned_entry_date"]))
                for signal in signals
                if signal.get("planned_entry_date")
            }
        )
        if not symbol_ids or not price_dates:
            return {}

        result = await db_session.execute(
            select(MdOhlcvAdj).where(
                MdOhlcvAdj.symbol_id.in_(symbol_ids),
                MdOhlcvAdj.trading_date.in_(price_dates),
            )
        )
        rows = result.scalars().all()
        prices: dict[int, dict[date, dict[str, float]]] = {}
        for row in rows:
            symbol_prices = prices.setdefault(row.symbol_id, {})
            symbol_prices[row.trading_date] = {
                "open": float(row.open_adj) if row.open_adj is not None else 0.0,
                "high": float(row.high_adj) if row.high_adj is not None else 0.0,
                "low": float(row.low_adj) if row.low_adj is not None else 0.0,
                "close": float(row.close_adj) if row.close_adj is not None else 0.0,
                "close_adj": float(row.close_adj) if row.close_adj is not None else 0.0,
                "value_traded_inr": float(row.value_traded)
                if row.value_traded is not None
                else 0.0,
            }

        missing_symbol_ids = [symbol_id for symbol_id in symbol_ids if symbol_id not in prices]
        if missing_symbol_ids and price_dates:
            ref_result = await db_session.execute(
                select(RefSymbol).where(RefSymbol.symbol_id.in_(missing_symbol_ids))
            )
            ref_symbols = {row.symbol_id: row for row in ref_result.scalars().all() if row.symbol}
            requested_symbols = sorted(
                {
                    ref_symbols[symbol_id].symbol.strip().upper()
                    for symbol_id in missing_symbol_ids
                    if symbol_id in ref_symbols
                }
            )
            if requested_symbols:
                market_db = get_market_db(read_only=True)
                daily_df = market_db.query_daily_multi(
                    requested_symbols,
                    min(price_dates).isoformat(),
                    max(price_dates).isoformat(),
                    columns=["symbol", "date", "open", "high", "low", "close", "volume"],
                )
                if not daily_df.is_empty():
                    symbol_id_by_symbol = {
                        ref_symbol.symbol.strip().upper(): symbol_id
                        for symbol_id, ref_symbol in ref_symbols.items()
                        if ref_symbol.symbol
                    }
                    for row in daily_df.iter_rows(named=True):
                        symbol = str(row.get("symbol") or "").strip().upper()
                        symbol_id = symbol_id_by_symbol.get(symbol)
                        trading_date = self._coerce_date(row.get("date"))
                        if symbol_id is None or trading_date is None:
                            continue
                        symbol_prices = prices.setdefault(symbol_id, {})
                        symbol_prices[trading_date] = {
                            "open": float(row.get("open") or 0.0),
                            "high": float(row.get("high") or 0.0),
                            "low": float(row.get("low") or 0.0),
                            "close": float(row.get("close") or 0.0),
                            "close_adj": float(row.get("close") or 0.0),
                            "value_traded_inr": (
                                float(row.get("close") or 0.0) * float(row.get("volume") or 0.0)
                            ),
                        }
        return prices

    async def _load_live_prices(
        self,
        db_session: Any,
        signals: list[dict[str, Any]],
        *,
        kite_client: KiteConnectClient,
    ) -> dict[int, dict[date, dict[str, float]]]:
        symbol_ids = sorted({int(signal["symbol_id"]) for signal in signals})
        if not symbol_ids:
            return {}

        result = await db_session.execute(
            select(RefSymbol).where(RefSymbol.symbol_id.in_(symbol_ids))
        )
        ref_symbols = {row.symbol_id: row for row in result.scalars().all()}
        instruments = [
            f"NSE:{ref_symbols[symbol_id].symbol}"
            for symbol_id in symbol_ids
            if symbol_id in ref_symbols and ref_symbols[symbol_id].symbol
        ]
        if not instruments:
            return {}

        ltp_payload = kite_client.ltp(instruments)
        price_date = next(
            (
                self._coerce_date(signal.get("planned_entry_date"))
                for signal in signals
                if self._coerce_date(signal.get("planned_entry_date")) is not None
            ),
            _utc_today(),
        )
        prices: dict[int, dict[date, dict[str, float]]] = {}
        for symbol_id, ref_symbol in ref_symbols.items():
            symbol = ref_symbol.symbol.strip().upper()
            quote = ltp_payload.get(f"NSE:{symbol}") or ltp_payload.get(symbol) or {}
            last_price = quote.get("last_price")
            if last_price in (None, ""):
                continue
            prices[symbol_id] = {
                price_date: {
                    "close": float(last_price),
                    "close_adj": float(last_price),
                    "value_traded_inr": None,
                }
            }
        return prices

    def _get_trader(self, session_id: str, risk_config: dict[str, Any] | None) -> PaperTrader:
        trader = self._traders.get(session_id)
        if trader is None:
            trader = PaperTrader(risk_config=self._risk_config_from_dict(risk_config))
            self._traders[session_id] = trader
        return trader

    async def _sync_trader_state(
        self,
        db_session: Any,
        session_id: str,
        trader: PaperTrader,
        cycle_date: date,
    ) -> None:
        last_reset_date = self._last_risk_reset_date.get(session_id)
        if last_reset_date != cycle_date:
            trader.reset_daily()
            self._last_risk_reset_date[session_id] = cycle_date

        try:
            result = await db_session.execute(
                select(PaperPosition).where(
                    PaperPosition.session_id == session_id,
                    PaperPosition.closed_at.is_(None),
                )
            )
            scalars = result.scalars()
            if inspect.isawaitable(scalars):
                scalars = await scalars
            positions = scalars.all()
            if inspect.isawaitable(positions):
                positions = await positions
        except Exception:
            logger.debug(
                "Skipping trader position hydration for session %s",
                session_id,
                exc_info=True,
            )
            return

        if isinstance(positions, list):
            trader.hydrate_positions(positions, session_id)

    async def prepare_session(
        self,
        sessionmaker: async_sessionmaker[Any],
        plan: PaperRuntimePlan,
        *,
        status: str,
    ) -> dict[str, Any]:
        effective_symbols = list(plan.symbols)
        effective_tokens = list(plan.instrument_tokens)
        queue_stats = {"queue_size": 0, "actionable_queue_size": 0}

        async with sessionmaker() as db_session:
            if plan.experiment_id and plan.trade_date:
                queue_stats = await self._fetch_queue_from_experiment(db_session, plan)
                effective_symbols = list(cast(list[str], queue_stats.get("symbols", [])))
                await create_or_update_paper_session(
                    db_session,
                    session_id=plan.session_id,
                    trade_date=plan.trade_date,
                    strategy_name=plan.strategy_name,
                    mode=plan.mode,
                    status=status,
                    experiment_id=plan.experiment_id,
                    symbols=effective_symbols,
                    strategy_params=self._public_strategy_params(plan),
                    risk_config=plan.risk_config,
                    notes=plan.notes,
                )
                await self._persist_queue_from_experiment(db_session, plan, queue_stats)
            elif plan.mode == "live" and plan.strategy_params.get("_live_watchlist_rows"):
                queue_stats = await self._fetch_queue_from_watchlist(db_session, plan)
                effective_symbols = list(cast(list[str], queue_stats.get("symbols", [])))
                await create_or_update_paper_session(
                    db_session,
                    session_id=plan.session_id,
                    trade_date=plan.trade_date,
                    strategy_name=plan.strategy_name,
                    mode=plan.mode,
                    status=status,
                    experiment_id=plan.experiment_id,
                    symbols=effective_symbols,
                    strategy_params=self._public_strategy_params(plan),
                    risk_config=plan.risk_config,
                    notes=plan.notes,
                )
                await self._persist_queue_from_experiment(db_session, plan, queue_stats)
            elif plan.trade_date and plan.symbols:
                queue_stats = await self._fetch_queue_from_runtime(db_session, plan)
                effective_symbols = list(cast(list[str], queue_stats.get("symbols", [])))
                if plan.mode == "live" and not effective_symbols:
                    effective_symbols = list(plan.symbols)
                await create_or_update_paper_session(
                    db_session,
                    session_id=plan.session_id,
                    trade_date=plan.trade_date,
                    strategy_name=plan.strategy_name,
                    mode=plan.mode,
                    status=status,
                    experiment_id=plan.experiment_id,
                    symbols=effective_symbols,
                    strategy_params=self._public_strategy_params(plan),
                    risk_config=plan.risk_config,
                    notes=plan.notes,
                )
                await self._persist_queue_from_experiment(db_session, plan, queue_stats)
            else:
                await create_or_update_paper_session(
                    db_session,
                    session_id=plan.session_id,
                    trade_date=plan.trade_date,
                    strategy_name=plan.strategy_name,
                    mode=plan.mode,
                    status=status,
                    experiment_id=plan.experiment_id,
                    symbols=plan.symbols,
                    strategy_params=self._public_strategy_params(plan),
                    risk_config=plan.risk_config,
                    notes=plan.notes,
                )
                synced_signals = await sync_paper_session_signals_from_signals(
                    db_session,
                    plan.session_id,
                )
                queue_stats = {
                    "queue_size": len(synced_signals),
                    "actionable_queue_size": len(synced_signals),
                }

            if not effective_tokens:
                effective_tokens = self._resolve_instrument_tokens(effective_symbols)
            feed_state = await upsert_paper_feed_state(
                db_session,
                session_id=plan.session_id,
                source=plan.feed_source,
                mode=plan.feed_mode,
                status="READY" if status in {"ACTIVE", "RUNNING", "PLANNING"} else status,
                subscription_count=len(effective_tokens) or len(effective_symbols),
                is_stale=False,
                metadata_json={
                    "feed_mode": plan.feed_mode,
                    "feed_source": plan.feed_source,
                    "experiment_id": plan.experiment_id,
                    "instrument_tokens": effective_tokens,
                    "observe_only": plan.observe_only,
                },
            )
            signals = await list_paper_session_signals(db_session, plan.session_id)
            summary = await get_paper_session_summary(db_session, plan.session_id)

        feed_plan = self.build_feed_plan(plan, instrument_tokens=effective_tokens)
        return {
            "session": summary["session"] if summary else None,
            "feed_state": {
                "session_id": feed_state.session_id,
                "source": feed_state.source,
                "mode": feed_state.mode,
                "status": feed_state.status,
                "subscription_count": feed_state.subscription_count,
                "is_stale": feed_state.is_stale,
            },
            "queue_size": int(queue_stats["queue_size"]),
            "actionable_queue_size": int(queue_stats["actionable_queue_size"]),
            "signals": signals,
            "feed_plan": feed_plan,
            "resolved_instrument_tokens": effective_tokens,
        }

    def build_feed_plan(
        self,
        plan: PaperRuntimePlan,
        *,
        instrument_tokens: list[int] | None = None,
    ) -> dict[str, Any]:
        tokens = list(
            instrument_tokens if instrument_tokens is not None else plan.instrument_tokens
        )
        batches = plan_subscription_batches(
            tokens, mode=plan.feed_mode, chunk_size=self.feed_batch_size
        )
        connection_url = None
        if plan.feed_source == "kite" and plan.kite_api_key and plan.kite_access_token:
            connection_url = build_websocket_url(plan.kite_api_key, plan.kite_access_token)
        return {
            "feed_source": plan.feed_source,
            "feed_mode": plan.feed_mode,
            "batch_size": self.feed_batch_size,
            "instrument_tokens": tokens,
            "batches": [{"mode": batch.mode, "tokens": batch.tokens} for batch in batches],
            "connection_url": connection_url,
        }

    async def snapshot(
        self,
        sessionmaker: async_sessionmaker[Any],
        session_id: str,
    ) -> PaperRuntimeSnapshot:
        async with sessionmaker() as db_session:
            summary = await get_paper_session_summary(db_session, session_id)
            feed_state = await get_paper_feed_state(db_session, session_id)
            signals = await list_paper_session_signals(db_session, session_id)

        feed_plan = {
            "captured_at": datetime.now(UTC).isoformat(),
            "session_id": session_id,
        }
        return PaperRuntimeSnapshot(
            session=summary["session"] if summary else None,
            feed_state=(
                {
                    "session_id": feed_state.session_id,
                    "source": feed_state.source,
                    "mode": feed_state.mode,
                    "status": feed_state.status,
                    "subscription_count": feed_state.subscription_count,
                    "is_stale": feed_state.is_stale,
                }
                if feed_state
                else None
            ),
            signals=signals,
            feed_plan=feed_plan,
        )

    async def execute_replay_cycle(
        self,
        sessionmaker: async_sessionmaker[Any],
        session_id: str,
    ) -> dict[str, Any]:
        async with sessionmaker() as db_session:
            summary = await get_paper_session_summary(db_session, session_id)
            if summary is None:
                raise ValueError(f"Paper session {session_id} not found")

            signals = self._normalize_runtime_signals(
                await list_session_signals(
                    db_session,
                    session_id,
                    states={"NEW", "QUALIFIED", "ALERTED", "ENTERED"},
                )
            )
            cycle_date = self._coerce_date(summary["session"].get("trade_date"))
            if cycle_date is None:
                cycle_date = next(
                    (
                        signal.get("planned_entry_date")
                        for signal in signals
                        if signal.get("planned_entry_date") is not None
                    ),
                    _utc_today(),
                )
            replay_signals = [{**signal, "planned_entry_date": cycle_date} for signal in signals]
            trader = self._get_trader(session_id, summary["session"].get("risk_config"))
            await self._sync_trader_state(db_session, session_id, trader, cycle_date)
            prices = await self._load_eod_prices(db_session, replay_signals)
            results = await trader.process_signals(replay_signals, prices, db_session, session_id)
            refreshed_summary = await get_paper_session_summary(db_session, session_id)

        return {
            "session_id": session_id,
            "mode": "replay",
            "processed_signals": len(results),
            "summary": refreshed_summary,
        }

    async def execute_live_cycle(
        self,
        sessionmaker: async_sessionmaker[Any],
        session_id: str,
        *,
        kite_client: KiteConnectClient,
    ) -> dict[str, Any]:
        async with sessionmaker() as db_session:
            summary = await get_paper_session_summary(db_session, session_id)
            if summary is None:
                raise ValueError(f"Paper session {session_id} not found")

            today = self._coerce_date(summary["session"].get("trade_date")) or _utc_today()
            signals = self._normalize_runtime_signals(
                await list_session_signals(
                    db_session,
                    session_id,
                    states={"NEW", "QUALIFIED", "ALERTED", "ENTERED"},
                )
            )
            live_signals = [{**signal, "planned_entry_date": today} for signal in signals]
            trader = self._get_trader(session_id, summary["session"].get("risk_config"))
            await self._sync_trader_state(db_session, session_id, trader, today)
            prices = await self._load_live_prices(
                db_session,
                live_signals,
                kite_client=kite_client,
            )
            results = await trader.process_signals(live_signals, prices, db_session, session_id)
            refreshed_summary = await get_paper_session_summary(db_session, session_id)

        return {
            "session_id": session_id,
            "mode": "live",
            "processed_signals": len(results),
            "summary": refreshed_summary,
        }

    async def process_live_ticks(
        self,
        sessionmaker: async_sessionmaker[Any],
        session_id: str,
        ticks: list[dict[str, Any]],
        *,
        observe_only: bool,
    ) -> dict[str, Any]:
        if not ticks:
            return {"session_id": session_id, "triggered": 0, "executed": 0}

        from nse_momentum_lab.db.models import PaperSessionSignal, Signal
        from nse_momentum_lab.services.paper.live_watchlist import check_intraday_trigger

        tick_by_token = {
            int(token): tick
            for tick in ticks
            if (token := tick.get("instrument_token")) is not None
        }
        if not tick_by_token:
            return {"session_id": session_id, "triggered": 0, "executed": 0}

        now_ist = datetime.now(IST)
        market_open = datetime.combine(now_ist.date(), nse_open_time(), tzinfo=IST)
        minutes_from_open = max(0, int((now_ist - market_open).total_seconds() // 60))

        async with sessionmaker() as db_session:
            summary = await get_paper_session_summary(db_session, session_id)
            if summary is None:
                return {"session_id": session_id, "triggered": 0, "executed": 0}

            today = self._coerce_date(summary["session"].get("trade_date")) or _utc_today()
            signals = self._normalize_runtime_signals(
                await list_session_signals(db_session, session_id)
            )
            promoted_signals: list[dict[str, Any]] = []
            triggered = 0

            for signal in signals:
                metadata = dict(signal.get("metadata_json") or {})
                if metadata.get("watch_state") not in {"WATCH", "ARMED"}:
                    continue
                token = metadata.get("instrument_token")
                if token in (None, ""):
                    continue
                try:
                    tick = tick_by_token[int(token)]
                except KeyError, TypeError, ValueError:
                    continue

                prev_close = metadata.get("prev_close")
                if prev_close in (None, ""):
                    continue
                ohlc = tick.get("ohlc") or {}
                current_high = tick.get("high") or ohlc.get("high") or tick.get("last_price")
                current_low = tick.get("low") or ohlc.get("low") or tick.get("last_price")
                trigger = check_intraday_trigger(
                    symbol=str(metadata.get("symbol") or ""),
                    trade_date=today,
                    prev_close=float(prev_close),
                    current_high=float(current_high) if current_high not in (None, "") else None,
                    current_low=float(current_low) if current_low not in (None, "") else None,
                    threshold=float(metadata.get("threshold") or 0.04),
                    direction=str(metadata.get("direction") or "long"),
                    entry_cutoff_minutes=int(metadata.get("entry_cutoff_minutes") or 30),
                    minutes_from_open=minutes_from_open,
                )
                metadata["watch_state"] = trigger["state"]
                metadata["watch_reason"] = trigger["reason"]
                metadata["last_price"] = tick.get("last_price")
                metadata["last_trigger_check_at"] = now_ist.isoformat()
                if not trigger["triggered"]:
                    if trigger["state"] == "REJECTED":
                        await db_session.execute(
                            update(PaperSessionSignal)
                            .where(
                                PaperSessionSignal.session_id == session_id,
                                PaperSessionSignal.signal_id == signal["signal_id"],
                            )
                            .values(
                                decision_status="REJECTED",
                                decision_reason=str(trigger.get("reason") or "") or None,
                                metadata_json=metadata,
                            )
                        )
                    await db_session.execute(
                        update(Signal)
                        .where(Signal.signal_id == signal["signal_id"])
                        .values(metadata_json=metadata)
                    )
                    continue

                triggered += 1
                metadata["triggered_at"] = now_ist.isoformat()
                metadata["trigger_price"] = trigger.get("trigger_price")
                target_state = "ARCHIVED" if observe_only else "NEW"
                target_status = "TRIGGERED_OBSERVE" if observe_only else "TRIGGERED"

                await db_session.execute(
                    update(Signal)
                    .where(Signal.signal_id == signal["signal_id"])
                    .values(
                        state=target_state,
                        planned_entry_date=today,
                        metadata_json=metadata,
                    )
                )
                await db_session.execute(
                    update(PaperSessionSignal)
                    .where(
                        PaperSessionSignal.session_id == session_id,
                        PaperSessionSignal.signal_id == signal["signal_id"],
                    )
                    .values(
                        decision_status=target_status,
                        decision_reason=str(trigger.get("reason") or "") or None,
                        metadata_json=metadata,
                    )
                )
                if not observe_only:
                    promoted_signals.append(
                        {
                            **signal,
                            "state": target_state,
                            "planned_entry_date": today,
                            "metadata_json": metadata,
                        }
                    )

            await db_session.commit()

            executed = 0
            if promoted_signals:
                trader = self._get_trader(session_id, summary["session"].get("risk_config"))
                await self._sync_trader_state(db_session, session_id, trader, today)
                prices: dict[int, dict[date, dict[str, float]]] = {}
                for signal in promoted_signals:
                    metadata = dict(signal.get("metadata_json") or {})
                    token = metadata.get("instrument_token")
                    if token in (None, ""):
                        continue
                    try:
                        tick = tick_by_token[int(token)]
                    except KeyError, TypeError, ValueError:
                        continue
                    last_price = tick.get("last_price")
                    if last_price in (None, ""):
                        continue
                    prices[int(signal["symbol_id"])] = {
                        today: {
                            "close": float(last_price),
                            "close_adj": float(last_price),
                            "value_traded_inr": None,
                        }
                    }
                if prices:
                    results = await trader.process_signals(
                        promoted_signals,
                        prices,
                        db_session,
                        session_id,
                    )
                    executed = len(results)

        return {
            "session_id": session_id,
            "triggered": triggered,
            "executed": executed,
            "observe_only": observe_only,
        }


def redact_credentials(plan: dict[str, Any]) -> dict[str, Any]:
    """Remove credentials from feed_plan before CLI output."""
    redacted = dict(plan)
    redacted.pop("connection_url", None)
    return redacted
