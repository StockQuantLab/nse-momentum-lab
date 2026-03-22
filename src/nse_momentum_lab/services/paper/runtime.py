from __future__ import annotations

import csv
import inspect
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from nse_momentum_lab.db.market_db import get_backtest_db
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
from nse_momentum_lab.services.kite.client import KiteConnectClient
from nse_momentum_lab.services.kite.ticker import (
    build_websocket_url,
    plan_subscription_batches,
)
from nse_momentum_lab.services.paper.engine import PaperTrader, RiskConfig

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
    def _signal_state_from_backtest_status(status: str) -> str:
        normalized = status.strip().lower()
        if normalized in ACTIONABLE_BACKTEST_STATUSES:
            return "NEW"
        return "ARCHIVED"

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
        }

    @staticmethod
    def _resolve_instrument_tokens(symbols: list[str]) -> list[int]:
        if not symbols or not INSTRUMENT_CACHE_PATH.exists():
            return []

        wanted = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        resolved: list[int] = []
        with INSTRUMENT_CACHE_PATH.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                tradingsymbol = str(row.get("tradingsymbol") or "").strip().upper()
                if tradingsymbol not in wanted:
                    continue
                try:
                    resolved.append(int(str(row.get("instrument_token") or "").strip()))
                except ValueError:
                    continue
        return list(dict.fromkeys(resolved))

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
        symbol_result = await db_session.execute(
            select(RefSymbol).where(RefSymbol.symbol.in_(symbols))
        )
        ref_symbols = {
            row.symbol.strip().upper(): row for row in symbol_result.scalars().all() if row.symbol
        }

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
            signal_state = self._signal_state_from_backtest_status(
                str(row.get("status") or "ARCHIVED")
            )
            signal_row = await upsert_signal(
                db_session,
                session_id=plan.session_id,
                symbol_id=ref_symbol.symbol_id,
                asof_date=plan.trade_date,
                strategy_hash=plan.experiment_id,
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
            positions = result.scalars().all()
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
                    strategy_params=plan.strategy_params,
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
                    strategy_params=plan.strategy_params,
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
