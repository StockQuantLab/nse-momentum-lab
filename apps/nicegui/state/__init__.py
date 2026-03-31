"""Shared state management for NiceGUI dashboard.

Provides persistent connections and reactive state across all pages.
"""

from __future__ import annotations

import asyncio
import logging as _logging
import sys
import threading as _threading
import time
from pathlib import Path

# Set up paths
_apps_root = Path(__file__).resolve().parent  # apps/nicegui/
_project_root = _apps_root.parent.parent  # project root
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))

from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
import polars as pl
from sqlalchemy import and_, func, select
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import MdOhlcvAdj, PaperPosition, RefSymbol
from nse_momentum_lab.db.paper import (
    get_paper_session_summary,
    list_paper_fills,
    list_paper_order_events,
    list_paper_orders,
    list_paper_session_signals,
    list_paper_sessions,
)
from nse_momentum_lab.db.market_db import (
    MarketDataDB,
    close_backtest_db,
    close_market_db,
    get_backtest_db,
    get_market_db,
)

# Lazy DB connections — created on first use, not at import time.
# This avoids a ~10s startup penalty from DuckDB view registration
# over 20K parquet files (daily + 5min + backtest catalogs).
_db: MarketDataDB | None = None
_backtest_db: MarketDataDB | None = None


def get_db() -> MarketDataDB:
    """Get the singleton market DuckDB connection (lazy-initialized)."""
    global _db
    if _db is None:
        _db = get_market_db(read_only=True)
    return _db


def get_backtest_db_ro() -> MarketDataDB:
    """Get the singleton backtest DuckDB connection (lazy-initialized)."""
    global _backtest_db
    if _backtest_db is None:
        _backtest_db = get_backtest_db(read_only=True)
    return _backtest_db


# Thread pool for running blocking DB calls off the async event loop
# (DuckDB is not async-native; running it directly on the event loop stalls NiceGUI)
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-worker")
# max_workers=1 is intentional: DuckDB's connection object (db.con) is NOT thread-safe
# for concurrent access. A single worker serializes all DB calls safely, while still
# freeing the asyncio event loop during slow queries (e.g. COUNT DISTINCT on Parquet).
_pg_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pg-worker")

# Global experiment cache (refreshed periodically)
_experiments_cache: pl.DataFrame | None = None
_experiments_cache_time: float = 0

# Global status cache - persist to disk for fast restarts
_status_cache: dict | None = None
_status_cache_time: float = 0
_STATUS_CACHE_FILE = Path.home() / ".cache" / "nseml_dashboard_status.json"

EXPERIMENT_CACHE_TTL = 60  # seconds
STATUS_CACHE_TTL = 300  # seconds  (5 minutes - heavier query, cache longer)
MARKET_MONITOR_CACHE_TTL = 60  # seconds

_market_monitor_cache: dict[tuple[str, int | None], tuple[float, pl.DataFrame]] = {}

# Optional: use a fast "lite" status for initial page load
_USE_LITE_STATUS_ON_FIRST_LOAD = True
_dashboard_resources_closed = False


def shutdown_dashboard_resources() -> None:
    """Release process-lifetime resources so the dashboard can exit cleanly."""
    global _dashboard_resources_closed
    if _dashboard_resources_closed:
        return
    _dashboard_resources_closed = True

    for executor in (_executor, _pg_executor):
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    for closer in (close_market_db, close_backtest_db):
        try:
            closer()
        except Exception:
            pass


def _run_pg_coro_sync(coro_factory: Callable[[], Awaitable[object]]) -> object:
    if sys.platform == "win32":
        selector_loop_cls = getattr(asyncio, "SelectorEventLoop", None)
        if selector_loop_cls is None:
            raise RuntimeError("asyncio.SelectorEventLoop is not available on this platform")
        with asyncio.Runner(loop_factory=selector_loop_cls) as runner:
            return runner.run(coro_factory())
    return asyncio.run(coro_factory())


async def _run_pg_coro(coro_factory: Callable[[], Awaitable[object]]) -> object:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_pg_executor, _run_pg_coro_sync, coro_factory)


def _fetch_experiments_sync(force_refresh: bool = False) -> pl.DataFrame:
    """Synchronous implementation — always call via get_experiments() or aget_experiments()."""
    global _experiments_cache, _experiments_cache_time

    now = time.time()
    if (
        _experiments_cache is None
        or force_refresh
        or (now - _experiments_cache_time) > EXPERIMENT_CACHE_TTL
    ):
        exps = get_backtest_db_ro().list_experiments()
        if not exps.is_empty():
            _experiments_cache = exps.with_columns(
                pl.col("status").cast(pl.Utf8).str.to_lowercase()
            ).filter(pl.col("status") == "completed")
            # DB returns ORDER BY created_at DESC; preserve that as primary sort
            if "created_at" in _experiments_cache.columns:
                _experiments_cache = _experiments_cache.sort("created_at", descending=True)
            else:
                _experiments_cache = _experiments_cache.sort("start_year", descending=True)
        else:
            _experiments_cache = pl.DataFrame()
        _experiments_cache_time = now

    return _experiments_cache


def get_experiments(force_refresh: bool = False) -> pl.DataFrame:
    """Get cached experiments list synchronously."""
    return _fetch_experiments_sync(force_refresh)


async def aget_experiments(force_refresh: bool = False) -> pl.DataFrame:
    """Async wrapper — runs the blocking DB call in a thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_experiments_sync(force_refresh))


def _load_status_from_disk() -> dict | None:
    """Load cached status from disk for instant first-page load."""
    try:
        if _STATUS_CACHE_FILE.exists():
            import json

            with open(_STATUS_CACHE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _save_status_to_disk(status: dict) -> None:
    """Save status to disk for fast subsequent loads."""
    try:
        _STATUS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        import json

        with open(_STATUS_CACHE_FILE, "w") as f:
            json.dump(status, f)
    except Exception:
        pass


def get_status_lite() -> dict:
    """Fast status check - only returns cached or lightweight data.

    Used for initial page load. Full status loads async in background.
    """
    # Try disk cache first (fastest - instant)
    cached = _load_status_from_disk()
    if cached:
        return cached

    # Fall back to minimal status if no cache
    return {
        "data_source": getattr(get_db(), "_data_source", "unknown"),
        "symbols": 0,
        "total_candles": 0,
        "date_range": "Loading...",
        "dataset_hash": None,
        "tables": {},
    }


def _fetch_status_sync() -> dict:
    """Synchronous implementation — always call via get_db_status() or aget_db_status()."""
    global _status_cache, _status_cache_time

    now = time.time()
    if _status_cache is None or (now - _status_cache_time) > STATUS_CACHE_TTL:
        _status_cache = get_db().get_status()
        _status_cache_time = now
        # Persist to disk for fast restarts
        _save_status_to_disk(_status_cache)

    return _status_cache


def get_db_status() -> dict:
    """Get current database status (synchronous, TTL-cached).

    Returns disk cache instantly if available, otherwise fetches fresh data.
    """
    global _status_cache, _status_cache_time

    # Check in-memory cache first
    if _status_cache is not None:
        return _status_cache

    # Try disk cache for instant load (no queries)
    disk_cache = _load_status_from_disk()
    if disk_cache:
        _status_cache = disk_cache
        _status_cache_time = time.time()
        return disk_cache

    # No cache available - fetch fresh (will be slow on first run)
    return _fetch_status_sync()


async def aget_db_status(lite: bool = False) -> dict:
    """Async wrapper — runs the blocking Parquet COUNT query in a thread pool.

    Args:
        lite: If True, returns cached/minimal data instantly without blocking.
              Use for initial page load, then refresh with full data.
    """
    if lite:
        return get_status_lite()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_status_sync)


def get_experiment(exp_id: str) -> dict | None:
    """Get experiment details by ID."""
    return get_backtest_db_ro().get_experiment(exp_id)


async def aget_experiment(exp_id: str) -> dict | None:
    """Async wrapper for get_experiment."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment(exp_id))


def get_experiment_trades(exp_id: str) -> pl.DataFrame:
    """Get all trades for an experiment."""
    df = get_backtest_db_ro().get_experiment_trades(exp_id)
    return df if not df.is_empty() else pl.DataFrame()


def get_experiment_execution_diagnostics(exp_id: str) -> pl.DataFrame:
    """Get execution diagnostics for an experiment."""
    df = get_backtest_db_ro().get_experiment_execution_diagnostics(exp_id)
    return df if not df.is_empty() else pl.DataFrame()


async def aget_experiment_trades(exp_id: str) -> pl.DataFrame:
    """Async wrapper for get_experiment_trades."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment_trades(exp_id))


async def aget_experiment_execution_diagnostics(exp_id: str) -> pl.DataFrame:
    """Async wrapper for get_experiment_execution_diagnostics."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor, lambda: get_experiment_execution_diagnostics(exp_id)
    )


def get_experiment_yearly_metrics(exp_id: str) -> pl.DataFrame:
    """Get yearly metrics for an experiment."""
    df = get_backtest_db_ro().get_experiment_yearly_metrics(exp_id)
    return df if not df.is_empty() else pl.DataFrame()


async def aget_experiment_yearly_metrics(exp_id: str) -> pl.DataFrame:
    """Async wrapper for get_experiment_yearly_metrics."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment_yearly_metrics(exp_id))


def delete_experiments_write(exp_ids: list[str]) -> tuple[int, str | None]:
    """Delete experiments from the write DB and refresh the dashboard snapshot.

    Opens a temporary write connection to backtest.duckdb, deletes the given
    experiments, then refreshes backtest_dashboard.duckdb to match.

    Returns (count_deleted, error_message_or_None).
    If a backtest is currently running (DB locked), returns (0, error).
    """
    global _experiments_cache, _experiments_cache_time

    if not exp_ids:
        return 0, None

    try:
        import duckdb as _duckdb
        from nse_momentum_lab.db.market_db import (
            BACKTEST_DUCKDB_FILE,
            BACKTEST_DASHBOARD_DUCKDB_FILE,
        )

        con = _duckdb.connect(str(BACKTEST_DUCKDB_FILE), read_only=False)
        try:
            con.execute("BEGIN TRANSACTION")
            for exp_id in exp_ids:
                con.execute("DELETE FROM bt_trade WHERE exp_id = ?", [exp_id])
                con.execute("DELETE FROM bt_yearly_metric WHERE exp_id = ?", [exp_id])
                con.execute("DELETE FROM bt_execution_diagnostic WHERE exp_id = ?", [exp_id])
                con.execute("DELETE FROM bt_experiment WHERE exp_id = ?", [exp_id])
            con.execute("COMMIT")

            # Refresh the read-only dashboard copy when available, but do not fail
            # the deletion if the legacy snapshot file is locked or unavailable.
            dash = str(BACKTEST_DASHBOARD_DUCKDB_FILE).replace("\\", "/").replace("'", "''")
            try:
                con.execute(f"ATTACH '{dash}' AS bt_read")
            except Exception:
                pass
            else:
                try:
                    for tbl in (
                        "bt_experiment",
                        "bt_yearly_metric",
                        "bt_trade",
                        "bt_execution_diagnostic",
                    ):
                        con.execute(f"CREATE OR REPLACE TABLE bt_read.{tbl} AS SELECT * FROM {tbl}")
                finally:
                    con.execute("DETACH bt_read")
        finally:
            con.close()

        # Invalidate the in-memory cache so the next load reflects the deletion
        _experiments_cache = None
        _experiments_cache_time = 0

        return len(exp_ids), None
    except Exception as exc:
        return 0, str(exc)


async def adelete_experiments_write(exp_ids: list[str]) -> tuple[int, str | None]:
    """Async wrapper — runs delete_experiments_write in the shared thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: delete_experiments_write(exp_ids))


def get_market_monitor_latest() -> pl.DataFrame:
    """Get the latest market monitor snapshot, if available."""
    cache_key = ("latest", None)
    now = time.time()
    cached = _market_monitor_cache.get(cache_key)
    if cached is not None and (now - cached[0]) <= MARKET_MONITOR_CACHE_TTL:
        return cached[1].clone()

    df = get_db().get_market_monitor_latest()
    value = df if not df.is_empty() else pl.DataFrame()
    _market_monitor_cache[cache_key] = (now, value)
    return value.clone()


def get_market_monitor_history(days: int = 252) -> pl.DataFrame:
    """Get recent market monitor history, if available."""
    cache_key = ("history", days)
    now = time.time()
    cached = _market_monitor_cache.get(cache_key)
    if cached is not None and (now - cached[0]) <= MARKET_MONITOR_CACHE_TTL:
        return cached[1].clone()

    df = get_db().get_market_monitor_history(days=days)
    value = df if not df.is_empty() else pl.DataFrame()
    _market_monitor_cache[cache_key] = (now, value)
    return value.clone()


def get_market_monitor_all() -> pl.DataFrame:
    """Get ALL market monitor history, if available."""
    cache_key = ("all", None)
    now = time.time()
    cached = _market_monitor_cache.get(cache_key)
    if cached is not None and (now - cached[0]) <= MARKET_MONITOR_CACHE_TTL:
        return cached[1].clone()

    df = get_db().get_market_monitor_all()
    value = df if not df.is_empty() else pl.DataFrame()
    _market_monitor_cache[cache_key] = (now, value)
    return value.clone()


async def aget_market_monitor_latest() -> pl.DataFrame:
    """Async wrapper for get_market_monitor_latest."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, get_market_monitor_latest)


async def aget_market_monitor_history(days: int = 252) -> pl.DataFrame:
    """Async wrapper for get_market_monitor_history."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: get_market_monitor_history(days=days))


async def aget_market_monitor_all() -> pl.DataFrame:
    """Async wrapper for get_market_monitor_all - fetches all available data."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, get_market_monitor_all)


def _strategy_display_name(row: dict) -> str:
    """Derive display name that includes breakout threshold when applicable."""
    import json as _json

    name = str(row.get("strategy_name", "?"))
    params: dict = {}
    if "params_json" in row and row.get("params_json") is not None:
        try:
            params = _json.loads(row["params_json"])
        except ValueError, TypeError:
            pass
    threshold = params.get("breakout_threshold")
    if threshold is not None:
        pct = round(float(threshold) * 100)
        return f"{name} {pct}%"
    return name


def _run_window_display(row: dict) -> str:
    """Return exact backtest date window when available, else year range."""
    import json as _json

    start_year = row.get("start_year", "?")
    end_year = row.get("end_year", "?")
    fallback = f"{start_year}-{end_year}"

    if "params_json" not in row or row.get("params_json") is None:
        return fallback

    try:
        params = _json.loads(row["params_json"])
    except TypeError, ValueError:
        return fallback

    start_date = params.get("start_date")
    end_date = params.get("end_date")
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    if start_date:
        return f"from {start_date}"
    if end_date:
        return f"to {end_date}"
    return fallback


def build_experiment_options(experiments_df: pl.DataFrame) -> dict[str, str]:
    """Build {label: exp_id} dict with human-readable labels, latest first.

    Label format: "2LYNCHBreakout 4% | 2025-04-01 to 2026-03-10 | 991 trades | Ret 136.4% | Mar 12 13:52"
    """
    options: dict[str, str] = {}
    for row in experiments_df.iter_rows(named=True):
        strategy = _strategy_display_name(row)
        window = _run_window_display(row)
        trades = int(row.get("total_trades", 0) or 0)
        ret = float(row.get("total_return_pct", 0) or 0)

        # Created-at date for disambiguation
        created = ""
        created_val = row.get("created_at")
        if created_val is not None:
            try:
                created = f" | {created_val.strftime('%b %d %H:%M')}"
            except AttributeError, TypeError:
                created = f" | {str(created_val)[:16]}"

        label = f"{strategy} | {window} | {trades:,} trades | Ret {ret:.1f}%{created}"
        options[label] = row["exp_id"]
    return options


def format_time(value) -> str:
    """Format DuckDB TIME to HH:MM string."""
    if value is None:
        return ""
    return str(value)[:5]


def prepare_trades_df(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare trades dataframe for display with proper formatting."""
    if df.is_empty():
        return df

    casts = []
    if "entry_date" in df.columns:
        casts.append(pl.col("entry_date").cast(pl.Date, strict=False))
    if "exit_date" in df.columns:
        casts.append(pl.col("exit_date").cast(pl.Date, strict=False))

    if casts:
        df = df.with_columns(casts)

    time_cols = []
    if "entry_time" in df.columns:
        time_cols.append(
            pl.col("entry_time").cast(pl.Utf8, strict=False).str.slice(0, 5).alias("entry_time")
        )
    if "exit_time" in df.columns:
        time_cols.append(
            pl.col("exit_time").cast(pl.Utf8, strict=False).str.slice(0, 5).alias("exit_time")
        )
    if time_cols:
        df = df.with_columns(time_cols)

    numeric_casts = []
    for col in ["pnl_pct", "pnl_r", "holding_days", "year", "entry_price", "exit_price"]:
        if col in df.columns:
            numeric_casts.append(pl.col(col).cast(pl.Float64, strict=False))
    if numeric_casts:
        df = df.with_columns(numeric_casts)

    return df


async def aget_paper_sessions(status: str | None = None, limit: int = 50) -> list[dict]:
    async def _load() -> list[dict]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            return await list_paper_sessions(session, status=status, limit=limit)

    return await _run_pg_coro(_load)


async def aget_paper_session_summary(session_id: str) -> dict | None:
    async def _load() -> dict | None:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            return await get_paper_session_summary(session, session_id)

    return await _run_pg_coro(_load)


async def aget_paper_session_signals(session_id: str) -> list[dict]:
    async def _load() -> list[dict]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            return await list_paper_session_signals(session, session_id)

    return await _run_pg_coro(_load)


async def aget_paper_session_orders(session_id: str, limit: int = 100) -> list[dict]:
    async def _load() -> list[dict]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            return await list_paper_orders(session, session_id, limit=limit)

    return await _run_pg_coro(_load)


async def aget_paper_session_fills(session_id: str, limit: int = 100) -> list[dict]:
    async def _load() -> list[dict]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            return await list_paper_fills(session, session_id, limit=limit)

    return await _run_pg_coro(_load)


async def aget_paper_session_events(session_id: str, limit: int = 100) -> list[dict]:
    async def _load() -> list[dict]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            return await list_paper_order_events(session, session_id, limit=limit)

    return await _run_pg_coro(_load)


async def aget_paper_positions(
    session_id: str | None = None,
    *,
    open_only: bool = True,
) -> list[dict]:
    async def _load() -> list[dict]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            query = select(PaperPosition)
            if open_only:
                query = query.where(PaperPosition.closed_at.is_(None))
            if session_id:
                query = query.where(PaperPosition.session_id == session_id)
            query = query.order_by(PaperPosition.opened_at.desc())
            result = await session.execute(query)
            rows = result.scalars().all()

            symbol_ids = sorted({row.symbol_id for row in rows})
            symbol_map: dict[int, str] = {}
            if symbol_ids:
                symbol_result = await session.execute(
                    select(RefSymbol.symbol_id, RefSymbol.symbol).where(
                        RefSymbol.symbol_id.in_(symbol_ids)
                    )
                )
                symbol_map = {
                    symbol_id: str(symbol).strip()
                    for symbol_id, symbol in symbol_result.all()
                    if symbol is not None and str(symbol).strip()
                }

            open_symbol_ids = sorted(
                {
                    row.symbol_id
                    for row in rows
                    if row.closed_at is None and row.symbol_id is not None
                }
            )
            latest_close_map: dict[int, float] = {}
            if open_symbol_ids:
                latest_dates = (
                    select(
                        MdOhlcvAdj.symbol_id.label("symbol_id"),
                        func.max(MdOhlcvAdj.trading_date).label("max_date"),
                    )
                    .where(MdOhlcvAdj.symbol_id.in_(open_symbol_ids))
                    .group_by(MdOhlcvAdj.symbol_id)
                    .subquery()
                )
                close_result = await session.execute(
                    select(MdOhlcvAdj.symbol_id, MdOhlcvAdj.close_adj).join(
                        latest_dates,
                        and_(
                            MdOhlcvAdj.symbol_id == latest_dates.c.symbol_id,
                            MdOhlcvAdj.trading_date == latest_dates.c.max_date,
                        ),
                    )
                )
                latest_close_map = {
                    symbol_id: float(close_adj)
                    for symbol_id, close_adj in close_result.all()
                    if close_adj is not None
                }

            return [
                {
                    "position_id": row.position_id,
                    "session_id": row.session_id,
                    "symbol_id": row.symbol_id,
                    "symbol": symbol_map.get(row.symbol_id),
                    "opened_at": row.opened_at.isoformat() if row.opened_at else None,
                    "closed_at": row.closed_at.isoformat() if row.closed_at else None,
                    "avg_entry": float(row.avg_entry) if row.avg_entry is not None else None,
                    "avg_exit": float(row.avg_exit) if row.avg_exit is not None else None,
                    "qty": float(row.qty) if row.qty is not None else None,
                    "pnl": float(row.pnl) if row.pnl is not None else None,
                    "market_price": latest_close_map.get(row.symbol_id),
                    "unrealized_pnl": (
                        (float(latest_close_map[row.symbol_id]) - float(row.avg_entry))
                        * float(row.qty)
                        if row.closed_at is None
                        and row.symbol_id in latest_close_map
                        and row.avg_entry is not None
                        and row.qty is not None
                        else None
                    ),
                    "state": row.state,
                    "metadata_json": row.metadata_json or {},
                }
                for row in rows
            ]

    return await _run_pg_coro(_load)


_experiment_callbacks: list = []


def on_new_experiments(callback) -> None:
    """Register callback for new experiments."""
    _experiment_callbacks.append(callback)


async def poll_new_experiments(force_refresh: bool = True) -> pl.DataFrame:
    """Check for new experiments and notify listeners."""
    global _experiments_cache, _experiments_cache_time

    old_count = len(_experiments_cache) if _experiments_cache is not None else 0
    result = await aget_experiments(force_refresh=force_refresh)
    new_count = len(_experiments_cache) if _experiments_cache is not None else 0

    if new_count > old_count and old_count > 0:
        for cb in _experiment_callbacks:
            cb()

    return result


# ---------------------------------------------------------------------------
# Kite ingestion — background runner (parquet-only; no DuckDB write)
# ---------------------------------------------------------------------------

_ingestion_log: list[str] = []
_ingestion_status: str = "idle"  # idle | running | completed | failed
_ingestion_mutex: _threading.Lock = _threading.Lock()


class _IngestionLogHandler(_logging.Handler):
    """Funnels kite-package log records into the shared UI log buffer."""

    def emit(self, record: _logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            with _ingestion_mutex:
                _ingestion_log.append(msg)
                if len(_ingestion_log) > 600:
                    del _ingestion_log[:100]
        except Exception:
            pass


def get_ingestion_status() -> str:
    """Return current ingestion status: idle | running | completed | failed."""
    return _ingestion_status


def get_ingestion_log() -> list[str]:
    """Return a snapshot of the ingestion log lines."""
    with _ingestion_mutex:
        return list(_ingestion_log)


def get_all_missing_tradeable_symbols() -> tuple[list[str], list[str]]:
    """Return (missing_daily, missing_5min) — symbols in Kite master but absent from parquet.

    Both lists may overlap for symbols with no data at all.
    Runs synchronously; call from a thread pool.
    """
    from nse_momentum_lab.db.market_db import PARQUET_DIR
    from nse_momentum_lab.services.kite.tradeable import get_parquet_symbols, get_tradeable_symbols

    tradeable = get_tradeable_symbols()
    if not tradeable:
        return [], []

    daily_symbols = get_parquet_symbols(PARQUET_DIR / "daily", layout="daily")
    five_min_symbols = get_parquet_symbols(PARQUET_DIR / "5min", layout="5min")

    missing_daily = sorted(tradeable - daily_symbols)
    missing_5min = sorted(tradeable - five_min_symbols)
    return missing_daily, missing_5min


def _ingestion_thread_worker(
    symbols_daily: list[str],
    symbols_5min: list[str],
    start_date: object,
    end_date: object,
) -> None:
    """Background worker — writes parquet only, no DuckDB interaction."""
    global _ingestion_status

    handler = _IngestionLogHandler()
    handler.setFormatter(_logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))
    kite_root = _logging.getLogger("nse_momentum_lab.services.kite")
    kite_root.addHandler(handler)

    try:
        from nse_momentum_lab.services.kite.scheduler import get_kite_scheduler

        scheduler = get_kite_scheduler()

        if symbols_daily:
            with _ingestion_mutex:
                _ingestion_log.append(
                    f"▶ Daily ingestion: {len(symbols_daily)} symbols  {start_date} → {end_date}"
                )
            result_d = scheduler.run_daily_range_ingestion(
                symbols=symbols_daily,
                start_date=start_date,  # type: ignore[arg-type]
                end_date=end_date,  # type: ignore[arg-type]
                update_features=False,
                resume=True,
            )
            with _ingestion_mutex:
                _ingestion_log.append(
                    f"✓ Daily done  succeeded={result_d.get('succeeded', 0)}"
                    f"  failed={result_d.get('failed', 0)}"
                    f"  zero_rows={result_d.get('zero_rows', 0)}"
                )

        if symbols_5min:
            with _ingestion_mutex:
                _ingestion_log.append(
                    f"▶ 5-min ingestion: {len(symbols_5min)} symbols  {start_date} → {end_date}"
                )
            result_5 = scheduler.run_5min_ingestion(
                symbols=symbols_5min,
                start_date=start_date,  # type: ignore[arg-type]
                end_date=end_date,  # type: ignore[arg-type]
                resume=True,
            )
            with _ingestion_mutex:
                _ingestion_log.append(
                    f"✓ 5-min done  succeeded={result_5.get('succeeded', 0)}"
                    f"  failed={result_5.get('failed', 0)}"
                    f"  zero_rows={result_5.get('zero_rows', 0)}"
                )

        with _ingestion_mutex:
            _ingestion_log.append(
                "✅ Ingestion complete. Run 'nseml-build-features' to rebuild feat_daily."
            )
        _ingestion_status = "completed"

    except Exception as exc:
        with _ingestion_mutex:
            _ingestion_log.append(f"❌ ERROR: {exc}")
        _ingestion_status = "failed"
    finally:
        kite_root.removeHandler(handler)


def trigger_missing_ingestion(
    start_date: object,
    end_date: object,
    run_daily: bool = True,
    run_5min: bool = True,
) -> str | None:
    """Start ingestion for missing symbols in a background thread.

    Returns an error string if already running, else None (started ok).
    No DuckDB writes — parquet only. Feature rebuild must be run separately.
    """
    global _ingestion_status, _ingestion_log

    if _ingestion_status == "running":
        return "Ingestion already running — check the log below"

    symbols_daily: list[str] = []
    symbols_5min: list[str] = []
    try:
        missing_d, missing_5 = get_all_missing_tradeable_symbols()
        if run_daily:
            symbols_daily = missing_d
        if run_5min:
            symbols_5min = missing_5
    except Exception as exc:
        return f"Could not resolve missing symbols: {exc}"

    if not symbols_daily and not symbols_5min:
        return "No missing symbols found — parquet coverage is complete"

    _ingestion_status = "running"
    with _ingestion_mutex:
        _ingestion_log.clear()
        _ingestion_log.append(
            f"Resolved {len(symbols_daily)} missing daily / {len(symbols_5min)} missing 5-min symbols"
        )

    _threading.Thread(
        target=_ingestion_thread_worker,
        args=(symbols_daily, symbols_5min, start_date, end_date),
        daemon=True,
        name="kite-missing-ingestion",
    ).start()
    return None


# ---------------------------------------------------------------------------
# Data Quality metrics
# ---------------------------------------------------------------------------
def _fetch_data_quality_metrics_sync() -> dict:
    """Compute data quality metrics via DuckDB. Runs in thread pool."""
    from nse_momentum_lab.db.market_db import PARQUET_DIR
    from nse_momentum_lab.services.kite.tradeable import (
        get_dead_symbols,
        get_parquet_symbols,
        get_tradeable_symbols,
    )

    metrics: dict = {}

    # Tradeable symbols from instrument master
    tradeable = get_tradeable_symbols()
    metrics["tradeable_count"] = len(tradeable)

    # Parquet symbol counts
    daily_dir = PARQUET_DIR / "daily"
    five_min_dir = PARQUET_DIR / "5min"
    daily_symbols = get_parquet_symbols(daily_dir, layout="daily")
    five_min_symbols = get_parquet_symbols(five_min_dir, layout="5min")
    local_parquet = daily_symbols | five_min_symbols
    metrics["daily_symbols"] = len(daily_symbols)
    metrics["five_min_symbols"] = len(five_min_symbols)
    metrics["total_parquet_symbols"] = len(local_parquet)

    # Active vs dead
    dead_daily = get_dead_symbols(daily_dir, tradeable, layout="daily") if tradeable else set()
    dead_5min = get_dead_symbols(five_min_dir, tradeable, layout="5min") if tradeable else set()
    dead_all = dead_daily | dead_5min
    metrics["dead_count"] = len(dead_all)
    covered_tradeable = local_parquet & tradeable
    missing_tradeable = tradeable - local_parquet
    metrics["covered_tradeable_count"] = len(covered_tradeable)
    metrics["missing_tradeable_count"] = len(missing_tradeable)
    metrics["missing_tradeable_sample"] = sorted(missing_tradeable)[:50]
    metrics["active_count"] = len(covered_tradeable)
    metrics["coverage_pct"] = (
        round(len(covered_tradeable) / len(tradeable) * 100, 1) if tradeable else 0
    )
    metrics["local_coverage_pct"] = (
        round(len(covered_tradeable) / len(local_parquet) * 100, 1) if local_parquet else 0
    )

    # DuckDB table row counts
    tables_info: dict[str, int] = {}
    con = get_db().con
    for table in [
        "feat_daily_core",
        "feat_intraday_core",
        "feat_2lynch_derived",
        "feat_event_core",
        "market_monitor_daily",
    ]:
        try:
            row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            tables_info[table] = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            tables_info[table] = 0
    metrics["tables"] = tables_info

    # Date range from feat_daily_core
    try:
        row = con.execute(
            "SELECT MIN(trading_date)::VARCHAR, MAX(trading_date)::VARCHAR FROM feat_daily_core"
        ).fetchone()
        if row:
            metrics["min_date"] = str(row[0]) if row[0] else None
            metrics["max_date"] = str(row[1]) if row[1] else None
            metrics["date_range"] = f"{row[0]} to {row[1]}" if row[0] else "-"
    except Exception:
        metrics["date_range"] = "-"
        metrics["min_date"] = None
        metrics["max_date"] = None

    # Parquet directory sizes
    daily_size = (
        sum(f.stat().st_size for f in daily_dir.rglob("*") if f.is_file())
        if daily_dir.exists()
        else 0
    )
    five_min_size = (
        sum(f.stat().st_size for f in five_min_dir.rglob("*") if f.is_file())
        if five_min_dir.exists()
        else 0
    )
    metrics["daily_size_bytes"] = daily_size
    metrics["five_min_size_bytes"] = five_min_size
    metrics["total_parquet_bytes"] = daily_size + five_min_size
    kite_reports_dir = Path(__file__).resolve().parents[3] / "data" / "raw" / "kite" / "reports"
    kite_dq_report = kite_reports_dir / "dq_summary_latest.json"
    metrics["five_min_timestamp_issue_count"] = 0
    metrics["five_min_timestamp_issue_sample"] = []
    metrics["latest_kite_dq_report"] = str(kite_dq_report) if kite_dq_report.exists() else None
    if kite_dq_report.exists():
        try:
            import json as _json

            report = _json.loads(kite_dq_report.read_text(encoding="utf-8"))
            alignment = report.get("timestamp_alignment", {}) or {}
            metrics["five_min_timestamp_issue_count"] = int(alignment.get("issue_count", 0) or 0)
            sample = alignment.get("issue_sample", []) or []
            metrics["five_min_timestamp_issue_sample"] = sample[:50]
        except Exception:
            pass

    # DuckDB file size
    from nse_momentum_lab.db.market_db import DUCKDB_FILE

    if DUCKDB_FILE.exists():
        metrics["duckdb_size_bytes"] = DUCKDB_FILE.stat().st_size
    else:
        metrics["duckdb_size_bytes"] = 0

    # Symbols by last-data-date (monthly buckets, last 12 months)
    freshness: list[dict] = []
    try:
        rows = con.execute("""
            WITH last_dates AS (
                SELECT symbol, MAX(trading_date) as last_date
                FROM feat_daily_core
                GROUP BY symbol
            )
            SELECT
                DATE_TRUNC('month', last_date)::VARCHAR as month,
                COUNT(*) as symbol_count
            FROM last_dates
            GROUP BY DATE_TRUNC('month', last_date)
            ORDER BY month DESC
            LIMIT 12
        """).fetchall()
        for r in rows:
            freshness.append({"month": str(r[0])[:7] if r[0] else "?", "count": int(r[1])})
    except Exception:
        pass
    metrics["freshness"] = freshness

    # Latest hygiene report
    from nse_momentum_lab.cli.data_hygiene import REPORTS_DIR

    reports = (
        sorted(REPORTS_DIR.glob("hygiene_*.json"), reverse=True) if REPORTS_DIR.exists() else []
    )
    metrics["latest_hygiene_report"] = str(reports[0]) if reports else None

    return metrics


def get_data_quality_metrics() -> dict:
    """Get data quality metrics (synchronous, for initial page load)."""
    return _fetch_data_quality_metrics_sync()


_dq_metrics_cache: dict | None = None
_dq_metrics_cache_time: float = 0


def _fetch_data_quality_metrics_cached() -> dict:
    """Cached version — avoids scanning filesystem on every page load."""
    global _dq_metrics_cache, _dq_metrics_cache_time
    now = time.monotonic()
    if _dq_metrics_cache is not None and (now - _dq_metrics_cache_time) < _DQ_CACHE_TTL:
        return _dq_metrics_cache
    _dq_metrics_cache = _fetch_data_quality_metrics_sync()
    _dq_metrics_cache_time = now
    return _dq_metrics_cache


async def aget_data_quality_metrics() -> dict:
    """Async wrapper -- runs heavy DuckDB queries in thread pool (cached)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_data_quality_metrics_cached)


# ---------------------------------------------------------------------------
# Deep data quality analytics (lazy tab queries)
# ---------------------------------------------------------------------------
_DQ_CACHE_TTL = 600  # 10 minutes

_universe_timeline_cache: list[dict] | None = None
_universe_timeline_cache_time: float = 0


def _fetch_universe_timeline_sync() -> list[dict]:
    global _universe_timeline_cache, _universe_timeline_cache_time
    now = time.monotonic()
    if (
        _universe_timeline_cache is not None
        and (now - _universe_timeline_cache_time) < _DQ_CACHE_TTL
    ):
        return _universe_timeline_cache
    rows = (
        get_db()
        .con.execute("""
        SELECT EXTRACT(year FROM date)::INT AS year,
               COUNT(DISTINCT symbol)::INT AS symbol_count,
               COUNT(*)::INT AS total_rows
        FROM v_daily GROUP BY year ORDER BY year
    """)
        .fetchall()
    )
    result = [{"year": r[0], "symbol_count": r[1], "total_rows": r[2]} for r in rows]
    _universe_timeline_cache = result
    _universe_timeline_cache_time = now
    return result


async def aget_universe_timeline() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_universe_timeline_sync)


_symbol_coverage_cache: list[dict] | None = None
_symbol_coverage_cache_time: float = 0


def _fetch_symbol_coverage_sync() -> list[dict]:
    global _symbol_coverage_cache, _symbol_coverage_cache_time
    now = time.monotonic()
    if _symbol_coverage_cache is not None and (now - _symbol_coverage_cache_time) < _DQ_CACHE_TTL:
        return _symbol_coverage_cache
    rows = (
        get_db()
        .con.execute("""
        SELECT symbol,
               MIN(date)::VARCHAR AS first_date,
               MAX(date)::VARCHAR AS last_date,
               COUNT(*)::INT AS total_rows,
               COUNT(DISTINCT date)::INT AS distinct_days,
               (DATEDIFF('day', MIN(date), MAX(date)) + 1)::INT AS calendar_span
        FROM v_daily GROUP BY symbol ORDER BY symbol
    """)
        .fetchall()
    )
    result = []
    for r in rows:
        span = max(r[5], 1)
        expected = span * 5 / 7 * 0.96  # weekday estimate with ~10 holidays/yr
        cov = min(round(r[4] / max(expected, 1) * 100, 1), 100.0)
        gap_est = max(0, round(expected) - r[4])
        result.append(
            {
                "symbol": r[0],
                "first_date": r[1],
                "last_date": r[2],
                "total_rows": r[3],
                "distinct_days": r[4],
                "calendar_span": span,
                "coverage_pct": cov,
                "gap_estimate": gap_est,
            }
        )
    _symbol_coverage_cache = result
    _symbol_coverage_cache_time = now
    return result


async def aget_symbol_coverage() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_symbol_coverage_sync)


_date_coverage_cache: list[dict] | None = None
_date_coverage_cache_time: float = 0


def _fetch_date_coverage_sync() -> list[dict]:
    global _date_coverage_cache, _date_coverage_cache_time
    now = time.monotonic()
    if _date_coverage_cache is not None and (now - _date_coverage_cache_time) < _DQ_CACHE_TTL:
        return _date_coverage_cache
    rows = (
        get_db()
        .con.execute("""
        SELECT date::VARCHAR AS trading_date, COUNT(DISTINCT symbol)::INT AS symbol_count
        FROM v_daily GROUP BY date ORDER BY date
    """)
        .fetchall()
    )
    result = [{"trading_date": r[0], "symbol_count": r[1]} for r in rows]
    _date_coverage_cache = result
    _date_coverage_cache_time = now
    return result


async def aget_date_coverage() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_date_coverage_sync)


_top_gaps_cache: list[dict] | None = None
_top_gaps_cache_time: float = 0


def _fetch_top_gaps_sync(limit: int = 200) -> list[dict]:
    global _top_gaps_cache, _top_gaps_cache_time
    now = time.monotonic()
    if _top_gaps_cache is not None and (now - _top_gaps_cache_time) < _DQ_CACHE_TTL:
        return _top_gaps_cache
    rows = (
        get_db()
        .con.execute(f"""
        WITH symbol_dates AS (
            SELECT symbol, date,
                   LAG(date) OVER (PARTITION BY symbol ORDER BY date) AS prev_date
            FROM v_daily
        )
        SELECT symbol, prev_date::VARCHAR AS gap_start, date::VARCHAR AS gap_end,
               DATEDIFF('day', prev_date, date)::INT AS gap_days
        FROM symbol_dates
        WHERE prev_date IS NOT NULL AND DATEDIFF('day', prev_date, date) > 5
        ORDER BY gap_days DESC
        LIMIT {int(limit)}
    """)
        .fetchall()
    )
    result = [{"symbol": r[0], "gap_start": r[1], "gap_end": r[2], "gap_days": r[3]} for r in rows]
    _top_gaps_cache = result
    _top_gaps_cache_time = now
    return result


async def aget_top_gaps(limit: int = 200) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_top_gaps_sync(limit))


def _fetch_symbol_gaps_sync(symbol: str) -> list[dict]:
    rows = (
        get_db()
        .con.execute(
            """
        WITH sym_dates AS (
            SELECT date,
                   LAG(date) OVER (ORDER BY date) AS prev_date
            FROM v_daily WHERE symbol = ?
        )
        SELECT prev_date::VARCHAR AS gap_start, date::VARCHAR AS gap_end,
               DATEDIFF('day', prev_date, date)::INT AS gap_days
        FROM sym_dates
        WHERE prev_date IS NOT NULL AND DATEDIFF('day', prev_date, date) > 3
        ORDER BY gap_days DESC
    """,
            [symbol],
        )
        .fetchall()
    )
    return [{"gap_start": r[0], "gap_end": r[1], "gap_days": r[2]} for r in rows]


async def aget_symbol_gaps(symbol: str) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_symbol_gaps_sync(symbol))


_freshness_cache: list[dict] | None = None
_freshness_cache_time: float = 0


def _fetch_freshness_buckets_sync() -> list[dict]:
    global _freshness_cache, _freshness_cache_time
    now = time.monotonic()
    if _freshness_cache is not None and (now - _freshness_cache_time) < _DQ_CACHE_TTL:
        return _freshness_cache
    rows = (
        get_db()
        .con.execute("""
        WITH last_dates AS (
            SELECT symbol, MAX(date) AS last_date FROM v_daily GROUP BY symbol
        ),
        bucketed AS (
            SELECT symbol, last_date,
                   CASE
                       WHEN CURRENT_DATE - last_date <= 7 THEN 'Fresh (<7d)'
                       WHEN CURRENT_DATE - last_date <= 30 THEN 'Recent (7-30d)'
                       WHEN CURRENT_DATE - last_date <= 90 THEN 'Stale (30-90d)'
                       ELSE 'Very Stale (>90d)'
                   END AS bucket,
                   CASE
                       WHEN CURRENT_DATE - last_date <= 7 THEN 1
                       WHEN CURRENT_DATE - last_date <= 30 THEN 2
                       WHEN CURRENT_DATE - last_date <= 90 THEN 3
                       ELSE 4
                   END AS sort_key
            FROM last_dates
        )
        SELECT bucket, COUNT(*)::INT AS count, sort_key,
               LIST(symbol ORDER BY symbol)[:50] AS sample_symbols
        FROM bucketed GROUP BY bucket, sort_key ORDER BY sort_key
    """)
        .fetchall()
    )
    result = [{"bucket": r[0], "count": r[1], "symbols": r[3]} for r in rows]
    _freshness_cache = result
    _freshness_cache_time = now
    return result


async def aget_freshness_buckets() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_freshness_buckets_sync)


_anomalies_cache: list[dict] | None = None
_anomalies_cache_time: float = 0


def _fetch_price_anomalies_sync(limit: int = 500) -> list[dict]:
    global _anomalies_cache, _anomalies_cache_time
    now = time.monotonic()
    if _anomalies_cache is not None and (now - _anomalies_cache_time) < _DQ_CACHE_TTL:
        return _anomalies_cache
    rows = (
        get_db()
        .con.execute(f"""
        WITH priced AS (
            SELECT symbol, date, open, high, low, close, volume,
                   LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
            FROM v_daily
        )
        SELECT * FROM (
            SELECT symbol, date::VARCHAR AS trading_date, 'OHLC Invalid' AS issue,
                   'High(' || ROUND(high,2) || ') < Low(' || ROUND(low,2) || ')' AS detail
            FROM priced WHERE high < low
            UNION ALL
            SELECT symbol, date::VARCHAR, 'Zero Volume', 'volume=0'
            FROM priced WHERE volume = 0 OR volume IS NULL
            UNION ALL
            SELECT symbol, date::VARCHAR, 'Extreme Move',
                   ROUND((close / NULLIF(prev_close, 0) - 1) * 100, 1)::VARCHAR || '%'
            FROM priced
            WHERE prev_close IS NOT NULL AND prev_close > 0
              AND ABS(close / prev_close - 1) > 0.30
            UNION ALL
            SELECT symbol, date::VARCHAR, 'Zero/Negative Price',
                   'close=' || ROUND(close, 2)
            FROM priced WHERE close <= 0
        ) sub
        ORDER BY trading_date DESC
        LIMIT {int(limit)}
    """)
        .fetchall()
    )
    result = [{"symbol": r[0], "trading_date": r[1], "issue": r[2], "detail": r[3]} for r in rows]
    _anomalies_cache = result
    _anomalies_cache_time = now
    return result


async def aget_price_anomalies(limit: int = 500) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_price_anomalies_sync(limit))


_available_symbols_cache: list[str] | None = None
_available_symbols_cache_time: float = 0


def _fetch_available_symbols_sync() -> list[str]:
    global _available_symbols_cache, _available_symbols_cache_time
    now = time.monotonic()
    if (
        _available_symbols_cache is not None
        and (now - _available_symbols_cache_time) < _DQ_CACHE_TTL
    ):
        return _available_symbols_cache
    rows = get_db().con.execute("SELECT DISTINCT symbol FROM v_daily ORDER BY symbol").fetchall()
    result = [r[0] for r in rows]
    _available_symbols_cache = result
    _available_symbols_cache_time = now
    return result


async def aget_available_symbols() -> list[str]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_available_symbols_sync)


def _fetch_symbol_profile_sync(symbol: str) -> dict | None:
    row = (
        get_db()
        .con.execute(
            """
        SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR,
               COUNT(*)::INT, COUNT(DISTINCT date)::INT,
               (DATEDIFF('day', MIN(date), MAX(date)) + 1)::INT
        FROM v_daily WHERE symbol = ?
    """,
            [symbol],
        )
        .fetchone()
    )
    if not row or row[2] == 0:
        return None
    span = max(row[4], 1)
    expected = span * 5 / 7 * 0.96
    cov = min(round(row[3] / max(expected, 1) * 100, 1), 100.0)

    # 5-min stats
    fivemin_row = (
        get_db()
        .con.execute(
            """
        SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR,
               COUNT(*)::INT, COUNT(DISTINCT date)::INT
        FROM v_5min WHERE symbol = ?
    """,
            [symbol],
        )
        .fetchone()
    )

    gaps = _fetch_symbol_gaps_sync(symbol)

    return {
        "symbol": symbol,
        "daily_first": row[0],
        "daily_last": row[1],
        "daily_rows": row[2],
        "daily_distinct_days": row[3],
        "daily_coverage_pct": cov,
        "fivemin_first": fivemin_row[0] if fivemin_row and fivemin_row[2] else None,
        "fivemin_last": fivemin_row[1] if fivemin_row and fivemin_row[2] else None,
        "fivemin_rows": fivemin_row[2] if fivemin_row else 0,
        "fivemin_days": fivemin_row[3] if fivemin_row else 0,
        "gaps": gaps,
    }


async def aget_symbol_profile(symbol: str) -> dict | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_symbol_profile_sync(symbol))
