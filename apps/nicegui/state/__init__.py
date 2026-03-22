"""Shared state management for NiceGUI dashboard.

Provides persistent connections and reactive state across all pages.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Set up paths
_apps_root = Path(__file__).resolve().parent  # apps/nicegui/
_project_root = _apps_root.parent.parent  # project root
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))

from typing import TYPE_CHECKING
import asyncio
from concurrent.futures import ThreadPoolExecutor
import polars as pl
import time
from sqlalchemy import and_, func, select

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
    list_walk_forward_folds,
)
from nse_momentum_lab.db.market_db import get_backtest_db, get_market_db, MarketDataDB

# Singleton DB connection - created once at server startup.
# Read-only so the dashboard can coexist with a running backtest writer
# (DuckDB allows unlimited concurrent readers alongside one writer).
db: MarketDataDB = get_market_db(read_only=True)
backtest_db: MarketDataDB = get_backtest_db(read_only=True)

# Thread pool for running blocking DB calls off the async event loop
# (DuckDB is not async-native; running it directly on the event loop stalls NiceGUI)
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-worker")
# max_workers=1 is intentional: DuckDB's connection object (db.con) is NOT thread-safe
# for concurrent access. A single worker serializes all DB calls safely, while still
# freeing the asyncio event loop during slow queries (e.g. COUNT DISTINCT on Parquet).

# Global experiment cache (refreshed periodically)
_experiments_cache: pl.DataFrame | None = None
_experiments_cache_time: float = 0

# Global status cache - persist to disk for fast restarts
_status_cache: dict | None = None
_status_cache_time: float = 0
_STATUS_CACHE_FILE = Path.home() / ".cache" / "nseml_dashboard_status.json"

EXPERIMENT_CACHE_TTL = 60  # seconds
STATUS_CACHE_TTL = 300  # seconds  (5 minutes - heavier query, cache longer)

# Optional: use a fast "lite" status for initial page load
_USE_LITE_STATUS_ON_FIRST_LOAD = True


def get_db() -> MarketDataDB:
    """Get the singleton DuckDB connection."""
    return db


def _fetch_experiments_sync(force_refresh: bool = False) -> pl.DataFrame:
    """Synchronous implementation — always call via get_experiments() or aget_experiments()."""
    global _experiments_cache, _experiments_cache_time

    now = time.time()
    if (
        _experiments_cache is None
        or force_refresh
        or (now - _experiments_cache_time) > EXPERIMENT_CACHE_TTL
    ):
        exps = backtest_db.list_experiments()
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
        "data_source": getattr(db, "_data_source", "unknown"),
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
        _status_cache = db.get_status()
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
    return backtest_db.get_experiment(exp_id)


async def aget_experiment(exp_id: str) -> dict | None:
    """Async wrapper for get_experiment."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment(exp_id))


def get_experiment_trades(exp_id: str) -> pl.DataFrame:
    """Get all trades for an experiment."""
    df = backtest_db.get_experiment_trades(exp_id)
    return df if not df.is_empty() else pl.DataFrame()


def get_experiment_execution_diagnostics(exp_id: str) -> pl.DataFrame:
    """Get execution diagnostics for an experiment."""
    df = backtest_db.get_experiment_execution_diagnostics(exp_id)
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
    df = backtest_db.get_experiment_yearly_metrics(exp_id)
    return df if not df.is_empty() else pl.DataFrame()


async def aget_experiment_yearly_metrics(exp_id: str) -> pl.DataFrame:
    """Async wrapper for get_experiment_yearly_metrics."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment_yearly_metrics(exp_id))


def get_market_monitor_latest() -> pl.DataFrame:
    """Get the latest market monitor snapshot, if available."""
    df = db.get_market_monitor_latest()
    return df if not df.is_empty() else pl.DataFrame()


def get_market_monitor_history(days: int = 252) -> pl.DataFrame:
    """Get recent market monitor history, if available."""
    df = db.get_market_monitor_history(days=days)
    return df if not df.is_empty() else pl.DataFrame()


def get_market_monitor_all() -> pl.DataFrame:
    """Get ALL market monitor history, if available."""
    df = db.get_market_monitor_all()
    return df if not df.is_empty() else pl.DataFrame()


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
    if threshold is not None and name not in ("Indian2LYNCH",):
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
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await list_paper_sessions(session, status=status, limit=limit)


async def aget_paper_session_summary(session_id: str) -> dict | None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await get_paper_session_summary(session, session_id)


async def aget_paper_session_signals(session_id: str) -> list[dict]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await list_paper_session_signals(session, session_id)


async def aget_paper_session_orders(session_id: str, limit: int = 100) -> list[dict]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await list_paper_orders(session, session_id, limit=limit)


async def aget_paper_session_fills(session_id: str, limit: int = 100) -> list[dict]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await list_paper_fills(session, session_id, limit=limit)


async def aget_paper_session_events(session_id: str, limit: int = 100) -> list[dict]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await list_paper_order_events(session, session_id, limit=limit)


async def aget_paper_positions(
    session_id: str | None = None,
    *,
    open_only: bool = True,
) -> list[dict]:
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
            {row.symbol_id for row in rows if row.closed_at is None and row.symbol_id is not None}
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
                    (latest_close_map.get(row.symbol_id) - float(row.avg_entry)) * float(row.qty)
                    if row.closed_at is None
                    and latest_close_map.get(row.symbol_id) is not None
                    and row.avg_entry is not None
                    and row.qty is not None
                    else None
                ),
                "state": row.state,
                "metadata_json": row.metadata_json or {},
            }
            for row in rows
        ]


async def aget_walk_forward_folds(session_id: str) -> list[dict]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await list_walk_forward_folds(session, session_id)


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
