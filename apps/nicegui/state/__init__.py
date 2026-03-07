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
import pandas as pd
import time

if TYPE_CHECKING:
    from nicegui import ui  # noqa: F401  # Imported for type hints only

from nse_momentum_lab.db.market_db import get_market_db, MarketDataDB

# Singleton DB connection - created once at server startup.
# Read-only so the dashboard can coexist with a running backtest writer
# (DuckDB allows unlimited concurrent readers alongside one writer).
db: MarketDataDB = get_market_db(read_only=True)

# Thread pool for running blocking DB calls off the async event loop
# (DuckDB is not async-native; running it directly on the event loop stalls NiceGUI)
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-worker")
# max_workers=1 is intentional: DuckDB's connection object (db.con) is NOT thread-safe
# for concurrent access. A single worker serializes all DB calls safely, while still
# freeing the asyncio event loop during slow queries (e.g. COUNT DISTINCT on Parquet).

# Global experiment cache (refreshed periodically)
_experiments_cache: pd.DataFrame | None = None
_experiments_cache_time: float = 0

# Global status cache
_status_cache: dict | None = None
_status_cache_time: float = 0

EXPERIMENT_CACHE_TTL = 60  # seconds
STATUS_CACHE_TTL = 120  # seconds  (heavier query — COUNT DISTINCT over full parquet)


def get_db() -> MarketDataDB:
    """Get the singleton DuckDB connection."""
    return db


def _fetch_experiments_sync(force_refresh: bool = False) -> pd.DataFrame:
    """Synchronous implementation — always call via get_experiments() or aget_experiments()."""
    global _experiments_cache, _experiments_cache_time

    now = time.time()
    if (
        _experiments_cache is None
        or force_refresh
        or (now - _experiments_cache_time) > EXPERIMENT_CACHE_TTL
    ):
        exps = db.list_experiments()
        if not exps.is_empty():
            _experiments_cache = exps.to_pandas()
            _experiments_cache["status"] = _experiments_cache["status"].astype(str).str.lower()
            _experiments_cache = _experiments_cache[_experiments_cache["status"] == "completed"]
            # DB returns ORDER BY created_at DESC; preserve that as primary sort
            if "created_at" in _experiments_cache.columns:
                _experiments_cache = _experiments_cache.sort_values(
                    by="created_at", ascending=False
                )
            else:
                _experiments_cache = _experiments_cache.sort_values(
                    by="start_year", ascending=False
                )
        else:
            _experiments_cache = pd.DataFrame()
        _experiments_cache_time = now

    return _experiments_cache


def get_experiments(force_refresh: bool = False) -> pd.DataFrame:
    """Get cached experiments list synchronously."""
    return _fetch_experiments_sync(force_refresh)


async def aget_experiments(force_refresh: bool = False) -> pd.DataFrame:
    """Async wrapper — runs the blocking DB call in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_experiments_sync(force_refresh))


def _fetch_status_sync() -> dict:
    """Synchronous implementation — always call via get_db_status() or aget_db_status()."""
    global _status_cache, _status_cache_time

    now = time.time()
    if _status_cache is None or (now - _status_cache_time) > STATUS_CACHE_TTL:
        _status_cache = db.get_status()
        _status_cache_time = now

    return _status_cache


def get_db_status() -> dict:
    """Get current database status (synchronous, TTL-cached)."""
    return _fetch_status_sync()


async def aget_db_status() -> dict:
    """Async wrapper — runs the blocking Parquet COUNT query in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _fetch_status_sync)


def get_experiment(exp_id: str) -> dict | None:
    """Get experiment details by ID."""
    return db.get_experiment(exp_id)


async def aget_experiment(exp_id: str) -> dict | None:
    """Async wrapper for get_experiment."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment(exp_id))


def get_experiment_trades(exp_id: str) -> pd.DataFrame:
    """Get all trades for an experiment."""
    df = db.get_experiment_trades(exp_id)
    return df.to_pandas() if not df.is_empty() else pd.DataFrame()


async def aget_experiment_trades(exp_id: str) -> pd.DataFrame:
    """Async wrapper for get_experiment_trades."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment_trades(exp_id))


def get_experiment_yearly_metrics(exp_id: str) -> pd.DataFrame:
    """Get yearly metrics for an experiment."""
    df = db.get_experiment_yearly_metrics(exp_id)
    return df.to_pandas() if not df.is_empty() else pd.DataFrame()


async def aget_experiment_yearly_metrics(exp_id: str) -> pd.DataFrame:
    """Async wrapper for get_experiment_yearly_metrics."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, lambda: get_experiment_yearly_metrics(exp_id))


def _strategy_display_name(row: pd.Series) -> str:
    """Derive display name that includes breakout threshold when applicable."""
    import json as _json

    name = str(row.get("strategy_name", "?"))
    params: dict = {}
    if "params_json" in row.index and pd.notna(row.get("params_json")):
        try:
            params = _json.loads(row["params_json"])
        except ValueError, TypeError:
            pass
    threshold = params.get("breakout_threshold")
    if threshold is not None and name not in ("Indian2LYNCH",):
        pct = round(float(threshold) * 100)
        return f"{name} {pct}%"
    return name


def build_experiment_options(experiments_df: pd.DataFrame) -> dict[str, str]:
    """Build {label: exp_id} dict with human-readable labels, latest first.

    Label format: "2LYNCHBreakout 4% | 2015-2025 | 7,073 trades | Ret 193.9% | Mar 01"
    """
    options: dict[str, str] = {}
    for _, row in experiments_df.iterrows():
        strategy = _strategy_display_name(row)
        start = row.get("start_year", "?")
        end = row.get("end_year", "?")
        trades = int(row.get("total_trades", 0) or 0)
        ret = float(row.get("total_return_pct", 0) or 0)

        # Created-at date for disambiguation
        created = ""
        if "created_at" in row.index and pd.notna(row["created_at"]):
            created = f" | {pd.Timestamp(row['created_at']).strftime('%b %d %H:%M')}"

        label = f"{strategy} | {start}-{end} | {trades:,} trades | Ret {ret:.1f}%{created}"
        options[label] = row["exp_id"]
    return options


def format_time(value) -> str:
    """Format DuckDB TIME to HH:MM string."""
    if value is None:
        return ""
    return str(value)[:5]


def prepare_trades_df(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare trades dataframe for display with proper formatting."""
    if df.empty:
        return df

    if "entry_date" in df.columns:
        df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    if "exit_date" in df.columns:
        df["exit_date"] = pd.to_datetime(df["exit_date"], errors="coerce")

    if "entry_time" in df.columns:
        df["entry_time"] = df["entry_time"].apply(format_time)
    if "exit_time" in df.columns:
        df["exit_time"] = df["exit_time"].apply(format_time)

    for col in ["pnl_pct", "pnl_r", "holding_days", "year", "entry_price", "exit_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


_experiment_callbacks: list = []


def on_new_experiments(callback) -> None:
    """Register callback for new experiments."""
    _experiment_callbacks.append(callback)


async def poll_new_experiments(force_refresh: bool = True) -> pd.DataFrame:
    """Check for new experiments and notify listeners."""
    global _experiments_cache, _experiments_cache_time

    old_count = len(_experiments_cache) if _experiments_cache is not None else 0
    result = await aget_experiments(force_refresh=force_refresh)
    new_count = len(_experiments_cache) if _experiments_cache is not None else 0

    if new_count > old_count and old_count > 0:
        for cb in _experiment_callbacks:
            cb()

    return result
