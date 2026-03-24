"""DuckDB-native backtest runner for the Indian 2LYNCH strategy.

Extracts the proven logic from ``scripts/backtest_10year_fixed.py`` into a
reusable service that persists results in DuckDB (bt_experiment / bt_trade / bt_yearly_metric tables).

Usage::

    from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
        BacktestParams, DuckDBBacktestRunner,
    )

    runner = DuckDBBacktestRunner()
    exp_id = runner.run(BacktestParams())
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from math import isclose
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

import duckdb
import numpy as np
import polars as pl
import psycopg
from tqdm.auto import tqdm

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.market_db import (
    BACKTEST_DUCKDB_FILE,
    MarketDataDB,
    get_backtest_db,
    get_market_db,
)
from nse_momentum_lab.services.backtest.engine import ExitReason, PositionSide
from nse_momentum_lab.services.backtest.intraday_execution import (
    IntradayExecutionResult,
    resolve_intraday_execution_from_5min,
)
from nse_momentum_lab.services.backtest.persistence import (
    BacktestArtifactPublisher,
    build_strategy_hash,
    upsert_exp_run_with_artifacts_sync,
)
from nse_momentum_lab.services.backtest.progress import BufferedProgressWriter
from nse_momentum_lab.services.backtest.signal_models import BacktestSignal, SignalMetadata
from nse_momentum_lab.services.backtest.strategy_registry import (
    StrategyDefinition,
    resolve_strategy,
)
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
)
from nse_momentum_lab.services.dataset import (
    build_code_hash,
    build_manifest_payload_from_snapshot,
    upsert_dataset_manifest_sync,
)
from nse_momentum_lab.utils import (
    ALL_FILTERS,
    compute_composite_hash,
    compute_short_hash,
    get_exit_time_for_reason,
    minutes_from_nse_open,
    normalize_candle_time,
)

logger = logging.getLogger(__name__)


class IntradayEntry(TypedDict):
    """Result from intraday entry resolver (threshold or ORH mode)."""

    entry_price: float
    initial_stop: float
    same_day_stop_hit: bool
    entry_ts: datetime
    same_day_exit_price: float | None
    same_day_exit_ts: datetime | None
    same_day_exit_time: time | None
    same_day_exit_reason: str | None
    carry_stop_next_session: float | None
    entry_time: time | None


@dataclass(frozen=True)
class ProgressContext:
    exp_id: str
    strategy_name: str
    strategy_hash: str
    dataset_hash: str
    params_json: str
    code_hash: str
    started_at: datetime
    progress_file: Path | None


@dataclass
class BacktestParams:
    """All parameters that define a unique backtest run.

    Changing any field produces a different ``to_hash()`` value, which means
    DuckDB will treat it as a new experiment rather than a cached duplicate.
    """

    # Universe
    universe_size: int = 500
    min_price: int = 10
    min_value_traded_inr: int = 3_000_000
    min_volume: int = 50_000

    # Filters
    min_filters: int = 5
    breakout_threshold: float = 0.04

    # Date range
    start_year: int = 2015
    end_year: int = 2025
    start_date: str | None = None
    end_date: str | None = None
    entry_timeframe: str = "5min"
    # Strategy selection
    strategy: str = "thresholdbreakout"

    # VectorBT engine config
    risk_per_trade_pct: float = 0.01
    portfolio_value: float = 1_000_000.0
    fees_per_trade: float = 0.001
    trail_activation_pct: float = 0.08
    trail_stop_pct: float = 0.02
    min_hold_days: int = 3
    time_stop_days: int = 5
    abnormal_profit_pct: float = 0.10
    abnormal_gap_exit_pct: float = 0.20
    abnormal_gap_mode: str = "immediate_exit"
    same_day_r_ladder: bool = True
    same_day_r_ladder_start_r: int = 2
    # Optional short-side override for same-day R-ladder start.
    # None uses same_day_r_ladder_start_r (default behavior).
    short_same_day_r_ladder_start_r: int | None = None
    short_post_day3_buffer_pct: float | None = None
    short_trail_activation_pct: float | None = None
    short_time_stop_days: int | None = None
    short_abnormal_profit_pct: float | None = None
    short_max_stop_dist_pct: float | None = None
    # Optional short-side cap for intraday initial stop distance: cap at N * ATR_20.
    # None disables the cap (default, preserves existing behavior).
    short_initial_stop_atr_cap_mult: float | None = None
    # Optional short-only same-day take-profit threshold.
    # Example: 0.02 exits same day once price moves +2% in favor.
    short_same_day_take_profit_pct: float | None = None
    follow_through_threshold: float = 0.0

    # FEE (Find and Enter Early) — Stockbee: enter in first N min of NSE open (09:15 IST)
    # 60min is the optimal window for NSE (vs 30min for US) due to pre-open auction.
    # Backtests (2015-2025, 1,776 stocks): 60min gives Calmar 27.06 vs 19.74 at 30min.
    entry_cutoff_minutes: int = 60  # 09:15 + 60 min = 10:15 cutoff
    # Short side often benefits from tighter entry windows around the opening burst.
    # When None, it falls back to ``entry_cutoff_minutes`` for backward-compatible
    # behavior; when set, used only for SHORT strategies.
    short_entry_cutoff_minutes: int | None = None

    # Maximum allowed distance from entry to stop.
    # Belt-and-suspenders guard: if stop is >8% below entry even within the time window,
    # the setup is invalid (e.g. stock crashed then bounced — FEE was still violated).
    max_stop_dist_pct: float = 0.08
    breakout_daily_candidate_budget: int = 30
    # Breakout ranking C-quality source:
    # True  -> use breakout-day feat values (current behavior)
    # False -> prefer T-1 feat values when available (fallbacks to current-day values)
    breakout_use_current_day_c_quality: bool = True
    # Parity toggle for threshold_breakout long-side carry semantics.
    # When enabled, apply filter_h as a hold-quality check after entry admission
    # (restores weak-close/breakeven-carry behavior used in earlier canonical runs).
    breakout_legacy_h_carry_rule: bool = False
    # Daily cap for breakdown (short) candidates. Analogous to breakout_daily_candidate_budget
    # but for short setups. Default is much tighter (5) because the 2% breakdown universe
    # is far larger than the 4% breakout universe in down-trending markets.
    breakdown_daily_candidate_budget: int = 5
    # Minimum rs_252 for short candidate. Default 0.0 = require rs_252 < 0 (current).
    # Set to e.g. -0.10 to require stock down >= 10% vs market YTD (tighter quality gate).
    breakdown_rs_min: float = 0.0
    # Whether to use the strict 3-of-4 filter_l for breakdown (adds close < ma_65_sma).
    # Default False = original 2-of-3 (close<ma20, ret5d<0, r2>=0.70).
    # Set True for 2% BD to require established medium-term downtrend confirmation.
    breakdown_strict_filter_l: bool = False
    # Phase 1d: use narrow-only filter_n (remove the 'OR prev_close > prev_open' clause).
    # Default False = original (narrow OR green T-1). True = narrow T-1 only.
    breakdown_filter_n_narrow_only: bool = False
    # Phase 2c: skip gap-down entries where stock already gapped down >= threshold at open.
    # Avoids chasing a move that already happened overnight.
    breakdown_skip_gap_down: bool = False
    # Phase 1b: max allowed prior 4%-down breakdown count in 90d.
    # -1 = disabled (no filter). 0 = no prior breakdowns allowed. 1 = at most 1 prior breakdown.
    # Avoids shorting already-exhausted stocks that have broken down repeatedly.
    breakdown_max_prior_breakdowns: int = -1
    # Phase 3d: optional market-breadth gate for breakdown.
    # When set (0.0 to 1.0), require pct(close < ma_20) across the market to be above threshold.
    # Keep unset (None) to preserve existing behavior.
    breakdown_breadth_threshold: float | None = None
    # Optional TI65 gate for short-side trend quality.
    # "off"     = keep current behavior (no direct TI65 check)
    # "bearish" = require MA7/MA65_SMA <= 0.95 (with close<MA20 fallback if MA65 unavailable)
    breakdown_ti65_mode: str = "off"
    # Phase 3e: require ATR expansion for breakdowns.
    # If true, require atr_20 > SMA20(atr_20) on the breakdown day.
    # This filters stale breakdowns where the move has already cooled down.
    breakdown_require_atr_expansion: bool = False

    # Parallel execution: number of worker threads for year-by-year backtest
    # Set to 1 for sequential (default), >1 for parallel (e.g., 4 for 4-way parallel)
    # Note: DuckDB read-only connections are thread-safe
    parallel_workers: int = 1

    # EP-specific parameters
    gap_threshold: float = 0.05
    gap_vs_atr_threshold: float = 1.5
    orh_window_minutes: int = 5
    delayed_entry_window: int = 30
    min_consolidation_days: int = 3
    require_real_catalyst: bool = False

    def to_hash(self) -> str:
        """Deterministic SHA-256 of all parameters (for dedup)."""
        serializable_fields = asdict(self)
        # Preserve previous hash behavior for the historical default case where the
        # short post-day3 buffer was not explicitly set.
        if serializable_fields.get("short_post_day3_buffer_pct") is None:
            serializable_fields["short_post_day3_buffer_pct"] = 0.0
        return compute_short_hash(serializable_fields, length=16)

    def to_vbt_config(
        self,
        direction: PositionSide = PositionSide.LONG,
        short_post_day3_buffer_pct: float | None = None,
    ) -> VectorBTConfig:
        resolved_buffer = (
            float(short_post_day3_buffer_pct)
            if short_post_day3_buffer_pct is not None
            else (
                self.short_post_day3_buffer_pct
                if self.short_post_day3_buffer_pct is not None
                else 0.0
            )
        )
        trail_activation_pct = self.trail_activation_pct
        time_stop_days = self.time_stop_days
        abnormal_profit_pct = self.abnormal_profit_pct
        if direction == PositionSide.SHORT:
            if self.short_trail_activation_pct is not None:
                trail_activation_pct = self.short_trail_activation_pct
            if self.short_time_stop_days is not None:
                time_stop_days = self.short_time_stop_days
            if self.short_abnormal_profit_pct is not None:
                abnormal_profit_pct = self.short_abnormal_profit_pct

        return VectorBTConfig(
            direction=direction,
            risk_per_trade_pct=self.risk_per_trade_pct,
            default_portfolio_value=self.portfolio_value,
            fees_per_trade=self.fees_per_trade,
            trail_activation_pct=trail_activation_pct,
            trail_stop_pct=self.trail_stop_pct,
            min_hold_days=self.min_hold_days,
            time_stop_days=time_stop_days,
            abnormal_profit_pct=abnormal_profit_pct,
            abnormal_gap_exit_pct=self.abnormal_gap_exit_pct,
            abnormal_gap_mode=self.abnormal_gap_mode,
            same_day_r_ladder=self.same_day_r_ladder,
            same_day_r_ladder_start_r=self.same_day_r_ladder_start_r,
            short_post_day3_buffer_pct=resolved_buffer,
            respect_same_day_exit_metadata=(
                direction == PositionSide.LONG and self.breakout_legacy_h_carry_rule
            ),
            follow_through_threshold=self.follow_through_threshold,
        )


class DuckDBBacktestRunner:
    """Orchestrates an end-to-end backtest and stores results in DuckDB."""

    DATASET_KIND = "duckdb_market_daily"
    RUN_LOGIC_VERSION = "duckdb_backtest_runner_v2026_03_12_breakout_ranking_budget"

    def __init__(
        self,
        db: MarketDataDB | None = None,
        results_db: MarketDataDB | None = None,
    ) -> None:
        self.db = db or get_market_db(read_only=True)
        if results_db is not None:
            self.results_db = results_db
        else:
            self._assert_no_conflicting_backtest_runtime()
            try:
                self.results_db = get_backtest_db()
            except Exception as exc:  # pragma: no cover - depends on host process state
                raise RuntimeError(
                    "Unable to open backtest DuckDB for writes. "
                    "Stop any running backtest/dashboard process and retry."
                ) from exc
        self._active_strategy: StrategyDefinition | None = None
        self._progress_writer: BufferedProgressWriter | None = None
        self._feat_daily_ready = False
        self._dataset_snapshot_cache: dict[str, object] | None = None
        self._liquid_symbols_cache: dict[tuple[int, int, int, int], list[str]] = {}

    @classmethod
    def _assert_no_conflicting_backtest_runtime(cls) -> None:
        """Fail fast when runtime state can lock backtest.duckdb on Windows."""
        lock_probe = None
        try:
            lock_probe = duckdb.connect(str(BACKTEST_DUCKDB_FILE), read_only=False)
        except Exception as exc:
            raise RuntimeError(
                f"backtest.duckdb is not writable (likely locked by another process): {exc}"
            ) from exc
        finally:
            if lock_probe is not None:
                lock_probe.close()

    def _uses_breakout_ranking(self) -> bool:
        strategy = self._active_strategy
        if strategy is None:
            return False
        return strategy.direction == PositionSide.LONG and strategy.family in {
            "threshold_breakout",
        }

    def _uses_breakdown_ranking(self) -> bool:
        strategy = self._active_strategy
        if strategy is None:
            return False
        return strategy.direction == PositionSide.SHORT and strategy.family in {
            "threshold_breakdown",
        }

    def _apply_breakdown_selection_ranking(
        self,
        df_filtered: pl.DataFrame,
        params: BacktestParams,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Rank and budget daily breakdown (short) candidates.

        Scoring rationale (short-specific, revised):
        The original rs_252-magnitude-dominant formula anti-selected in downtrend markets
        because every stock has negative rs_252 and picking the "most negative" selects the
        most beaten-up stocks (mean-reversion risk), not fresh breakdowns.

        v2 formula (H-dominant): picking stocks with extreme close near day-low created a
        different problem — these stocks have wide stops (session_high far from entry) and
        are often exhaustion moves rather than trend continuation. High DD resulted.

        v3 formula (R²-primary): prioritises orderly downtrend continuation:
        - R² quality: orderly 65d downtrend = continuation more likely + tighter stops (primary,
          0-5000 pts). Orderly = small daily variation → range typically narrower → stop closer
          to entry → smaller loss when wrong.
        - H quality: close near the day's low = confirms selling pressure (secondary, 0-2000 pts)
          close_pos_in_range range after filter_h: 0.0-0.30; lower = closer to low = better
        - C quality: quieter prior day = better consolidation before breakdown (0-1000 pts)
          vol_dryup_ratio: lower = more volume compression on prior day = better setup
        - Freshness: fewer prior breakdowns in 90d = not an exhausted short (0-300 pts)
          uses prior_breakdowns_90d (downside counter), not prior_breakouts_90d (upside)
        - rs_252 tiebreaker: moderate negative YTD relative return (0-200 pts, capped at -40%)
          capped to avoid rewarding the most beaten-up stocks
        - Liquidity tiebreaker: higher value_traded_inr breaks final ties
        """
        if df_filtered.is_empty():
            return df_filtered, df_filtered

        # R² quality: orderly downtrend (PRIMARY, 0-5000) — orderly trend has tighter typical
        # stops and higher continuation probability
        r2_score = (pl.col("r2_65").fill_null(0.0).clip(0.0, 1.0) * 5_000.0).alias(
            "selection_r2_quality"
        )

        # H quality: close near day low (SECONDARY, 0-2000) — filter_h ensures <= 0.30
        h_score = (
            ((0.30 - pl.col("close_pos_in_range").fill_null(0.30).clip(0.0, 0.30)) / 0.30) * 2_000.0
        ).alias("selection_h_quality")

        # C quality: quiet prior day (0-1000); lower vol_dryup = more compressed = better
        c_score = (
            ((1.3 - pl.col("vol_dryup_ratio").fill_null(1.3).clip(0.0, 1.3)) / 1.3) * 1_000.0
        ).alias("selection_c_strength")

        # Freshness: fewer prior breakdowns (0-300) using downside counter
        freshness_score = (
            pl.when(pl.col("prior_breakdowns_90d").fill_null(0) == 0)
            .then(300)
            .when(pl.col("prior_breakdowns_90d").fill_null(0) == 1)
            .then(200)
            .when(pl.col("prior_breakdowns_90d").fill_null(0) == 2)
            .then(100)
            .otherwise(0)
        ).alias("selection_freshness")

        # rs_252 tiebreaker: cap at -0.40 to avoid rewarding most-beaten-up stocks (0-200)
        rs_score = (pl.col("rs_252").fill_null(0.0).clip(-0.40, 0.0).abs() / 0.40 * 200.0).alias(
            "selection_rs_score"
        )

        ranked = df_filtered.with_columns(
            h_score, r2_score, c_score, freshness_score, rs_score
        ).with_columns(
            (
                pl.col("selection_h_quality")
                + pl.col("selection_r2_quality")
                + pl.col("selection_c_strength")
                + pl.col("selection_freshness").cast(pl.Float64)
                + pl.col("selection_rs_score")
            )
            .cast(pl.Float64)
            .alias("selection_score")
        )

        ranked = ranked.sort(
            ["trading_date", "selection_score", "value_traded_inr", "symbol"],
            descending=[False, True, True, False],
        ).with_columns(
            pl.col("symbol").cum_count().over("trading_date").alias("selection_rank"),
            pl.lit(int(params.breakdown_daily_candidate_budget)).alias("selection_budget"),
        )
        # Map to diagnostic component columns (y_score repurposed for freshness/breakdown counter)
        ranked = ranked.with_columns(
            pl.col("selection_h_quality").alias("selection_n_score"),
            pl.col("selection_r2_quality").alias("selection_r2_quality"),
            pl.col("selection_freshness").alias("selection_y_score"),
            pl.col("selection_c_strength").alias("selection_c_strength"),
            pl.col("selection_rs_score").alias("selection_rs_score"),
        )

        budget = int(params.breakdown_daily_candidate_budget)
        if budget <= 0:
            return ranked, ranked.head(0)
        return (
            ranked.filter(pl.col("selection_rank") <= budget),
            ranked.filter(pl.col("selection_rank") > budget),
        )

    @staticmethod
    def _resolve_short_post_day3_buffer_pct(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> float:
        if params.short_post_day3_buffer_pct is not None:
            return params.short_post_day3_buffer_pct

        if (
            strategy is not None
            and strategy.direction == PositionSide.SHORT
            and isclose(params.breakout_threshold, 0.02, abs_tol=1e-9)
        ):
            return 0.005

        return 0.0

    @staticmethod
    def _resolve_entry_cutoff_minutes(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> int:
        if strategy is not None and strategy.direction == PositionSide.SHORT:
            return (
                params.short_entry_cutoff_minutes
                if params.short_entry_cutoff_minutes is not None
                else params.entry_cutoff_minutes
            )
        return params.entry_cutoff_minutes

    @staticmethod
    def _resolve_max_stop_dist_pct(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> float:
        if (
            params.short_max_stop_dist_pct is not None
            and strategy is not None
            and strategy.direction == PositionSide.SHORT
        ):
            return params.short_max_stop_dist_pct
        return params.max_stop_dist_pct

    @staticmethod
    def _resolve_short_trail_activation_pct(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> float:
        if (
            params.short_trail_activation_pct is not None
            and strategy is not None
            and strategy.direction == PositionSide.SHORT
        ):
            return params.short_trail_activation_pct
        return params.trail_activation_pct

    @staticmethod
    def _resolve_short_time_stop_days(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> int:
        if (
            params.short_time_stop_days is not None
            and strategy is not None
            and strategy.direction == PositionSide.SHORT
        ):
            return params.short_time_stop_days
        return params.time_stop_days

    @staticmethod
    def _resolve_short_abnormal_profit_pct(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> float:
        if (
            params.short_abnormal_profit_pct is not None
            and strategy is not None
            and strategy.direction == PositionSide.SHORT
        ):
            return params.short_abnormal_profit_pct
        return params.abnormal_profit_pct

    @staticmethod
    def _resolve_same_day_r_ladder_start_r(
        params: BacktestParams,
        strategy: StrategyDefinition | None = None,
    ) -> int:
        if (
            params.short_same_day_r_ladder_start_r is not None
            and strategy is not None
            and strategy.direction == PositionSide.SHORT
        ):
            return int(params.short_same_day_r_ladder_start_r)
        return int(params.same_day_r_ladder_start_r)

    @staticmethod
    def _build_backtest_logic_fingerprint() -> str:
        """Hash the core backtest logic files so code changes create new experiment IDs."""
        module_dir = Path(__file__).resolve().parent
        logic_files = [
            module_dir / "duckdb_backtest_runner.py",
            module_dir / "strategy_families.py",
            module_dir / "strategy_registry.py",
            module_dir / "intraday_execution.py",
            module_dir / "vectorbt_engine.py",
            module_dir / "engine.py",
            module_dir / "filters.py",
            module_dir / "signal_models.py",
        ]
        file_digests: dict[str, str] = {}
        for path in logic_files:
            try:
                file_digests[path.name] = compute_short_hash(
                    path.read_text(encoding="utf-8"), length=16
                )
            except OSError:
                file_digests[path.name] = "missing"
        return compute_short_hash(file_digests, length=16)

    @staticmethod
    def _build_filter_snapshot(
        row: dict[str, object],
        active_filter_cols: list[str],
        hold_quality_cols: list[str],
    ) -> dict[str, bool]:
        return {
            col: bool(row.get(col, False))
            for col in sorted({*active_filter_cols, *hold_quality_cols})
        }

    @staticmethod
    def _coerce_int(value: object, default: int = 0) -> int:
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(float(value))
            except ValueError:
                return default
        return default

    @staticmethod
    def _build_selection_components(row: dict[str, object]) -> dict[str, object]:
        return {
            "c_strength": DuckDBBacktestRunner._coerce_int(row.get("selection_c_strength"), 0),
            "y_score": DuckDBBacktestRunner._coerce_int(row.get("selection_y_score"), 0),
            "n_score": DuckDBBacktestRunner._coerce_int(row.get("selection_n_score"), 0),
            "r2_quality": float(row.get("selection_r2_quality", 0.0) or 0.0),
            "value_traded_inr": float(row.get("value_traded_inr", 0.0) or 0.0),
            "daily_budget": DuckDBBacktestRunner._coerce_int(row.get("selection_budget"), 0),
        }

    @staticmethod
    def _build_diag_entry(
        *,
        year: int,
        row: dict[str, object],
        active_filter_cols: list[str],
        hold_quality_cols: list[str],
    ) -> tuple[dict[str, object], dict[str, bool], bool]:
        sig_date = row["trading_date"]
        if isinstance(sig_date, datetime):
            sig_date = sig_date.date()
        filter_snapshot = DuckDBBacktestRunner._build_filter_snapshot(
            row, active_filter_cols, hold_quality_cols
        )
        hold_quality_passed = (
            all(bool(row.get(col, False)) for col in hold_quality_cols)
            if hold_quality_cols
            else True
        )
        diag_entry: dict[str, object] = {
            "year": year,
            "signal_date": sig_date,
            "symbol": row["symbol"],
            "status": "queued_for_execution",
            "reason": "eligible",
            "entry_time": None,
            "entry_price": None,
            "initial_stop": None,
            "filters_json": filter_snapshot,
            "hold_quality_passed": hold_quality_passed,
            "executed_exit_reason": None,
            "pnl_pct": None,
            "selection_score": float(row.get("selection_score", 0.0) or 0.0),
            "selection_rank": DuckDBBacktestRunner._coerce_int(row.get("selection_rank"), 0),
            "selection_components_json": DuckDBBacktestRunner._build_selection_components(row),
        }
        return diag_entry, filter_snapshot, hold_quality_passed

    def _apply_breakout_selection_ranking(
        self,
        df_filtered: pl.DataFrame,
        params: BacktestParams,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        if df_filtered.is_empty():
            return df_filtered, df_filtered

        use_current_day_c = bool(params.breakout_use_current_day_c_quality)

        # Ensure optional T-1 columns exist for the toggle path.
        optional_prev_cols = (
            "prev_vol_dryup_ratio",
            "prev_atr_compress_ratio",
            "prev_range_percentile",
        )
        for col in optional_prev_cols:
            if col not in df_filtered.columns:
                df_filtered = df_filtered.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

        if use_current_day_c:
            c_vol_expr = pl.col("vol_dryup_ratio").fill_null(99.0)
            c_atr_expr = pl.col("atr_compress_ratio").fill_null(99.0)
            c_rng_expr = pl.col("range_percentile").fill_null(99.0)
        else:
            # Prefer T-1 C-quality if present, fallback to breakout-day values.
            c_vol_expr = pl.coalesce(
                [pl.col("prev_vol_dryup_ratio"), pl.col("vol_dryup_ratio"), pl.lit(99.0)]
            )
            c_atr_expr = pl.coalesce(
                [pl.col("prev_atr_compress_ratio"), pl.col("atr_compress_ratio"), pl.lit(99.0)]
            )
            c_rng_expr = pl.coalesce(
                [pl.col("prev_range_percentile"), pl.col("range_percentile"), pl.lit(99.0)]
            )

        ranked = df_filtered.with_columns(
            (
                (c_vol_expr <= 1.0).cast(pl.Int64)
                + (c_atr_expr <= 1.10).cast(pl.Int64)
                + (c_rng_expr <= 0.60).cast(pl.Int64)
            ).alias("selection_c_strength"),
            (
                pl.when(pl.col("prior_breakouts_30d").fill_null(0) <= 0)
                .then(3)
                .when(pl.col("prior_breakouts_30d").fill_null(0) == 1)
                .then(2)
                .when(pl.col("prior_breakouts_30d").fill_null(0) == 2)
                .then(1)
                .otherwise(0)
            ).alias("selection_y_score"),
            (
                pl.when(
                    pl.col("prev_close").is_not_null()
                    & pl.col("prev_open").is_not_null()
                    & (pl.col("prev_close") < pl.col("prev_open"))
                )
                .then(2)
                .when(
                    pl.col("prev_high").is_not_null()
                    & pl.col("prev_low").is_not_null()
                    & pl.col("atr_20").is_not_null()
                    & ((pl.col("prev_high") - pl.col("prev_low")) < (pl.col("atr_20") * 0.5))
                )
                .then(1)
                .otherwise(0)
            ).alias("selection_n_score"),
            pl.col("r2_65").clip(0.0, 1.0).fill_null(0.0).alias("selection_r2_quality"),
        ).with_columns(
            (
                pl.col("selection_c_strength") * 10_000
                + pl.col("selection_y_score") * 1_000
                + ((pl.col("selection_r2_quality") * 100).round(0).cast(pl.Int64) * 5)
                + pl.col("selection_n_score")
            )
            .cast(pl.Float64)
            .alias("selection_score")
        )

        ranked = ranked.sort(
            ["trading_date", "selection_score", "value_traded_inr", "symbol"],
            descending=[False, True, True, False],
        ).with_columns(
            pl.col("symbol").cum_count().over("trading_date").alias("selection_rank"),
            pl.lit(int(params.breakout_daily_candidate_budget)).alias("selection_budget"),
        )

        budget = int(params.breakout_daily_candidate_budget)
        if budget <= 0:
            return ranked, ranked.head(0)
        return (
            ranked.filter(pl.col("selection_rank") <= budget),
            ranked.filter(pl.col("selection_rank") > budget),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def build_experiment_id(params_hash: str, dataset_hash: str, code_hash: str = "default") -> str:
        """Stable experiment ID derived from params + dataset fingerprints."""
        return compute_composite_hash(params_hash, dataset_hash, code_hash, length=16)

    def run(
        self,
        params: BacktestParams | None = None,
        *,
        force: bool = False,
        snapshot: bool = False,
        progress_file: Path | None = None,
    ) -> str:
        """Run a backtest and persist results. Returns the experiment ID.

        If *force* is False and an experiment with the same hash already
        exists, the run is skipped and the existing exp_id is returned.
        """
        started_at = datetime.now(UTC)
        params = params or BacktestParams()
        self._active_strategy = resolve_strategy(params.strategy)
        strategy_name = self._active_strategy.name
        self._validate_required_lineage_dependencies()
        params_hash = params.to_hash()
        code_hash = build_code_hash(
            "duckdb_backtest_runner",
            {
                "strategy": strategy_name,
                "strategy_version": self._active_strategy.version,
                "run_logic_version": self.RUN_LOGIC_VERSION,
                "logic_fingerprint": self._build_backtest_logic_fingerprint(),
            },
        )
        dataset_snapshot = self._get_dataset_snapshot()
        dataset_hash = str(dataset_snapshot["dataset_hash"])
        exp_id = self.build_experiment_id(params_hash, dataset_hash, code_hash)
        strategy_hash = build_strategy_hash(strategy_name, params_hash)
        params_json = json.dumps(asdict(params), sort_keys=True)

        if progress_file is None:
            progress_dir = self.results_db.db_path.parent / "progress"
            progress_file = progress_dir / f"{exp_id}.ndjson"

        existing_exp = (
            self.results_db.get_experiment(exp_id)
            if self.results_db.experiment_exists(exp_id)
            else None
        )
        if not force and existing_exp is not None:
            existing_status = str(existing_exp.get("status") or "").lower().strip()
            if existing_status == "completed":
                logger.info("[SKIP] Experiment %s already exists. Use --force to re-run.", exp_id)
                return exp_id
            logger.info(
                "[CLEANUP] Removing stale experiment %s with status='%s'.", exp_id, existing_status
            )
            self.results_db.delete_experiment(exp_id)

        # Delete stale data if forcing a re-run
        if force and self.results_db.experiment_exists(exp_id):
            self.results_db.delete_experiment(exp_id)

        logger.info("[START] Experiment %s", exp_id)
        logger.info("  Params hash  : %s", params_hash)
        logger.info("  Dataset hash : %s", dataset_hash)
        self._progress_writer = BufferedProgressWriter(
            write_interval_seconds=60,
            progress_file=progress_file,
        )
        progress_context = ProgressContext(
            exp_id=exp_id,
            strategy_name=strategy_name,
            strategy_hash=strategy_hash,
            dataset_hash=dataset_hash,
            params_json=params_json,
            code_hash=code_hash,
            started_at=started_at,
            progress_file=progress_file,
        )
        manifest_payload = build_manifest_payload_from_snapshot(
            dataset_kind=self.DATASET_KIND,
            snapshot=dataset_snapshot,
            code_hash=code_hash,
            params_hash=params_hash,
        )
        upsert_dataset_manifest_sync(manifest_payload)
        self._emit_progress(
            context=progress_context,
            status="RUNNING",
            stage="starting",
            progress_pct=0.0,
            message="Initializing backtest run",
        )

        try:
            self.results_db.save_experiment(
                exp_id=exp_id,
                strategy_name=strategy_name,
                params_json=params_json,
                start_year=params.start_year,
                end_year=params.end_year,
                params_hash=params_hash,
                dataset_hash=dataset_hash,
                code_hash=code_hash,
                data_source=str(dataset_snapshot["source_type"]),
                dataset_snapshot=dataset_snapshot,
            )
            self._emit_progress(
                context=progress_context,
                status="RUNNING",
                stage="materializing_features",
                progress_pct=5.0,
                message="Building/validating feat_daily",
            )

            # Ensure features are available in the market catalog.
            self._ensure_feat_daily_available()

            # Get universe
            symbols = self._get_liquid_symbols(params)
            logger.info("Universe: %d symbols", len(symbols))
            effective_start_year, effective_end_year = self._effective_year_range(params)
            total_years = max(effective_end_year - effective_start_year + 1, 1)
            self._emit_progress(
                context=progress_context,
                status="RUNNING",
                stage="running_years",
                progress_pct=10.0,
                message=(
                    f"Universe ready ({len(symbols)} symbols). "
                    f"Running {effective_start_year}-{effective_end_year}."
                ),
            )

            def on_year_start(year: int, completed_years: int, years_total: int) -> None:
                pct = min(90.0, 10.0 + (completed_years / max(years_total, 1)) * 80.0)
                self._emit_progress(
                    context=progress_context,
                    status="RUNNING",
                    stage="running_year",
                    progress_pct=pct,
                    message=(
                        f"Year {year} started ({completed_years}/{years_total} years completed)"
                    ),
                )

            def on_year_complete(
                year: int,
                completed_years: int,
                years_total: int,
                stats: dict,
            ) -> None:
                pct = min(92.0, 10.0 + (completed_years / max(years_total, 1)) * 82.0)
                self._emit_progress(
                    context=progress_context,
                    status="RUNNING",
                    stage="year_complete",
                    progress_pct=pct,
                    message=(
                        f"Year {year} complete: trades={int(stats.get('trades', 0))}, "
                        f"return={float(stats.get('return_pct', 0.0)):+.2f}% "
                        f"({completed_years}/{years_total})"
                    ),
                )

            def on_year_heartbeat(
                year: int,
                completed_years: int,
                years_total: int,
                message: str,
            ) -> None:
                pct = min(89.0, 10.0 + ((completed_years + 0.5) / max(years_total, 1)) * 80.0)
                self._emit_progress(
                    context=progress_context,
                    status="RUNNING",
                    stage="running_year",
                    progress_pct=pct,
                    message=f"Year {year}: {message}",
                )

            yearly_results, all_trades, all_execution_diagnostics = self._run_year_by_year(
                params,
                symbols,
                on_year_start=on_year_start,
                on_year_complete=on_year_complete,
                on_year_heartbeat=on_year_heartbeat,
            )
            self._emit_progress(
                context=progress_context,
                status="RUNNING",
                stage="persisting_results",
                progress_pct=94.0,
                message=(
                    f"Persisting {len(all_trades)} trades and {total_years} yearly metric rows"
                ),
            )
            self._persist_results(
                exp_id,
                params,
                yearly_results,
                all_trades,
                all_execution_diagnostics,
            )
            self._emit_progress(
                context=progress_context,
                status="RUNNING",
                stage="publishing_artifacts",
                progress_pct=97.0,
                message="Publishing run artifacts to MinIO and Postgres",
            )

            finished_at = datetime.now(UTC)
            self._persist_postgres_lineage(
                exp_id=exp_id,
                params=params,
                strategy_name=strategy_name,
                strategy_hash=strategy_hash,
                params_hash=params_hash,
                dataset_hash=dataset_hash,
                code_hash=code_hash,
                yearly_results=yearly_results,
                all_trades=all_trades,
                started_at=started_at,
                finished_at=finished_at,
                snapshot=snapshot,
            )
        except Exception as exc:
            finished_at = datetime.now(UTC)
            self._emit_progress(
                context=progress_context,
                status="FAILED",
                stage="failed",
                progress_pct=None,
                message=f"Backtest failed: {exc}",
                force_write=True,
                finished_at=finished_at,
            )
            if self.results_db.experiment_exists(exp_id):
                self.results_db.delete_experiment(exp_id)
            raise

        return exp_id

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_optional_iso_date(value: str | None, field_name: str) -> date | None:
        if value is None:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc

    def _ensure_feat_daily_available(self) -> None:
        if self._feat_daily_ready:
            return
        try:
            self.db.con.execute("SELECT 1 FROM feat_daily LIMIT 1").fetchone()
        except Exception as exc:
            if getattr(self.db, "_read_only", False):
                raise RuntimeError(
                    "feat_daily is not available in market.duckdb. "
                    "Run `doppler run -- uv run nseml-build-features` once before backtesting."
                ) from exc
            self.db.build_feat_daily_table()
        self._feat_daily_ready = True

    def _get_dataset_snapshot(self) -> dict[str, object]:
        if self._dataset_snapshot_cache is None:
            self._dataset_snapshot_cache = self.db.get_dataset_snapshot()
        return self._dataset_snapshot_cache

    def _emit_progress(
        self,
        *,
        context: ProgressContext,
        status: str,
        stage: str,
        message: str,
        progress_pct: float | None,
        force_write: bool = False,
        finished_at: datetime | None = None,
    ) -> None:
        if self._progress_writer is not None:
            self._progress_writer.emit(
                progress_pct=progress_pct,
                stage=stage,
                message=message,
                exp_id=context.exp_id,
                strategy_name=context.strategy_name,
                strategy_hash=context.strategy_hash,
                dataset_hash=context.dataset_hash,
                params_json=context.params_json,
                code_hash=context.code_hash,
                started_at=context.started_at,
                status=status,
                finished_at=finished_at,
                force_write=force_write,
                postgres_upsert_fn=upsert_exp_run_with_artifacts_sync,
            )
            return

        pct_label = "--.-%" if progress_pct is None else f"{progress_pct:5.1f}%"
        logger.info("[PROGRESS] %s [%s] %s", pct_label, stage, message)

        heartbeat_at = datetime.now(UTC)
        if context.progress_file is not None:
            context.progress_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "timestamp": heartbeat_at.isoformat(),
                "exp_id": context.exp_id,
                "status": status,
                "stage": stage,
                "progress_pct": progress_pct,
                "message": message,
            }
            with context.progress_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, sort_keys=True) + "\n")

        upsert_exp_run_with_artifacts_sync(
            exp_hash=context.exp_id,
            strategy_name=context.strategy_name,
            strategy_hash=context.strategy_hash,
            dataset_hash=context.dataset_hash,
            params_json=context.params_json,
            code_sha=context.code_hash,
            status=status,
            started_at=context.started_at,
            finished_at=finished_at,
            metrics={},
            artifacts=[],
            progress_stage=stage,
            progress_message=message,
            progress_pct=progress_pct,
            heartbeat_at=heartbeat_at,
        )

    @staticmethod
    def _build_symbol_id_map(symbols: list[str]) -> dict[str, int]:
        """Build deterministic, collision-free integer IDs for symbol strings."""
        unique_symbols = sorted(set(symbols))
        return {symbol: symbol_id for symbol_id, symbol in enumerate(unique_symbols, start=1)}

    def _get_liquid_symbols(self, params: BacktestParams) -> list[str]:
        """Get top liquid symbols based on the backtest date range.

        Uses the effective backtest period (start_year to end_year) to rank stocks
        by liquidity. This prevents look-ahead bias where stocks would be ranked
        by future performance.
        """
        effective_start_year, effective_end_year = self._effective_year_range(params)
        cache_key = (
            effective_start_year,
            effective_end_year,
            int(params.min_price),
            int(params.universe_size),
        )
        cached = self._liquid_symbols_cache.get(cache_key)
        if cached is not None:
            return cached

        # Use the backtest date range for liquidity ranking to avoid look-ahead bias
        # For very short backtests, extend the window to get a more stable ranking
        liquidity_start = date(effective_start_year, 1, 1)
        liquidity_end = date(effective_end_year, 12, 31)

        query = """
        SELECT symbol, AVG(close * volume) AS avg_value_traded
        FROM v_daily
        WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND close >= ?
        GROUP BY symbol
        ORDER BY avg_value_traded DESC
        LIMIT ?
        """
        result = self.db.con.execute(
            query,
            [
                liquidity_start.isoformat(),
                liquidity_end.isoformat(),
                params.min_price,
                params.universe_size,
            ],
        ).fetchdf()
        symbols = result["symbol"].to_list()
        self._liquid_symbols_cache[cache_key] = symbols
        return symbols

    def _effective_year_range(self, params: BacktestParams) -> tuple[int, int]:
        window_start = self._parse_optional_iso_date(params.start_date, "start_date")
        window_end = self._parse_optional_iso_date(params.end_date, "end_date")
        if window_start and window_end and window_start > window_end:
            raise ValueError("start_date must be less than or equal to end_date")

        effective_start_year = params.start_year
        effective_end_year = params.end_year
        if window_start:
            effective_start_year = max(effective_start_year, window_start.year)
        if window_end:
            effective_end_year = min(effective_end_year, window_end.year)
        if effective_start_year > effective_end_year:
            raise ValueError(
                "No overlapping run period: start/end year does not overlap with start_date/end_date"
            )
        return effective_start_year, effective_end_year

    def _run_year_by_year(
        self,
        params: BacktestParams,
        symbols: list[str],
        *,
        on_year_start: Callable[[int, int, int], None] | None = None,
        on_year_complete: Callable[[int, int, int, dict], None] | None = None,
        on_year_heartbeat: Callable[[int, int, int, str], None] | None = None,
    ) -> tuple[dict[int, dict], list[dict], list[dict]]:
        """Run backtest year by year using parameterized queries.

        Supports parallel execution when params.parallel_workers > 1.
        Each year runs independently in a separate thread.
        """
        window_start = self._parse_optional_iso_date(params.start_date, "start_date")
        window_end = self._parse_optional_iso_date(params.end_date, "end_date")
        effective_start_year, effective_end_year = self._effective_year_range(params)

        years_total = effective_end_year - effective_start_year + 1
        years_to_run = list(range(effective_start_year, effective_end_year + 1))

        # Determine if we should run in parallel
        use_parallel = params.parallel_workers > 1 and years_total > 1

        if use_parallel:
            logger.info(
                "Running %d years in parallel with %d workers",
                years_total,
                params.parallel_workers,
            )
            return self._run_year_by_year_parallel(
                params,
                symbols,
                years_to_run,
                window_start,
                window_end,
                on_year_start,
                on_year_complete,
                on_year_heartbeat,
                years_total,
            )
        else:
            return self._run_year_by_year_sequential(
                params,
                symbols,
                years_to_run,
                window_start,
                window_end,
                on_year_start,
                on_year_complete,
                on_year_heartbeat,
                years_total,
            )

    def _run_year_by_year_sequential(
        self,
        params: BacktestParams,
        symbols: list[str],
        years_to_run: list[int],
        window_start: date | None,
        window_end: date | None,
        on_year_start: Callable[[int, int, int], None] | None,
        on_year_complete: Callable[[int, int, int, dict], None] | None,
        on_year_heartbeat: Callable[[int, int, int, str], None] | None,
        years_total: int,
    ) -> tuple[dict[int, dict], list[dict], list[dict]]:
        """Run backtest sequentially year by year (original implementation)."""
        yearly_results: dict[int, dict] = {}
        all_trades: list[dict] = []
        all_execution_diagnostics: list[dict] = []

        for idx, year in enumerate(years_to_run, start=1):
            year_window_start = date(year, 1, 1)
            year_window_end = date(year, 12, 31)
            if window_start and window_start > year_window_start:
                year_window_start = window_start
            if window_end and window_end < year_window_end:
                year_window_end = window_end

            if on_year_start:
                on_year_start(year, idx - 1, years_total)
            logger.info("[YEAR %d] Running...", year)

            year_heartbeat_cb: Callable[[str], None] | None = None
            if on_year_heartbeat:

                def year_heartbeat_cb(message: str, *, _year: int = year, _idx: int = idx) -> None:
                    on_year_heartbeat(
                        _year,
                        _idx - 1,
                        years_total,
                        message,
                    )

            stats, trades, execution_diagnostics = self._run_single_year(
                params,
                symbols,
                year,
                year_window_start,
                year_window_end,
                heartbeat_cb=year_heartbeat_cb,
            )
            yearly_results[year] = stats
            all_trades.extend(trades)
            all_execution_diagnostics.extend(execution_diagnostics)

            if stats["trades"] > 0:
                logger.info(
                    "  Trades: %d  Return: %+.2f%%  Win Rate: %.1f%%  Avg R: %.2f  (%d/%d)",
                    stats["trades"],
                    stats["return_pct"],
                    stats["win_rate_pct"],
                    stats["avg_r"],
                    idx,
                    years_total,
                )
            if stats.get("skipped_intraday_entry", 0) > 0:
                logger.info(
                    "  Skipped (no 5min breakout entry found): %d",
                    int(stats["skipped_intraday_entry"]),
                )
            if on_year_complete:
                on_year_complete(year, idx, years_total, stats)

        return yearly_results, all_trades, all_execution_diagnostics

    def _run_year_by_year_parallel(
        self,
        params: BacktestParams,
        symbols: list[str],
        years_to_run: list[int],
        window_start: date | None,
        window_end: date | None,
        on_year_start: Callable[[int, int, int], None] | None,
        on_year_complete: Callable[[int, int, int, dict], None] | None,
        on_year_heartbeat: Callable[[int, int, int, str], None] | None,
        years_total: int,
    ) -> tuple[dict[int, dict], list[dict], list[dict]]:
        """Run backtest in parallel using ThreadPoolExecutor.

        Each year runs independently. Results are collected and aggregated.
        DuckDB read-only connections are thread-safe for concurrent reads.
        """
        from threading import Lock

        yearly_results: dict[int, dict] = {}
        all_trades: list[dict] = []
        all_execution_diagnostics: list[dict] = []
        results_lock = Lock()

        def run_single_year_thread(year: int, idx: int) -> tuple[int, dict, list, list]:
            """Run a single year in a worker thread."""
            year_window_start = date(year, 1, 1)
            year_window_end = date(year, 12, 31)
            if window_start and window_start > year_window_start:
                year_window_start = window_start
            if window_end and window_end < year_window_end:
                year_window_end = window_end

            if on_year_start:
                on_year_start(year, idx - 1, years_total)
            logger.info("[YEAR %d] [Thread %d] Running...", year, idx)

            # Note: Disable heartbeat in parallel mode to avoid concurrent callback issues
            stats, trades, execution_diagnostics = self._run_single_year(
                params,
                symbols,
                year,
                year_window_start,
                year_window_end,
                heartbeat_cb=None,  # No heartbeat in parallel mode
            )

            if stats["trades"] > 0:
                logger.info(
                    "[YEAR %d] [Thread %d] Trades: %d  Return: %+.2f%%  Win Rate: %.1f%%  Avg R: %.2f",
                    year,
                    idx,
                    stats["trades"],
                    stats["return_pct"],
                    stats["win_rate_pct"],
                    stats["avg_r"],
                )
            if stats.get("skipped_intraday_entry", 0) > 0:
                logger.info(
                    "[YEAR %d] [Thread %d] Skipped (no 5min entry): %d",
                    year,
                    idx,
                    int(stats["skipped_intraday_entry"]),
                )
            if on_year_complete:
                on_year_complete(year, idx, years_total, stats)

            return year, stats, trades, execution_diagnostics

        # Run all years in parallel
        with ThreadPoolExecutor(max_workers=params.parallel_workers) as executor:
            # Submit all jobs
            futures = {
                executor.submit(run_single_year_thread, year, idx): year
                for idx, year in enumerate(years_to_run, start=1)
            }

            # Collect results as they complete
            for future in as_completed(futures):
                year, stats, trades, execution_diagnostics = future.result()
                with results_lock:
                    yearly_results[year] = stats
                    all_trades.extend(trades)
                    all_execution_diagnostics.extend(execution_diagnostics)

        # Sort results by year for consistent output
        sorted_results = dict(sorted(yearly_results.items()))
        return sorted_results, all_trades, all_execution_diagnostics

    def _run_single_year(
        self,
        params: BacktestParams,
        symbols: list[str],
        year: int,
        year_window_start: date,
        year_window_end: date,
        heartbeat_cb: Callable[[str], None] | None = None,
    ) -> tuple[dict, list[dict], list[dict]]:
        """Run backtest for a single year, return stats, trades, and diagnostics."""
        empty_stats = {
            "year": year,
            "signals": 0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "return_pct": 0,
            "win_rate_pct": 0,
            "avg_r": 0,
            "max_dd_pct": 0,
            "profit_factor": 0,
            "avg_holding_days": 0,
            "exit_reasons": {},
            "skipped_intraday_entry": 0,
            "execution_diagnostics": {},
        }

        if not self._active_strategy:
            raise RuntimeError(
                "Strategy was not initialized. Call run() before running yearly backtests."
            )
        if not symbols:
            return empty_stats, [], []

        is_short = self._active_strategy.direction == PositionSide.SHORT
        max_stop_dist_pct = self._resolve_max_stop_dist_pct(params, self._active_strategy)

        # Strategy-specific signal candidate query.
        query, params_tuple = self._active_strategy.build_candidate_query(
            params,
            symbols,
            year_window_start,
            year_window_end,
        )

        # Execute with parameterized values.
        query_result = self.db.con.execute(query, params_tuple)
        try:
            df_pl = query_result.pl()
        except (
            Exception
        ) as exc:  # pragma: no cover - compatibility fallback for older duckdb builds
            logger.debug(
                "DuckDB .pl() failed for strategy candidate query (%s); falling back to Arrow",
                exc,
            )
            df_pl = pl.from_arrow(query_result.arrow())

        if df_pl.is_empty():
            return empty_stats, [], []
        total_signals = df_pl.height

        # Use strategy-specific entry filter columns if defined, else fall back to
        # generic filter_columns / 2LYNCH defaults. Carry-quality filters (e.g. H)
        # are evaluated after entry admission, not here.
        active_filter_cols = (
            self._active_strategy.entry_filter_columns
            or self._active_strategy.filter_columns
            or [f.value for f in ALL_FILTERS]
        )
        hold_quality_cols = self._active_strategy.hold_quality_filter_columns or []
        if (
            not hold_quality_cols
            and self._active_strategy.family == "threshold_breakout"
            and bool(getattr(params, "breakout_legacy_h_carry_rule", False))
        ):
            hold_quality_cols = ["filter_h"]
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

        # Build filter count expression generically from active filter columns.
        filter_sum_expr = pl.col(active_filter_cols[0]).cast(int)
        for col in active_filter_cols[1:]:
            filter_sum_expr = filter_sum_expr + pl.col(col).cast(int)
        df_pl = df_pl.with_columns(filter_sum_expr.alias("filters_passed"))

        effective_min_filters = (
            self._active_strategy.min_filters_override
            if self._active_strategy.min_filters_override is not None
            else params.min_filters
        )
        df_filtered = df_pl.filter(pl.col("filters_passed") >= effective_min_filters)

        if df_filtered.height == 0:
            return empty_stats, [], []

        ranking_rejected = pl.DataFrame()
        if self._uses_breakout_ranking():
            df_filtered, ranking_rejected = self._apply_breakout_selection_ranking(
                df_filtered, params
            )
        elif self._uses_breakdown_ranking():
            df_filtered, ranking_rejected = self._apply_breakdown_selection_ranking(
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

        last_heartbeat = datetime.now(UTC)

        def maybe_heartbeat(message: str) -> None:
            nonlocal last_heartbeat
            if heartbeat_cb is None:
                return
            now = datetime.now(UTC)
            if (now - last_heartbeat).total_seconds() < 120:
                return
            heartbeat_cb(message)
            last_heartbeat = now

        # Load price data for signal symbols
        signal_symbols = df_filtered["symbol"].unique().to_list()
        symbol_id_by_symbol = self._build_symbol_id_map(signal_symbols)
        id_to_symbol = {symbol_id: symbol for symbol, symbol_id in symbol_id_by_symbol.items()}

        price_data: dict[int, dict] = {}
        value_traded_inr: dict[int, float] = {}
        start_date = date(year - 1, 12, 1)
        end_date = date(year + 1, 1, 31)

        fallback_value_traded_inr = 50_000_000.0
        maybe_heartbeat(f"loading daily price data (0/{len(signal_symbols)} symbols)")

        # Bulk load daily OHLCV for all signal symbols once to avoid per-symbol query overhead.
        daily_df = self.db.query_daily_multi(
            signal_symbols,
            start_date.isoformat(),
            end_date.isoformat(),
            columns=["symbol", "date", "open", "high", "low", "close"],
        )
        if not daily_df.is_empty():
            grouped_daily = daily_df.partition_by(["symbol"], as_dict=True, maintain_order=True)
            loaded_symbols: set[str] = set()
            with tqdm(
                total=len(grouped_daily), desc="loading daily price data", unit="symbols"
            ) as price_bar:
                for group_key, symbol_daily in grouped_daily.items():
                    symbol = str(group_key[0]) if isinstance(group_key, tuple) else str(group_key)
                    symbol_id = symbol_id_by_symbol.get(symbol)
                    if symbol_id is None:
                        price_bar.update(1)
                        continue

                    symbol_price_map: dict[date, dict[str, float]] = {}
                    dates = symbol_daily["date"].to_list()
                    opens = symbol_daily["open"].to_list()
                    highs = symbol_daily["high"].to_list()
                    lows = symbol_daily["low"].to_list()
                    closes = symbol_daily["close"].to_list()
                    for dt, open_px, high_px, low_px, close_px in zip(
                        dates, opens, highs, lows, closes, strict=False
                    ):
                        if (
                            dt is None
                            or open_px is None
                            or high_px is None
                            or low_px is None
                            or close_px is None
                        ):
                            continue
                        trading_day = dt.date() if isinstance(dt, datetime) else dt
                        if not isinstance(trading_day, date):
                            continue
                        symbol_price_map[trading_day] = {
                            "open_adj": float(open_px),
                            "close_adj": float(close_px),
                            "high_adj": float(high_px),
                            "low_adj": float(low_px),
                        }

                    if symbol_price_map:
                        price_data[symbol_id] = symbol_price_map
                        loaded_symbols.add(symbol)
                        if len(loaded_symbols) % 200 == 0:
                            maybe_heartbeat(
                                "loading daily price data "
                                f"({len(loaded_symbols)}/{len(signal_symbols)} symbols)"
                            )
                    price_bar.update(1)
            maybe_heartbeat(
                f"loading daily price data ({len(loaded_symbols)}/{len(signal_symbols)} symbols)"
            )

        # Bulk load static liquidity feature once, then map symbol -> avg dollar volume.
        vol_df = self.db.get_avg_dollar_vol_20_by_symbol(
            signal_symbols, start_date.isoformat(), end_date.isoformat()
        )
        if not vol_df.is_empty():
            for row in vol_df.iter_rows(named=True):
                symbol = row["symbol"]
                symbol_id = symbol_id_by_symbol.get(symbol)
                if symbol_id is None:
                    continue
                avg_vol = row["avg_dollar_vol_20"]
                if avg_vol is not None:
                    avg_vol_float = float(avg_vol)
                    if avg_vol_float > 0:
                        value_traded_inr[symbol_id] = avg_vol_float

        for symbol_id in symbol_id_by_symbol.values():
            value_traded_inr.setdefault(symbol_id, fallback_value_traded_inr)

        # Build VectorBT signals
        vbt_signals = []
        skipped_intraday_entry = 0
        execution_diagnostics: list[dict[str, object]] = []
        signal_context: dict[tuple[int, date], dict[str, object]] = {}
        intraday_entry_by_signal: dict[tuple[str, date], IntradayEntry] = {}
        if params.entry_timeframe.lower() == "5min":
            is_ep_proxy = self._active_strategy.family == "ep_proxy"
            orh_window = params.orh_window_minutes if is_ep_proxy else 0
            intraday_entry_by_signal = self._resolve_intraday_entries_bulk(
                df_filtered=df_filtered,
                breakout_threshold=params.breakout_threshold,
                entry_cutoff_minutes=self._resolve_entry_cutoff_minutes(
                    params, self._active_strategy
                ),
                is_short=is_short,
                orh_window_minutes=orh_window,
                same_day_r_ladder=params.same_day_r_ladder,
                same_day_r_ladder_start_r=self._resolve_same_day_r_ladder_start_r(
                    params, self._active_strategy
                ),
                short_initial_stop_atr_cap_mult=(
                    params.short_initial_stop_atr_cap_mult if is_short else None
                ),
                short_same_day_take_profit_pct=(
                    params.short_same_day_take_profit_pct if is_short else None
                ),
                heartbeat_cb=maybe_heartbeat,
            )

        for row in ranking_rejected.iter_rows(named=True):
            diag_entry, _filter_snapshot, hold_quality_passed = self._build_diag_entry(
                year=year,
                row=row,
                active_filter_cols=active_filter_cols,
                hold_quality_cols=hold_quality_cols,
            )
            diag_entry["status"] = "skipped_rank_budget"
            diag_entry["reason"] = "below_daily_rank_budget"
            diag_entry["hold_quality_passed"] = hold_quality_passed
            execution_diagnostics.append(diag_entry)

        with tqdm(
            total=df_filtered.height, desc="assembling VectorBT signals", unit="signals"
        ) as signal_bar:
            for row in df_filtered.iter_rows(named=True):
                maybe_heartbeat(
                    f"assembling VectorBT signals ({len(vbt_signals)}/{df_filtered.height})"
                )
                symbol = row["symbol"]
                symbol_id = symbol_id_by_symbol[symbol]
                sig_date = row["trading_date"]
                if isinstance(sig_date, datetime):
                    sig_date = sig_date.date()
                if not isinstance(sig_date, date):
                    signal_bar.update(1)
                    continue

                diag_entry, filter_snapshot, hold_quality_passed = self._build_diag_entry(
                    year=year,
                    row=row,
                    active_filter_cols=active_filter_cols,
                    hold_quality_cols=hold_quality_cols,
                )

                if symbol_id not in price_data:
                    diag_entry["status"] = "skipped_no_price_data"
                    diag_entry["reason"] = "missing_daily_price_data"
                    execution_diagnostics.append(diag_entry)
                    signal_bar.update(1)
                    continue

                intraday_entry = None
                if params.entry_timeframe.lower() == "5min":
                    intraday_entry = intraday_entry_by_signal.get((symbol, sig_date))
                    if intraday_entry is None:
                        skipped_intraday_entry += 1
                        diag_entry["status"] = "skipped_no_intraday_entry"
                        diag_entry["reason"] = "no_5min_breakout_before_cutoff"
                        execution_diagnostics.append(diag_entry)
                        signal_bar.update(1)
                        continue

                # Legacy daily execution fallback.
                if intraday_entry is None:
                    entry_price = float(row["open"]) if row["open"] is not None else None
                    if is_short:
                        # Stop for SHORT is above entry: use day high, then prev_high
                        initial_stop = (
                            row["high"] if row.get("high") is not None else row.get("prev_high")
                        )
                    else:
                        initial_stop = row["low"] if row["low"] is not None else row.get("prev_low")
                    same_day_stop_hit = False
                else:
                    entry_price = float(intraday_entry["entry_price"])
                    initial_stop = float(intraday_entry["initial_stop"])
                    same_day_stop_hit = bool(intraday_entry["same_day_stop_hit"])
                    diag_entry["entry_time"] = intraday_entry.get("entry_time")

                    if entry_price is None or initial_stop is None:
                        skipped_intraday_entry += 1
                        diag_entry["status"] = "skipped_invalid_intraday_entry"
                        diag_entry["reason"] = "missing_entry_price_or_stop"
                        execution_diagnostics.append(diag_entry)
                        signal_bar.update(1)
                        continue

                    # Max stop distance guard: FEE implies a tight stop.
                    # For LONG: stop must not be too far below entry.
                    # For SHORT: stop must not be too far above entry.
                    if entry_price > 0:
                        if is_short and initial_stop > entry_price * (1 + max_stop_dist_pct):
                            skipped_intraday_entry += 1
                            diag_entry["status"] = "skipped_stop_too_wide"
                            diag_entry["reason"] = "short_stop_above_max_distance"
                            execution_diagnostics.append(diag_entry)
                            signal_bar.update(1)
                            continue
                        elif not is_short and initial_stop < entry_price * (
                            1 - params.max_stop_dist_pct
                        ):
                            skipped_intraday_entry += 1
                            diag_entry["status"] = "skipped_stop_too_wide"
                            diag_entry["reason"] = "long_stop_below_max_distance"
                            execution_diagnostics.append(diag_entry)
                            signal_bar.update(1)
                            continue
                filters_passed = (
                    int(row["filters_passed"]) if row["filters_passed"] is not None else 0
                )
                gap_pct = row.get("gap_pct", row.get("anchor_gap_pct", 0.0))
                entry_time = intraday_entry.get("entry_time") if intraday_entry else None
                same_day_exit_price = (
                    intraday_entry.get("same_day_exit_price") if intraday_entry else None
                )
                same_day_exit_reason = (
                    intraday_entry.get("same_day_exit_reason") if intraday_entry else None
                )
                same_day_exit_ts = (
                    intraday_entry.get("same_day_exit_ts") if intraday_entry else None
                )
                same_day_exit_time = (
                    intraday_entry.get("same_day_exit_time") if intraday_entry else None
                )
                carry_stop_next_session = (
                    intraday_entry.get("carry_stop_next_session") if intraday_entry else None
                )
                carry_action = "normal"

                if intraday_entry is not None:
                    (
                        same_day_exit_price,
                        same_day_exit_reason,
                        same_day_exit_ts,
                        same_day_exit_time,
                        carry_stop_next_session,
                        carry_action,
                    ) = self._apply_hold_quality_carry_rule(
                        hold_quality_passed=hold_quality_passed,
                        entry_price=entry_price,
                        close_price=float(row["close"]) if row.get("close") is not None else None,
                        carry_stop_next_session=carry_stop_next_session,
                        same_day_exit_price=same_day_exit_price,
                        same_day_exit_reason=same_day_exit_reason,
                        same_day_exit_ts=same_day_exit_ts,
                        same_day_exit_time=same_day_exit_time,
                        signal_date=sig_date,
                        is_short=is_short,
                    )

                diag_entry["entry_time"] = entry_time
                diag_entry["entry_price"] = entry_price
                diag_entry["initial_stop"] = initial_stop
                diag_entry["filters_json"] = filter_snapshot
                diag_entry["hold_quality_passed"] = hold_quality_passed

                signal_context[(symbol_id, sig_date)] = {
                    "gap_pct": gap_pct,
                    "filters_passed": filters_passed,
                    "entry_time": entry_time,
                    "filter_snapshot": filter_snapshot,
                    "hold_quality_passed": hold_quality_passed,
                    "entry_filter_columns": list(active_filter_cols),
                    "hold_quality_columns": list(hold_quality_cols),
                    "carry_action": carry_action,
                    "selection_score": diag_entry.get("selection_score"),
                    "selection_rank": diag_entry.get("selection_rank"),
                    "selection_components": diag_entry.get("selection_components_json", {}),
                }
                execution_diagnostics.append(diag_entry)

                vbt_signals.append(
                    BacktestSignal(
                        signal_date=sig_date,
                        symbol_id=symbol_id,
                        symbol=symbol,
                        initial_stop=initial_stop,
                        metadata=SignalMetadata(
                            gap_pct=gap_pct,
                            atr=row["atr_20"] if row["atr_20"] else 0.0,
                            filters_passed=filters_passed,
                            entry_price=entry_price,
                            same_day_stop_hit=same_day_stop_hit,
                            entry_time=entry_time,
                            entry_ts=intraday_entry.get("entry_ts") if intraday_entry else None,
                            same_day_exit_ts=same_day_exit_ts,
                            carry_stop_next_session=carry_stop_next_session,
                            extra={
                                "same_day_exit_price": same_day_exit_price,
                                "same_day_exit_reason": same_day_exit_reason,
                                "same_day_exit_time": same_day_exit_time,
                                "filter_snapshot": filter_snapshot,
                                "hold_quality_passed": hold_quality_passed,
                                "entry_filter_columns": list(active_filter_cols),
                                "hold_quality_columns": list(hold_quality_cols),
                                "carry_action": carry_action,
                                "selection_score": diag_entry.get("selection_score"),
                                "selection_rank": diag_entry.get("selection_rank"),
                                "selection_components": diag_entry.get(
                                    "selection_components_json", {}
                                ),
                            },
                        ),
                    )
                )
                signal_bar.update(1)

        result = None
        vectorbt_return_pct = 0.0
        vectorbt_max_dd_pct = 0.0
        if vbt_signals:
            strategy_label = self._active_strategy.label_for_year(year)
            direction = self._active_strategy.direction
            vbt_config = params.to_vbt_config(
                direction=direction,
                short_post_day3_buffer_pct=self._resolve_short_post_day3_buffer_pct(
                    params,
                    self._active_strategy,
                ),
            )
            engine = VectorBTEngine(config=vbt_config)
            result = engine.run_backtest(
                strategy_name=strategy_label,
                signals=vbt_signals,
                price_data=price_data,
                value_traded_inr=value_traded_inr,
            )
            vectorbt_return_pct = result.total_return * 100
            vectorbt_max_dd_pct = result.max_drawdown * 100

        # Build trade dicts — direction-aware PnL percentage
        trades_out: list[dict] = []
        for t in result.trades if result is not None else []:
            hd = (t.exit_date - t.entry_date).days if t.exit_date and t.entry_date else 0
            pct = 0.0
            if t.entry_price and t.exit_price and t.entry_price > 0:
                if is_short:
                    pct = ((t.entry_price - t.exit_price) / t.entry_price) * 100
                else:
                    pct = ((t.exit_price - t.entry_price) / t.entry_price) * 100

            real_symbol = id_to_symbol.get(t.symbol_id, t.symbol)

            # Find gap_pct and filters_passed for this trade
            context = signal_context.get((t.symbol_id, t.entry_date), {})
            gap_pct = context.get("gap_pct")
            filters_passed = context.get("filters_passed")

            # Timestamped exits from vectorbt timeline are authoritative.
            exit_time = t.exit_time
            if exit_time is None and t.exit_reason is not None:
                exit_time = get_exit_time_for_reason(t.exit_reason.value)

            trades_out.append(
                {
                    "year": year,
                    "entry_date": t.entry_date,
                    "exit_date": t.exit_date,
                    "symbol": real_symbol,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                    "pnl_pct": pct,
                    "r_multiple": t.pnl_r if t.pnl_r else 0.0,
                    "exit_reason": t.exit_reason.value if t.exit_reason else "unknown",
                    "holding_days": hd,
                    "gap_pct": gap_pct,
                    "filters_passed": filters_passed,
                    "entry_time": t.entry_time or context.get("entry_time"),
                    "exit_time": exit_time,
                    "entry_mode": t.entry_mode,
                    "qty": t.qty,
                    "initial_stop": t.initial_stop,
                    "fees": t.fees,
                    "slippage_bps": t.slippage_bps,
                    "mfe_r": t.mfe_r,
                    "mae_r": t.mae_r,
                    "exit_rule_version": t.exit_rule_version,
                    "selection_score": context.get("selection_score"),
                    "selection_rank": context.get("selection_rank"),
                    "reason_json": json.dumps(
                        {
                            "filter_snapshot": context.get("filter_snapshot", {}),
                            "hold_quality_passed": context.get("hold_quality_passed"),
                            "entry_filter_columns": context.get("entry_filter_columns", []),
                            "hold_quality_columns": context.get("hold_quality_columns", []),
                            "selection_score": context.get("selection_score"),
                            "selection_rank": context.get("selection_rank"),
                            "selection_components": context.get("selection_components", {}),
                        },
                        sort_keys=True,
                    ),
                    "timeline_version": "mixed_resolution_v1",
                }
            )

        executed_by_signal: dict[tuple[str, date], dict] = {}
        for trade in trades_out:
            key = (str(trade["symbol"]), trade["entry_date"])
            executed_by_signal[key] = trade

        for diag in execution_diagnostics:
            if diag.get("status") != "queued_for_execution":
                continue
            key = (str(diag["symbol"]), diag["signal_date"])
            executed_trade = executed_by_signal.get(key)
            if executed_trade is None:
                diag["status"] = "not_executed_portfolio"
                diag["reason"] = "capital_or_position_limits"
                continue
            diag["status"] = "executed"
            diag["reason"] = "executed"
            diag["executed_exit_reason"] = executed_trade.get("exit_reason")
            diag["pnl_pct"] = executed_trade.get("pnl_pct")

        if not trades_out:
            signals_after_filters = len(df_filtered) + len(ranking_rejected)
            execution_summary = {
                "signals_total": total_signals,
                "signals_filtered": signals_after_filters,
                "signals_after_rank_budget": len(df_filtered),
                "queued_for_execution": len(vbt_signals),
                "executed": 0,
                "not_executed_portfolio": sum(
                    1 for d in execution_diagnostics if d.get("status") == "not_executed_portfolio"
                ),
                "skipped_rank_budget": sum(
                    1 for d in execution_diagnostics if d.get("status") == "skipped_rank_budget"
                ),
                "skipped_no_intraday_entry": sum(
                    1
                    for d in execution_diagnostics
                    if d.get("status") == "skipped_no_intraday_entry"
                ),
                "skipped_no_price_data": sum(
                    1 for d in execution_diagnostics if d.get("status") == "skipped_no_price_data"
                ),
                "skipped_stop_too_wide": sum(
                    1 for d in execution_diagnostics if d.get("status") == "skipped_stop_too_wide"
                ),
            }
            return (
                {
                    **empty_stats,
                    "signals": signals_after_filters,
                    "filtered_signals": signals_after_filters,
                    "skipped_intraday_entry": skipped_intraday_entry,
                    "execution_diagnostics": execution_summary,
                },
                [],
                execution_diagnostics,
            )

        total_return_pct = vectorbt_return_pct

        wins = sum(1 for t in trades_out if float(t.get("pnl_pct", 0.0)) > 0)
        losses = sum(1 for t in trades_out if float(t.get("pnl_pct", 0.0)) < 0)
        total_trades = len(trades_out)
        win_rate_pct = (wins / total_trades * 100) if total_trades else 0.0

        exit_reasons: dict[str, int] = {}
        holding_days: list[int] = []
        total_gains = 0.0
        total_losses_val = 0.0
        r_values: list[float] = []
        for t in trades_out:
            reason = str(t.get("exit_reason", "unknown"))
            exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
            hd = int(t.get("holding_days", 0) or 0)
            holding_days.append(hd)
            pnl_pct = float(t.get("pnl_pct", 0.0) or 0.0)
            if pnl_pct > 0:
                total_gains += pnl_pct
            elif pnl_pct < 0:
                total_losses_val += abs(pnl_pct)
            r_values.append(float(t.get("r_multiple", 0.0) or 0.0))

        profit_factor = total_gains / total_losses_val if total_losses_val > 0 else 0.0

        stats = {
            "year": year,
            "signals": len(df_filtered) + len(ranking_rejected),
            "trades": total_trades,
            "wins": wins,
            "losses": losses,
            "return_pct": total_return_pct,
            "win_rate_pct": win_rate_pct,
            "avg_r": float(np.mean(r_values)) if r_values else 0.0,
            "max_dd_pct": vectorbt_max_dd_pct,
            "profit_factor": profit_factor,
            "avg_holding_days": float(np.mean(holding_days)) if holding_days else 0.0,
            "exit_reasons": exit_reasons,
            "skipped_intraday_entry": skipped_intraday_entry,
            "execution_diagnostics": {
                "signals_total": total_signals,
                "signals_filtered": len(df_filtered) + len(ranking_rejected),
                "signals_after_rank_budget": len(df_filtered),
                "queued_for_execution": len(vbt_signals),
                "executed": len(trades_out),
                "not_executed_portfolio": max(len(vbt_signals) - len(trades_out), 0),
                "skipped_rank_budget": sum(
                    1 for d in execution_diagnostics if d.get("status") == "skipped_rank_budget"
                ),
                "skipped_no_intraday_entry": sum(
                    1
                    for d in execution_diagnostics
                    if d.get("status") == "skipped_no_intraday_entry"
                ),
                "skipped_no_price_data": sum(
                    1 for d in execution_diagnostics if d.get("status") == "skipped_no_price_data"
                ),
                "skipped_stop_too_wide": sum(
                    1 for d in execution_diagnostics if d.get("status") == "skipped_stop_too_wide"
                ),
            },
        }

        return stats, trades_out, execution_diagnostics

    @staticmethod
    def _minutes_from_nse_open(candle_time: object) -> int | None:
        """Return minutes elapsed since NSE market open (09:15 IST) for a candle_time value.

        Handles datetime.time, datetime.datetime, and integer microseconds (DuckDB TIME64).
        Returns None if the format is unrecognised.
        """
        return minutes_from_nse_open(candle_time)

    @staticmethod
    def _normalize_candle_time(candle_time: object) -> time | None:
        """Convert a raw DuckDB candle_time value to a datetime.time object.

        DuckDB TIME columns are returned as microseconds-since-midnight (int)
        when fetched via the Python API. This helper normalises all three
        possible types: datetime, time, and int/float microseconds.
        """
        return normalize_candle_time(candle_time)

    @staticmethod
    def _apply_hold_quality_carry_rule(
        *,
        hold_quality_passed: bool,
        entry_price: float | None,
        close_price: float | None,
        carry_stop_next_session: float | None,
        same_day_exit_price: float | None,
        same_day_exit_reason: str | None,
        same_day_exit_ts: datetime | None,
        same_day_exit_time: time | None,
        signal_date: date,
        is_short: bool,
    ) -> tuple[float | None, str | None, datetime | None, time | None, float | None, str]:
        """Apply post-entry carry logic driven by hold-quality filters."""
        if hold_quality_passed or same_day_exit_reason is not None:
            return (
                same_day_exit_price,
                same_day_exit_reason,
                same_day_exit_ts,
                same_day_exit_time,
                carry_stop_next_session,
                "normal",
            )

        if entry_price is None or close_price is None:
            return (
                same_day_exit_price,
                same_day_exit_reason,
                same_day_exit_ts,
                same_day_exit_time,
                carry_stop_next_session,
                "normal",
            )

        # Keep short-side H=false behavior conservative: exit at the close.
        if is_short:
            return (
                float(close_price),
                ExitReason.WEAK_CLOSE_EXIT.value,
                datetime.combine(signal_date, time(15, 30)),
                time(15, 30),
                carry_stop_next_session,
                "weak_close_exit",
            )

        close_failed_entry = close_price <= entry_price
        if close_failed_entry:
            return (
                float(close_price),
                ExitReason.WEAK_CLOSE_EXIT.value,
                datetime.combine(signal_date, time(15, 30)),
                time(15, 30),
                carry_stop_next_session,
                "weak_close_exit",
            )

        base_carry_stop = (
            carry_stop_next_session if carry_stop_next_session is not None else entry_price
        )
        tightened_stop = (
            min(float(base_carry_stop), float(entry_price))
            if is_short
            else max(float(base_carry_stop), float(entry_price))
        )
        return (
            same_day_exit_price,
            same_day_exit_reason,
            same_day_exit_ts,
            same_day_exit_time,
            tightened_stop,
            "breakeven_carry",
        )

    @staticmethod
    def _simulate_same_day_stop_execution(
        *,
        rows: list[dict[str, object]],
        entry_idx: int,
        entry_price: float,
        initial_stop: float,
        is_short: bool,
        same_day_r_ladder: bool,
        same_day_r_ladder_start_r: int = 2,
    ) -> tuple[bool, float | None, time | None, str | None]:
        """Simulate same-day post-entry stop execution on 5-min bars.

        Scans only bars after the entry bar (no intrabar sequencing assumptions).
        If same_day_r_ladder is enabled, ratchet stop by R-steps before checking stop-hit on each bar.
        """
        stop_level = float(initial_stop)
        risk = (
            (float(initial_stop) - float(entry_price))
            if is_short
            else (float(entry_price) - float(initial_stop))
        )

        for follow_row in rows[entry_idx + 1 :]:
            high_px = float(follow_row["high"])
            low_px = float(follow_row["low"])

            if same_day_r_ladder and risk > 0:
                if is_short:
                    realized_r = (float(entry_price) - low_px) / risk
                    r_steps = int(np.floor(realized_r))
                    if r_steps >= same_day_r_ladder_start_r:
                        locked_r = float(max(0, r_steps - same_day_r_ladder_start_r))
                        candidate_stop = float(entry_price) - (locked_r * risk)
                        stop_level = min(stop_level, candidate_stop)
                else:
                    realized_r = (high_px - float(entry_price)) / risk
                    r_steps = int(np.floor(realized_r))
                    if r_steps >= same_day_r_ladder_start_r:
                        locked_r = float(max(0, r_steps - same_day_r_ladder_start_r))
                        candidate_stop = float(entry_price) + (locked_r * risk)
                        stop_level = max(stop_level, candidate_stop)

            stop_hit = high_px >= stop_level if is_short else low_px <= stop_level
            if not stop_hit:
                continue

            if is_short:
                if stop_level < float(entry_price):
                    reason = ExitReason.STOP_TRAIL.value
                elif np.isclose(stop_level, float(entry_price)):
                    reason = ExitReason.STOP_BREAKEVEN.value
                else:
                    reason = ExitReason.STOP_INITIAL.value
            else:
                if stop_level > float(entry_price):
                    reason = ExitReason.STOP_TRAIL.value
                elif np.isclose(stop_level, float(entry_price)):
                    reason = ExitReason.STOP_BREAKEVEN.value
                else:
                    reason = ExitReason.STOP_INITIAL.value

            exit_time = DuckDBBacktestRunner._normalize_candle_time(follow_row.get("candle_time"))
            return True, float(stop_level), exit_time, reason

        return False, None, None, None

    @staticmethod
    def _resolve_intraday_entry_from_5min(
        candles: pl.DataFrame,
        breakout_price: float,
        entry_cutoff_minutes: int = 30,
        is_short: bool = False,
        orh_window_minutes: int = 0,
        same_day_r_ladder: bool = False,
        same_day_r_ladder_start_r: int = 2,
        short_initial_stop_atr: float | None = None,
        short_initial_stop_atr_cap_mult: float | None = None,
        short_same_day_take_profit_pct: float | None = None,
    ) -> IntradayEntry | None:
        result: IntradayExecutionResult | None = resolve_intraday_execution_from_5min(
            candles,
            breakout_price=breakout_price,
            entry_cutoff_minutes=entry_cutoff_minutes,
            is_short=is_short,
            orh_window_minutes=orh_window_minutes,
            same_day_r_ladder=same_day_r_ladder,
            same_day_r_ladder_start_r=same_day_r_ladder_start_r,
            short_initial_stop_atr=short_initial_stop_atr,
            short_initial_stop_atr_cap_mult=short_initial_stop_atr_cap_mult,
            short_same_day_take_profit_pct=short_same_day_take_profit_pct,
        )
        if result is None:
            return None
        return {
            "entry_price": result.entry_price,
            "initial_stop": result.initial_stop,
            "same_day_stop_hit": result.same_day_exit_ts is not None,
            "entry_ts": result.entry_ts,
            "same_day_exit_price": result.same_day_exit_price,
            "same_day_exit_ts": result.same_day_exit_ts,
            "same_day_exit_time": result.same_day_exit_time,
            "same_day_exit_reason": result.same_day_exit_reason.value
            if result.same_day_exit_reason is not None
            else None,
            "carry_stop_next_session": result.carry_stop_next_session,
            "entry_time": result.entry_time,
        }

    def _resolve_intraday_entries_bulk(
        self,
        *,
        df_filtered: pl.DataFrame,
        breakout_threshold: float,
        entry_cutoff_minutes: int = 30,
        is_short: bool = False,
        orh_window_minutes: int = 0,
        same_day_r_ladder: bool = False,
        same_day_r_ladder_start_r: int = 2,
        short_initial_stop_atr_cap_mult: float | None = None,
        short_same_day_take_profit_pct: float | None = None,
        heartbeat_cb: Callable[[str], None] | None = None,
    ) -> dict[tuple[str, date], IntradayEntry]:
        """Resolve intraday entries for all signal days with one 5-min batch query.

        Threshold mode (orh_window_minutes=0):
            LONG: trigger = prev_close * (1 + threshold), look for high >= trigger.
            SHORT: trigger = prev_close * (1 - threshold), look for low <= trigger.

        ORH mode (orh_window_minutes>0, LONG only):
            Observe first orh_window_minutes to build ORH, then enter on ORH break.
            breakout_price stored as prev_close (sanity-check reference only).
        """
        targets = (
            df_filtered.select(["symbol", "trading_date", "prev_close", "atr_20"])
            .drop_nulls(["symbol", "trading_date", "prev_close"])
            .unique(subset=["symbol", "trading_date"], keep="first", maintain_order=True)
        )
        if targets.is_empty():
            return {}

        breakout_price_by_key: dict[tuple[str, date], float] = {}
        short_initial_stop_atr_by_key: dict[tuple[str, date], float] = {}
        for symbol_raw, trading_date_raw, prev_close_raw, atr_20_raw in targets.iter_rows():
            if trading_date_raw is None or prev_close_raw is None:
                continue
            if isinstance(trading_date_raw, datetime):
                trading_day = trading_date_raw.date()
            elif isinstance(trading_date_raw, date):
                trading_day = trading_date_raw
            else:
                try:
                    trading_day = date.fromisoformat(str(trading_date_raw))
                except ValueError:
                    continue

            prev_close = float(prev_close_raw)
            if prev_close <= 0:
                continue
            symbol = str(symbol_raw)
            if orh_window_minutes > 0:
                # ORH mode: store prev_close as sanity-check reference (trigger is ORH from candles)
                breakout_price_by_key[(symbol, trading_day)] = prev_close
            else:
                # Threshold mode: SHORT trigger below, LONG trigger above prev_close
                multiplier = (1 - breakout_threshold) if is_short else (1 + breakout_threshold)
                breakout_price_by_key[(symbol, trading_day)] = prev_close * multiplier
            if is_short and short_initial_stop_atr_cap_mult is not None and atr_20_raw is not None:
                atr_20 = float(atr_20_raw)
                if atr_20 > 0:
                    short_initial_stop_atr_by_key[(symbol, trading_day)] = atr_20
        if not breakout_price_by_key:
            return {}

        join_df = pl.DataFrame(
            [
                {"symbol": symbol, "trading_date": trading_day}
                for symbol, trading_day in breakout_price_by_key
            ]
        )
        tmp_name = "tmp_intraday_signal_days"
        self.db.con.register(tmp_name, join_df.to_arrow())
        try:
            candles = self.db.con.execute(
                f"""
                SELECT c.symbol, c.date AS trading_date, c.candle_time, c.open, c.high, c.low
                FROM v_5min c
                INNER JOIN {tmp_name} t
                  ON c.symbol = t.symbol
                 AND c.date = t.trading_date
                ORDER BY c.symbol, c.date, c.candle_time
                """
            ).pl()
        finally:
            try:
                self.db.con.unregister(tmp_name)
            except Exception:
                pass

        if candles.is_empty():
            return {}

        resolved_entries: dict[tuple[str, date], IntradayEntry] = {}
        grouped = candles.partition_by(
            ["symbol", "trading_date"], as_dict=True, maintain_order=True
        )
        total_groups = len(grouped)
        with tqdm(
            total=total_groups, desc="processing 5-min entries", unit="signal-days"
        ) as entry_bar:
            for idx, (group_key, group_candles) in enumerate(grouped.items(), start=1):
                if heartbeat_cb is not None and idx % 250 == 0:
                    heartbeat_cb(f"processing 5-min entries ({idx}/{total_groups} signal-days)")
                symbol = str(group_key[0])
                trading_day_raw = group_key[1]
                trading_day_from_group: date | None
                if isinstance(trading_day_raw, datetime):
                    trading_day_from_group = trading_day_raw.date()
                elif isinstance(trading_day_raw, date):
                    trading_day_from_group = trading_day_raw
                else:
                    try:
                        trading_day_from_group = date.fromisoformat(str(trading_day_raw))
                    except ValueError:
                        entry_bar.update(1)
                        continue

                breakout_price = breakout_price_by_key.get((symbol, trading_day_from_group))
                if breakout_price is None:
                    entry_bar.update(1)
                    continue

                intraday_entry = self._resolve_intraday_entry_from_5min(
                    group_candles,
                    breakout_price,
                    entry_cutoff_minutes,
                    is_short=is_short,
                    orh_window_minutes=orh_window_minutes,
                    same_day_r_ladder=same_day_r_ladder,
                    same_day_r_ladder_start_r=same_day_r_ladder_start_r,
                    short_initial_stop_atr=short_initial_stop_atr_by_key.get(
                        (symbol, trading_day_from_group)
                    ),
                    short_initial_stop_atr_cap_mult=short_initial_stop_atr_cap_mult,
                    short_same_day_take_profit_pct=short_same_day_take_profit_pct,
                )
                if intraday_entry is not None:
                    resolved_entries[(symbol, trading_day_from_group)] = intraday_entry
                entry_bar.update(1)

        return resolved_entries

    def _resolve_intraday_entry(
        self,
        *,
        symbol: str,
        trading_date: date,
        prev_close: float | None,
        breakout_threshold: float,
        entry_cutoff_minutes: int = 30,
        same_day_r_ladder: bool = False,
        same_day_r_ladder_start_r: int = 2,
    ) -> IntradayEntry | None:
        if prev_close is None or prev_close <= 0:
            return None

        breakout_price = prev_close * (1 + breakout_threshold)
        candles = self.db.query_5min(
            symbol=symbol,
            start_date=trading_date.isoformat(),
            end_date=trading_date.isoformat(),
            columns=["candle_time", "open", "high", "low", "close", "volume"],
        )
        return self._resolve_intraday_entry_from_5min(
            candles,
            breakout_price,
            entry_cutoff_minutes,
            same_day_r_ladder=same_day_r_ladder,
            same_day_r_ladder_start_r=same_day_r_ladder_start_r,
        )

    def _persist_results(
        self,
        exp_id: str,
        params: BacktestParams,
        yearly_results: dict[int, dict],
        all_trades: list[dict],
        all_execution_diagnostics: list[dict],
    ) -> None:
        """Write results to DuckDB tables."""
        # Save trades
        self.results_db.save_trades(exp_id, all_trades)
        self.results_db.save_execution_diagnostics(exp_id, all_execution_diagnostics)

        # Save yearly metrics
        for _year, stats in yearly_results.items():
            self.results_db.save_yearly_metric(exp_id, stats)

        # Compute aggregates
        total_return = sum(s["return_pct"] for s in yearly_results.values())
        num_years = params.end_year - params.start_year + 1
        ann_return = total_return / num_years if num_years else 0
        total_trades = sum(s["trades"] for s in yearly_results.values())
        total_wins = sum(s["wins"] for s in yearly_results.values())
        win_rate = (total_wins / total_trades * 100) if total_trades else 0
        max_dd = max((s["max_dd_pct"] for s in yearly_results.values()), default=0)

        # Overall profit factor
        total_gains = sum(t["pnl_pct"] for t in all_trades if t["pnl_pct"] > 0)
        total_losses = sum(abs(t["pnl_pct"]) for t in all_trades if t["pnl_pct"] < 0)
        pf = total_gains / total_losses if total_losses else 0

        self.results_db.update_experiment_metrics(
            exp_id=exp_id,
            total_return_pct=total_return,
            annualized_return_pct=ann_return,
            total_trades=total_trades,
            win_rate_pct=win_rate,
            max_drawdown_pct=max_dd,
            profit_factor=pf,
        )
        self.results_db.refresh_backtest_read_snapshot()

    @staticmethod
    def _to_trades_df(all_trades: list[dict]) -> pl.DataFrame:
        if not all_trades:
            return pl.DataFrame(
                schema={
                    "year": pl.Int64,
                    "entry_date": pl.Date,
                    "exit_date": pl.Date,
                    "symbol": pl.Utf8,
                    "entry_price": pl.Float64,
                    "exit_price": pl.Float64,
                    "pnl_pct": pl.Float64,
                    "r_multiple": pl.Float64,
                    "exit_reason": pl.Utf8,
                    "holding_days": pl.Float64,
                    "gap_pct": pl.Float64,
                    "filters_passed": pl.Int64,
                }
            )
        trades_df = pl.DataFrame(all_trades)
        casts: list[pl.Expr] = []
        if "entry_date" in trades_df.columns:
            casts.append(pl.col("entry_date").cast(pl.Date, strict=False))
        if "exit_date" in trades_df.columns:
            casts.append(pl.col("exit_date").cast(pl.Date, strict=False))
        if casts:
            trades_df = trades_df.with_columns(casts)
        return trades_df.sort(["entry_date", "symbol"], nulls_last=True)

    @staticmethod
    def _to_yearly_df(yearly_results: dict[int, dict]) -> pl.DataFrame:
        rows: list[dict[str, float | int]] = []
        for year in sorted(yearly_results):
            stats = yearly_results[year]
            rows.append(
                {
                    "year": year,
                    "signals": int(stats.get("signals", 0)),
                    "trades": int(stats.get("trades", 0)),
                    "wins": int(stats.get("wins", 0)),
                    "losses": int(stats.get("losses", 0)),
                    "return_pct": float(stats.get("return_pct", 0.0)),
                    "win_rate_pct": float(stats.get("win_rate_pct", 0.0)),
                    "avg_r": float(stats.get("avg_r", 0.0)),
                    "max_dd_pct": float(stats.get("max_dd_pct", 0.0)),
                    "profit_factor": float(stats.get("profit_factor", 0.0)),
                    "avg_holding_days": float(stats.get("avg_holding_days", 0.0)),
                }
            )
        return pl.DataFrame(rows)

    @staticmethod
    def _to_equity_df(trades_df: pl.DataFrame) -> pl.DataFrame:
        if trades_df.is_empty() or "pnl_pct" not in trades_df.columns:
            return pl.DataFrame(
                schema={
                    "entry_date": pl.Date,
                    "symbol": pl.Utf8,
                    "pnl_pct": pl.Float64,
                    "cumulative_return_pct": pl.Float64,
                    "drawdown_pct": pl.Float64,
                }
            )

        equity = (
            trades_df.sort("entry_date", nulls_last=True)
            .with_columns(pl.col("pnl_pct").cast(pl.Float64, strict=False).fill_null(0.0))
            .with_columns(pl.col("pnl_pct").cum_sum().alias("cumulative_return_pct"))
            .with_columns(pl.col("cumulative_return_pct").cum_max().alias("running_peak_pct"))
            .with_columns(
                (pl.col("cumulative_return_pct") - pl.col("running_peak_pct")).alias("drawdown_pct")
            )
        )
        return equity.select(
            ["entry_date", "symbol", "pnl_pct", "cumulative_return_pct", "drawdown_pct"]
        )

    def _persist_postgres_lineage(
        self,
        *,
        exp_id: str,
        params: BacktestParams,
        strategy_name: str,
        strategy_hash: str,
        params_hash: str,
        dataset_hash: str,
        code_hash: str,
        yearly_results: dict[int, dict],
        all_trades: list[dict],
        started_at: datetime,
        finished_at: datetime,
        snapshot: bool,
    ) -> None:
        exp = self.results_db.get_experiment(exp_id) or {}
        metrics = {
            "total_return_pct": float(exp.get("total_return_pct") or 0.0),
            "annualized_return_pct": float(exp.get("annualized_return_pct") or 0.0),
            "total_trades": float(exp.get("total_trades") or 0.0),
            "win_rate_pct": float(exp.get("win_rate_pct") or 0.0),
            "max_drawdown_pct": float(exp.get("max_drawdown_pct") or 0.0),
            "profit_factor": float(exp.get("profit_factor") or 0.0),
        }

        publisher = BacktestArtifactPublisher()
        trades_df = self._to_trades_df(all_trades)
        yearly_df = self._to_yearly_df(yearly_results)
        equity_df = self._to_equity_df(trades_df)
        summary = {
            "exp_id": exp_id,
            "strategy_name": strategy_name,
            "params_hash": params_hash,
            "dataset_hash": dataset_hash,
            "code_hash": code_hash,
            "created_at": finished_at.isoformat(),
            "metrics": metrics,
        }
        artifacts = publisher.publish_run_artifacts(
            exp_id=exp_id,
            trades_df=trades_df,
            yearly_df=yearly_df,
            equity_df=equity_df,
            summary=summary,
        )
        if snapshot:
            snapshot_path = self._create_snapshot_copy(dataset_hash=dataset_hash, exp_id=exp_id)
            try:
                artifacts.append(
                    publisher.publish_duckdb_snapshot(
                        exp_id=exp_id,
                        dataset_hash=dataset_hash,
                        snapshot_path=snapshot_path,
                    )
                )
            finally:
                if snapshot_path.exists():
                    snapshot_path.unlink()

        upsert_exp_run_with_artifacts_sync(
            exp_hash=exp_id,
            strategy_name=strategy_name,
            strategy_hash=strategy_hash,
            dataset_hash=dataset_hash,
            params_json=json.dumps(asdict(params), sort_keys=True),
            code_sha=code_hash,
            status="SUCCEEDED",
            started_at=started_at,
            finished_at=finished_at,
            metrics=metrics,
            artifacts=artifacts,
            progress_stage="completed",
            progress_message="Backtest run completed",
            progress_pct=100.0,
            heartbeat_at=finished_at,
        )

    @staticmethod
    def _validate_required_lineage_dependencies() -> None:
        """Fail fast if Postgres/MinIO persistence dependencies are unavailable."""
        try:
            settings = get_settings()
        except Exception as exc:
            raise RuntimeError(
                "Backtest requires Doppler-injected DATABASE_URL and MinIO credentials."
            ) from exc
        if settings.database_url is None:
            raise RuntimeError(
                "DATABASE_URL is required. Run via Doppler so Postgres lineage can be persisted."
            )
        if (
            settings.minio_endpoint is None
            or not settings.minio_access_key
            or not settings.minio_secret_key
        ):
            raise RuntimeError(
                "MinIO settings are required. Ensure MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY are set."
            )

        try:
            with psycopg.connect(str(settings.database_url)) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        except Exception as exc:
            raise RuntimeError(
                "Postgres is unreachable. Start infrastructure and run with Doppler-injected secrets."
            ) from exc

        try:
            BacktestArtifactPublisher()
        except Exception as exc:
            raise RuntimeError(
                "MinIO artifacts store is unreachable. Start MinIO and verify credentials."
            ) from exc

    def _create_snapshot_copy(self, *, dataset_hash: str, exp_id: str) -> Path:
        snapshot_dir = self.results_db.db_path.parent / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"{exp_id}_{dataset_hash}_{uuid4().hex[:8]}.duckdb"

        db_list = self.results_db.con.execute("PRAGMA database_list").fetchall()
        source_catalog = db_list[0][1]
        escaped_path = str(snapshot_path).replace("\\", "/").replace("'", "''")

        self.results_db.con.execute(f"ATTACH '{escaped_path}' AS snapshot_db")
        try:
            self.results_db.con.execute(f"COPY FROM DATABASE {source_catalog} TO snapshot_db")
        finally:
            self.results_db.con.execute("DETACH snapshot_db")

        return snapshot_path

    def _print_summary(self, exp: dict | None) -> None:
        if not exp:
            return

        logger.info("=" * 60)
        logger.info("BACKTEST COMPLETE")
        logger.info("=" * 60)
        logger.info("  Experiment ID : %s", exp["exp_id"])
        logger.info("  Total Return  : %.2f%%", exp["total_return_pct"])
        logger.info("  Annualized    : %.2f%%", exp["annualized_return_pct"])
        logger.info("  Total Trades  : %d", exp["total_trades"])
        logger.info("  Win Rate      : %.1f%%", exp["win_rate_pct"])
        logger.info("  Max Drawdown  : %.1f%%", exp["max_drawdown_pct"])
        logger.info("  Profit Factor : %.2f", exp["profit_factor"])
        logger.info("View results in the dashboard:")
        logger.info("  doppler run -- uv run nseml-dashboard")
