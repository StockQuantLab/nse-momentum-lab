"""DuckDB-native backtest runner for the Indian 2LYNCH strategy.

Extracts the proven logic from ``scripts/backtest_10year_fixed.py`` into a
reusable service that persists results in DuckDB (bt_experiment / bt_trade /
bt_yearly_metric tables).

Usage::

    from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
        BacktestParams, DuckDBBacktestRunner,
    )

    runner = DuckDBBacktestRunner()
    exp_id = runner.run(BacktestParams())
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
import polars as pl
import psycopg
from tqdm.auto import tqdm

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.market_db import MarketDataDB, get_market_db
from nse_momentum_lab.services.backtest.engine import ExitReason
from nse_momentum_lab.services.backtest.persistence import (
    BacktestArtifactPublisher,
    build_strategy_hash,
    upsert_exp_run_with_artifacts_sync,
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

logger = logging.getLogger(__name__)


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
    follow_through_threshold: float = 0.0

    # FEE (Find and Enter Early) — Stockbee: enter in first N min of NSE open (09:15 IST)
    # 60min is the optimal window for NSE (vs 30min for US) due to pre-open auction.
    # Backtests (2015-2025, 1,776 stocks): 60min gives Calmar 27.06 vs 19.74 at 30min.
    entry_cutoff_minutes: int = 60  # 09:15 + 60 min = 10:15 cutoff

    # Maximum allowed distance from entry to stop.
    # Belt-and-suspenders guard: if stop is >8% below entry even within the time window,
    # the setup is invalid (e.g. stock crashed then bounced — FEE was still violated).
    max_stop_dist_pct: float = 0.08

    def to_hash(self) -> str:
        """Deterministic SHA-256 of all parameters (for dedup)."""
        blob = json.dumps(asdict(self), sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def to_vbt_config(self) -> VectorBTConfig:
        return VectorBTConfig(
            risk_per_trade_pct=self.risk_per_trade_pct,
            default_portfolio_value=self.portfolio_value,
            fees_per_trade=self.fees_per_trade,
            trail_activation_pct=self.trail_activation_pct,
            trail_stop_pct=self.trail_stop_pct,
            min_hold_days=self.min_hold_days,
            time_stop_days=self.time_stop_days,
            abnormal_profit_pct=self.abnormal_profit_pct,
            abnormal_gap_exit_pct=self.abnormal_gap_exit_pct,
            follow_through_threshold=self.follow_through_threshold,
        )


class DuckDBBacktestRunner:
    """Orchestrates an end-to-end 2LYNCH backtest and stores results in DuckDB."""

    DATASET_KIND = "duckdb_market_daily"
    RUN_LOGIC_VERSION = "duckdb_backtest_runner_v2026_03_01_guard_1pt5x_vbt_conflict"

    def __init__(self, db: MarketDataDB | None = None) -> None:
        self.db = db or get_market_db()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def build_experiment_id(params_hash: str, dataset_hash: str, code_hash: str = "default") -> str:
        """Stable experiment ID derived from params + dataset fingerprints."""
        blob = f"{params_hash}:{dataset_hash}:{code_hash}"
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

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
        self._validate_required_lineage_dependencies()
        params_hash = params.to_hash()
        code_hash = build_code_hash(
            "duckdb_backtest_runner",
            {
                "strategy": "Indian2LYNCH",
                "run_logic_version": self.RUN_LOGIC_VERSION,
            },
        )
        dataset_snapshot = self.db.get_dataset_snapshot()
        dataset_hash = str(dataset_snapshot["dataset_hash"])
        exp_id = self.build_experiment_id(params_hash, dataset_hash, code_hash)
        strategy_hash = build_strategy_hash("Indian2LYNCH", params_hash)
        params_json = json.dumps(asdict(params), sort_keys=True)

        if progress_file is None:
            progress_dir = self.db.db_path.parent / "progress"
            progress_file = progress_dir / f"{exp_id}.ndjson"

        existing_exp = self.db.get_experiment(exp_id) if self.db.experiment_exists(exp_id) else None
        if not force and existing_exp is not None:
            existing_status = str(existing_exp.get("status") or "").lower().strip()
            if existing_status == "completed":
                logger.info("[SKIP] Experiment %s already exists. Use --force to re-run.", exp_id)
                return exp_id
            logger.info(
                "[CLEANUP] Removing stale experiment %s with status='%s'.", exp_id, existing_status
            )
            self.db.delete_experiment(exp_id)

        # Delete stale data if forcing a re-run
        if force and self.db.experiment_exists(exp_id):
            self.db.delete_experiment(exp_id)

        logger.info("[START] Experiment %s", exp_id)
        logger.info("  Params hash  : %s", params_hash)
        logger.info("  Dataset hash : %s", dataset_hash)
        self.db.register_dataset_snapshot(dataset_snapshot)
        manifest_payload = build_manifest_payload_from_snapshot(
            dataset_kind=self.DATASET_KIND,
            snapshot=dataset_snapshot,
            code_hash=code_hash,
            params_hash=params_hash,
        )
        upsert_dataset_manifest_sync(manifest_payload)
        self._emit_progress(
            exp_id=exp_id,
            strategy_hash=strategy_hash,
            dataset_hash=dataset_hash,
            params_json=params_json,
            code_hash=code_hash,
            started_at=started_at,
            status="RUNNING",
            stage="starting",
            progress_pct=0.0,
            message="Initializing backtest run",
            progress_file=progress_file,
        )

        try:
            self.db.save_experiment(
                exp_id=exp_id,
                strategy_name="Indian2LYNCH",
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
                exp_id=exp_id,
                strategy_hash=strategy_hash,
                dataset_hash=dataset_hash,
                params_json=params_json,
                code_hash=code_hash,
                started_at=started_at,
                status="RUNNING",
                stage="materializing_features",
                progress_pct=5.0,
                message="Building/validating feat_daily",
                progress_file=progress_file,
            )

            # Ensure features are built
            self.db.build_feat_daily_table()

            # Get universe
            symbols = self._get_liquid_symbols(params)
            logger.info("Universe: %d symbols", len(symbols))
            effective_start_year, effective_end_year = self._effective_year_range(params)
            total_years = max(effective_end_year - effective_start_year + 1, 1)
            self._emit_progress(
                exp_id=exp_id,
                strategy_hash=strategy_hash,
                dataset_hash=dataset_hash,
                params_json=params_json,
                code_hash=code_hash,
                started_at=started_at,
                status="RUNNING",
                stage="running_years",
                progress_pct=10.0,
                message=(
                    f"Universe ready ({len(symbols)} symbols). "
                    f"Running {effective_start_year}-{effective_end_year}."
                ),
                progress_file=progress_file,
            )

            def on_year_start(year: int, completed_years: int, years_total: int) -> None:
                pct = min(90.0, 10.0 + (completed_years / max(years_total, 1)) * 80.0)
                self._emit_progress(
                    exp_id=exp_id,
                    strategy_hash=strategy_hash,
                    dataset_hash=dataset_hash,
                    params_json=params_json,
                    code_hash=code_hash,
                    started_at=started_at,
                    status="RUNNING",
                    stage="running_year",
                    progress_pct=pct,
                    message=(
                        f"Year {year} started ({completed_years}/{years_total} years completed)"
                    ),
                    progress_file=progress_file,
                )

            def on_year_complete(
                year: int,
                completed_years: int,
                years_total: int,
                stats: dict,
            ) -> None:
                pct = min(92.0, 10.0 + (completed_years / max(years_total, 1)) * 82.0)
                self._emit_progress(
                    exp_id=exp_id,
                    strategy_hash=strategy_hash,
                    dataset_hash=dataset_hash,
                    params_json=params_json,
                    code_hash=code_hash,
                    started_at=started_at,
                    status="RUNNING",
                    stage="year_complete",
                    progress_pct=pct,
                    message=(
                        f"Year {year} complete: trades={int(stats.get('trades', 0))}, "
                        f"return={float(stats.get('return_pct', 0.0)):+.2f}% "
                        f"({completed_years}/{years_total})"
                    ),
                    progress_file=progress_file,
                )

            def on_year_heartbeat(
                year: int,
                completed_years: int,
                years_total: int,
                message: str,
            ) -> None:
                pct = min(89.0, 10.0 + ((completed_years + 0.5) / max(years_total, 1)) * 80.0)
                self._emit_progress(
                    exp_id=exp_id,
                    strategy_hash=strategy_hash,
                    dataset_hash=dataset_hash,
                    params_json=params_json,
                    code_hash=code_hash,
                    started_at=started_at,
                    status="RUNNING",
                    stage="running_year",
                    progress_pct=pct,
                    message=f"Year {year}: {message}",
                    progress_file=progress_file,
                )

            yearly_results, all_trades = self._run_year_by_year(
                params,
                symbols,
                on_year_start=on_year_start,
                on_year_complete=on_year_complete,
                on_year_heartbeat=on_year_heartbeat,
            )
            self._emit_progress(
                exp_id=exp_id,
                strategy_hash=strategy_hash,
                dataset_hash=dataset_hash,
                params_json=params_json,
                code_hash=code_hash,
                started_at=started_at,
                status="RUNNING",
                stage="persisting_results",
                progress_pct=94.0,
                message=(
                    f"Persisting {len(all_trades)} trades and {total_years} yearly metric rows"
                ),
                progress_file=progress_file,
            )
            self._persist_results(exp_id, params, yearly_results, all_trades)
            self._emit_progress(
                exp_id=exp_id,
                strategy_hash=strategy_hash,
                dataset_hash=dataset_hash,
                params_json=params_json,
                code_hash=code_hash,
                started_at=started_at,
                status="RUNNING",
                stage="publishing_artifacts",
                progress_pct=97.0,
                message="Publishing run artifacts to MinIO and Postgres",
                progress_file=progress_file,
            )

            finished_at = datetime.now(UTC)
            self._persist_postgres_lineage(
                exp_id=exp_id,
                params=params,
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
                exp_id=exp_id,
                strategy_hash=strategy_hash,
                dataset_hash=dataset_hash,
                params_json=params_json,
                code_hash=code_hash,
                started_at=started_at,
                status="FAILED",
                stage="failed",
                progress_pct=None,
                message=f"Backtest failed: {exc}",
                progress_file=progress_file,
                finished_at=finished_at,
            )
            if self.db.experiment_exists(exp_id):
                self.db.delete_experiment(exp_id)
            raise

        exp = self.db.get_experiment(exp_id)
        self._print_summary(exp)

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

    def _emit_progress(
        self,
        *,
        exp_id: str,
        strategy_hash: str,
        dataset_hash: str,
        params_json: str,
        code_hash: str,
        started_at: datetime,
        status: str,
        stage: str,
        message: str,
        progress_pct: float | None,
        progress_file: Path | None,
        finished_at: datetime | None = None,
    ) -> None:
        pct_label = "--.-%" if progress_pct is None else f"{progress_pct:5.1f}%"
        logger.info("[PROGRESS] %s [%s] %s", pct_label, stage, message)

        heartbeat_at = datetime.now(UTC)
        if progress_file is not None:
            progress_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "timestamp": heartbeat_at.isoformat(),
                "exp_id": exp_id,
                "status": status,
                "stage": stage,
                "progress_pct": progress_pct,
                "message": message,
            }
            with progress_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, sort_keys=True) + "\n")

        upsert_exp_run_with_artifacts_sync(
            exp_hash=exp_id,
            strategy_name="Indian2LYNCH",
            strategy_hash=strategy_hash,
            dataset_hash=dataset_hash,
            params_json=params_json,
            code_sha=code_hash,
            status=status,
            started_at=started_at,
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
        return result["symbol"].to_list()

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
    ) -> tuple[dict[int, dict], list[dict]]:
        """Run backtest year by year using parameterized queries."""
        yearly_results: dict[int, dict] = {}
        all_trades: list[dict] = []
        window_start = self._parse_optional_iso_date(params.start_date, "start_date")
        window_end = self._parse_optional_iso_date(params.end_date, "end_date")
        effective_start_year, effective_end_year = self._effective_year_range(params)

        years_total = effective_end_year - effective_start_year + 1

        for idx, year in enumerate(range(effective_start_year, effective_end_year + 1), start=1):
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

            stats, trades = self._run_single_year(
                params,
                symbols,
                year,
                year_window_start,
                year_window_end,
                heartbeat_cb=year_heartbeat_cb,
            )
            yearly_results[year] = stats
            all_trades.extend(trades)

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

        return yearly_results, all_trades

    def _run_single_year(
        self,
        params: BacktestParams,
        symbols: list[str],
        year: int,
        year_window_start: date,
        year_window_end: date,
        heartbeat_cb: Callable[[str], None] | None = None,
    ) -> tuple[dict, list[dict]]:
        """Run backtest for a single year, return (stats_dict, trades_list)."""
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
        }

        # Signal generation SQL (proven logic from backtest_10year_fixed.py)
        # Uses parameterized query to prevent SQL injection
        symbols_placeholders = ",".join("?" for _ in symbols)
        query = f"""
        WITH numbered_daily AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
            FROM v_daily
        WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND symbol IN ({symbols_placeholders})
        ),
        with_lag AS (
            SELECT
                symbol, date AS trading_date, open, high, low, close, volume,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                LAG(high) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
                LAG(low) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
                LAG(open) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
                (LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag2,
                (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
                close * volume AS value_traded_inr
            FROM numbered_daily WHERE rn > 1
        ),
        breakout_days AS (
            SELECT * FROM with_lag
            WHERE ((high - prev_close) / NULLIF(prev_close, 0)) >= {params.breakout_threshold}
              AND prev_close IS NOT NULL
              AND close >= {params.min_price}
              AND value_traded_inr >= {params.min_value_traded_inr}
              AND volume >= {params.min_volume}
              AND ret_1d_lag1 IS NOT NULL
        ),
        with_features AS (
            SELECT g.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.atr_compress_ratio, f.range_percentile,
                f.prior_breakouts_30d, f.prior_breakouts_90d, f.r2_65,
                f.ma_7, f.ma_65_sma
            FROM breakout_days g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
        )
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_low, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close > ma_20) AS above_ma20,
            (ret_5d > 0) AS positive_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile,
            prior_breakouts_90d,
            (close_pos_in_range >= 0.70) AS filter_h,
            ((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open) AS filter_n,
            (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
            (vol_dryup_ratio < 1.3) AS filter_c,
            (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER)
             + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
            (ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0) AS filter_2
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
        ORDER BY trading_date, symbol
        """

        # Execute with parameterized values (dates first, then symbols)
        df = self.db.con.execute(
            query,
            [year_window_start.isoformat(), year_window_end.isoformat(), *symbols],
        ).fetchdf()
        if df.empty:
            return empty_stats, []

        df_pl = pl.from_pandas(df)

        # Apply filters
        for f in ["filter_h", "filter_n", "filter_2", "filter_y", "filter_c", "filter_l"]:
            df_pl = df_pl.with_columns(pl.col(f).fill_null(False))

        df_pl = df_pl.with_columns(
            (
                pl.col("filter_h").cast(int)
                + pl.col("filter_n").cast(int)
                + pl.col("filter_2").cast(int)
                + pl.col("filter_y").cast(int)
                + pl.col("filter_c").cast(int)
                + pl.col("filter_l").cast(int)
            ).alias("filters_passed")
        )

        df_filtered = df_pl.filter(pl.col("filters_passed") >= params.min_filters)

        if df_filtered.height == 0:
            return {**empty_stats, "signals": len(df)}, []

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
            loaded_symbols: set[str] = set()
            with tqdm(
                total=daily_df.height, desc="loading daily price data", unit="rows"
            ) as price_bar:
                for row in daily_df.iter_rows(named=True):
                    symbol = row["symbol"]
                    symbol_id = symbol_id_by_symbol.get(symbol)
                    if symbol_id is None:
                        price_bar.update(1)
                        continue
                    dt = row["date"]
                    if isinstance(dt, datetime):
                        dt = dt.date()
                    price_data.setdefault(symbol_id, {})[dt] = {
                        "open_adj": float(row["open"]),
                        "close_adj": float(row["close"]),
                        "high_adj": float(row["high"]),
                        "low_adj": float(row["low"]),
                    }
                    if symbol not in loaded_symbols:
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
        signal_context: dict[tuple[int, date], dict[str, float | int | None]] = {}
        intraday_entry_by_signal: dict[tuple[str, date], dict[str, float | bool]] = {}
        if params.entry_timeframe.lower() == "5min":
            intraday_entry_by_signal = self._resolve_intraday_entries_bulk(
                df_filtered=df_filtered,
                breakout_threshold=params.breakout_threshold,
                entry_cutoff_minutes=params.entry_cutoff_minutes,
                heartbeat_cb=maybe_heartbeat,
            )
        with tqdm(
            total=df_filtered.height, desc="assembling VectorBT signals", unit="signals"
        ) as signal_bar:
            for row in df_filtered.iter_rows(named=True):
                maybe_heartbeat(
                    f"assembling VectorBT signals ({len(vbt_signals)}/{df_filtered.height})"
                )
                symbol = row["symbol"]
                symbol_id = symbol_id_by_symbol[symbol]
                if symbol_id not in price_data:
                    signal_bar.update(1)
                    continue
                sig_date = row["trading_date"]
                if isinstance(sig_date, datetime):
                    sig_date = sig_date.date()

                intraday_entry = None
                if params.entry_timeframe.lower() == "5min":
                    intraday_entry = intraday_entry_by_signal.get((symbol, sig_date))
                    if intraday_entry is None:
                        skipped_intraday_entry += 1
                        continue

                # Legacy daily execution fallback.
                if intraday_entry is None:
                    entry_price = float(row["open"]) if row["open"] is not None else None
                    initial_stop = row["low"] if row["low"] is not None else row["prev_low"]
                    same_day_stop_hit = False
                else:
                    entry_price = float(intraday_entry["entry_price"])
                    initial_stop = float(intraday_entry["initial_stop"])
                    same_day_stop_hit = bool(intraday_entry["same_day_stop_hit"])

                    if entry_price is None or initial_stop is None:
                        skipped_intraday_entry += 1
                        signal_bar.update(1)
                        continue

                    # Max stop distance guard: Stockbee FEE implies a tight stop.
                    # If stop is >max_stop_dist_pct below entry, the setup is invalid
                    # (e.g. crash-then-bounce where morning low is far from late entry).
                    if entry_price > 0 and initial_stop < entry_price * (
                        1 - params.max_stop_dist_pct
                    ):
                        skipped_intraday_entry += 1
                        signal_bar.update(1)
                        continue
                signal_context[(symbol_id, sig_date)] = {
                    "gap_pct": row["gap_pct"],
                    "filters_passed": int(row["filters_passed"])
                    if row["filters_passed"] is not None
                    else None,
                    "entry_time": intraday_entry.get("entry_time") if intraday_entry else None,
                }
                vbt_signals.append(
                    (
                        sig_date,
                        symbol_id,
                        symbol,
                        initial_stop,
                        {
                            "gap_pct": row["gap_pct"],
                            "atr": row["atr_20"] if row["atr_20"] else 0.0,
                            "filters_passed": signal_context[(symbol_id, sig_date)][
                                "filters_passed"
                            ],
                            "entry_price": entry_price,
                            "same_day_stop_hit": same_day_stop_hit,
                        },
                    )
                )

        if not vbt_signals:
            return {
                **empty_stats,
                "signals": len(df),
                "filtered_signals": len(df_filtered),
                "skipped_intraday_entry": skipped_intraday_entry,
            }, []

        # Run VectorBT engine
        engine = VectorBTEngine(config=params.to_vbt_config())
        result = engine.run_backtest(
            strategy_name=f"Indian2LYNCH_{year}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
        )

        # Gather exit reasons
        exit_reasons: dict[str, int] = {}
        for t in result.trades:
            if t.exit_reason:
                r = t.exit_reason.value
                exit_reasons[r] = exit_reasons.get(r, 0) + 1

        wins = sum(1 for t in result.trades if t.pnl and t.pnl > 0)
        losses = sum(1 for t in result.trades if t.pnl and t.pnl < 0)

        # Calculate holding days and profit factor
        holding_days = []
        total_gains = 0.0
        total_losses_val = 0.0
        for t in result.trades:
            if t.exit_date and t.entry_date:
                holding_days.append((t.exit_date - t.entry_date).days)
            if t.entry_price and t.exit_price and t.entry_price > 0:
                pct = ((t.exit_price - t.entry_price) / t.entry_price) * 100
                if pct > 0:
                    total_gains += pct
                else:
                    total_losses_val += abs(pct)

        profit_factor = total_gains / total_losses_val if total_losses_val > 0 else 0

        stats = {
            "year": year,
            "signals": len(df_filtered),
            "trades": len(result.trades),
            "wins": wins,
            "losses": losses,
            "return_pct": result.total_return * 100,
            "win_rate_pct": result.win_rate * 100,
            "avg_r": result.avg_r,
            "max_dd_pct": result.max_drawdown * 100,
            "profit_factor": profit_factor,
            "avg_holding_days": float(np.mean(holding_days)) if holding_days else 0,
            "exit_reasons": exit_reasons,
            "skipped_intraday_entry": skipped_intraday_entry,
        }

        # Build trade dicts
        trades_out: list[dict] = []
        for t in result.trades:
            hd = (t.exit_date - t.entry_date).days if t.exit_date and t.entry_date else 0
            pct = 0.0
            if t.entry_price and t.exit_price and t.entry_price > 0:
                pct = ((t.exit_price - t.entry_price) / t.entry_price) * 100

            real_symbol = id_to_symbol.get(t.symbol_id, t.symbol)

            # Find gap_pct and filters_passed for this trade
            context = signal_context.get((t.symbol_id, t.entry_date), {})
            gap_pct = context.get("gap_pct")
            filters_passed = context.get("filters_passed")

            # Infer exit time from exit reason.
            # Gap-based exits occur at market open (09:15 IST).
            # Close-based exits occur at market close (15:30 IST).
            # Intraday stop exits have unknown timing — stored as NULL.
            if t.exit_reason in (ExitReason.ABNORMAL_GAP_EXIT, ExitReason.GAP_THROUGH_STOP):
                exit_time = time(9, 15)
            elif t.exit_reason in (
                ExitReason.TIME_STOP,
                ExitReason.ABNORMAL_PROFIT,
                ExitReason.EXIT_EOD,
                ExitReason.DELISTING,
            ):
                exit_time = time(15, 30)
            else:
                exit_time = None  # STOP_INITIAL / STOP_TRAIL etc. — intraday time unknown

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
                    "entry_time": context.get("entry_time"),
                    "exit_time": exit_time,
                }
            )

        return stats, trades_out

    @staticmethod
    def _minutes_from_nse_open(candle_time: object) -> int | None:
        """Return minutes elapsed since NSE market open (09:15 IST) for a candle_time value.

        Handles datetime.time, datetime.datetime, and integer microseconds (DuckDB TIME64).
        Returns None if the format is unrecognised.
        """
        import datetime as _dt

        if isinstance(candle_time, _dt.datetime):
            return (candle_time.hour - 9) * 60 + (candle_time.minute - 15)
        if isinstance(candle_time, _dt.time):
            return (candle_time.hour - 9) * 60 + (candle_time.minute - 15)
        if isinstance(candle_time, (int, float)):
            # DuckDB stores TIME as microseconds since midnight
            total_minutes = int(candle_time) // 60_000_000
            return total_minutes - (9 * 60 + 15)
        return None

    @staticmethod
    def _normalize_candle_time(candle_time: object) -> time | None:
        """Convert a raw DuckDB candle_time value to a datetime.time object.

        DuckDB TIME columns are returned as microseconds-since-midnight (int)
        when fetched via the Python API. This helper normalises all three
        possible types: datetime, time, and int/float microseconds.
        """
        import datetime as _dt

        if isinstance(candle_time, _dt.datetime):
            return candle_time.time()
        if isinstance(candle_time, _dt.time):
            return candle_time
        if isinstance(candle_time, (int, float)):
            total_seconds = int(candle_time) // 1_000_000
            h, remainder = divmod(total_seconds, 3600)
            m, s = divmod(remainder, 60)
            try:
                return _dt.time(h, m, s)
            except ValueError:
                return None
        return None

    @staticmethod
    def _resolve_intraday_entry_from_5min(
        candles: pl.DataFrame,
        breakout_price: float,
        entry_cutoff_minutes: int = 30,
    ) -> dict[str, float | bool] | None:
        """Resolve first intraday breakout touch and stop level known at entry time.

        FEE (Find and Enter Early) — Stockbee: enter in the first 30 minutes of
        NSE open (09:15 IST cutoff 09:45). Candles after the cutoff are ignored.
        Stop = low of the day up to the entry bar (naturally tight on early entries).
        """
        if candles.is_empty():
            return None

        rows = list(candles.sort("candle_time").iter_rows(named=True))
        day_low_before = float("inf")

        for idx, row in enumerate(rows):
            # FEE cutoff: reject candles that arrive after the entry window.
            candle_time = row.get("candle_time")
            if candle_time is not None:
                mins = DuckDBBacktestRunner._minutes_from_nse_open(candle_time)
                if mins is not None and mins > entry_cutoff_minutes:
                    break  # All subsequent candles are also past cutoff (sorted)

            open_px = float(row["open"])
            high_px = float(row["high"])
            low_px = float(row["low"])

            known_low_at_bar_open = min(day_low_before, open_px)
            if high_px >= breakout_price:
                entry_price = open_px if open_px >= breakout_price else breakout_price

                # Sanity check: entry must not be wildly above the daily-derived breakout
                # price. Two data quality issues this catches:
                #   1. Wrong instrument in 5-min Parquet (GABRIEL, GHCL): 5-100x ratio
                #   2. Unadjusted 5-min vs adjusted daily (RELAXO bonus 2015): ~2x ratio
                # NSE max circuit = 20%, so max legitimate ratio = (1.20/1.04) ≈ 1.15x.
                # Threshold of 1.5x gives generous headroom while catching both bug types.
                if entry_price > breakout_price * 1.5:
                    return None

                initial_stop = known_low_at_bar_open

                # Same-day stop hit check only on subsequent completed candles.
                same_day_stop_hit = False
                for follow_row in rows[idx + 1 :]:
                    if float(follow_row["low"]) <= initial_stop:
                        same_day_stop_hit = True
                        break

                return {
                    "entry_price": entry_price,
                    "initial_stop": initial_stop,
                    "same_day_stop_hit": same_day_stop_hit,
                    "entry_time": DuckDBBacktestRunner._normalize_candle_time(candle_time),
                }

            day_low_before = min(day_low_before, low_px)

        return None

    def _resolve_intraday_entries_bulk(
        self,
        *,
        df_filtered: pl.DataFrame,
        breakout_threshold: float,
        entry_cutoff_minutes: int = 30,
        heartbeat_cb: Callable[[str], None] | None = None,
    ) -> dict[tuple[str, date], dict[str, float | bool]]:
        """Resolve intraday entries for all signal days with one 5-min batch query."""
        targets = (
            df_filtered.select(["symbol", "trading_date", "prev_close"])
            .drop_nulls(["symbol", "trading_date", "prev_close"])
            .unique(subset=["symbol", "trading_date"], keep="first", maintain_order=True)
        )
        if targets.is_empty():
            return {}

        targets_pd = targets.to_pandas()
        targets_pd["trading_date"] = pd.to_datetime(
            targets_pd["trading_date"], errors="coerce"
        ).dt.date
        targets_pd = targets_pd.dropna(subset=["trading_date"])
        if targets_pd.empty:
            return {}

        breakout_price_by_key: dict[tuple[str, date], float] = {}
        for row in targets_pd.itertuples(index=False):
            prev_close = float(row.prev_close)
            if prev_close <= 0:
                continue
            breakout_price_by_key[(str(row.symbol), row.trading_date)] = prev_close * (
                1 + breakout_threshold
            )
        if not breakout_price_by_key:
            return {}

        join_df = pd.DataFrame(
            {
                "symbol": [key[0] for key in breakout_price_by_key],
                "trading_date": [key[1].isoformat() for key in breakout_price_by_key],
            }
        )
        tmp_name = "tmp_intraday_signal_days"
        self.db.con.register(tmp_name, join_df)
        try:
            candles = self.db.con.execute(
                f"""
                SELECT c.symbol, c.date AS trading_date, c.candle_time, c.open, c.high, c.low
                FROM v_5min c
                INNER JOIN {tmp_name} t
                  ON c.symbol = t.symbol
                 AND c.date = CAST(t.trading_date AS DATE)
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

        resolved_entries: dict[tuple[str, date], dict[str, float | bool]] = {}
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
                trading_day: date | None
                if isinstance(trading_day_raw, datetime):
                    trading_day = trading_day_raw.date()
                elif isinstance(trading_day_raw, date):
                    trading_day = trading_day_raw
                else:
                    try:
                        trading_day = date.fromisoformat(str(trading_day_raw))
                    except ValueError:
                        entry_bar.update(1)
                        continue

                breakout_price = breakout_price_by_key.get((symbol, trading_day))
                if breakout_price is None:
                    entry_bar.update(1)
                    continue

                intraday_entry = self._resolve_intraday_entry_from_5min(
                    group_candles, breakout_price, entry_cutoff_minutes
                )
                if intraday_entry is not None:
                    resolved_entries[(symbol, trading_day)] = intraday_entry
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
    ) -> dict[str, float | bool] | None:
        if prev_close is None or prev_close <= 0:
            return None

        breakout_price = prev_close * (1 + breakout_threshold)
        candles = self.db.query_5min(
            symbol=symbol,
            start_date=trading_date.isoformat(),
            end_date=trading_date.isoformat(),
            columns=["candle_time", "open", "high", "low", "close", "volume"],
        )
        return self._resolve_intraday_entry_from_5min(candles, breakout_price, entry_cutoff_minutes)

    def _persist_results(
        self,
        exp_id: str,
        params: BacktestParams,
        yearly_results: dict[int, dict],
        all_trades: list[dict],
    ) -> None:
        """Write results to DuckDB tables."""
        # Save trades
        self.db.save_trades(exp_id, all_trades)

        # Save yearly metrics
        for _year, stats in yearly_results.items():
            self.db.save_yearly_metric(exp_id, stats)

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

        self.db.update_experiment_metrics(
            exp_id=exp_id,
            total_return_pct=total_return,
            annualized_return_pct=ann_return,
            total_trades=total_trades,
            win_rate_pct=win_rate,
            max_drawdown_pct=max_dd,
            profit_factor=pf,
        )

    @staticmethod
    def _to_trades_df(all_trades: list[dict]) -> pd.DataFrame:
        if not all_trades:
            return pd.DataFrame(
                columns=[
                    "year",
                    "entry_date",
                    "exit_date",
                    "symbol",
                    "entry_price",
                    "exit_price",
                    "pnl_pct",
                    "r_multiple",
                    "exit_reason",
                    "holding_days",
                    "gap_pct",
                    "filters_passed",
                ]
            )
        trades_df = pd.DataFrame(all_trades)
        if "entry_date" in trades_df.columns:
            trades_df["entry_date"] = pd.to_datetime(trades_df["entry_date"], errors="coerce")
        if "exit_date" in trades_df.columns:
            trades_df["exit_date"] = pd.to_datetime(trades_df["exit_date"], errors="coerce")
        return trades_df.sort_values(["entry_date", "symbol"], na_position="last")

    @staticmethod
    def _to_yearly_df(yearly_results: dict[int, dict]) -> pd.DataFrame:
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
        return pd.DataFrame(rows)

    @staticmethod
    def _to_equity_df(trades_df: pd.DataFrame) -> pd.DataFrame:
        if trades_df.empty or "pnl_pct" not in trades_df.columns:
            return pd.DataFrame(
                columns=[
                    "entry_date",
                    "symbol",
                    "pnl_pct",
                    "cumulative_return_pct",
                    "drawdown_pct",
                ]
            )

        equity = trades_df.copy()
        equity["pnl_pct"] = pd.to_numeric(equity["pnl_pct"], errors="coerce").fillna(0.0)
        equity = equity.sort_values("entry_date", na_position="last")
        equity["cumulative_return_pct"] = equity["pnl_pct"].cumsum()
        equity["running_peak_pct"] = equity["cumulative_return_pct"].cummax()
        equity["drawdown_pct"] = equity["cumulative_return_pct"] - equity["running_peak_pct"]
        return equity[
            ["entry_date", "symbol", "pnl_pct", "cumulative_return_pct", "drawdown_pct"]
        ].copy()

    def _persist_postgres_lineage(
        self,
        *,
        exp_id: str,
        params: BacktestParams,
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
        exp = self.db.get_experiment(exp_id) or {}
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
            "strategy_name": "Indian2LYNCH",
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
            strategy_name="Indian2LYNCH",
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
        snapshot_dir = self.db.db_path.parent / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"{exp_id}_{dataset_hash}_{uuid4().hex[:8]}.duckdb"

        db_list = self.db.con.execute("PRAGMA database_list").fetchall()
        source_catalog = db_list[0][1]
        escaped_path = str(snapshot_path).replace("\\", "/").replace("'", "''")

        self.db.con.execute(f"ATTACH '{escaped_path}' AS snapshot_db")
        try:
            self.db.con.execute(f"COPY FROM DATABASE {source_catalog} TO snapshot_db")
        finally:
            self.db.con.execute("DETACH snapshot_db")

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
