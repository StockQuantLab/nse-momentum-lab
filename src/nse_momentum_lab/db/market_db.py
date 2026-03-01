"""
DuckDB market data layer for NSE Momentum Lab.

DuckDB handles all market data analytics:
  - 5-min OHLCV candles queried directly from Parquet
  - Daily OHLCV queried directly from Parquet
  - feat_daily as a materialized table with idempotent rebuild checks
  - Backtest result storage in the DuckDB catalog

PostgreSQL remains the source of truth for operational metadata and APIs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
DUCKDB_FILE = DATA_DIR / "market.duckdb"

# Bump when feat_daily SQL logic changes.
FEAT_DAILY_QUERY_VERSION = "feat_daily_v2lynch_ti65_2026_03_01"


@dataclass(frozen=True)
class DataLakeConfig:
    """Runtime data-lake contract for DuckDB Parquet reads."""

    mode: str
    local_parquet_dir: Path
    bucket: str
    daily_prefix: str
    five_min_prefix: str
    endpoint: str | None
    access_key: str | None
    secret_key: str | None
    secure: bool

    @classmethod
    def from_env(cls) -> DataLakeConfig:
        mode = os.getenv("DATA_LAKE_MODE", "local").strip().lower()
        if mode not in {"local", "minio"}:
            raise ValueError("DATA_LAKE_MODE must be either 'local' or 'minio'")
        local_parquet_dir = Path(os.getenv("DATA_LAKE_LOCAL_DIR", str(PARQUET_DIR)))
        bucket = os.getenv("DATA_LAKE_BUCKET", "market-data").strip()
        daily_prefix = os.getenv("DATA_LAKE_DAILY_PREFIX", "parquet/daily").strip("/")
        five_min_prefix = os.getenv("DATA_LAKE_5MIN_PREFIX", "parquet/5min").strip("/")

        endpoint = os.getenv("MINIO_ENDPOINT")
        secure_env = os.getenv("MINIO_SECURE", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if endpoint:
            secure = endpoint.strip().lower().startswith("https://")
        else:
            host = os.getenv("MINIO_HOST", "127.0.0.1").strip()
            port = os.getenv("MINIO_PORT", "9003").strip()
            scheme = "https" if secure_env else "http"
            endpoint = f"{scheme}://{host}:{port}"
            secure = secure_env

        access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")

        return cls(
            mode=mode,
            local_parquet_dir=local_parquet_dir,
            bucket=bucket,
            daily_prefix=daily_prefix,
            five_min_prefix=five_min_prefix,
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )


class MarketDataDB:
    """
    Central DuckDB access point for all market data.

    Supports two read modes:
      1. Local Parquet filesystem globs
      2. MinIO/S3 Parquet globs via DuckDB httpfs
    """

    def __init__(
        self,
        db_path: Path | None = None,
        read_only: bool = False,
        lake: DataLakeConfig | None = None,
    ):
        self.lake = lake or DataLakeConfig.from_env()

        if db_path is None:
            db_path = Path(os.getenv("DUCKDB_PATH", str(DUCKDB_FILE)))
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.con = duckdb.connect(str(db_path), read_only=read_only)
        self._parquet_dir = self.lake.local_parquet_dir
        self._data_source = self.lake.mode
        self._five_min_glob = ""
        self._daily_glob = ""
        self._has_5min = False
        self._has_daily = False
        self._setup()
        self._ensure_backtest_tables()

    @staticmethod
    def _sql_literal(value: str) -> str:
        return value.replace("'", "''")

    def _build_data_globs(self) -> tuple[str, str]:
        if self.lake.mode == "minio":
            daily_glob = f"s3://{self.lake.bucket}/{self.lake.daily_prefix}/*/*.parquet"
            five_min_glob = f"s3://{self.lake.bucket}/{self.lake.five_min_prefix}/*/*.parquet"
            return five_min_glob, daily_glob

        parquet_abs = self._parquet_dir.resolve()
        five_min_glob = str(parquet_abs / "5min" / "*" / "*.parquet").replace("\\", "/")
        daily_glob = str(parquet_abs / "daily" / "*" / "*.parquet").replace("\\", "/")
        return five_min_glob, daily_glob

    def _configure_s3_for_duckdb(self) -> None:
        endpoint = self.lake.endpoint
        if not endpoint:
            raise RuntimeError("MinIO endpoint is required when DATA_LAKE_MODE=minio")

        if not self.lake.access_key or not self.lake.secret_key:
            raise RuntimeError(
                "MinIO credentials are required when DATA_LAKE_MODE=minio "
                "(MINIO_ACCESS_KEY/MINIO_SECRET_KEY or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD)."
            )

        # Load httpfs extension - install if not already loaded
        try:
            self.con.execute("LOAD httpfs")
        except duckdb.CatalogException:
            # Extension not loaded, install it first
            self.con.execute("INSTALL httpfs")
            self.con.execute("LOAD httpfs")

        parsed = urlparse(endpoint)
        host_port = parsed.netloc or parsed.path
        if not host_port:
            raise RuntimeError(f"Invalid MINIO_ENDPOINT: {endpoint}")

        self.con.execute(f"SET s3_endpoint='{self._sql_literal(host_port)}'")
        self.con.execute(f"SET s3_access_key_id='{self._sql_literal(self.lake.access_key)}'")
        self.con.execute(f"SET s3_secret_access_key='{self._sql_literal(self.lake.secret_key)}'")
        self.con.execute("SET s3_url_style='path'")
        self.con.execute(f"SET s3_use_ssl={'true' if self.lake.secure else 'false'}")

    def _register_view(self, view_name: str, glob_path: str, file_count: int | None = None) -> bool:
        if file_count is not None and file_count == 0:
            return False

        try:
            self.con.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{self._sql_literal(glob_path)}', hive_partitioning=false)
            """)
            # Validate that the view is queryable.
            self.con.execute(f"SELECT * FROM {view_name} LIMIT 1").fetchall()
            return True
        except Exception as exc:
            logger.warning("Failed to create %s: %s", view_name, exc)
            return False

    def _setup(self) -> None:
        """Register Parquet glob views. Fast: reads metadata only."""
        self._five_min_glob, self._daily_glob = self._build_data_globs()

        if self.lake.mode == "minio":
            self._configure_s3_for_duckdb()
            self._has_5min = self._register_view("v_5min", self._five_min_glob)
            self._has_daily = self._register_view("v_daily", self._daily_glob)
            if self._has_5min:
                logger.info("Registered 5-min view from MinIO: %s", self._five_min_glob)
            if self._has_daily:
                logger.info("Registered daily view from MinIO: %s", self._daily_glob)
        else:
            five_min_files = list(self._parquet_dir.glob("5min/**/*.parquet"))
            daily_files = list(self._parquet_dir.glob("daily/**/*.parquet"))

            self._has_5min = self._register_view(
                "v_5min", self._five_min_glob, file_count=len(five_min_files)
            )
            self._has_daily = self._register_view(
                "v_daily", self._daily_glob, file_count=len(daily_files)
            )

            if self._has_5min:
                logger.info("Registered 5-min view: %d files", len(five_min_files))
            if self._has_daily:
                logger.info("Registered daily view: %d files", len(daily_files))

        if not self._has_5min:
            logger.warning("No 5-min Parquet files found.")
        if not self._has_daily:
            logger.warning("No daily Parquet files found.")

    # ------------------------------------------------------------------
    # Backtest result storage + idempotency metadata
    # ------------------------------------------------------------------

    def _ensure_column(self, table: str, column: str, column_sql: str) -> None:
        rows = self.con.execute(f"PRAGMA table_info('{table}')").fetchall()
        existing = {r[1] for r in rows}
        if column not in existing:
            self.con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_sql}")

    def _ensure_backtest_tables(self) -> None:
        """Create backtest result and state tables if they do not exist."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bt_experiment (
                exp_id          VARCHAR PRIMARY KEY,
                strategy_name   VARCHAR NOT NULL,
                params_json     VARCHAR NOT NULL,
                params_hash     VARCHAR,
                dataset_hash    VARCHAR,
                code_hash       VARCHAR,
                data_source     VARCHAR DEFAULT 'local',
                dataset_snapshot_json VARCHAR DEFAULT '{}',
                start_year      INTEGER NOT NULL,
                end_year        INTEGER NOT NULL,
                total_return_pct    DOUBLE DEFAULT 0,
                annualized_return_pct DOUBLE DEFAULT 0,
                total_trades    INTEGER DEFAULT 0,
                win_rate_pct    DOUBLE DEFAULT 0,
                max_drawdown_pct DOUBLE DEFAULT 0,
                profit_factor   DOUBLE DEFAULT 0,
                status          VARCHAR DEFAULT 'running',
                created_at      TIMESTAMP DEFAULT current_timestamp
            )
        """)

        # Backward compatibility with older catalogs.
        self._ensure_column("bt_experiment", "params_hash", "VARCHAR")
        self._ensure_column("bt_experiment", "dataset_hash", "VARCHAR")
        self._ensure_column("bt_experiment", "code_hash", "VARCHAR")
        self._ensure_column("bt_experiment", "data_source", "VARCHAR DEFAULT 'local'")
        self._ensure_column(
            "bt_experiment",
            "dataset_snapshot_json",
            "VARCHAR DEFAULT '{}'",
        )

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bt_trade (
                exp_id          VARCHAR NOT NULL,
                symbol          VARCHAR NOT NULL,
                entry_date      DATE NOT NULL,
                exit_date       DATE,
                entry_price     DOUBLE,
                exit_price      DOUBLE,
                pnl_pct         DOUBLE,
                pnl_r           DOUBLE,
                exit_reason     VARCHAR,
                holding_days    INTEGER,
                gap_pct         DOUBLE,
                filters_passed  INTEGER,
                year            INTEGER,
                entry_time      TIME,
                exit_time       TIME
            )
        """)
        # Backward compatibility: add timestamp columns to existing catalogs.
        self._ensure_column("bt_trade", "entry_time", "TIME")
        self._ensure_column("bt_trade", "exit_time", "TIME")

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bt_yearly_metric (
                exp_id          VARCHAR NOT NULL,
                year            INTEGER NOT NULL,
                signals         INTEGER DEFAULT 0,
                trades          INTEGER DEFAULT 0,
                wins            INTEGER DEFAULT 0,
                losses          INTEGER DEFAULT 0,
                return_pct      DOUBLE DEFAULT 0,
                win_rate_pct    DOUBLE DEFAULT 0,
                avg_r           DOUBLE DEFAULT 0,
                max_dd_pct      DOUBLE DEFAULT 0,
                profit_factor   DOUBLE DEFAULT 0,
                avg_holding_days DOUBLE DEFAULT 0,
                exit_reasons_json VARCHAR DEFAULT '{}',
                PRIMARY KEY (exp_id, year)
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bt_dataset_snapshot (
                dataset_hash    VARCHAR PRIMARY KEY,
                source_type     VARCHAR NOT NULL,
                daily_glob      VARCHAR NOT NULL,
                five_min_glob   VARCHAR NOT NULL,
                snapshot_json   VARCHAR NOT NULL,
                created_at      TIMESTAMP DEFAULT current_timestamp
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bt_materialization_state (
                table_name      VARCHAR PRIMARY KEY,
                dataset_hash    VARCHAR NOT NULL,
                query_version   VARCHAR NOT NULL,
                row_count       BIGINT DEFAULT 0,
                updated_at      TIMESTAMP DEFAULT current_timestamp
            )
        """)

        # Query acceleration for experiment drill-down views.
        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_bt_trade_exp_entry
            ON bt_trade(exp_id, entry_date)
        """)
        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_bt_trade_exp_symbol_entry
            ON bt_trade(exp_id, symbol, entry_date)
        """)
        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_bt_yearly_metric_exp_year
            ON bt_yearly_metric(exp_id, year)
        """)
        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_bt_experiment_params_dataset
            ON bt_experiment(params_hash, dataset_hash)
        """)

    def _view_snapshot(self, view: str) -> dict[str, int | str | None]:
        if (view == "v_daily" and not self._has_daily) or (view == "v_5min" and not self._has_5min):
            return {
                "rows": 0,
                "symbols": 0,
                "min_date": None,
                "max_date": None,
            }

        row = self.con.execute(
            f"""SELECT
                    COUNT(*)::BIGINT AS rows,
                    COUNT(DISTINCT symbol)::BIGINT AS symbols,
                    MIN(date)::VARCHAR AS min_date,
                    MAX(date)::VARCHAR AS max_date
                FROM {view}"""
        ).fetchone()
        return {
            "rows": int(row[0]) if row and row[0] is not None else 0,
            "symbols": int(row[1]) if row and row[1] is not None else 0,
            "min_date": row[2] if row else None,
            "max_date": row[3] if row else None,
        }

    def get_dataset_snapshot(self) -> dict[str, object]:
        """Capture a deterministic snapshot of the active Parquet dataset."""
        daily = self._view_snapshot("v_daily")
        five_min = self._view_snapshot("v_5min")

        payload = {
            "source_type": self._data_source,
            "daily_glob": self._daily_glob,
            "five_min_glob": self._five_min_glob,
            "daily": daily,
            "five_min": five_min,
        }
        blob = json.dumps(payload, sort_keys=True)
        dataset_hash = hashlib.sha256(blob.encode()).hexdigest()[:16]

        return {
            **payload,
            "dataset_hash": dataset_hash,
        }

    def register_dataset_snapshot(self, snapshot: dict[str, object]) -> None:
        self.con.execute(
            """INSERT OR REPLACE INTO bt_dataset_snapshot
               (dataset_hash, source_type, daily_glob, five_min_glob, snapshot_json)
               VALUES (?, ?, ?, ?, ?)""",
            [
                str(snapshot["dataset_hash"]),
                str(snapshot["source_type"]),
                str(snapshot["daily_glob"]),
                str(snapshot["five_min_glob"]),
                json.dumps(snapshot, sort_keys=True),
            ],
        )

    def _get_materialization_state(self, table_name: str) -> dict[str, object] | None:
        row = self.con.execute(
            """SELECT table_name, dataset_hash, query_version, row_count, updated_at
               FROM bt_materialization_state WHERE table_name = ?""",
            [table_name],
        ).fetchone()
        if not row:
            return None
        return {
            "table_name": row[0],
            "dataset_hash": row[1],
            "query_version": row[2],
            "row_count": int(row[3]) if row[3] is not None else 0,
            "updated_at": row[4],
        }

    def _upsert_materialization_state(
        self,
        table_name: str,
        dataset_hash: str,
        query_version: str,
        row_count: int,
    ) -> None:
        self.con.execute(
            """INSERT OR REPLACE INTO bt_materialization_state
               (table_name, dataset_hash, query_version, row_count, updated_at)
               VALUES (?, ?, ?, ?, current_timestamp)""",
            [table_name, dataset_hash, query_version, row_count],
        )

    def experiment_exists(self, exp_id: str) -> bool:
        """Check if an experiment with this ID already exists."""
        row = self.con.execute("SELECT 1 FROM bt_experiment WHERE exp_id = ?", [exp_id]).fetchone()
        return row is not None

    def save_experiment(
        self,
        exp_id: str,
        strategy_name: str,
        params_json: str,
        start_year: int,
        end_year: int,
        *,
        params_hash: str | None = None,
        dataset_hash: str | None = None,
        code_hash: str | None = None,
        data_source: str | None = None,
        dataset_snapshot: dict[str, object] | None = None,
    ) -> None:
        """Insert a new experiment record (status='running')."""
        self.con.execute(
            """INSERT INTO bt_experiment
               (exp_id, strategy_name, params_json, params_hash, dataset_hash, code_hash, data_source,
                dataset_snapshot_json, start_year, end_year)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                exp_id,
                strategy_name,
                params_json,
                params_hash,
                dataset_hash,
                code_hash,
                data_source or self._data_source,
                json.dumps(dataset_snapshot or {}, sort_keys=True),
                start_year,
                end_year,
            ],
        )

    def update_experiment_metrics(
        self,
        exp_id: str,
        total_return_pct: float,
        annualized_return_pct: float,
        total_trades: int,
        win_rate_pct: float,
        max_drawdown_pct: float,
        profit_factor: float,
    ) -> None:
        """Update aggregate metrics on a completed experiment."""
        self.con.execute(
            """UPDATE bt_experiment
               SET total_return_pct = ?, annualized_return_pct = ?,
                   total_trades = ?, win_rate_pct = ?, max_drawdown_pct = ?,
                   profit_factor = ?, status = 'completed'
               WHERE exp_id = ?""",
            [
                total_return_pct,
                annualized_return_pct,
                total_trades,
                win_rate_pct,
                max_drawdown_pct,
                profit_factor,
                exp_id,
            ],
        )

    def save_trades(self, exp_id: str, trades: list[dict]) -> None:
        """Bulk-insert trade records for an experiment."""
        if not trades:
            return
        rows = [
            (
                exp_id,
                t["symbol"],
                t["entry_date"],
                t.get("exit_date"),
                t.get("entry_price"),
                t.get("exit_price"),
                t.get("pnl_pct", 0),
                t.get("r_multiple", 0),
                t.get("exit_reason", "unknown"),
                t.get("holding_days", 0),
                t.get("gap_pct"),
                t.get("filters_passed"),
                t.get("year"),
                t.get("entry_time"),
                t.get("exit_time"),
            )
            for t in trades
        ]
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.executemany(
                """INSERT INTO bt_trade
                   (exp_id, symbol, entry_date, exit_date, entry_price, exit_price,
                    pnl_pct, pnl_r, exit_reason, holding_days, gap_pct, filters_passed, year,
                    entry_time, exit_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            self.con.execute("COMMIT")
        except Exception as e:
            self.con.execute("ROLLBACK")
            logger.error("Failed to save trades: %s", e)
            raise

    def save_yearly_metric(self, exp_id: str, metric: dict) -> None:
        """Insert a yearly metric record."""
        self.con.execute(
            """INSERT OR REPLACE INTO bt_yearly_metric
               (exp_id, year, signals, trades, wins, losses, return_pct,
                win_rate_pct, avg_r, max_dd_pct, profit_factor,
                avg_holding_days, exit_reasons_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                exp_id,
                metric["year"],
                metric.get("signals", 0),
                metric.get("trades", 0),
                metric.get("wins", 0),
                metric.get("losses", 0),
                metric.get("return_pct", 0),
                metric.get("win_rate_pct", 0),
                metric.get("avg_r", 0),
                metric.get("max_dd_pct", 0),
                metric.get("profit_factor", 0),
                metric.get("avg_holding_days", 0),
                json.dumps(metric.get("exit_reasons", {})),
            ],
        )

    def get_experiment(self, exp_id: str) -> dict | None:
        """Fetch a single experiment record."""
        row = self.con.execute("SELECT * FROM bt_experiment WHERE exp_id = ?", [exp_id]).fetchone()
        if not row:
            return None
        cols = [d[0] for d in self.con.description]
        return dict(zip(cols, row, strict=False))

    def get_experiment_trades(self, exp_id: str) -> pl.DataFrame:
        """Fetch all trades for an experiment as a Polars DataFrame."""
        return self.con.execute(
            "SELECT * FROM bt_trade WHERE exp_id = ? ORDER BY entry_date", [exp_id]
        ).pl()

    def get_experiment_yearly_metrics(self, exp_id: str) -> pl.DataFrame:
        """Fetch yearly metrics for an experiment."""
        return self.con.execute(
            "SELECT * FROM bt_yearly_metric WHERE exp_id = ? ORDER BY year", [exp_id]
        ).pl()

    def list_experiments(self) -> pl.DataFrame:
        """List all experiments ordered by creation time."""
        return self.con.execute(
            """SELECT exp_id, strategy_name, params_hash, dataset_hash, code_hash, data_source,
                      start_year, end_year, total_return_pct, annualized_return_pct,
                      total_trades, win_rate_pct, max_drawdown_pct, status, created_at
               FROM bt_experiment ORDER BY created_at DESC"""
        ).pl()

    def delete_experiment(self, exp_id: str) -> None:
        """Delete an experiment and its trades/metrics."""
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute("DELETE FROM bt_trade WHERE exp_id = ?", [exp_id])
            self.con.execute("DELETE FROM bt_yearly_metric WHERE exp_id = ?", [exp_id])
            self.con.execute("DELETE FROM bt_experiment WHERE exp_id = ?", [exp_id])
            self.con.execute("COMMIT")
        except Exception as e:
            self.con.execute("ROLLBACK")
            logger.error("Failed to delete experiment '%s': %s", exp_id, e)
            raise

    def _require_data(self, view: str = "v_5min") -> None:
        """Raise a clear error if Parquet data has not been loaded yet."""
        available = {"v_5min": self._has_5min, "v_daily": self._has_daily}
        if not available.get(view, False):
            view_name = view.replace("v_", "").replace("-", " ")
            raise RuntimeError(f"{view_name} Parquet data not found.")

    def build_feat_daily_table(self, force: bool = False) -> int:
        """
        Pre-compute daily features across all symbols.

        Features:
            - ret_1d: 1-day return
            - ret_5d: 5-day return
            - atr_20: 20-day Average True Range
            - range_pct: (high - low) / close
            - close_pos_in_range: (close - low) / (high - low)
            - ma_20: 20-day moving average
            - ma_65: 65-day moving average
            - rs_252: 252-day relative strength
            - vol_20: 20-day average volume
            - dollar_vol_20: 20-day average dollar volume
            - 2LYNCH filters:
                - r2_65: R-squared of 65-day linear trend
                - atr_compress_ratio: Current ATR / 50-day avg ATR
                - range_percentile: Price position in 252-day range
                - vol_dryup_ratio: Recent volume / 20-day avg volume
                - prior_breakouts_90d: Count of 4%+ gaps in last 90 days
        """
        self._require_data("v_daily")
        snapshot = self.get_dataset_snapshot()
        dataset_hash = str(snapshot["dataset_hash"])

        if not force and self._table_exists("feat_daily"):
            row = self.con.execute("SELECT COUNT(*) FROM feat_daily").fetchone()
            n = int(row[0]) if row and row[0] is not None else 0
            state = self._get_materialization_state("feat_daily")
            if (
                n > 0
                and state is not None
                and state["dataset_hash"] == dataset_hash
                and state["query_version"] == FEAT_DAILY_QUERY_VERSION
            ):
                logger.info("feat_daily is up-to-date (%d rows).", n)
                return int(n)

            if n > 0 and state is None:
                columns = self.con.execute("DESCRIBE feat_daily").fetchall()
                col_names = {c[0] for c in columns}
                required = {
                    "r2_65",
                    "atr_compress_ratio",
                    "range_percentile",
                    "vol_dryup_ratio",
                    "prior_breakouts_30d",
                    "prior_breakouts_90d",
                }
                if required.issubset(col_names):
                    self._upsert_materialization_state(
                        table_name="feat_daily",
                        dataset_hash=dataset_hash,
                        query_version=FEAT_DAILY_QUERY_VERSION,
                        row_count=int(n),
                    )
                    self.register_dataset_snapshot(snapshot)
                    logger.info("feat_daily state initialized (%d rows).", n)
                    return int(n)

        logger.info("Building feat_daily materialized table with 2LYNCH filters...")
        self.con.execute("DROP TABLE IF EXISTS feat_daily")
        self._create_feat_daily_table()

        self.con.execute("CREATE INDEX idx_feat_symbol_date ON feat_daily(symbol, trading_date)")
        row = self.con.execute("SELECT COUNT(*) FROM feat_daily").fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        self._upsert_materialization_state(
            table_name="feat_daily",
            dataset_hash=dataset_hash,
            query_version=FEAT_DAILY_QUERY_VERSION,
            row_count=n,
        )
        self.register_dataset_snapshot(snapshot)
        logger.info("feat_daily built with 2LYNCH features: %d rows", n)
        return n

    def _create_feat_daily_table(self) -> None:
        self.con.execute("""
            CREATE TABLE feat_daily AS
            WITH base AS (
                SELECT
                    symbol,
                    date AS trading_date,
                    close,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS close_1d,
                    LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS close_5d,
                    LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) AS close_20d,
                    LAG(close, 65) OVER (PARTITION BY symbol ORDER BY date) AS close_65d,
                    LAG(close, 252) OVER (PARTITION BY symbol ORDER BY date) AS close_252d,
                    high,
                    low,
                    open,
                    volume,
                    close * volume AS dollar_vol
                FROM v_daily
            ),
            features AS (
                SELECT
                    symbol,
                    trading_date,
                    (close / NULLIF(close_1d, 0)) - 1 AS ret_1d,
                    (close / NULLIF(close_5d, 0)) - 1 AS ret_5d,
                    (high - low) AS true_range,
                    (high - low) / NULLIF(close, 0) AS range_pct,
                    (close - low) / NULLIF(high - low, 0) AS close_pos_in_range,
                    close_20d AS ma_20,
                    close_65d AS ma_65,
                    (close / NULLIF(close_252d, 0)) - 1 AS rs_252,
                    volume,
                    dollar_vol,
                    open,
                    close
                FROM base
                WHERE close IS NOT NULL
            ),
            smoothed AS (
                SELECT
                    symbol,
                    trading_date,
                    ret_1d,
                    ret_5d,
                    AVG(true_range) OVER (PARTITION BY symbol ORDER BY trading_date ROWS 19 PRECEDING) AS atr_20,
                    range_pct,
                    close_pos_in_range,
                    ma_20,
                    ma_65,
                    -- TI65: true rolling averages (MA7 / MA65 >= 1.05 = trend intensity)
                    AVG(close) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 64 PRECEDING AND CURRENT ROW) AS ma_65_sma,
                    rs_252,
                    volume,
                    AVG(volume) OVER (PARTITION BY symbol ORDER BY trading_date ROWS 19 PRECEDING) AS vol_20,
                    AVG(dollar_vol) OVER (PARTITION BY symbol ORDER BY trading_date ROWS 19 PRECEDING) AS dollar_vol_20,
                    open,
                    close
                FROM features
            ),
            with_rownum AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trading_date) AS rn
                FROM smoothed
            ),
            lynch_features AS (
                SELECT
                    symbol,
                    trading_date,
                    ret_1d,
                    ret_5d,
                    atr_20,
                    range_pct,
                    close_pos_in_range,
                    ma_20,
                    ma_65,
                    ma_7,
                    ma_65_sma,
                    rs_252,
                    vol_20,
                    dollar_vol_20,
                    -- R-squared of 65-day linear regression (close vs time)
                    REGR_R2(close, rn) OVER (
                        PARTITION BY symbol ORDER BY trading_date
                        ROWS BETWEEN 64 PRECEDING AND CURRENT ROW
                    ) AS r2_65,
                    -- ATR compression ratio
                    atr_20 / NULLIF(AVG(atr_20) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 49 PRECEDING AND 1 PRECEDING), 0) AS atr_compress_ratio,
                    -- Range percentile (252-day)
                    (close - MIN(close) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW))
                    / NULLIF(MAX(close) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
                               - MIN(close) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW), 0) AS range_percentile,
                    -- Volume dryup ratio
                    volume / NULLIF(vol_20, 0) AS vol_dryup_ratio,
                    open,
                    close
                FROM with_rownum
            ),
            breakouts AS (
                SELECT
                    symbol,
                    trading_date,
                    ret_1d,
                    ret_5d,
                    atr_20,
                    range_pct,
                    close_pos_in_range,
                    ma_20,
                    ma_65,
                    ma_7,
                    ma_65_sma,
                    rs_252,
                    vol_20,
                    dollar_vol_20,
                    r2_65,
                    atr_compress_ratio,
                    range_percentile,
                    vol_dryup_ratio,
                    -- Count prior 4%+ breakouts in last 30 days (Young breakout filter)
                    SUM(CASE WHEN ret_1d >= 0.04 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING) AS prior_breakouts_30d,
                    -- Keep 90d for backward compat
                    SUM(CASE WHEN ret_1d >= 0.04 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 89 PRECEDING AND 1 PRECEDING) AS prior_breakouts_90d
                FROM lynch_features
            )
            SELECT
                symbol,
                trading_date,
                ret_1d,
                ret_5d,
                atr_20,
                range_pct,
                close_pos_in_range,
                ma_20,
                ma_65,
                ma_7,
                ma_65_sma,
                rs_252,
                vol_20,
                dollar_vol_20,
                r2_65,
                atr_compress_ratio,
                range_percentile,
                vol_dryup_ratio,
                prior_breakouts_30d,
                prior_breakouts_90d
            FROM breakouts
        """)

    def build_all(self, force: bool = False) -> None:
        """Build all materialized tables."""
        logger.info("Building materialized feature tables...")
        self.build_feat_daily_table(force=force)
        logger.info("Done: market.duckdb is ready for backtesting.")

    def drop_and_rebuild(self) -> None:
        """Drop all materialized tables and rebuild from Parquet."""
        logger.info("Dropping and rebuilding all materialized tables...")
        for table in ["feat_daily"]:
            self.con.execute(f"DROP TABLE IF EXISTS {table}")
        self.build_all(force=True)

    def query_5min(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Fetch 5-min candles for a symbol over a date range."""
        self._require_data("v_5min")
        cols = ", ".join(columns) if columns else "*"
        return self.con.execute(
            f"SELECT {cols} FROM v_5min WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY candle_time",
            [symbol, start_date, end_date],
        ).pl()

    def query_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Fetch daily candles for a symbol over a date range."""
        self._require_data("v_daily")
        cols = ", ".join(columns) if columns else "*"
        return self.con.execute(
            f"SELECT {cols} FROM v_daily WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date",
            [symbol, start_date, end_date],
        ).pl()

    def query_daily_multi(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Fetch daily candles for multiple symbols.

        Uses parameterized query to prevent SQL injection.
        Column names are validated against an allowlist before interpolation.
        """
        self._require_data("v_daily")
        if not symbols:
            return pl.DataFrame()

        # Validate column names against allowlist to prevent SQL injection
        allowed_columns = {
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "rs_252",
            "range_pct",
            "dollar_vol_20",
            "ma_20",
            "ma_65",
            "atr_20",
            "vol_20",
            "ret_1d",
            "ret_5d",
            "close_pos_in_range",
            "created_at",
        }
        if columns:
            invalid = set(columns) - allowed_columns
            if invalid:
                raise ValueError(f"Invalid columns: {invalid}")
            cols = ", ".join(columns)
        else:
            cols = "*"

        # Use parameterized query with placeholder expansion for IN clause
        placeholders = ",".join("?" for _ in symbols)
        return self.con.execute(
            f"""SELECT {cols} FROM v_daily
                WHERE symbol IN ({placeholders}) AND date >= ? AND date <= ?
                ORDER BY symbol, date""",
            [*symbols, start_date, end_date],
        ).pl()

    def get_features(self, symbol: str, trading_date: str) -> dict | None:
        """Get pre-computed features for a symbol on a date."""
        if not self._table_exists("feat_daily"):
            return None

        row = self.con.execute(
            """SELECT symbol, trading_date, ret_1d, ret_5d, atr_20, range_pct,
                      close_pos_in_range, ma_20, ma_65, ma_7, ma_65_sma, rs_252, vol_20, dollar_vol_20
               FROM feat_daily WHERE symbol = ? AND trading_date = ?""",
            [symbol, trading_date],
        ).fetchone()

        if not row:
            return None

        numeric_cols = [
            "ret_1d",
            "ret_5d",
            "atr_20",
            "range_pct",
            "close_pos_in_range",
            "ma_20",
            "ma_65",
            "ma_7",
            "ma_65_sma",
            "rs_252",
            "vol_20",
            "dollar_vol_20",
        ]
        keys = ["symbol", "trading_date", *numeric_cols]

        result = {}
        for i, key in enumerate(keys):
            val = row[i]
            if i >= 2 and val is not None:
                val = float(val)
            result[key] = val
        return result

    def _table_exists(self, table: str) -> bool:
        """Check if a materialized table exists."""
        try:
            self.con.execute(f"SELECT COUNT(*) FROM {table}")
            return True
        except duckdb.CatalogException:
            # Table does not exist
            return False
        except Exception as e:
            # Unexpected error - log it but treat as missing
            logger.warning("Unexpected error checking table '%s': %s", table, e)
            return False

    def get_features_range(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> pl.DataFrame:
        """Get pre-computed features for multiple symbols over a date range.

        Uses parameterized query to prevent SQL injection.
        """
        if not self._table_exists("feat_daily") or not symbols:
            return pl.DataFrame()

        # Use parameterized query with placeholder expansion for IN clause
        placeholders = ",".join("?" for _ in symbols)
        return self.con.execute(
            f"""SELECT * FROM feat_daily
                WHERE symbol IN ({placeholders}) AND trading_date >= ? AND trading_date <= ?
                ORDER BY symbol, trading_date""",
            [*symbols, start_date, end_date],
        ).pl()

    def get_avg_dollar_vol_20_by_symbol(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> pl.DataFrame:
        """Return per-symbol AVG(dollar_vol_20) over a date range.

        Uses parameterized query to prevent SQL injection.
        """
        if not self._table_exists("feat_daily") or not symbols:
            return pl.DataFrame()

        # Use parameterized query with placeholder expansion for IN clause
        placeholders = ",".join("?" for _ in symbols)
        return self.con.execute(
            f"""SELECT symbol, AVG(dollar_vol_20) AS avg_dollar_vol_20
                FROM feat_daily
                WHERE symbol IN ({placeholders}) AND trading_date >= ? AND trading_date <= ?
                GROUP BY symbol""",
            [*symbols, start_date, end_date],
        ).pl()

    def get_trading_days(self, symbol: str, start_date: str, end_date: str) -> list[str]:
        """Return sorted list of trading dates (ISO strings) for a symbol."""
        self._require_data("v_daily")
        rows = self.con.execute(
            "SELECT DISTINCT date::VARCHAR FROM v_daily WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date",
            [symbol, start_date, end_date],
        ).fetchall()
        return [r[0] for r in rows]

    def get_available_symbols(self) -> list[str]:
        """List all symbols available in the Parquet dataset."""
        if self._has_5min:
            view = "v_5min"
        elif self._has_daily:
            view = "v_daily"
        else:
            return []

        try:
            rows = self.con.execute(
                f"SELECT DISTINCT symbol FROM {view} ORDER BY symbol"
            ).fetchall()
            return [r[0] for r in rows]
        except duckdb.CatalogException:
            # View doesn't exist
            return []
        except Exception as e:
            logger.warning("Failed to get available symbols from '%s': %s", view, e)
            return []

    def get_date_range(self, symbol: str) -> tuple[str, str] | None:
        """Min and max dates for a symbol in the daily dataset."""
        self._require_data("v_daily")
        row = self.con.execute(
            "SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM v_daily WHERE symbol = ?",
            [symbol],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return (row[0], row[1])

    def get_status(self) -> dict:
        """System status: source mode, loaded views, and materialized table sizes."""
        tables_status: dict[str, int] = {}
        status: dict[str, object] = {
            "data_source": self._data_source,
            "daily_glob": self._daily_glob,
            "five_min_glob": self._five_min_glob,
            "parquet_5min": self._has_5min,
            "parquet_daily": self._has_daily,
            "tables": tables_status,
        }
        for table in [
            "feat_daily",
            "bt_experiment",
            "bt_trade",
            "bt_yearly_metric",
            "bt_dataset_snapshot",
            "bt_materialization_state",
        ]:
            try:
                row = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                n = int(row[0]) if row and row[0] is not None else 0
                tables_status[table] = n
            except duckdb.CatalogException:
                # Table doesn't exist
                tables_status[table] = 0
            except Exception as e:
                logger.warning("Failed to get count for table '%s': %s", table, e)
                tables_status[table] = 0

        if self._has_daily:
            try:
                row = self.con.execute(
                    "SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM v_daily"
                ).fetchone()
                if row:
                    status["symbols"] = int(row[0]) if row[0] is not None else 0
                    status["total_candles"] = int(row[1]) if row[1] is not None else 0
                    status["date_range"] = f"{row[2]} to {row[3]}"
                snapshot = self.get_dataset_snapshot()
                status["dataset_hash"] = snapshot["dataset_hash"]
            except Exception as e:
                logger.warning("Failed to get daily status: %s", e)
        elif self._has_5min:
            try:
                row = self.con.execute(
                    "SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM v_5min"
                ).fetchone()
                if row:
                    status["symbols"] = int(row[0]) if row[0] is not None else 0
                    status["total_candles"] = int(row[1]) if row[1] is not None else 0
                    status["date_range"] = f"{row[2]} to {row[3]}"
            except Exception as e:
                logger.warning("Failed to get 5min status: %s", e)

        return status

    def close(self) -> None:
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


_db: MarketDataDB | None = None


def get_market_db() -> MarketDataDB:
    """Return the global MarketDataDB instance (creates on first call)."""
    global _db
    if _db is None:
        _db = MarketDataDB()
    return _db


def close_market_db() -> None:
    global _db
    if _db is not None:
        _db.close()
        _db = None
