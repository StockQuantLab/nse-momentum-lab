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
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
DUCKDB_FILE = DATA_DIR / "market.duckdb"
BACKTEST_DUCKDB_FILE = DATA_DIR / "backtest.duckdb"
BACKTEST_DASHBOARD_DUCKDB_FILE = DATA_DIR / "backtest_dashboard.duckdb"

# Bump when feat_daily SQL logic changes.
FEAT_DAILY_QUERY_VERSION = "feat_daily_v2lynch_ti65_2026_03_27_true_range"
MARKET_MONITOR_QUERY_VERSION = "market_monitor_v3_2026_03_20_ma40_t2108"
MARKET_MONITOR_INCREMENTAL_LOOKBACK_SESSIONS = 130


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
        self._read_only = read_only
        self._parquet_dir = self.lake.local_parquet_dir
        self._data_source = self.lake.mode
        self._five_min_glob = ""
        self._daily_glob = ""
        self._has_5min = False
        self._has_daily = False
        self._setup()
        if not self._read_only:
            self._ensure_backtest_tables()
            self._ensure_dq_tables()

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

        # In read-only mode, use TEMP VIEW so no write is attempted on the DB file.
        view_qualifier = "TEMP VIEW" if self._read_only else "VIEW"
        try:
            self.con.execute(f"""
                CREATE OR REPLACE {view_qualifier} {view_name} AS
                SELECT * FROM read_parquet('{self._sql_literal(glob_path)}', hive_partitioning=false, union_by_name=true)
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

    def _table_columns(self, table: str) -> set[str]:
        """Return the current column names for a table, or an empty set if it is missing."""
        try:
            rows = self.con.execute(f"PRAGMA table_info('{table}')").fetchall()
        except duckdb.CatalogException:
            return set()
        except Exception as exc:
            logger.warning("Unexpected error reading columns for '%s': %s", table, exc)
            return set()
        return {str(row[1]) for row in rows if len(row) > 1 and row[1]}

    def _select_existing_columns(
        self,
        table: str,
        desired_columns: list[str],
        *,
        order_by: str | None = None,
        where_clause: str | None = None,
        params: list[Any] | None = None,
    ) -> pl.DataFrame:
        """Select a stable projection while tolerating older catalogs missing new columns."""
        existing_columns = self._table_columns(table)
        if not existing_columns:
            return pl.DataFrame()

        selected_columns = [column for column in desired_columns if column in existing_columns]
        if not selected_columns:
            return pl.DataFrame()

        query = f"SELECT {', '.join(selected_columns)} FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by}"

        frame = self.con.execute(query, params or []).pl()
        missing_columns = [column for column in desired_columns if column not in frame.columns]
        if missing_columns:
            frame = frame.with_columns([pl.lit(None).alias(column) for column in missing_columns])
        return frame.select(desired_columns)

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
                wf_run_id       VARCHAR,
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
        self._ensure_column("bt_experiment", "wf_run_id", "VARCHAR")
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
                position_value  DOUBLE,
                gross_pnl       DOUBLE,
                net_pnl         DOUBLE,
                total_costs     DOUBLE,
                pnl_pct         DOUBLE,
                pnl_r           DOUBLE,
                exit_reason     VARCHAR,
                holding_days    INTEGER,
                gap_pct         DOUBLE,
                filters_passed  INTEGER,
                year            INTEGER,
                entry_time      TIME,
                exit_time       TIME,
                commission_model VARCHAR
            )
        """)
        # Backward compatibility: add timestamp columns to existing catalogs.
        self._ensure_column("bt_trade", "entry_time", "TIME")
        self._ensure_column("bt_trade", "exit_time", "TIME")
        self._ensure_column("bt_trade", "position_value", "DOUBLE")
        self._ensure_column("bt_trade", "gross_pnl", "DOUBLE")
        self._ensure_column("bt_trade", "net_pnl", "DOUBLE")
        self._ensure_column("bt_trade", "total_costs", "DOUBLE")
        self._ensure_column("bt_trade", "commission_model", "VARCHAR")

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

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bt_execution_diagnostic (
                exp_id              VARCHAR NOT NULL,
                year                INTEGER,
                signal_date         DATE,
                symbol              VARCHAR,
                status              VARCHAR,
                reason              VARCHAR,
                entry_time          TIME,
                entry_price         DOUBLE,
                initial_stop        DOUBLE,
                filters_json        VARCHAR,
                hold_quality_passed BOOLEAN,
                executed_exit_reason VARCHAR,
                pnl_pct             DOUBLE,
                selection_score     DOUBLE,
                selection_rank      INTEGER,
                selection_components_json VARCHAR
            )
        """)
        self._ensure_column("bt_execution_diagnostic", "selection_score", "DOUBLE")
        self._ensure_column("bt_execution_diagnostic", "selection_rank", "INTEGER")
        self._ensure_column("bt_execution_diagnostic", "selection_components_json", "VARCHAR")

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
        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_bt_experiment_wf_run_id
            ON bt_experiment(wf_run_id)
        """)

    # ------------------------------------------------------------------
    # Data quality issue registry
    # ------------------------------------------------------------------

    def _ensure_dq_tables(self) -> None:
        """Create the data_quality_issues table if it does not exist."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_issues (
                symbol       VARCHAR NOT NULL,
                issue_code   VARCHAR NOT NULL,
                severity     VARCHAR DEFAULT 'WARNING',
                details      VARCHAR DEFAULT '',
                is_active    BOOLEAN DEFAULT TRUE,
                acknowledged BOOLEAN DEFAULT FALSE,
                first_seen   TIMESTAMP DEFAULT current_timestamp,
                last_seen    TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (symbol, issue_code)
            )
        """)
        # Migration: add acknowledged column to existing installs
        try:
            self.con.execute(
                "ALTER TABLE data_quality_issues ADD COLUMN acknowledged BOOLEAN DEFAULT FALSE"
            )
        except Exception:
            pass  # column already exists

    def upsert_data_quality_issues(
        self,
        symbols: list[str],
        issue_code: str,
        details: str = "",
        severity: str = "WARNING",
    ) -> int:
        """Insert or reactivate data quality issues for a list of symbols.

        Returns the number of rows upserted.
        """
        if not symbols:
            return 0
        rows = [(symbol, issue_code, severity, details) for symbol in symbols]
        self.con.executemany(
            """INSERT INTO data_quality_issues (symbol, issue_code, severity, details, is_active, last_seen)
               VALUES (?, ?, ?, ?, TRUE, now())
               ON CONFLICT (symbol, issue_code) DO UPDATE SET
                   severity = EXCLUDED.severity,
                   details  = EXCLUDED.details,
                   is_active = TRUE,
                   last_seen = now()""",
            rows,
        )
        return len(rows)

    def deactivate_data_quality_issue(
        self,
        issue_code: str,
        keep_symbols: list[str] | None = None,
    ) -> int:
        """Mark issues as inactive. Optionally keep specific symbols active.

        Returns the number of rows deactivated.
        """
        # Count rows that will be deactivated before the UPDATE.
        if keep_symbols:
            placeholders = ",".join("?" for _ in keep_symbols)
            count_row = self.con.execute(
                f"""SELECT COUNT(*) FROM data_quality_issues
                    WHERE issue_code = ? AND is_active = TRUE
                      AND symbol NOT IN ({placeholders})""",
                [issue_code, *keep_symbols],
            ).fetchone()
            self.con.execute(
                f"""UPDATE data_quality_issues
                    SET is_active = FALSE
                    WHERE issue_code = ? AND is_active = TRUE
                      AND symbol NOT IN ({placeholders})""",
                [issue_code, *keep_symbols],
            )
        else:
            count_row = self.con.execute(
                """SELECT COUNT(*) FROM data_quality_issues
                   WHERE issue_code = ? AND is_active = TRUE""",
                [issue_code],
            ).fetchone()
            self.con.execute(
                """UPDATE data_quality_issues
                   SET is_active = FALSE
                   WHERE issue_code = ? AND is_active = TRUE""",
                [issue_code],
            )
        return int(count_row[0]) if count_row else 0

    def acknowledge_data_quality_issues(
        self,
        *,
        issue_code: str | None = None,
        symbols: list[str] | None = None,
    ) -> int:
        """Mark DQ issues as acknowledged (operator-reviewed, known/expected).

        Filters by issue_code and/or symbols. Returns the number of rows updated.
        """
        conditions = ["is_active = TRUE", "acknowledged = FALSE"]
        params: list[str] = []
        if issue_code:
            conditions.append("issue_code = ?")
            params.append(issue_code)
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            conditions.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        where = " AND ".join(conditions)
        count_row = self.con.execute(
            f"SELECT COUNT(*) FROM data_quality_issues WHERE {where}", params
        ).fetchone()
        self.con.execute(
            f"UPDATE data_quality_issues SET acknowledged = TRUE WHERE {where}", params
        )
        return int(count_row[0]) if count_row else 0

    def query_active_dq_issues(
        self,
        issue_code: str | None = None,
        severity: str | None = None,
        include_acknowledged: bool = False,
    ) -> pl.DataFrame:
        """Return active data quality issues, optionally filtered."""
        conditions = ["is_active = TRUE"]
        params: list[str] = []
        if not include_acknowledged:
            conditions.append("acknowledged = FALSE")
        if issue_code:
            conditions.append("issue_code = ?")
            params.append(issue_code)
        if severity:
            conditions.append("severity = ?")
            params.append(severity)
        where = " AND ".join(conditions)
        return self.con.execute(
            f"""SELECT symbol, issue_code, severity, details, acknowledged,
                       first_seen, last_seen
                FROM data_quality_issues
                WHERE {where}
                ORDER BY severity, symbol""",
            params,
        ).pl()

    def refresh_backtest_read_snapshot(self) -> None:
        """Refresh read-only dashboard copy of backtest tables."""
        if self._read_only:
            return

        target_path = Path(
            os.getenv("BACKTEST_DASHBOARD_DUCKDB_PATH", str(BACKTEST_DASHBOARD_DUCKDB_FILE))
        )
        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if target_path.resolve() == self.db_path.resolve():
                return
        except OSError:
            pass

        escaped_path = str(target_path).replace("\\", "/").replace("'", "''")
        try:
            self.con.execute(f"ATTACH '{escaped_path}' AS bt_read")
        except Exception as exc:
            logger.warning(
                "Skipping backtest dashboard snapshot refresh because %s is locked: %s",
                target_path,
                exc,
            )
            return
        try:
            self.con.execute(
                "CREATE OR REPLACE TABLE bt_read.bt_experiment AS SELECT * FROM bt_experiment"
            )
            self.con.execute(
                "CREATE OR REPLACE TABLE bt_read.bt_yearly_metric AS SELECT * FROM bt_yearly_metric"
            )
            self.con.execute("CREATE OR REPLACE TABLE bt_read.bt_trade AS SELECT * FROM bt_trade")
            self.con.execute(
                "CREATE OR REPLACE TABLE bt_read.bt_execution_diagnostic "
                "AS SELECT * FROM bt_execution_diagnostic"
            )
        finally:
            try:
                self.con.execute("DETACH bt_read")
            except Exception as exc:
                logger.warning("Failed to detach bt_read after snapshot refresh: %s", exc)

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

    @staticmethod
    def _snapshot_component_hash(snapshot: dict[str, object] | None) -> str:
        if not snapshot:
            return ""
        blob = json.dumps(snapshot, sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

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
        wf_run_id: str | None = None,
        data_source: str | None = None,
        dataset_snapshot: dict[str, object] | None = None,
    ) -> None:
        """Insert a new experiment record (status='running')."""
        self.con.execute(
            """INSERT INTO bt_experiment
               (exp_id, strategy_name, params_json, params_hash, dataset_hash, code_hash, wf_run_id,
                data_source,
                dataset_snapshot_json, start_year, end_year)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                exp_id,
                strategy_name,
                params_json,
                params_hash,
                dataset_hash,
                code_hash,
                wf_run_id,
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
                t.get("position_value"),
                t.get("gross_pnl"),
                t.get("net_pnl"),
                t.get("total_costs"),
                t.get("pnl_pct", 0),
                t.get("r_multiple", 0),
                t.get("exit_reason", "unknown"),
                t.get("holding_days", 0),
                t.get("gap_pct"),
                t.get("filters_passed"),
                t.get("year"),
                t.get("entry_time"),
                t.get("exit_time"),
                t.get("commission_model"),
            )
            for t in trades
        ]
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.executemany(
                """INSERT INTO bt_trade
                   (exp_id, symbol, entry_date, exit_date, entry_price, exit_price,
                    position_value, gross_pnl, net_pnl, total_costs, pnl_pct, pnl_r,
                    exit_reason, holding_days, gap_pct, filters_passed, year,
                    entry_time, exit_time, commission_model)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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

    def save_execution_diagnostics(self, exp_id: str, diagnostics: list[dict]) -> None:
        """Bulk-insert execution diagnostic records for an experiment."""
        if not diagnostics:
            return
        rows = [
            (
                exp_id,
                d.get("year"),
                d.get("signal_date"),
                d.get("symbol"),
                d.get("status"),
                d.get("reason"),
                d.get("entry_time"),
                d.get("entry_price"),
                d.get("initial_stop"),
                json.dumps(d.get("filters_json") or {}),
                bool(d.get("hold_quality_passed", False)),
                d.get("executed_exit_reason"),
                d.get("pnl_pct"),
                float(d.get("selection_score") or 0.0),
                int(d.get("selection_rank") or 0),
                json.dumps(d.get("selection_components_json") or {}),
            )
            for d in diagnostics
        ]
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.executemany(
                """INSERT INTO bt_execution_diagnostic
                   (exp_id, year, signal_date, symbol, status, reason,
                    entry_time, entry_price, initial_stop, filters_json,
                    hold_quality_passed, executed_exit_reason, pnl_pct,
                    selection_score, selection_rank, selection_components_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            self.con.execute("COMMIT")
        except Exception as e:
            self.con.execute("ROLLBACK")
            logger.error("Failed to save execution diagnostics: %s", e)
            raise

    def get_experiment(self, exp_id: str) -> dict | None:
        """Fetch a single experiment record."""
        row = self.con.execute("SELECT * FROM bt_experiment WHERE exp_id = ?", [exp_id]).fetchone()
        if not row:
            return None
        cols = [d[0] for d in self.con.description]
        return dict(zip(cols, row, strict=False))

    def list_experiments_for_wf_run_id(self, wf_run_id: str) -> list[dict[str, Any]]:
        """Return all experiments linked to a walk-forward run."""
        if "wf_run_id" not in self._table_columns("bt_experiment"):
            return []

        order_by = (
            "created_at DESC"
            if "created_at" in self._table_columns("bt_experiment")
            else "exp_id DESC"
        )
        result = self.con.execute(
            f"""SELECT *
                FROM bt_experiment
                WHERE wf_run_id = ?
                ORDER BY {order_by}""",
            [wf_run_id],
        )
        rows = result.fetchall()
        cols = [d[0] for d in result.description]
        return [dict(zip(cols, row, strict=False)) for row in rows]

    def get_experiment_cleanup_summary(self, exp_id: str) -> dict[str, Any] | None:
        """Return row counts for all DuckDB tables tied to one experiment."""
        exp = self.get_experiment(exp_id)
        if exp is None:
            return None

        counts_row = self.con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM bt_trade WHERE exp_id = ?) AS trade_rows,
                (SELECT COUNT(*) FROM bt_yearly_metric WHERE exp_id = ?) AS yearly_metric_rows,
                (SELECT COUNT(*) FROM bt_execution_diagnostic WHERE exp_id = ?) AS diagnostic_rows
            """,
            [exp_id, exp_id, exp_id],
        ).fetchone()
        trade_rows = int(counts_row[0] or 0) if counts_row else 0
        yearly_metric_rows = int(counts_row[1] or 0) if counts_row else 0
        diagnostic_rows = int(counts_row[2] or 0) if counts_row else 0
        return {
            "exp_id": exp_id,
            "wf_run_id": exp.get("wf_run_id"),
            "strategy_name": exp.get("strategy_name"),
            "status": exp.get("status"),
            "trade_rows": trade_rows,
            "yearly_metric_rows": yearly_metric_rows,
            "diagnostic_rows": diagnostic_rows,
            "experiment_rows": 1,
            "total_rows": 1 + trade_rows + yearly_metric_rows + diagnostic_rows,
        }

    def get_experiment_trades(self, exp_id: str) -> pl.DataFrame:
        """Fetch all trades for an experiment as a Polars DataFrame."""
        return self.con.execute(
            "SELECT * FROM bt_trade WHERE exp_id = ? ORDER BY entry_date", [exp_id]
        ).pl()

    def get_experiment_execution_diagnostics(self, exp_id: str) -> pl.DataFrame:
        """Fetch execution diagnostics for an experiment as a Polars DataFrame."""
        return self.con.execute(
            "SELECT * FROM bt_execution_diagnostic WHERE exp_id = ? ORDER BY signal_date, symbol",
            [exp_id],
        ).pl()

    def get_experiment_yearly_metrics(self, exp_id: str) -> pl.DataFrame:
        """Fetch yearly metrics for an experiment."""
        return self.con.execute(
            "SELECT * FROM bt_yearly_metric WHERE exp_id = ? ORDER BY year", [exp_id]
        ).pl()

    def list_experiments(self) -> pl.DataFrame:
        """List all experiments ordered by creation time."""
        return self._select_existing_columns(
            "bt_experiment",
            [
                "exp_id",
                "strategy_name",
                "params_json",
                "params_hash",
                "dataset_hash",
                "code_hash",
                "wf_run_id",
                "data_source",
                "start_year",
                "end_year",
                "total_return_pct",
                "annualized_return_pct",
                "total_trades",
                "win_rate_pct",
                "max_drawdown_pct",
                "status",
                "created_at",
            ],
            order_by="created_at DESC"
            if "created_at" in self._table_columns("bt_experiment")
            else "exp_id DESC",
        )

    def delete_experiment(self, exp_id: str) -> None:
        """Delete an experiment and its trades/metrics."""
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute("DELETE FROM bt_trade WHERE exp_id = ?", [exp_id])
            self.con.execute("DELETE FROM bt_yearly_metric WHERE exp_id = ?", [exp_id])
            self.con.execute("DELETE FROM bt_execution_diagnostic WHERE exp_id = ?", [exp_id])
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

            # Fast column migration: if only prior_breakdowns_90d is missing, add it
            # in a single CTAS pass from feat_daily (no parquet reads required).
            if n > 0:
                columns = self.con.execute("DESCRIBE feat_daily").fetchall()
                col_names = {c[0] for c in columns}
                base_required = {
                    "r2_65",
                    "atr_compress_ratio",
                    "range_percentile",
                    "vol_dryup_ratio",
                    "prior_breakouts_30d",
                    "prior_breakouts_90d",
                }
                if base_required.issubset(col_names) and "prior_breakdowns_90d" not in col_names:
                    logger.info(
                        "feat_daily missing prior_breakdowns_90d — running fast column migration "
                        "(%d rows, no parquet reads)...",
                        n,
                    )
                    # Transactional migration: keep existing feat_daily intact on any failure.
                    self.con.execute("BEGIN TRANSACTION")
                    try:
                        self.con.execute("DROP TABLE IF EXISTS feat_daily_migration")
                        self.con.execute("""
                            CREATE TABLE feat_daily_migration AS
                            SELECT *,
                                COALESCE(SUM(CASE WHEN ret_1d <= -0.04 THEN 1 ELSE 0 END) OVER (
                                    PARTITION BY symbol ORDER BY trading_date
                                    ROWS BETWEEN 89 PRECEDING AND 1 PRECEDING
                                ), 0)::INTEGER AS prior_breakdowns_90d
                            FROM feat_daily
                        """)
                        migrated_row = self.con.execute(
                            "SELECT COUNT(*) FROM feat_daily_migration"
                        ).fetchone()
                        migrated_n = int(migrated_row[0]) if migrated_row and migrated_row[0] else 0
                        if migrated_n != int(n):
                            raise RuntimeError(
                                f"feat_daily migration row-count mismatch: expected {n}, got {migrated_n}"
                            )

                        self.con.execute("DROP TABLE feat_daily")
                        self.con.execute("ALTER TABLE feat_daily_migration RENAME TO feat_daily")
                        self.con.execute(
                            "CREATE INDEX idx_feat_symbol_date ON feat_daily(symbol, trading_date)"
                        )
                        self._upsert_materialization_state(
                            table_name="feat_daily",
                            dataset_hash=dataset_hash,
                            query_version=FEAT_DAILY_QUERY_VERSION,
                            row_count=int(n),
                        )
                        self.register_dataset_snapshot(snapshot)
                        self.con.execute("COMMIT")
                    except Exception:
                        self.con.execute("ROLLBACK")
                        raise

                    logger.info("feat_daily fast migration complete (%d rows).", n)
                    return int(n)

                # All columns already present — just update state/version
                if {*base_required, "prior_breakdowns_90d"}.issubset(col_names):
                    self._upsert_materialization_state(
                        table_name="feat_daily",
                        dataset_hash=dataset_hash,
                        query_version=FEAT_DAILY_QUERY_VERSION,
                        row_count=int(n),
                    )
                    self.register_dataset_snapshot(snapshot)
                    logger.info("feat_daily state updated (%d rows).", n)
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
                    GREATEST(
                        high - low,
                        ABS(high - close_1d),
                        ABS(low - close_1d)
                    ) AS true_range,
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
                    OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 89 PRECEDING AND 1 PRECEDING) AS prior_breakouts_90d,
                    -- Count prior 4%+ down days in last 90 days (exhausted short filter)
                    SUM(CASE WHEN ret_1d <= -0.04 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 89 PRECEDING AND 1 PRECEDING) AS prior_breakdowns_90d
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
                prior_breakouts_90d,
                prior_breakdowns_90d
            FROM breakouts
        """)

    def build_all(
        self,
        force: bool = False,
        use_modular: bool = True,
        since_date: date | None = None,
    ) -> None:
        """Build all materialized tables.

        Args:
            force: Force rebuild even if up-to-date
            use_modular: Use new modular feature store (default: True).
                        Set False for legacy monolithic feat_daily behavior.
        """
        logger.info("Building materialized feature tables...")
        if use_modular:
            self._build_modular_features(force=force, since_date=since_date)
        else:
            self.build_feat_daily_table(force=force)
        if since_date is not None:
            self.build_market_monitor_incremental(since_date=since_date, force=force)
        else:
            self.build_market_monitor_table(force=force)
        logger.info("Done: market.duckdb is ready for backtesting.")

    def _build_modular_features(
        self,
        force: bool = False,
        since_date: date | None = None,
    ) -> None:
        """Build modular feature store (feat_daily_core, feat_intraday_core, etc.)."""
        from nse_momentum_lab.features import create_legacy_feat_daily_view
        from nse_momentum_lab.features.progress import FeatureBuildProgressReporter

        if since_date is not None and not force:
            progress = FeatureBuildProgressReporter()
            source_summary = self._summarize_feature_sources()
            progress.emit(
                stage="start",
                message=(
                    f"Starting incremental feature build since {since_date} ({source_summary})"
                ),
                status="running",
                progress_pct=0.0,
                step=0,
                step_total=4,
                pending_features=4,
            )
            try:
                daily_rows = self.build_feat_daily_core(force=force, since_date=since_date)
                progress.emit(
                    stage="feat_daily_core",
                    message=f"feat_daily_core rebuilt since {since_date}: {daily_rows:,} rows",
                    status="success",
                    progress_pct=25.0,
                    step=1,
                    step_total=4,
                    pending_features=3,
                    feature_name="feat_daily_core",
                    row_count=daily_rows,
                )
                intraday_rows = self.build_feat_intraday_core(
                    force=force,
                    since_date=since_date,
                    progress=progress,
                )
                progress.emit(
                    stage="feat_intraday_core",
                    message=(
                        f"feat_intraday_core rebuilt since {since_date}: {intraday_rows:,} rows"
                    ),
                    status="success",
                    progress_pct=50.0,
                    step=2,
                    step_total=4,
                    pending_features=2,
                    feature_name="feat_intraday_core",
                    row_count=intraday_rows,
                )
                event_rows = self.build_feat_event_core(force=force, since_date=since_date)
                progress.emit(
                    stage="feat_event_core",
                    message=f"feat_event_core rebuilt since {since_date}: {event_rows:,} rows",
                    status="success",
                    progress_pct=75.0,
                    step=3,
                    step_total=4,
                    pending_features=1,
                    feature_name="feat_event_core",
                    row_count=event_rows,
                )
                derived_rows = self.build_2lynch_derived(force=force, since_date=since_date)
                progress.emit(
                    stage="feat_2lynch_derived",
                    message=(
                        f"feat_2lynch_derived rebuilt since {since_date}: {derived_rows:,} rows"
                    ),
                    status="success",
                    progress_pct=100.0,
                    step=4,
                    step_total=4,
                    pending_features=0,
                    feature_name="feat_2lynch_derived",
                    row_count=derived_rows,
                )
                logger.info(
                    "Incremental feature build complete since %s: daily=%d intraday=%d event=%d derived=%d",
                    since_date,
                    daily_rows,
                    intraday_rows,
                    event_rows,
                    derived_rows,
                )
                progress.emit(
                    stage="complete",
                    message=(
                        f"Incremental feature build complete since {since_date}: "
                        f"daily={daily_rows} intraday={intraday_rows} event={event_rows} "
                        f"derived={derived_rows}"
                    ),
                    status="success",
                    progress_pct=100.0,
                    step=4,
                    step_total=4,
                    pending_features=0,
                )
            except Exception as exc:
                progress.emit(
                    stage="failed",
                    message=f"Incremental feature build failed since {since_date}: {exc}",
                    status="failed",
                    progress_pct=None,
                    step=None,
                    step_total=4,
                    pending_features=None,
                    error_message=str(exc),
                )
                raise
        else:
            from nse_momentum_lab.features import IncrementalFeatureMaterializer

            materializer = IncrementalFeatureMaterializer()
            summary = materializer.build_all(self.con, force=force, stop_on_error=False)

            logger.info(
                "Feature build complete: %d success, %d skipped, %d failed in %.1fs",
                summary.successful,
                summary.skipped,
                summary.failed,
                summary.total_duration_seconds,
            )

        # Create backward-compatible feat_daily view
        create_legacy_feat_daily_view(self.con)
        logger.info("Created backward-compatible feat_daily view")

    def build_feat_daily_core(
        self,
        force: bool = False,
        dataset_hash: str | None = None,
        since_date: date | None = None,
        symbols: list[str] | None = None,
    ) -> int:
        """Build feat_daily_core materialized table.

        This is the new modular feature store approach.
        Returns the row count of the built table.
        """
        from nse_momentum_lab.features.daily_core import build_feat_daily_core

        return build_feat_daily_core(
            self.con,
            force=force,
            dataset_hash=dataset_hash,
            since_date=since_date,
            symbols=symbols,
        )

    def build_feat_intraday_core(
        self,
        force: bool = False,
        since_date: date | None = None,
        symbols: list[str] | None = None,
        year_start: int | None = None,
        year_end: int | None = None,
        progress=None,
    ) -> int:
        """Build feat_intraday_core materialized table.

        Returns the row count of the built table.
        """
        from nse_momentum_lab.features.intraday_core import build_feat_intraday_core

        return build_feat_intraday_core(
            self.con,
            force=force,
            since_date=since_date,
            symbols=symbols,
            year_start=year_start,
            year_end=year_end,
            progress=progress,
        )

    def build_feat_event_core(
        self,
        force: bool = False,
        since_date: date | None = None,
    ) -> int:
        """Build feat_event_core materialized table (placeholder).

        Returns the row count of the built table.
        """
        from nse_momentum_lab.features.event_core import build_feat_event_core

        return build_feat_event_core(self.con, force=force, since_date=since_date)

    def build_2lynch_derived(
        self,
        force: bool = False,
        since_date: date | None = None,
        symbols: list[str] | None = None,
    ) -> int:
        """Build feat_2lynch_derived materialized table.

        Returns the row count of the built table.
        """
        from nse_momentum_lab.features.strategy_derived import build_2lynch_derived

        return build_2lynch_derived(self.con, force=force, since_date=since_date, symbols=symbols)

    def drop_and_rebuild(self, use_modular: bool = True) -> None:
        """Drop all materialized tables and rebuild from Parquet.

        Args:
            use_modular: Drop and rebuild modular feature store tables.
        """
        logger.info("Dropping and rebuilding all materialized tables...")

        if use_modular:
            # Drop new modular feature tables
            for table in [
                "feat_2lynch_derived",
                "feat_intraday_core",
                "feat_event_core",
                "feat_daily_core",
            ]:
                self.con.execute(f"DROP TABLE IF EXISTS {table}")
            self.build_all(force=True, use_modular=True)
        else:
            # Legacy behavior
            for table in ["feat_daily"]:
                self.con.execute(f"DROP TABLE IF EXISTS {table}")
            self.build_all(force=True, use_modular=False)

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
            """SELECT symbol, date AS trading_date, ret_1d, ret_5d, atr_20, range_pct,
                      close_pos_in_range, ma_20, ma_65, ma_7, ma_65_sma, rs_252, vol_20, dollar_vol_20
               FROM feat_daily WHERE symbol = ? AND date = ?""",
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

    def _market_monitor_select_sql(
        self,
        *,
        source_filter_sql: str = "",
        output_filter_sql: str = "",
    ) -> str:
        return f"""
            WITH symbol_base AS (
                SELECT
                    symbol,
                    trading_date,
                    close,
                    ma_20,
                    COUNT(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
                    ) AS ma_40_count,
                    AVG(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
                    ) AS ma_40,
                    atr_20,
                    vol_20,
                    dollar_vol_20,
                    ret_1d,
                    ret_5d,
                    atr_compress_ratio,
                    range_percentile_252 AS range_percentile,
                    MIN(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 64 PRECEDING AND CURRENT ROW
                    ) AS low_65,
                    MAX(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 64 PRECEDING AND CURRENT ROW
                    ) AS high_65,
                    MIN(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS low_20,
                    MAX(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS high_20,
                    MIN(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 33 PRECEDING AND CURRENT ROW
                    ) AS low_34,
                    MAX(close) OVER (
                        PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 33 PRECEDING AND CURRENT ROW
                    ) AS high_34
                FROM feat_daily_core
                {source_filter_sql}
            ),
            eligible AS (
                SELECT
                    *,
                    CASE
                        WHEN close >= 10
                         AND dollar_vol_20 >= 3000000
                         AND ma_40_count = 40
                         AND ma_20 IS NOT NULL
                         AND atr_20 IS NOT NULL
                        THEN TRUE
                        ELSE FALSE
                    END AS is_eligible
                FROM symbol_base
            ),
            daily_raw AS (
                SELECT
                    trading_date,
                    COUNT(*) FILTER (WHERE is_eligible) AS universe_size,
                    COUNT(*) FILTER (WHERE is_eligible AND ret_1d >= 0.04) AS up_4pct_count,
                    COUNT(*) FILTER (WHERE is_eligible AND ret_1d <= -0.04) AS down_4pct_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close > ma_20) AS pct_above_ma20_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close <= ma_20) AS pct_below_ma20_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close > ma_40) AS pct_above_ma40_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close <= ma_40) AS pct_below_ma40_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close >= 1.25 * low_65) AS up_25q_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close <= 0.75 * high_65) AS down_25q_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close >= 1.25 * low_20) AS up_25m_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close <= 0.75 * high_20) AS down_25m_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close >= 1.50 * low_20) AS up_50m_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close <= 0.50 * high_20) AS down_50m_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close >= 1.13 * low_34) AS up_13_34_count,
                    COUNT(*) FILTER (WHERE is_eligible AND close <= 0.87 * high_34) AS down_13_34_count
                FROM eligible
                GROUP BY trading_date
            ),
            daily_windowed AS (
                SELECT
                    trading_date,
                    universe_size,
                    up_4pct_count,
                    down_4pct_count,
                    CASE WHEN universe_size > 0 THEN 100.0 * up_4pct_count / universe_size ELSE NULL END AS up_4pct_pct,
                    CASE WHEN universe_size > 0 THEN 100.0 * down_4pct_count / universe_size ELSE NULL END AS down_4pct_pct,
                    CASE
                        WHEN SUM(down_4pct_count) OVER (
                            ORDER BY trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                        ) > 0
                        THEN SUM(up_4pct_count) OVER (
                            ORDER BY trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                        )::DOUBLE / NULLIF(
                            SUM(down_4pct_count) OVER (
                                ORDER BY trading_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                            ), 0
                        )
                        ELSE NULL
                    END AS ratio_5d,
                    CASE
                        WHEN SUM(down_4pct_count) OVER (
                            ORDER BY trading_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                        ) > 0
                        THEN SUM(up_4pct_count) OVER (
                            ORDER BY trading_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                        )::DOUBLE / NULLIF(
                            SUM(down_4pct_count) OVER (
                                ORDER BY trading_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                            ), 0
                        )
                        ELSE NULL
                    END AS ratio_10d,
                    up_25q_count,
                    down_25q_count,
                    CASE WHEN universe_size > 0 THEN 100.0 * up_25q_count / universe_size ELSE NULL END AS up_25q_pct,
                    CASE WHEN universe_size > 0 THEN 100.0 * down_25q_count / universe_size ELSE NULL END AS down_25q_pct,
                    CASE WHEN universe_size > 0 THEN 100.0 * pct_above_ma40_count / universe_size ELSE NULL END AS pct_above_ma40,
                    CASE WHEN universe_size > 0 THEN 100.0 * pct_above_ma40_count / universe_size ELSE NULL END AS t2108_equivalent_pct,
                    CASE WHEN universe_size > 0 THEN 100.0 * pct_below_ma40_count / universe_size ELSE NULL END AS pct_below_ma40,
                    up_25m_count,
                    down_25m_count,
                    up_50m_count,
                    down_50m_count,
                    up_13_34_count,
                    down_13_34_count,
                    CASE WHEN universe_size > 0 THEN 100.0 * pct_above_ma20_count / universe_size ELSE NULL END AS pct_above_ma20,
                    CASE WHEN universe_size > 0 THEN 100.0 * pct_below_ma20_count / universe_size ELSE NULL END AS pct_below_ma20
                FROM daily_raw
            ),
            classified AS (
                SELECT
                    *,
                    CASE
                        WHEN up_25q_count > down_25q_count * 1.1 THEN 'bullish'
                        WHEN down_25q_count > up_25q_count * 1.1 THEN 'bearish'
                        ELSE 'transition'
                    END AS primary_regime,
                    CASE
                        WHEN up_25q_count > down_25q_count * 1.1 AND ratio_10d >= 2.0 THEN 'long_favored'
                        WHEN down_25q_count > up_25q_count * 1.1 AND ratio_10d <= 0.5 THEN 'short_favored'
                        WHEN down_25q_count > up_25q_count * 1.1 AND ratio_10d > 0.5 THEN 'rebound_watch'
                        WHEN up_25q_count > down_25q_count * 1.1 AND ratio_10d < 2.0 THEN 'correction_watch'
                        ELSE 'mixed'
                    END AS tactical_regime,
                    CASE
                        WHEN up_25q_count > down_25q_count * 1.1 AND ratio_10d >= 2.0 THEN 2.0
                        WHEN down_25q_count > up_25q_count * 1.1 AND ratio_10d <= 0.5 THEN -2.0
                        WHEN up_25q_count > down_25q_count * 1.1 THEN 1.0
                        WHEN down_25q_count > up_25q_count * 1.1 THEN -1.0
                        ELSE 0.0
                    END AS aggression_score,
                    CASE
                        WHEN up_25q_count > down_25q_count * 1.1 AND ratio_10d >= 2.0 THEN 'aggressive'
                        WHEN down_25q_count > up_25q_count * 1.1 AND ratio_10d <= 0.5 THEN 'aggressive'
                        WHEN up_25q_count > down_25q_count * 1.1 OR down_25q_count > up_25q_count * 1.1 THEN 'standard'
                        ELSE 'defensive'
                    END AS posture_label,
                    CASE
                        WHEN up_25q_count > down_25q_count * 1.1 AND ratio_10d >= 2.0 THEN
                            '["bullish_thrust","long_favored"]'
                        WHEN down_25q_count > up_25q_count * 1.1 AND ratio_10d <= 0.5 THEN
                            '["bearish_thrust","short_favored"]'
                        WHEN up_25q_count > down_25q_count * 1.1 AND ratio_10d < 2.0 THEN
                            '["bullish_regime","correction_watch"]'
                        WHEN down_25q_count > up_25q_count * 1.1 AND ratio_10d > 0.5 THEN
                            '["bearish_regime","rebound_watch"]'
                        ELSE '["mixed"]'
                    END AS alert_flags_json
                FROM daily_windowed
            )
            SELECT
                trading_date,
                universe_size,
                up_4pct_count,
                down_4pct_count,
                up_4pct_pct,
                down_4pct_pct,
                ratio_5d,
                ratio_10d,
                pct_above_ma40,
                t2108_equivalent_pct,
                pct_below_ma40,
                up_25q_count,
                down_25q_count,
                up_25q_pct,
                down_25q_pct,
                up_25m_count,
                down_25m_count,
                up_50m_count,
                down_50m_count,
                up_13_34_count,
                down_13_34_count,
                pct_above_ma20,
                pct_below_ma20,
                primary_regime,
                tactical_regime,
                aggression_score,
                posture_label,
                alert_flags_json
            FROM classified
            {output_filter_sql}
            ORDER BY trading_date
        """

    def _market_monitor_incremental_lookback_date(self, rebuild_start_date: date) -> date:
        row = self.con.execute(
            """
            WITH recent_sessions AS (
                SELECT DISTINCT trading_date
                FROM feat_daily_core
                WHERE trading_date < ?
                ORDER BY trading_date DESC
                LIMIT ?
            )
            SELECT MIN(trading_date) FROM recent_sessions
            """,
            [rebuild_start_date, MARKET_MONITOR_INCREMENTAL_LOOKBACK_SESSIONS],
        ).fetchone()
        if row and row[0] is not None:
            return row[0]
        return rebuild_start_date

    def build_market_monitor_table(self, force: bool = False) -> int:
        """Materialize the Market Monitor daily breadth table."""
        if self._read_only:
            logger.info("Skipping market_monitor_daily build in read-only mode.")
            return 0

        snapshot = self.get_dataset_snapshot()
        dataset_hash = str(snapshot["dataset_hash"])
        daily_hash = self._snapshot_component_hash(snapshot.get("daily"))

        self.build_feat_daily_core(force=force, dataset_hash=daily_hash)

        if not force and self._table_exists("market_monitor_daily"):
            row = self.con.execute("SELECT COUNT(*) FROM market_monitor_daily").fetchone()
            n = int(row[0]) if row and row[0] is not None else 0
            state = self._get_materialization_state("market_monitor_daily")
            if (
                n > 0
                and state is not None
                and state["dataset_hash"] == dataset_hash
                and state["query_version"] == MARKET_MONITOR_QUERY_VERSION
            ):
                logger.info("market_monitor_daily is up-to-date (%d rows).", n)
                return n

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS market_monitor_daily (
                trading_date        DATE PRIMARY KEY,
                universe_size       INTEGER,
                up_4pct_count       INTEGER,
                down_4pct_count     INTEGER,
                up_4pct_pct         DOUBLE,
                down_4pct_pct       DOUBLE,
                ratio_5d            DOUBLE,
                ratio_10d           DOUBLE,
                pct_above_ma40      DOUBLE,
                t2108_equivalent_pct DOUBLE,
                pct_below_ma40      DOUBLE,
                up_25q_count        INTEGER,
                down_25q_count      INTEGER,
                up_25q_pct          DOUBLE,
                down_25q_pct        DOUBLE,
                up_25m_count        INTEGER,
                down_25m_count      INTEGER,
                up_50m_count        INTEGER,
                down_50m_count      INTEGER,
                up_13_34_count      INTEGER,
                down_13_34_count    INTEGER,
                pct_above_ma20      DOUBLE,
                pct_below_ma20      DOUBLE,
                primary_regime      VARCHAR,
                tactical_regime     VARCHAR,
                aggression_score    DOUBLE,
                posture_label       VARCHAR,
                alert_flags_json    VARCHAR DEFAULT '[]'
            )
        """)

        if not self._table_exists("feat_daily_core"):
            logger.warning("feat_daily_core is missing; created empty market_monitor_daily table.")
            row = self.con.execute("SELECT COUNT(*) FROM market_monitor_daily").fetchone()
            return int(row[0] or 0) if row else 0

        self.con.execute("DROP TABLE IF EXISTS market_monitor_daily")
        self.con.execute(
            f"""
            CREATE TABLE market_monitor_daily AS
            {self._market_monitor_select_sql()}
            """
        )
        row = self.con.execute("SELECT COUNT(*) FROM market_monitor_daily").fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        self._upsert_materialization_state(
            table_name="market_monitor_daily",
            dataset_hash=dataset_hash,
            query_version=MARKET_MONITOR_QUERY_VERSION,
            row_count=n,
        )
        self.register_dataset_snapshot(snapshot)
        logger.info("market_monitor_daily built: %d rows", n)
        return n

    def _summarize_feature_sources(self) -> str:
        """Summarize active source views for operator progress messages."""
        parts: list[str] = []
        for view_name, label in (("v_daily", "daily"), ("v_5min", "5min")):
            try:
                row = self.con.execute(
                    f"""
                    SELECT
                        COUNT(*)::BIGINT AS rows,
                        COUNT(DISTINCT symbol)::BIGINT AS symbols
                    FROM {view_name}
                    """
                ).fetchone()
                if row:
                    parts.append(
                        f"{label}={int(row[1]) if row[1] is not None else 0:,} symbols/"
                        f"{int(row[0]) if row[0] is not None else 0:,} rows"
                    )
            except Exception:
                continue
        return ", ".join(parts) if parts else "sources unavailable"

    def build_market_monitor_incremental(
        self,
        since_date: date | None = None,
        *,
        force: bool = False,
    ) -> int:
        """Incrementally update market_monitor_daily from a specified date.

        Args:
            since_date: Inclusive date to rebuild from. If None, rebuilds only dates
                after the latest existing row.
            force: Force feat_daily_core refresh before updating market monitor.

        Returns:
            Number of rows in the table after update.
        """
        if self._read_only:
            logger.info("Skipping market_monitor_daily incremental build in read-only mode.")
            return 0

        if not self._table_exists("market_monitor_daily"):
            logger.info("market_monitor_daily does not exist; running full build.")
            return self.build_market_monitor_table(force=force)

        snapshot = self.get_dataset_snapshot()
        dataset_hash = str(snapshot["dataset_hash"])
        daily_hash = self._snapshot_component_hash(snapshot.get("daily"))
        self.build_feat_daily_core(
            force=force,
            dataset_hash=daily_hash,
            since_date=since_date,
        )

        # Determine the first trading date that needs to be rebuilt.
        if since_date is None:
            max_date_result = self.con.execute(
                "SELECT MAX(trading_date) AS max_date FROM market_monitor_daily"
            ).fetchone()
            if not max_date_result or max_date_result[0] is None:
                logger.info("market_monitor_daily is empty; running full build.")
                return self.build_market_monitor_table(force=force)
            rebuild_start_date = max_date_result[0] + timedelta(days=1)
        else:
            rebuild_start_date = since_date

        if not self._table_exists("feat_daily_core"):
            logger.info("feat_daily_core is missing; building it first.")
            self.build_feat_daily_core(
                force=force,
                dataset_hash=daily_hash,
                since_date=since_date,
            )

        lookback_date = self._market_monitor_incremental_lookback_date(rebuild_start_date)
        logger.info(
            "Incremental market_monitor_daily build: rebuild from %s using %d-session context back to %s",
            rebuild_start_date,
            MARKET_MONITOR_INCREMENTAL_LOOKBACK_SESSIONS,
            lookback_date,
        )

        self.con.execute(
            """
            DELETE FROM market_monitor_daily
            WHERE trading_date >= ?
            """,
            [rebuild_start_date],
        )
        self.con.execute(
            f"""
            INSERT INTO market_monitor_daily
            {
                self._market_monitor_select_sql(
                    source_filter_sql="WHERE trading_date >= ?",
                    output_filter_sql="WHERE trading_date >= ?",
                )
            }
            """,
            [lookback_date, rebuild_start_date],
        )

        row = self.con.execute("SELECT COUNT(*) FROM market_monitor_daily").fetchone()
        n = int(row[0]) if row and row[0] is not None else 0

        self._upsert_materialization_state(
            table_name="market_monitor_daily",
            dataset_hash=dataset_hash,
            query_version=MARKET_MONITOR_QUERY_VERSION,
            row_count=n,
        )
        self.register_dataset_snapshot(snapshot)
        logger.info("market_monitor_daily incremental update complete: %d rows", n)
        return n

    def get_market_monitor_latest(self) -> pl.DataFrame:
        """Return the latest Market Monitor snapshot, or an empty frame."""
        if not self._table_exists("market_monitor_daily"):
            return pl.DataFrame()
        try:
            return self.con.execute(
                "SELECT * FROM market_monitor_daily ORDER BY trading_date DESC LIMIT 1"
            ).pl()
        except duckdb.CatalogException:
            return pl.DataFrame()
        except Exception as exc:
            logger.warning("Failed to load latest market monitor row: %s", exc)
            return pl.DataFrame()

    def get_market_monitor_history(self, days: int = 252) -> pl.DataFrame:
        """Return recent Market Monitor history, or an empty frame."""
        if not self._table_exists("market_monitor_daily"):
            return pl.DataFrame()

        limit = max(int(days or 0), 1)
        try:
            return self.con.execute(
                "SELECT * FROM market_monitor_daily ORDER BY trading_date DESC LIMIT ?",
                [limit],
            ).pl()
        except duckdb.CatalogException:
            return pl.DataFrame()
        except Exception as exc:
            logger.warning("Failed to load market monitor history: %s", exc)
            return pl.DataFrame()

    def get_market_monitor_all(self) -> pl.DataFrame:
        """Return ALL Market Monitor history without limit, or an empty frame."""
        if not self._table_exists("market_monitor_daily"):
            return pl.DataFrame()

        try:
            return self.con.execute(
                "SELECT * FROM market_monitor_daily ORDER BY trading_date DESC",
            ).pl()
        except duckdb.CatalogException:
            return pl.DataFrame()
        except Exception as exc:
            logger.warning("Failed to load all market monitor data: %s", exc)
            return pl.DataFrame()

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
                WHERE symbol IN ({placeholders}) AND date >= ? AND date <= ?
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
            "feat_daily_core",
            "feat_intraday_core",
            "feat_event_core",
            "feat_2lynch_derived",
            "feat_daily",
            "market_monitor_daily",
            "bt_experiment",
            "bt_trade",
            "bt_yearly_metric",
            "bt_dataset_snapshot",
            "bt_materialization_state",
            "data_quality_issues",
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

    def get_feature_status(self) -> dict:
        """Get status of all feature sets from the feature registry."""
        from nse_momentum_lab.features import get_feature_registry
        from nse_momentum_lab.features.materializer import IncrementalFeatureMaterializer

        registry = get_feature_registry()
        materializer = IncrementalFeatureMaterializer(registry)

        features = {}
        for feat_def in registry.list_all():
            state = materializer.get_feature_state(self.con, feat_def.name)
            if state:
                features[feat_def.name] = {
                    "name": feat_def.name,
                    "version": feat_def.version,
                    "layer": feat_def.layer,
                    "row_count": state.row_count,
                    "min_date": state.min_date.isoformat() if state.min_date else None,
                    "max_date": state.max_date.isoformat() if state.max_date else None,
                    "status": state.status,
                }
            else:
                features[feat_def.name] = {
                    "name": feat_def.name,
                    "version": feat_def.version,
                    "layer": feat_def.layer,
                    "status": "not_built",
                }

        return {
            "features": features,
            "total_count": len(features),
        }

    def close(self) -> None:
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


_market_db: MarketDataDB | None = None
_backtest_db: MarketDataDB | None = None


def get_market_db(read_only: bool = False) -> MarketDataDB:
    """Return the global MarketDataDB instance (creates on first call).

    Pass read_only=True for read-only consumers (e.g. dashboard) so they can
    coexist with a running backtest writer without hitting the DuckDB lock.
    """
    global _market_db
    if _market_db is None:
        market_path = Path(os.getenv("DUCKDB_PATH", str(DUCKDB_FILE)))
        _market_db = MarketDataDB(db_path=market_path, read_only=read_only)
    return _market_db


def get_backtest_db(read_only: bool = False) -> MarketDataDB:
    """Return the global Backtest DuckDB instance.

    Backtest results are persisted to a dedicated catalog to avoid contention
    with dashboard reads of the market catalog.
    """
    global _backtest_db
    if _backtest_db is None:
        env_name = "BACKTEST_DUCKDB_PATH"
        default_path = BACKTEST_DUCKDB_FILE
        backtest_path = Path(os.getenv(env_name, str(default_path)))
        _backtest_db = MarketDataDB(db_path=backtest_path, read_only=read_only)
    return _backtest_db


def close_market_db() -> None:
    global _market_db
    if _market_db is not None:
        _market_db.close()
        _market_db = None


def close_backtest_db() -> None:
    global _backtest_db
    if _backtest_db is not None:
        _backtest_db.close()
        _backtest_db = None
