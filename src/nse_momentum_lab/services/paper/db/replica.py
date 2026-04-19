"""Engine-side snapshot writer for paper trading DuckDB replica.

DuckDB uses exclusive file locking, so the dashboard cannot read the same file
the live engine is writing to. This module creates periodic point-in-time
snapshots that the dashboard consumes via ReplicaConsumer.

Phase 1 approach: ATTACH + CREATE OR REPLACE TABLE per tracked table, matching
the pattern in market_db.py refresh_backtest_read_snapshot().
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Sequence
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# Tables that will be replicated from the source paper trading DuckDB.
# Extend this list as new paper-trading DuckDB tables are added.
DEFAULT_PAPER_TABLES: Sequence[str] = (
    "paper_sessions",
    "paper_signals",
    "paper_positions",
    "paper_orders",
    "paper_fills",
    "paper_feed_state",
    "paper_session_signals",
    "paper_bar_checkpoints",
    "alert_log",
)

# Alias used inside ATTACH statements.
_ATTACH_ALIAS = "paper_replica"


class ReplicaSync:
    """Creates point-in-time snapshots of paper.duckdb for dashboard reads.

    Usage from the live engine::

        sync = ReplicaSync(
            source_path=Path("data/paper.duckdb"),
            dashboard_path=Path("data/paper_dashboard.duckdb"),
        )
        # After writing to paper.duckdb ...
        sync.mark_dirty()
        sync.maybe_sync(source_conn=duckdb_conn)
    """

    def __init__(
        self,
        source_path: Path,
        dashboard_path: Path,
        tables: Sequence[str] | None = None,
        min_interval_sec: float = 5.0,
    ) -> None:
        self._source_path = Path(source_path)
        self._dashboard_path = Path(dashboard_path)
        self._tables = list(tables or DEFAULT_PAPER_TABLES)
        self._min_interval_sec = min_interval_sec

        self._dirty = False
        self._last_sync_time: float = 0.0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def mark_dirty(self) -> None:
        """Mark that the source DB has been written to since the last sync."""
        with self._lock:
            self._dirty = True

    def maybe_sync(self, source_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        """Sync if dirty and enough time has passed since the last sync.

        Parameters
        ----------
        source_conn:
            An existing read-write DuckDB connection to the source database.
            If *None*, a short-lived connection is opened and closed internally.
            Passing the engine's own connection avoids opening a second writer
            to the same file (DuckDB single-writer constraint).
        """
        with self._lock:
            if not self._dirty:
                return
            elapsed = time.monotonic() - self._last_sync_time
            if elapsed < self._min_interval_sec:
                return
            self._do_sync(source_conn)

    def force_sync(self, source_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        """Force immediate sync regardless of debounce timer or dirty flag."""
        with self._lock:
            self._do_sync(source_conn)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _do_sync(self, source_conn: duckdb.DuckDBPyConnection | None) -> None:
        """Perform the actual ATTACH + CREATE OR REPLACE TABLE sync."""
        self._last_sync_time = time.monotonic()

        self._dashboard_path.parent.mkdir(parents=True, exist_ok=True)

        # Guard against source == target (would corrupt data).
        try:
            if self._dashboard_path.resolve() == self._source_path.resolve():
                logger.warning(
                    "Replica sync skipped: dashboard path equals source path (%s)",
                    self._dashboard_path,
                )
                return
        except OSError:
            pass

        own_conn = source_conn is None
        if own_conn:
            try:
                source_conn = duckdb.connect(str(self._source_path))
            except Exception as exc:
                logger.error("Failed to open source DuckDB for replica sync: %s", exc)
                return

        assert source_conn is not None  # for type narrowing

        # Windows path handling: DuckDB wants forward slashes and escaped quotes.
        escaped_path = str(self._dashboard_path).replace("\\", "/").replace("'", "''")

        try:
            source_conn.execute(f"ATTACH '{escaped_path}' AS {_ATTACH_ALIAS}")
        except Exception as exc:
            logger.warning(
                "Skipping paper dashboard snapshot because %s is locked: %s",
                self._dashboard_path,
                exc,
            )
            if own_conn:
                source_conn.close()
            return

        try:
            self._sync_tables(source_conn)
        finally:
            try:
                source_conn.execute(f"DETACH {_ATTACH_ALIAS}")
            except Exception as exc:
                logger.warning("Failed to detach %s after snapshot: %s", _ATTACH_ALIAS, exc)
            if own_conn:
                source_conn.close()

        # Clear dirty flag only after a successful sync cycle.
        self._dirty = False
        logger.debug(
            "Paper replica sync completed -> %s (%d tables)",
            self._dashboard_path,
            len(self._tables),
        )

    def _sync_tables(self, source_conn: duckdb.DuckDBPyConnection) -> None:
        """Copy each tracked table from source to the attached replica."""
        available_tables = {
            row[0]
            for row in source_conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }

        synced = 0
        for table in self._tables:
            if table not in available_tables:
                continue
            try:
                source_conn.execute(
                    f"CREATE OR REPLACE TABLE {_ATTACH_ALIAS}.{table} AS SELECT * FROM {table}"
                )
                synced += 1
            except Exception as exc:
                logger.warning("Failed to sync table %s to replica: %s", table, exc)

        if synced == 0:
            logger.warning(
                "No tables were synced to paper replica (checked %d tables, %d present in source)",
                len(self._tables),
                len(available_tables & set(self._tables)),
            )
