"""Dashboard-side reader for paper trading DuckDB replica.

Provides read-only access to the snapshot file produced by ReplicaSync.
The dashboard process opens this file independently -- it never conflicts
with the engine's DuckDB because they operate on different files.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


class ReplicaConsumer:
    """Read-only consumer for dashboard to access paper trading data.

    Usage from the dashboard process::

        consumer = ReplicaConsumer(Path("data/paper_dashboard.duckdb"))
        conn = consumer.get_connection()
        rows = conn.execute("SELECT * FROM paper_session").fetchall()
        # conn stays open -- reused on next call until TTL expires.
        ...
        consumer.close()  # on shutdown

    The connection is opened in read-only mode so it never locks the file
    against the engine's next ReplicaSync cycle.  DuckDB allows multiple
    readers on the same file as long as no writer holds it.
    """

    def __init__(
        self,
        dashboard_path: Path,
        ttl_seconds: float = 30.0,
    ) -> None:
        self._path = Path(dashboard_path)
        self._ttl_seconds = ttl_seconds

        self._conn: duckdb.DuckDBPyConnection | None = None
        self._conn_opened_at: float = 0.0
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_connection(self) -> duckdb.DuckDBPyConnection | None:
        """Get a read-only connection, reconnecting if TTL expired.

        Returns *None* if the replica file is missing or cannot be opened
        (e.g. first run before the engine has created a snapshot, or file
        is corrupted).
        """
        with self._lock:
            # Reconnect if TTL has expired.
            if self._conn is not None:
                age = time.monotonic() - self._conn_opened_at
                if age >= self._ttl_seconds:
                    self._close_conn()

            if self._conn is not None:
                return self._conn

            if not self._path.exists():
                logger.debug(
                    "Paper dashboard replica not found at %s -- engine has not "
                    "created it yet or first run",
                    self._path,
                )
                return None

            try:
                self._conn = duckdb.connect(str(self._path), read_only=True)
                self._conn_opened_at = time.monotonic()
            except Exception as exc:
                logger.warning(
                    "Failed to open paper dashboard replica at %s: %s",
                    self._path,
                    exc,
                )
                return None

            return self._conn

    def get_stale_seconds(self) -> float:
        """Return seconds since the replica file was last modified.

        Returns *float('inf')* if the file does not exist, so callers can
        uniformly treat a missing file as maximally stale.
        """
        try:
            mtime = self._path.stat().st_mtime
            return time.time() - mtime
        except OSError:
            return float("inf")

    def execute(self, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]] | None:
        """Execute SQL against the replica; returns rows as list of dicts.

        Returns *None* if the replica is unavailable.
        """
        conn = self.get_connection()
        if conn is None:
            return None
        try:
            if parameters:
                result = conn.execute(sql, parameters)
            else:
                result = conn.execute(sql)
            desc = result.description
            rows = result.fetchall()
            if not desc:
                return []
            col_names = [d[0] for d in desc]
            return [dict(zip(col_names, row, strict=False)) for row in rows]
        except Exception as exc:
            logger.warning("Replica query failed: %s", exc)
            # Connection may be stale; force reconnect on next call.
            self._close_conn()
            return None

    def close(self) -> None:
        """Clean up the held connection."""
        with self._lock:
            self._close_conn()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _close_conn(self) -> None:
        """Close the current connection if open. Must be called under lock."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                logger.debug("Error closing replica connection: %s", exc)
            finally:
                self._conn = None
                self._conn_opened_at = 0.0
