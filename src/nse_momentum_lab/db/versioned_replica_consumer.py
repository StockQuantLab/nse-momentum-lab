"""Versioned replica consumer for DuckDB databases.

Reads the pointer file produced by :class:`VersionedReplicaSync` to find the
latest version, then opens a read-only DuckDB connection to that version file.
When the writer creates a new version, the consumer detects the change via
the pointer file and reconnects transparently.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


class VersionedReplicaConsumer:
    """Read-only consumer for versioned DuckDB replicas.

    Usage from the dashboard process::

        consumer = VersionedReplicaConsumer(
            replica_dir=Path("data/backtest_replica"),
            prefix="backtest_replica",
            fallback_path=Path("data/backtest.duckdb"),
        )
        conn = consumer.get_connection()
        rows = conn.execute("SELECT * FROM bt_experiment").fetchall()

    When no versioned replica exists yet (first start), the consumer falls
    back to *fallback_path* opened in read-only mode, ensuring the dashboard
    works during the migration period.
    """

    def __init__(
        self,
        *,
        replica_dir: Path,
        prefix: str,
        ttl_seconds: float = 30.0,
        fallback_path: Path | None = None,
    ) -> None:
        self._replica_dir = Path(replica_dir)
        self._prefix = prefix
        self._ttl_seconds = ttl_seconds
        self._fallback_path = Path(fallback_path) if fallback_path else None

        self._conn: duckdb.DuckDBPyConnection | None = None
        self._conn_version: int | None = None
        self._conn_opened_at: float = 0.0
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_connection(self) -> duckdb.DuckDBPyConnection | None:
        """Get a read-only connection to the latest replica version.

        Checks the pointer file for a version change and reconnects if needed.
        Returns *None* if neither a replica nor a fallback is available.
        """
        with self._lock:
            self._reconnect_if_stale()
            return self._conn

    def execute(self, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]] | None:
        """Execute SQL against the replica; returns rows as list of dicts.

        Returns *None* if the replica is unavailable.
        """
        conn = self.get_connection()
        if conn is None:
            return None
        try:
            result = conn.execute(sql, parameters) if parameters else conn.execute(sql)
            desc = result.description
            rows = result.fetchall()
            if not desc:
                return []
            col_names = [d[0] for d in desc]
            return [dict(zip(col_names, row, strict=False)) for row in rows]
        except Exception as exc:
            logger.warning("Replica query failed: %s", exc)
            self._close_conn()
            return None

    def get_stale_seconds(self) -> float:
        """Return seconds since the current replica file was last modified."""
        path = self._current_version_path()
        if path is None:
            return float("inf")
        try:
            return time.time() - path.stat().st_mtime
        except OSError:
            return float("inf")

    def close(self) -> None:
        """Clean up the held connection."""
        with self._lock:
            self._close_conn()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _pointer_path(self) -> Path:
        return self._replica_dir / f"{self._prefix}_latest"

    def _version_path(self, version: int) -> Path:
        return self._replica_dir / f"{self._prefix}_v{version}.duckdb"

    def _read_pointer_version(self) -> int | None:
        """Read the pointer file and return the version number, or None."""
        pointer = self._pointer_path()
        try:
            text = pointer.read_text().strip()
            if text.startswith("v"):
                return int(text[1:])
            return int(text)
        except OSError, ValueError:
            return None

    def _scan_latest_version(self) -> int | None:
        """Scan the replica directory for the highest version file."""
        best: int | None = None
        try:
            for p in self._replica_dir.glob(f"{self._prefix}_v*.duckdb"):
                try:
                    v_str = p.stem.split("_v")[-1]
                    v = int(v_str)
                    if best is None or v > best:
                        best = v
                except ValueError, IndexError:
                    pass
        except OSError:
            pass
        return best

    def _current_version_path(self) -> Path | None:
        """Return the path of the current version file, or None."""
        if self._conn_version is not None:
            return self._version_path(self._conn_version)
        return None

    def _reconnect_if_stale(self) -> None:
        """Check for version changes and reconnect if needed. Must be called under lock."""
        latest_version = self._read_pointer_version()

        if latest_version is None:
            latest_version = self._scan_latest_version()

        # If we have a versioned replica, check if it changed.
        if latest_version is not None:
            if latest_version != self._conn_version:
                logger.debug(
                    "Replica version changed: %s -> %s, reconnecting",
                    self._conn_version,
                    latest_version,
                )
                self._close_conn()
                path = self._version_path(latest_version)
                if path.exists():
                    try:
                        self._conn = duckdb.connect(str(path), read_only=True)
                        self._conn_version = latest_version
                        self._conn_opened_at = time.monotonic()
                    except Exception as exc:
                        logger.warning("Failed to open replica v%d: %s", latest_version, exc)
                return

        # No versioned replica available — try fallback.
        if self._conn is None and self._fallback_path is not None:
            if self._fallback_path.exists():
                try:
                    self._conn = duckdb.connect(str(self._fallback_path), read_only=True)
                    self._conn_version = None
                    self._conn_opened_at = time.monotonic()
                    logger.debug("Opened fallback DB: %s", self._fallback_path)
                except Exception as exc:
                    logger.warning("Failed to open fallback %s: %s", self._fallback_path, exc)
            return

        # TTL refresh: close and reopen if TTL expired.
        if self._conn is not None and self._conn_version is not None:
            age = time.monotonic() - self._conn_opened_at
            if age >= self._ttl_seconds:
                version = self._conn_version
                self._close_conn()
                path = self._version_path(version)
                if path.exists():
                    try:
                        self._conn = duckdb.connect(str(path), read_only=True)
                        self._conn_version = version
                        self._conn_opened_at = time.monotonic()
                    except Exception:
                        pass

    def _close_conn(self) -> None:
        """Close the current connection if open. Must be called under lock."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            finally:
                self._conn = None
                self._conn_version = None
                self._conn_opened_at = 0.0
