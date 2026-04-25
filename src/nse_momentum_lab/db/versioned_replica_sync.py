"""Versioned replica sync for DuckDB databases.

Creates versioned point-in-time copies of a source DuckDB database for
concurrent read access by the dashboard process.  Each sync creates a new
versioned file (e.g., ``backtest_replica_v3.duckdb``) and atomically updates
a pointer file that consumers read to find the latest version.

This solves Windows file-locking: DuckDB's exclusive write lock prevents the
dashboard from reading the same file the engine is writing to.  Versioned
replicas let the writer and reader operate on entirely different files,
coordinating only via a lightweight pointer file.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Sequence
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# Backtest tables replicated from backtest.duckdb
DEFAULT_BACKTEST_TABLES: Sequence[str] = (
    "bt_experiment",
    "bt_yearly_metric",
    "bt_trade",
    "bt_execution_diagnostic",
)

# Paper trading tables replicated from paper.duckdb
DEFAULT_PAPER_TABLES: Sequence[str] = (
    "paper_sessions",
    "paper_signals",
    "paper_positions",
    "paper_orders",
    "paper_fills",
    "paper_feed_state",
    "paper_feed_audit",
    "paper_session_signals",
    "paper_bar_checkpoints",
    "alert_log",
)


class VersionedReplicaSync:
    """Creates versioned point-in-time replicas of a DuckDB database.

    Usage from the writer process::

        sync = VersionedReplicaSync(
            source_path=Path("data/backtest.duckdb"),
            replica_dir=Path("data/backtest_replica"),
            prefix="backtest_replica",
            tables=["bt_experiment", "bt_trade"],
        )
        # After writing to backtest.duckdb ...
        sync.mark_dirty()
        sync.maybe_sync(source_conn=writer_conn)

    When *tables* is ``None``, the entire database is copied via
    ``COPY FROM DATABASE`` (DuckDB 1.1+).  Otherwise, only the listed
    tables are copied via ATTACH + CREATE OR REPLACE TABLE.
    """

    def __init__(
        self,
        *,
        source_path: Path,
        replica_dir: Path,
        prefix: str,
        min_interval_sec: float = 5.0,
        max_versions: int = 3,
        tables: Sequence[str] | None = None,
    ) -> None:
        self._source_path = Path(source_path)
        self._replica_dir = Path(replica_dir)
        self._prefix = prefix
        self._min_interval_sec = min_interval_sec
        self._max_versions = max(1, max_versions)
        self._tables = list(tables) if tables is not None else None

        self._dirty_gen: int = 0
        self._synced_gen: int = 0
        self._last_sync_time: float = 0.0
        self._current_version: int = 0
        self._syncing: bool = False
        self._batch_depth: int = 0
        self._lock = threading.Lock()

        self._replica_dir.mkdir(parents=True, exist_ok=True)
        self._current_version = self._detect_current_version()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def mark_dirty(self) -> None:
        """Signal that the source DB has been written to since the last sync."""
        with self._lock:
            self._dirty_gen += 1

    # ------------------------------------------------------------------
    # Batch suppression for multi-write operations
    # ------------------------------------------------------------------

    def begin_batch(self) -> None:
        """Suppress maybe_sync calls until end_batch() is called."""
        with self._lock:
            self._batch_depth += 1

    def end_batch(self, source_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        """End batch suppression and force sync if writes occurred."""
        with self._lock:
            self._batch_depth = max(0, self._batch_depth - 1)
            dirty = self._dirty_gen > self._synced_gen
        if self._batch_depth == 0 and dirty:
            self.force_sync(source_conn)

    def maybe_sync(self, source_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        """Sync if dirty and enough time has passed since the last sync.

        Parameters
        ----------
        source_conn:
            An existing read-write DuckDB connection to the source database.
            When provided, the sync runs synchronously on the calling thread
            (required on Windows to avoid double-attach).
        """
        with self._lock:
            if self._batch_depth > 0:
                return
            if self._dirty_gen <= self._synced_gen:
                return
            if self._syncing:
                return
            elapsed = time.monotonic() - self._last_sync_time
            if elapsed < self._min_interval_sec:
                return
            self._syncing = True

        try:
            self._do_sync(source_conn)
        finally:
            with self._lock:
                self._syncing = False

    def force_sync(self, source_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        """Force immediate sync regardless of debounce timer or dirty flag."""
        # Wait for any in-progress sync to finish (max 60s).
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            with self._lock:
                if not self._syncing:
                    break
            time.sleep(0.05)

        with self._lock:
            self._syncing = True

        try:
            self._do_sync(source_conn)
        finally:
            with self._lock:
                self._syncing = False

    def get_current_version(self) -> int:
        """Return the last successfully synced version number."""
        with self._lock:
            return self._current_version

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _pointer_path(self) -> Path:
        return self._replica_dir / f"{self._prefix}_latest"

    def _version_path(self, version: int) -> Path:
        return self._replica_dir / f"{self._prefix}_v{version}.duckdb"

    def _detect_current_version(self) -> int:
        """Read the pointer file to find the current version, or scan directory."""
        pointer = self._pointer_path()
        try:
            text = pointer.read_text().strip()
            if text.startswith("v"):
                return int(text[1:])
            return int(text)
        except OSError, ValueError:
            pass

        # Fallback: scan directory for highest version file.
        best = 0
        try:
            for p in self._replica_dir.glob(f"{self._prefix}_v*.duckdb"):
                try:
                    stem = p.stem  # e.g. "backtest_replica_v3"
                    v_str = stem.split("_v")[-1]
                    v = int(v_str)
                    if v > best:
                        best = v
                except ValueError, IndexError:
                    pass
        except OSError:
            pass
        return best

    def _write_pointer(self, version: int) -> None:
        """Atomically update the pointer file to point to *version*."""
        pointer = self._pointer_path()
        tmp = pointer.with_suffix(".tmp")
        tmp.write_text(f"v{version}")
        tmp.replace(pointer)

    def _resolve_source_ident(self, source_conn: duckdb.DuckDBPyConnection) -> str:
        """Get the database identifier for the source connection."""
        rows = source_conn.execute(
            "SELECT database_name FROM duckdb_databases() WHERE NOT internal"
        ).fetchall()
        if rows:
            return rows[0][0]
        return "main"

    @staticmethod
    def _quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _do_sync(self, source_conn: duckdb.DuckDBPyConnection | None) -> None:
        """Perform the actual versioned copy."""
        own_conn = source_conn is None
        if own_conn:
            try:
                source_conn = duckdb.connect(str(self._source_path))
            except Exception as exc:
                logger.error("Failed to open source DB for replica sync: %s", exc)
                return

        assert source_conn is not None

        # Checkpoint the WAL so the copy sees all committed data.
        try:
            source_conn.execute("CHECKPOINT")
        except Exception:
            pass

        # Determine new version.
        with self._lock:
            self._current_version += 1
            new_version = self._current_version

        target_path = self._version_path(new_version)
        tmp_path = target_path.with_suffix(".duckdb.tmp")

        try:
            if self._tables is None:
                # Full database copy via COPY FROM DATABASE.
                success = self._sync_via_copy_database(source_conn, tmp_path)
                if not success:
                    logger.warning(
                        "COPY FROM DATABASE failed, falling back to ATTACH for %s",
                        self._prefix,
                    )
                    self._sync_via_attach_tables(source_conn, tmp_path)
            else:
                self._sync_via_attach_tables(source_conn, tmp_path)

            # Atomic rename: tmp → final version file.
            tmp_path.replace(target_path)

            # Update pointer atomically.
            self._write_pointer(new_version)

            with self._lock:
                self._synced_gen = self._dirty_gen
                self._last_sync_time = time.monotonic()

            self._prune_old_versions(new_version)

            logger.debug(
                "Versioned replica sync completed: %s v%d (%s)",
                self._prefix,
                new_version,
                target_path.name,
            )
        except Exception as exc:
            logger.error("Versioned replica sync failed for %s: %s", self._prefix, exc)
            # Clean up failed tmp file.
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            # Roll back version counter.
            with self._lock:
                self._current_version = new_version - 1
        finally:
            if own_conn:
                try:
                    source_conn.close()
                except Exception:
                    pass

    def _sync_via_copy_database(
        self, source_conn: duckdb.DuckDBPyConnection, target_path: Path
    ) -> bool:
        """Try to copy the entire database using COPY FROM DATABASE."""
        escaped = str(target_path).replace("\\", "/").replace("'", "''")
        source_ident = self._resolve_source_ident(source_conn)
        try:
            source_conn.execute(
                f"COPY FROM DATABASE {self._quote_identifier(source_ident)} TO '{escaped}'"
            )
            return True
        except Exception as exc:
            logger.debug("COPY FROM DATABASE failed: %s", exc)
            # Clean up partial file.
            try:
                Path(target_path).unlink(missing_ok=True)
            except OSError:
                pass
            return False

    def _sync_via_attach_tables(
        self, source_conn: duckdb.DuckDBPyConnection, target_path: Path
    ) -> None:
        """Copy specific tables via ATTACH + CREATE OR REPLACE TABLE.

        When *self._tables* is None (full-copy mode), all user tables are copied.
        """
        escaped = str(target_path).replace("\\", "/").replace("'", "''")
        alias = f"{self._prefix}_dst"

        source_conn.execute(f"ATTACH '{escaped}' AS {alias}")
        try:
            available = {
                row[0]
                for row in source_conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }

            tables_to_copy = self._tables if self._tables is not None else sorted(available)

            synced = 0
            for table in tables_to_copy:
                if table not in available:
                    continue
                qtable = self._quote_identifier(table)
                source_conn.execute(
                    f"CREATE OR REPLACE TABLE {alias}.{qtable} AS SELECT * FROM {qtable}"
                )
                synced += 1

            logger.debug(
                "ATTACH sync: %d/%d tables copied for %s",
                synced,
                len(tables_to_copy),
                self._prefix,
            )
        finally:
            try:
                source_conn.execute(f"DETACH {alias}")
            except Exception:
                pass

    def _prune_old_versions(self, current_version: int) -> None:
        """Delete replica files older than max_versions."""
        for v in range(1, current_version - self._max_versions + 1):
            path = self._version_path(v)
            if path.exists():
                try:
                    path.unlink()
                    logger.debug("Pruned old replica: %s", path.name)
                except OSError:
                    pass


def migrate_legacy_snapshot(
    legacy_path: Path,
    replica_dir: Path,
    prefix: str,
) -> None:
    """Convert a legacy single-file snapshot into a versioned replica v1.

    Safe to call repeatedly — skips if the pointer file already exists.
    """
    if not legacy_path.exists():
        return

    replica_dir = Path(replica_dir)
    replica_dir.mkdir(parents=True, exist_ok=True)

    pointer = replica_dir / f"{prefix}_latest"
    if pointer.exists():
        return  # already migrated

    v1_path = replica_dir / f"{prefix}_v1.duckdb"
    if not v1_path.exists():
        import shutil

        shutil.copy2(str(legacy_path), str(v1_path))

    tmp = pointer.with_suffix(".tmp")
    tmp.write_text("v1")
    tmp.replace(pointer)
    logger.info("Migrated legacy snapshot %s -> %s", legacy_path, v1_path)
