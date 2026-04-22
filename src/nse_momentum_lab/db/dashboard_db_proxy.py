"""Dashboard DB proxy — auto-resolves to the latest versioned replica.

Wraps a :class:`MarketDataDB` (or similar) factory so that every attribute
access checks for a replica version change and reconnects transparently.
The dashboard code imports a proxy instance and calls methods on it as if
it were a regular ``MarketDataDB`` — the proxy handles version tracking
internally.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class _DashboardDBProxy:
    """Proxy that auto-resolves to the latest versioned replica on every access.

    Usage::

        proxy = _DashboardDBProxy(
            db_factory=lambda path: MarketDataDB(db_path=path, read_only=True),
            consumer=VersionedReplicaConsumer(
                replica_dir=Path("data/backtest_replica"),
                prefix="backtest_replica",
            ),
        )
        # Every attribute access auto-checks the pointer file:
        experiments = proxy.list_experiments()

    Parameters
    ----------
    db_factory:
        Callable ``(Path) -> object`` that creates a new DB wrapper instance
        for the given DuckDB file path.  The factory is called only when the
        version changes, not on every attribute access.
    consumer:
        A :class:`VersionedReplicaConsumer` instance that tracks the pointer
        file and provides read-only DuckDB connections.
    """

    def __init__(
        self,
        *,
        db_factory: Callable[[Path], Any],
        consumer: Any,  # VersionedReplicaConsumer — avoid circular import
    ) -> None:
        # Use object.__setattr__ to avoid triggering __setattr__ delegation.
        object.__setattr__(self, "_db_factory", db_factory)
        object.__setattr__(self, "_consumer", consumer)
        object.__setattr__(self, "_db", None)
        object.__setattr__(self, "_last_version", None)
        object.__setattr__(self, "_lock", threading.Lock())

    def __getattr__(self, name: str) -> Any:
        lock: threading.Lock = object.__getattribute__(self, "_lock")
        with lock:
            db = self._refresh_if_needed()
        return getattr(db, name)

    def __setattr__(self, name: str, value: Any) -> None:
        # Allow setting our own internal attributes normally.
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        # Delegate all other sets to the underlying DB object.
        lock: threading.Lock = object.__getattribute__(self, "_lock")
        with lock:
            db = self._refresh_if_needed()
        setattr(db, name, value)

    def _refresh_if_needed(self) -> Any:
        """Check for version changes and reconnect if needed. Must be called under lock."""
        consumer = object.__getattribute__(self, "_consumer")
        db_factory = object.__getattribute__(self, "_db_factory")
        current_db = object.__getattribute__(self, "_db")
        last_version = object.__getattribute__(self, "_last_version")

        version = consumer.get_latest_version()

        # Version changed — rebuild the DB wrapper.
        if version is not None and version != last_version:
            if current_db is not None:
                try:
                    current_db.close()
                except Exception:
                    pass

            path = consumer.get_replica_path()
            if path is not None:
                new_db = db_factory(path)
                object.__setattr__(self, "_db", new_db)
                object.__setattr__(self, "_last_version", version)
                logger.debug("Dashboard proxy reconnected to %s v%d", consumer._prefix, version)
                return new_db

        # No versioned replica yet — try fallback via consumer connection.
        if current_db is None:
            conn = consumer.get_connection()
            if conn is not None and consumer._fallback_path is not None:
                fallback = consumer._fallback_path
                if fallback.exists():
                    new_db = db_factory(fallback)
                    object.__setattr__(self, "_db", new_db)
                    object.__setattr__(self, "_last_version", None)
                    logger.debug("Dashboard proxy using fallback: %s", fallback)
                    return new_db
            raise AttributeError("Dashboard DB not available (no replica and no fallback)")

        return current_db

    def close(self) -> None:
        """Release the held DB connection."""
        lock: threading.Lock = object.__getattribute__(self, "_lock")
        with lock:
            db = object.__getattribute__(self, "_db")
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
                object.__setattr__(self, "_db", None)
                object.__setattr__(self, "_last_version", None)
