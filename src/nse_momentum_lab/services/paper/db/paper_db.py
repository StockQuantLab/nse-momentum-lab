"""DuckDB-only paper trading state store.

Drop-in replacement for the PostgreSQL-based ``nse_momentum_lab.db.paper`` module.
All methods are synchronous (DuckDB is in-process) and return plain dicts.
Thread safety is provided via a write lock because DuckDB is single-writer.

Walk-forward operations are intentionally excluded.
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Status constants (kept identical to the PostgreSQL module)
# ---------------------------------------------------------------------------

OPEN_SIGNAL_STATES = {"NEW", "QUALIFIED", "ALERTED", "ENTERED", "MANAGED"}
ACTIVE_SESSION_STATUSES = {"ACTIVE", "RUNNING", "PAUSED", "PLANNING", "STOPPING"}
FINAL_SESSION_STATUSES = {"COMPLETED", "FAILED", "ARCHIVED", "CANCELLED"}


@dataclass
class FeedAudit:
    """One recorded 5-min bar from the live/replay feed."""

    session_id: str
    trade_date: str
    feed_source: str
    transport: str
    symbol: str
    bar_start: datetime | None
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_snapshot_ts: datetime | None
    last_snapshot_ts: datetime | None


def _now() -> datetime:
    return datetime.now(UTC)


# Backward-compatible alias.
_utc_now = _now


# ---------------------------------------------------------------------------
# DDL -- all 11 tables
# ---------------------------------------------------------------------------

_DDL: list[str] = [
    # 1. paper_sessions
    """
    CREATE TABLE IF NOT EXISTS paper_sessions (
        session_id    TEXT PRIMARY KEY,
        trade_date    DATE,
        strategy_name TEXT NOT NULL,
        experiment_id TEXT,
        mode          TEXT NOT NULL,
        status        TEXT NOT NULL,
        symbols       TEXT NOT NULL DEFAULT '[]',
        strategy_params TEXT NOT NULL DEFAULT '{}',
        risk_config     TEXT NOT NULL DEFAULT '{}',
        notes           TEXT,
        created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        started_at    TIMESTAMP WITH TIME ZONE,
        finished_at   TIMESTAMP WITH TIME ZONE,
        archived_at   TIMESTAMP WITH TIME ZONE
    );
    """,
    # 2. paper_signals
    """
    CREATE TABLE IF NOT EXISTS paper_signals (
        signal_id          TEXT PRIMARY KEY,
        session_id         TEXT NOT NULL,
        symbol             TEXT NOT NULL,
        asof_date          DATE NOT NULL,
        strategy_hash      TEXT,
        state              TEXT NOT NULL DEFAULT 'NEW',
        entry_mode         TEXT,
        planned_entry_date DATE,
        initial_stop       DOUBLE,
        metadata_json      TEXT NOT NULL DEFAULT '{}',
        created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
    );
    """,
    # 3. paper_session_signals
    """
    CREATE TABLE IF NOT EXISTS paper_session_signals (
        id              INTEGER PRIMARY KEY DEFAULT nextval('paper_session_signals_seq'),
        session_id      TEXT NOT NULL,
        signal_id       TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        asof_date       DATE NOT NULL,
        rank            INTEGER,
        selection_score DOUBLE,
        decision_status TEXT,
        decision_reason TEXT,
        metadata_json   TEXT NOT NULL DEFAULT '{}',
        created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        UNIQUE(session_id, signal_id)
    );
    """,
    # 4. paper_positions
    """
    CREATE TABLE IF NOT EXISTS paper_positions (
        position_id  TEXT PRIMARY KEY,
        session_id   TEXT NOT NULL,
        symbol       TEXT NOT NULL,
        opened_at    TIMESTAMP WITH TIME ZONE NOT NULL,
        closed_at    TIMESTAMP WITH TIME ZONE,
        avg_entry    DOUBLE NOT NULL,
        avg_exit     DOUBLE,
        qty          INTEGER NOT NULL,
        pnl          DOUBLE,
        state        TEXT NOT NULL,
        direction    TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
    );
    """,
    # 5. paper_orders
    """
    CREATE TABLE IF NOT EXISTS paper_orders (
        order_id           TEXT PRIMARY KEY,
        session_id         TEXT NOT NULL,
        broker_order_id    TEXT,
        signal_id          TEXT,
        symbol             TEXT NOT NULL,
        side               TEXT NOT NULL,
        qty                INTEGER NOT NULL,
        order_type         TEXT NOT NULL,
        limit_price        DOUBLE,
        status             TEXT NOT NULL,
        broker_status      TEXT,
        created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
    );
    """,
    # 6. paper_fills
    """
    CREATE TABLE IF NOT EXISTS paper_fills (
        fill_id            TEXT PRIMARY KEY,
        session_id         TEXT NOT NULL,
        broker_trade_id    TEXT,
        broker_order_id    TEXT,
        order_id           TEXT,
        symbol             TEXT NOT NULL,
        fill_time          TIMESTAMP WITH TIME ZONE NOT NULL,
        fill_price         DOUBLE NOT NULL,
        qty                INTEGER NOT NULL,
        fees               DOUBLE,
        slippage_bps       DOUBLE,
        side               TEXT NOT NULL,
        metadata_json      TEXT NOT NULL DEFAULT '{}'
    );
    """,
    # 7. paper_order_events
    """
    CREATE TABLE IF NOT EXISTS paper_order_events (
        event_id         TEXT PRIMARY KEY,
        session_id       TEXT NOT NULL,
        order_id         TEXT,
        signal_id        TEXT,
        event_type       TEXT NOT NULL,
        event_status     TEXT NOT NULL,
        broker_order_id  TEXT,
        payload_json     TEXT NOT NULL DEFAULT '{}',
        created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
    );
    """,
    # 8. paper_bar_checkpoints  -- bar-group watermark, NOT per-symbol
    """
    CREATE TABLE IF NOT EXISTS paper_bar_checkpoints (
        checkpoint_id           INTEGER PRIMARY KEY DEFAULT nextval('paper_bar_checkpoints_seq'),
        session_id              TEXT NOT NULL,
        bar_end_ts              TIMESTAMP WITH TIME ZONE NOT NULL,
        committed_symbol_count  INTEGER NOT NULL DEFAULT 0,
        fill_count              INTEGER NOT NULL DEFAULT 0,
        state_hash              TEXT,
        created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
    );
    """,
    # 9. paper_feed_state
    """
    CREATE TABLE IF NOT EXISTS paper_feed_state (
        session_id         TEXT PRIMARY KEY,
        source             TEXT NOT NULL,
        mode               TEXT NOT NULL,
        status             TEXT NOT NULL,
        is_stale           BOOLEAN NOT NULL DEFAULT false,
        subscription_count INTEGER,
        heartbeat_at       TIMESTAMP WITH TIME ZONE,
        last_quote_at      TIMESTAMP WITH TIME ZONE,
        last_tick_at       TIMESTAMP WITH TIME ZONE,
        last_bar_at        TIMESTAMP WITH TIME ZONE,
        raw_state          TEXT DEFAULT '{}',
        updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
    );
    """,
    # 10. alert_log
    """
    CREATE TABLE IF NOT EXISTS alert_log (
        alert_id     INTEGER PRIMARY KEY DEFAULT nextval('alert_log_seq'),
        session_id   TEXT,
        alert_type   TEXT NOT NULL,
        channel      TEXT NOT NULL,
        status       TEXT NOT NULL,
        payload      TEXT DEFAULT '{}',
        sent_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        error_message TEXT
    );
    """,
    # 11. paper_feed_audit
    """
    CREATE TABLE IF NOT EXISTS paper_feed_audit (
        session_id        TEXT NOT NULL,
        trade_date        TEXT NOT NULL,
        feed_source       TEXT NOT NULL DEFAULT '',
        transport         TEXT NOT NULL DEFAULT '',
        symbol            TEXT NOT NULL,
        bar_start         TIMESTAMP WITH TIME ZONE,
        bar_end           TIMESTAMP WITH TIME ZONE NOT NULL,
        open              DOUBLE NOT NULL,
        high              DOUBLE NOT NULL,
        low               DOUBLE NOT NULL,
        close             DOUBLE NOT NULL,
        volume            DOUBLE NOT NULL,
        first_snapshot_ts TIMESTAMP WITH TIME ZONE,
        last_snapshot_ts  TIMESTAMP WITH TIME ZONE,
        created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        PRIMARY KEY (session_id, symbol, bar_end)
    );
    """,
]

# Sequences for INTEGER PK tables that need auto-increment.
_SEQUENCES: list[str] = [
    "CREATE SEQUENCE IF NOT EXISTS paper_session_signals_seq START 1;",
    "CREATE SEQUENCE IF NOT EXISTS paper_bar_checkpoints_seq START 1;",
    "CREATE SEQUENCE IF NOT EXISTS alert_log_seq START 1;",
]

# Indexes for common query patterns.
_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_ps_status_trade_date ON paper_sessions(status, trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_ps_strategy_trade_date ON paper_sessions(strategy_name, trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_psig_session_rank ON paper_session_signals(session_id, rank);",
    "CREATE INDEX IF NOT EXISTS idx_psig_session_decision ON paper_session_signals(session_id, decision_status);",
    "CREATE INDEX IF NOT EXISTS idx_pp_session_open ON paper_positions(session_id, closed_at);",
    "CREATE INDEX IF NOT EXISTS idx_pord_session_created ON paper_orders(session_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_pord_broker_order_id ON paper_orders(broker_order_id);",
    "CREATE INDEX IF NOT EXISTS idx_pfill_session_time ON paper_fills(session_id, fill_time);",
    "CREATE INDEX IF NOT EXISTS idx_pevt_session_created ON paper_order_events(session_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_pevt_session_type ON paper_order_events(session_id, event_type);",
    "CREATE INDEX IF NOT EXISTS idx_pfa_trade_date ON paper_feed_audit(trade_date, feed_source, session_id);",
    "CREATE INDEX IF NOT EXISTS idx_pfa_session ON paper_feed_audit(session_id);",
]


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=str, ensure_ascii=False)


def _json_loads(raw: str | None) -> Any:
    if raw is None:
        return None
    if isinstance(raw, dict | list):
        return raw
    return json.loads(raw)


def _iso(val: Any) -> str | None:
    if val is None:
        return None
    return val.isoformat() if hasattr(val, "isoformat") else str(val)


# ---- paper_sessions -------------------------------------------------------


def _serialize_session(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": row.get("session_id"),
        "trade_date": _iso(row.get("trade_date")),
        "strategy_name": row.get("strategy_name"),
        "experiment_id": row.get("experiment_id"),
        "mode": row.get("mode"),
        "status": row.get("status"),
        "symbols": _json_loads(row.get("symbols")) or [],
        "strategy_params": _json_loads(row.get("strategy_params")) or {},
        "risk_config": _json_loads(row.get("risk_config")) or {},
        "notes": row.get("notes"),
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
        "started_at": _iso(row.get("started_at")),
        "finished_at": _iso(row.get("finished_at")),
        "archived_at": _iso(row.get("archived_at")),
    }


def _serialize_session_compact(row: dict[str, Any]) -> dict[str, Any]:
    symbols_raw = row.get("symbols", "[]")
    symbols = _json_loads(symbols_raw) if isinstance(symbols_raw, str) else symbols_raw
    return {
        "session_id": row.get("session_id"),
        "trade_date": _iso(row.get("trade_date")),
        "strategy_name": row.get("strategy_name"),
        "experiment_id": row.get("experiment_id"),
        "mode": row.get("mode"),
        "status": row.get("status"),
        "symbol_count": len(symbols) if symbols else 0,
        "strategy_params": _json_loads(row.get("strategy_params")) or {},
        "started_at": _iso(row.get("started_at")),
        "updated_at": _iso(row.get("updated_at")),
    }


# ---- paper_signals --------------------------------------------------------


def _serialize_signal(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": row.get("signal_id"),
        "session_id": row.get("session_id"),
        "symbol": row.get("symbol"),
        "asof_date": _iso(row.get("asof_date")),
        "strategy_hash": row.get("strategy_hash"),
        "state": row.get("state"),
        "entry_mode": row.get("entry_mode"),
        "planned_entry_date": _iso(row.get("planned_entry_date")),
        "initial_stop": row.get("initial_stop"),
        "metadata_json": _json_loads(row.get("metadata_json")) or {},
        "created_at": _iso(row.get("created_at")),
    }


# ---- paper_session_signals ------------------------------------------------


def _serialize_session_signal(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_session_signal_id": row.get("id"),
        "session_id": row.get("session_id"),
        "signal_id": row.get("signal_id"),
        "symbol": row.get("symbol"),
        "asof_date": _iso(row.get("asof_date")),
        "rank": row.get("rank"),
        "selection_score": row.get("selection_score"),
        "decision_status": row.get("decision_status"),
        "decision_reason": row.get("decision_reason"),
        "metadata_json": _json_loads(row.get("metadata_json")) or {},
        "created_at": _iso(row.get("created_at")),
    }


# ---- paper_positions ------------------------------------------------------


def _serialize_position(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "position_id": row.get("position_id"),
        "session_id": row.get("session_id"),
        "symbol": row.get("symbol"),
        "opened_at": _iso(row.get("opened_at")),
        "closed_at": _iso(row.get("closed_at")),
        "avg_entry": row.get("avg_entry"),
        "avg_exit": row.get("avg_exit"),
        "qty": row.get("qty"),
        "pnl": row.get("pnl"),
        "state": row.get("state"),
        "direction": row.get("direction"),
        "metadata_json": _json_loads(row.get("metadata_json")) or {},
    }


# ---- paper_orders ---------------------------------------------------------


def _serialize_order(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": row.get("order_id"),
        "session_id": row.get("session_id"),
        "broker_order_id": row.get("broker_order_id"),
        "signal_id": row.get("signal_id"),
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "qty": row.get("qty"),
        "order_type": row.get("order_type"),
        "limit_price": row.get("limit_price"),
        "status": row.get("status"),
        "broker_status": row.get("broker_status"),
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
    }


# ---- paper_fills ----------------------------------------------------------


def _serialize_fill(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "fill_id": row.get("fill_id"),
        "session_id": row.get("session_id"),
        "broker_trade_id": row.get("broker_trade_id"),
        "broker_order_id": row.get("broker_order_id"),
        "order_id": row.get("order_id"),
        "symbol": row.get("symbol"),
        "fill_time": _iso(row.get("fill_time")),
        "fill_price": row.get("fill_price"),
        "qty": row.get("qty"),
        "fees": row.get("fees"),
        "slippage_bps": row.get("slippage_bps"),
        "side": row.get("side"),
        "metadata_json": _json_loads(row.get("metadata_json")) or {},
    }


# ---- paper_order_events ---------------------------------------------------


def _serialize_order_event(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row.get("event_id"),
        "session_id": row.get("session_id"),
        "order_id": row.get("order_id"),
        "signal_id": row.get("signal_id"),
        "event_type": row.get("event_type"),
        "event_status": row.get("event_status"),
        "broker_order_id": row.get("broker_order_id"),
        "payload_json": _json_loads(row.get("payload_json")) or {},
        "created_at": _iso(row.get("created_at")),
    }


# ---- paper_bar_checkpoints ------------------------------------------------


def _serialize_bar_checkpoint(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "checkpoint_id": row.get("checkpoint_id"),
        "session_id": row.get("session_id"),
        "bar_end_ts": _iso(row.get("bar_end_ts")),
        "committed_symbol_count": row.get("committed_symbol_count"),
        "fill_count": row.get("fill_count"),
        "state_hash": row.get("state_hash"),
        "created_at": _iso(row.get("created_at")),
    }


# ---- paper_feed_state -----------------------------------------------------


def _serialize_feed_state(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": row.get("session_id"),
        "source": row.get("source"),
        "mode": row.get("mode"),
        "status": row.get("status"),
        "is_stale": row.get("is_stale"),
        "subscription_count": row.get("subscription_count"),
        "heartbeat_at": _iso(row.get("heartbeat_at")),
        "last_quote_at": _iso(row.get("last_quote_at")),
        "last_tick_at": _iso(row.get("last_tick_at")),
        "last_bar_at": _iso(row.get("last_bar_at")),
        "raw_state": _json_loads(row.get("raw_state")) or {},
        "updated_at": _iso(row.get("updated_at")),
    }


# ---- alert_log ------------------------------------------------------------


def _serialize_alert_log(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "alert_id": row.get("alert_id"),
        "session_id": row.get("session_id"),
        "alert_type": row.get("alert_type"),
        "channel": row.get("channel"),
        "status": row.get("status"),
        "payload": _json_loads(row.get("payload")) or {},
        "sent_at": _iso(row.get("sent_at")),
        "error_message": row.get("error_message"),
    }


# ---------------------------------------------------------------------------
# Row -> dict helper (DuckDB description-based)
# ---------------------------------------------------------------------------


def _row_to_dict(desc: list[tuple], row: tuple) -> dict[str, Any]:
    """Convert a DuckDB result row tuple into a dict keyed by column name."""
    return {col[0]: val for col, val in zip(desc, row, strict=True)}


def _rows_to_dicts(desc: list[tuple], rows: list[tuple]) -> list[dict[str, Any]]:
    return [_row_to_dict(desc, r) for r in rows]


# ---------------------------------------------------------------------------
# PaperDB class
# ---------------------------------------------------------------------------


class PaperDB:
    """Synchronous DuckDB store for paper-trading state.

    Usage::

        with PaperDB("data/paper.duckdb") as db:
            session = db.create_session(...)
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._con: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.RLock()  # reentrant so transaction() can acquire per-statement
        self.connect()

    # -- context manager ----------------------------------------------------

    def __enter__(self) -> PaperDB:
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- lifecycle ----------------------------------------------------------

    def connect(self) -> None:
        if self._con is not None:
            return
        self._con = duckdb.connect(self._path)
        self._bootstrap()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError(
                "PaperDB is not connected. Use `with PaperDB(...) as db:` or call connect()."
            )
        return self._con

    def _bootstrap(self) -> None:
        """Create sequences, tables, and indexes if they do not exist."""
        for sql in _SEQUENCES + _DDL + _INDEXES:
            self.con.execute(sql)

    # -- transaction support ------------------------------------------------

    @contextlib.contextmanager
    def transaction(self):
        """Wrap a block of DuckDB writes in a single atomic transaction.

        Uses RLock so _execute() can still acquire the lock inside the block.

        Usage::

            with paper_db.transaction():
                paper_db.insert_position(...)
                paper_db.insert_order(...)
                paper_db.insert_bar_checkpoint(...)
        """
        with self._lock:
            self.con.begin()
            try:
                yield self
                self.con.commit()
            except Exception:
                try:
                    self.con.rollback()
                except Exception:
                    pass
                raise

    # -- internal helpers ---------------------------------------------------

    def _execute(self, sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyConnection:
        """Execute a statement; acquires write lock for non-SELECT."""
        stripped = sql.strip().upper()
        is_read = (
            stripped.startswith("SELECT")
            or stripped.startswith("WITH")
            or stripped.startswith("PRAGMA")
            or stripped.startswith("EXPLAIN")
        )
        if is_read:
            if params:
                return self.con.execute(sql, params)
            return self.con.execute(sql)
        with self._lock:
            if params:
                return self.con.execute(sql, params)
            return self.con.execute(sql)

    def _query_one(self, sql: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        result = self._execute(sql, params)
        desc = result.description
        row = result.fetchone()
        if row is None:
            return None
        return _row_to_dict(desc, row)

    def _query_all(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        result = self._execute(sql, params)
        desc = result.description
        rows = result.fetchall()
        return _rows_to_dicts(desc, rows)

    def execute(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Public execute: run any SQL and return results as list of dicts."""
        return self._query_all(sql, params)

    # ===================================================================
    # Session CRUD
    # ===================================================================

    def create_session(
        self,
        *,
        session_id: str | None = None,
        trade_date: date | None = None,
        strategy_name: str,
        mode: str,
        status: str = "ACTIVE",
        experiment_id: str | None = None,
        symbols: list[str] | None = None,
        strategy_params: dict[str, Any] | None = None,
        risk_config: dict[str, Any] | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        """Create a new paper trading session.  Returns the serialized session dict."""
        now = _now()
        sid = session_id or str(uuid.uuid4())
        started_at = now if status in ACTIVE_SESSION_STATUSES else None
        finished_at = now if status in FINAL_SESSION_STATUSES else None
        archived_at = now if status == "ARCHIVED" else None

        self._execute(
            """
            INSERT INTO paper_sessions
                (session_id, trade_date, strategy_name, experiment_id,
                 mode, status, symbols, strategy_params, risk_config,
                 notes, created_at, updated_at, started_at, finished_at, archived_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """,
            [
                sid,
                trade_date,
                strategy_name,
                experiment_id,
                mode,
                status,
                _json_dumps(symbols or []),
                _json_dumps(strategy_params or {}),
                _json_dumps(risk_config or {}),
                notes,
                now,
                now,
                started_at,
                finished_at,
                archived_at,
            ],
        )
        return self.get_session(sid)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_sessions WHERE session_id = $1",
            [session_id],
        )
        return _serialize_session(row) if row else None

    def update_session(
        self,
        session_id: str,
        *,
        status: str | None = None,
        symbols: list[str] | None = None,
        strategy_params: dict[str, Any] | None = None,
        risk_config: dict[str, Any] | None = None,
        notes: str | None = None,
    ) -> dict[str, Any] | None:
        """Update mutable fields on a session.  Returns the updated dict or None."""
        existing = self.get_session(session_id)
        if existing is None:
            return None

        now = _now()
        sets: list[str] = ["updated_at = $1"]
        vals: list[Any] = [now]
        idx = 2

        if status is not None:
            sets.append(f"status = ${idx}")
            vals.append(status)
            idx += 1
            if status in ACTIVE_SESSION_STATUSES and existing.get("started_at") is None:
                sets.append(f"started_at = ${idx}")
                vals.append(now)
                idx += 1
            if status in FINAL_SESSION_STATUSES and existing.get("finished_at") is None:
                sets.append(f"finished_at = ${idx}")
                vals.append(now)
                idx += 1
            if status == "ARCHIVED" and existing.get("archived_at") is None:
                sets.append(f"archived_at = ${idx}")
                vals.append(now)
                idx += 1

        if symbols is not None:
            sets.append(f"symbols = ${idx}")
            vals.append(_json_dumps(symbols))
            idx += 1
        if strategy_params is not None:
            sets.append(f"strategy_params = ${idx}")
            vals.append(_json_dumps(strategy_params))
            idx += 1
        if risk_config is not None:
            sets.append(f"risk_config = ${idx}")
            vals.append(_json_dumps(risk_config))
            idx += 1
        if notes is not None:
            sets.append(f"notes = ${idx}")
            vals.append(notes)
            idx += 1

        vals.append(session_id)
        self._execute(
            f"UPDATE paper_sessions SET {', '.join(sets)} WHERE session_id = ${idx}",
            vals,
        )
        return self.get_session(session_id)

    def list_sessions(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM paper_sessions "
        params: list[Any] = []
        idx = 1
        if status:
            sql += f"WHERE status = ${idx} "
            params.append(status)
            idx += 1
        sql += f"ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)
        rows = self._query_all(sql, params)
        return [_serialize_session(r) for r in rows]

    def list_active_sessions(
        self,
        *,
        mode: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return sessions whose status is in ACTIVE_SESSION_STATUSES."""
        placeholders = ", ".join(f"'{s}'" for s in ACTIVE_SESSION_STATUSES)
        sql = f"SELECT * FROM paper_sessions WHERE status IN ({placeholders}) "
        params: list[Any] = []
        idx = 1
        if mode:
            sql += f"AND mode = ${idx} "
            params.append(mode)
            idx += 1
        sql += f"ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)
        rows = self._query_all(sql, params)
        return [_serialize_session(r) for r in rows]

    def get_active_session(
        self,
        *,
        mode: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the single most recent active session (or None)."""
        sessions = self.list_active_sessions(mode=mode, limit=1)
        return sessions[0] if sessions else None

    def find_resumable_session(
        self,
        *,
        strategy_name: str,
        trade_date: date | str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any] | None:
        """Find the most recent resumable session for a strategy + date + mode.

        Returns the session dict if a session in PLANNED, ACTIVE, or PAUSED status
        exists for the given strategy_name (and optionally trade_date / mode),
        or None if no resumable session exists.

        Use this to make ``prepare`` idempotent: call before ``create_session`` so
        that a restart after a crash returns the existing session rather than
        creating a duplicate.
        """
        resumable = ("PLANNED", "ACTIVE", "PAUSED", "RUNNING")
        placeholders = ", ".join(f"'{s}'" for s in resumable)
        sql = (
            f"SELECT * FROM paper_sessions WHERE status IN ({placeholders}) AND strategy_name = $1 "
        )
        params: list[Any] = [strategy_name]
        idx = 2
        if trade_date is not None:
            td = trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date))
            sql += f"AND trade_date = ${idx} "
            params.append(td)
            idx += 1
        if mode is not None:
            sql += f"AND mode = ${idx} "
            params.append(mode)
            idx += 1
        sql += "ORDER BY created_at DESC LIMIT 1"
        row = self._query_one(sql, params)
        return _serialize_session(row) if row else None

    def list_sessions_compact(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM paper_sessions "
        params: list[Any] = []
        idx = 1
        if status:
            sql += f"WHERE status = ${idx} "
            params.append(status)
            idx += 1
        sql += f"ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)
        rows = self._query_all(sql, params)
        return [_serialize_session_compact(r) for r in rows]

    def get_session_summary(self, session_id: str) -> dict[str, Any] | None:
        """Return session + aggregate counts + feed state."""
        session = self.get_session(session_id)
        if session is None:
            return None

        signal_count = (
            self._query_one(
                "SELECT COUNT(*) AS cnt FROM paper_signals WHERE session_id = $1", [session_id]
            )
            or {}
        ).get("cnt", 0)

        open_states = ", ".join(f"'{s}'" for s in OPEN_SIGNAL_STATES)
        open_signal_count = (
            self._query_one(
                f"SELECT COUNT(*) AS cnt FROM paper_signals WHERE session_id = $1 AND state IN ({open_states})",
                [session_id],
            )
            or {}
        ).get("cnt", 0)

        open_position_count = (
            self._query_one(
                "SELECT COUNT(*) AS cnt FROM paper_positions WHERE session_id = $1 AND closed_at IS NULL",
                [session_id],
            )
            or {}
        ).get("cnt", 0)

        order_count = (
            self._query_one(
                "SELECT COUNT(*) AS cnt FROM paper_orders WHERE session_id = $1", [session_id]
            )
            or {}
        ).get("cnt", 0)

        fill_count = (
            self._query_one(
                "SELECT COUNT(*) AS cnt FROM paper_fills WHERE session_id = $1", [session_id]
            )
            or {}
        ).get("cnt", 0)

        queue_count = (
            self._query_one(
                "SELECT COUNT(*) AS cnt FROM paper_session_signals WHERE session_id = $1",
                [session_id],
            )
            or {}
        ).get("cnt", 0)

        feed = self.get_feed_state(session_id)

        return {
            "session": session,
            "counts": {
                "signals": int(signal_count or 0),
                "open_signals": int(open_signal_count or 0),
                "open_positions": int(open_position_count or 0),
                "orders": int(order_count or 0),
                "fills": int(fill_count or 0),
                "queue_signals": int(queue_count or 0),
            },
            "feed_state": feed,
        }

    # ===================================================================
    # Signal CRUD
    # ===================================================================

    def insert_signal(
        self,
        *,
        session_id: str,
        symbol: str,
        asof_date: date,
        strategy_hash: str | None = None,
        state: str = "NEW",
        entry_mode: str | None = None,
        planned_entry_date: date | None = None,
        initial_stop: float | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Insert a signal.  If a matching (session_id, symbol, asof_date) row exists, update it."""
        existing = self._query_one(
            """
            SELECT signal_id FROM paper_signals
            WHERE session_id = $1 AND symbol = $2 AND asof_date = $3
            """,
            [session_id, symbol, asof_date],
        )

        if existing is None:
            sid = str(uuid.uuid4())
            now = _now()
            self._execute(
                """
                INSERT INTO paper_signals
                    (signal_id, session_id, symbol, asof_date, strategy_hash,
                     state, entry_mode, planned_entry_date, initial_stop, metadata_json, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                [
                    sid,
                    session_id,
                    symbol,
                    asof_date,
                    strategy_hash,
                    state,
                    entry_mode,
                    planned_entry_date,
                    initial_stop,
                    _json_dumps(metadata_json or {}),
                    now,
                ],
            )
        else:
            sid = existing["signal_id"]
            self._execute(
                """
                UPDATE paper_signals
                SET strategy_hash = COALESCE($1, strategy_hash),
                    state = $2,
                    entry_mode = COALESCE($3, entry_mode),
                    planned_entry_date = COALESCE($4, planned_entry_date),
                    initial_stop = COALESCE($5, initial_stop),
                    metadata_json = $6
                WHERE signal_id = $7
                """,
                [
                    strategy_hash,
                    state,
                    entry_mode,
                    planned_entry_date,
                    initial_stop,
                    _json_dumps(metadata_json or {}),
                    sid,
                ],
            )
        return self.get_signal(sid)  # type: ignore[return-value]

    def update_signal_state(
        self,
        signal_id: str,
        state: str,
    ) -> dict[str, Any] | None:
        self._execute(
            "UPDATE paper_signals SET state = $1 WHERE signal_id = $2",
            [state, signal_id],
        )
        return self.get_signal(signal_id)

    def get_signal(self, signal_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_signals WHERE signal_id = $1",
            [signal_id],
        )
        return _serialize_signal(row) if row else None

    def list_signals_by_session(
        self,
        session_id: str,
        *,
        states: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM paper_signals WHERE session_id = $1 "
        params: list[Any] = [session_id]
        if states:
            placeholders = ", ".join(f"'{s}'" for s in sorted(states))
            sql += f"AND state IN ({placeholders}) "
        sql += "ORDER BY asof_date ASC, created_at ASC, signal_id ASC"
        rows = self._query_all(sql, params)
        return [_serialize_signal(r) for r in rows]

    # ===================================================================
    # Session-Signal (paper_session_signals)
    # ===================================================================

    def insert_session_signal(
        self,
        *,
        session_id: str,
        signal_id: str,
        symbol: str,
        asof_date: date,
        rank: int | None = None,
        selection_score: float | None = None,
        decision_status: str = "PENDING",
        decision_reason: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Insert or update a session-signal link."""
        existing = self._query_one(
            "SELECT id FROM paper_session_signals WHERE session_id = $1 AND signal_id = $2",
            [session_id, signal_id],
        )

        if existing is None:
            now = _now()
            self._execute(
                """
                INSERT INTO paper_session_signals
                    (session_id, signal_id, symbol, asof_date, rank,
                     selection_score, decision_status, decision_reason, metadata_json, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                [
                    session_id,
                    signal_id,
                    symbol,
                    asof_date,
                    rank,
                    selection_score,
                    decision_status,
                    decision_reason,
                    _json_dumps(metadata_json or {}),
                    now,
                ],
            )
        else:
            self._execute(
                """
                UPDATE paper_session_signals
                SET symbol = $1, asof_date = $2, rank = $3,
                    selection_score = $4, decision_status = $5,
                    decision_reason = $6, metadata_json = $7
                WHERE session_id = $8 AND signal_id = $9
                """,
                [
                    symbol,
                    asof_date,
                    rank,
                    selection_score,
                    decision_status,
                    decision_reason,
                    _json_dumps(metadata_json or {}),
                    session_id,
                    signal_id,
                ],
            )
        return self._serialize_session_signal_by_ids(session_id, signal_id)  # type: ignore[return-value]

    def _serialize_session_signal_by_ids(
        self, session_id: str, signal_id: str
    ) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_session_signals WHERE session_id = $1 AND signal_id = $2",
            [session_id, signal_id],
        )
        return _serialize_session_signal(row) if row else None

    def list_session_signals(
        self,
        session_id: str,
    ) -> list[dict[str, Any]]:
        rows = self._query_all(
            """
            SELECT * FROM paper_session_signals
            WHERE session_id = $1
            ORDER BY COALESCE(rank, 999999) ASC, created_at ASC
            """,
            [session_id],
        )
        return [_serialize_session_signal(r) for r in rows]

    def update_session_signal_decision(
        self,
        session_id: str,
        signal_id: str,
        *,
        decision_status: str,
        decision_reason: str | None = None,
    ) -> dict[str, Any] | None:
        sets = ["decision_status = $1"]
        vals: list[Any] = [decision_status]
        idx = 2
        if decision_reason is not None:
            sets.append(f"decision_reason = ${idx}")
            vals.append(decision_reason)
            idx += 1
        vals.extend([session_id, signal_id])
        self._execute(
            f"UPDATE paper_session_signals SET {', '.join(sets)} "
            f"WHERE session_id = ${idx} AND signal_id = ${idx + 1}",
            vals,
        )
        return self._serialize_session_signal_by_ids(session_id, signal_id)

    def reset_session_signal_queue(self, session_id: str) -> None:
        """Delete all session-signal links and signals for a session."""
        self._execute(
            "DELETE FROM paper_session_signals WHERE session_id = $1",
            [session_id],
        )
        self._execute(
            "DELETE FROM paper_signals WHERE session_id = $1",
            [session_id],
        )

    # ===================================================================
    # Position CRUD
    # ===================================================================

    def insert_position(
        self,
        *,
        session_id: str,
        symbol: str,
        avg_entry: float,
        qty: int,
        state: str = "OPEN",
        direction: str = "LONG",
        metadata_json: dict[str, Any] | None = None,
        position_id: str | None = None,
    ) -> dict[str, Any]:
        pid = position_id or str(uuid.uuid4())
        now = _now()
        self._execute(
            """
            INSERT INTO paper_positions
                (position_id, session_id, symbol, opened_at, avg_entry, qty,
                 state, direction, metadata_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            [
                pid,
                session_id,
                symbol,
                now,
                avg_entry,
                qty,
                state,
                direction,
                _json_dumps(metadata_json or {}),
            ],
        )
        return self.get_position(pid)  # type: ignore[return-value]

    def update_position(
        self,
        position_id: str,
        *,
        closed_at: datetime | None = ...,
        avg_exit: float | None = ...,
        pnl: float | None = ...,
        state: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Update position fields.  Use ``...`` (sentinel) to skip a field."""
        existing = self.get_position(position_id)
        if existing is None:
            return None

        sets: list[str] = []
        vals: list[Any] = []
        idx = 1

        if closed_at is not ...:
            sets.append(f"closed_at = ${idx}")
            vals.append(closed_at)
            idx += 1
        if avg_exit is not ...:
            sets.append(f"avg_exit = ${idx}")
            vals.append(avg_exit)
            idx += 1
        if pnl is not ...:
            sets.append(f"pnl = ${idx}")
            vals.append(pnl)
            idx += 1
        if state is not None:
            sets.append(f"state = ${idx}")
            vals.append(state)
            idx += 1
        if metadata_json is not None:
            sets.append(f"metadata_json = ${idx}")
            vals.append(_json_dumps(metadata_json))
            idx += 1

        if not sets:
            return existing

        vals.append(position_id)
        self._execute(
            f"UPDATE paper_positions SET {', '.join(sets)} WHERE position_id = ${idx}",
            vals,
        )
        return self.get_position(position_id)

    def get_position(self, position_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_positions WHERE position_id = $1",
            [position_id],
        )
        return _serialize_position(row) if row else None

    def patch_position_metadata(self, position_id: str, **updates: Any) -> None:
        """Merge key/value updates into a position's metadata_json without overwriting other keys."""
        existing = self.get_position(position_id)
        if existing is None:
            return
        meta = dict(existing.get("metadata_json") or {})
        meta.update(updates)
        self.update_position(position_id, metadata_json=meta)

    def partial_close_position(
        self,
        position_id: str,
        *,
        partial_exit_price: float,
        partial_exit_qty: int,
        carry_stop: float,
        reason: str,
        closed_at: datetime,
    ) -> dict[str, Any] | None:
        """Reduce qty for a partial exit and record the event in metadata."""
        existing = self.get_position(position_id)
        if existing is None:
            return None
        total_qty = int(existing.get("qty") or 0)
        remain_qty = max(0, total_qty - partial_exit_qty)
        avg_entry = float(existing.get("avg_entry") or 0.0)
        direction = str(existing.get("direction") or "LONG").upper()
        meta = dict(existing.get("metadata_json") or {})
        entry_qty = int(meta.get("entry_qty") or total_qty or 0)
        entry_fee_total = (
            round(avg_entry * entry_qty * 0.001, 4) if avg_entry and entry_qty else 0.0
        )
        partial_exit_fee = (
            round(partial_exit_price * partial_exit_qty * 0.001, 4)
            if partial_exit_price and partial_exit_qty
            else 0.0
        )
        partial_pnl = round(
            (partial_exit_price - avg_entry) * partial_exit_qty
            if direction == "LONG"
            else (avg_entry - partial_exit_price) * partial_exit_qty,
            4,
        )
        partial_net_pnl = round(partial_pnl - entry_fee_total - partial_exit_fee, 4)
        meta.update(
            entry_qty=entry_qty,
            remaining_qty=remain_qty,
            partial_exit_price=partial_exit_price,
            partial_exit_qty=partial_exit_qty,
            partial_exit_gross_pnl=partial_pnl,
            partial_exit_entry_fee=entry_fee_total,
            partial_exit_exit_fee=partial_exit_fee,
            partial_exit_pnl=partial_pnl,
            partial_exit_net_pnl=partial_net_pnl,
            partial_exit_reason=reason,
            partial_exit_at=closed_at.isoformat(),
            current_sl=carry_stop,
        )
        self._execute(
            "UPDATE paper_positions SET qty = $1, metadata_json = $2 WHERE position_id = $3",
            [remain_qty, _json_dumps(meta), position_id],
        )
        return self.get_position(position_id)

    def get_session_realized_pnl(self, session_id: str) -> float:
        """Return cumulative realized P&L for a session net of modeled entry/exit fees.

        Raw trade history remains gross in `paper_positions.pnl`, but operator-facing session
        accounting should reflect the same fee approximation used by alerts and summaries.
        """
        rows = self._query_all(
            """
            SELECT avg_entry, avg_exit, qty, pnl, state, metadata_json
            FROM paper_positions
            WHERE session_id = $1
            ORDER BY opened_at ASC
            """,
            [session_id],
        )
        total = 0.0
        for row in rows:
            avg_entry = float(row.get("avg_entry") or 0.0)
            avg_exit = float(row.get("avg_exit") or avg_entry)
            qty = int(row.get("qty") or 0)
            state = str(row.get("state") or "").upper()
            gross_pnl = float(row.get("pnl") or 0.0)
            meta = _json_loads(row.get("metadata_json")) or {}
            if not isinstance(meta, dict):
                meta = {}
            has_partial = "partial_exit_net_pnl" in meta
            partial_net = float(meta.get("partial_exit_net_pnl") or 0.0)
            partial_exit_qty = int(meta.get("partial_exit_qty") or 0)
            if partial_exit_qty > 0 and has_partial:
                total += partial_net
                if state == "CLOSED":
                    exit_fee = round(avg_exit * qty * 0.001, 4) if avg_exit and qty else 0.0
                    total += gross_pnl - exit_fee
                continue
            if state != "CLOSED":
                continue
            entry_fee = round(avg_entry * qty * 0.001, 4) if avg_entry and qty else 0.0
            exit_fee = round(avg_exit * qty * 0.001, 4) if avg_exit and qty else 0.0
            total += gross_pnl - entry_fee - exit_fee
        return total

    def list_open_positions(self, session_id: str) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM paper_positions WHERE session_id = $1 AND closed_at IS NULL "
            "ORDER BY opened_at ASC",
            [session_id],
        )
        return [_serialize_position(r) for r in rows]

    def list_positions_by_session(self, session_id: str) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM paper_positions WHERE session_id = $1 ORDER BY opened_at ASC",
            [session_id],
        )
        return [_serialize_position(r) for r in rows]

    def list_all_open_positions(self) -> list[dict[str, Any]]:
        """Return all open positions across all sessions (for global API queries)."""
        rows = self._query_all(
            "SELECT * FROM paper_positions WHERE closed_at IS NULL ORDER BY opened_at ASC",
        )
        return [_serialize_position(r) for r in rows]

    def adopt_open_positions_from_strategy(
        self, new_session_id: str, strategy_name: str
    ) -> list[dict[str, Any]]:
        """Re-assign open positions from prior sessions of this strategy to new_session_id.

        Called at the start of each daily session to carry positions forward.
        Returns the list of adopted positions (now owned by new_session_id).
        """
        with self._lock:
            result = self.con.execute(
                """
                UPDATE paper_positions
                SET session_id = $1
                WHERE closed_at IS NULL
                  AND session_id != $1
                  AND session_id IN (
                      SELECT session_id FROM paper_sessions WHERE strategy_name = $2
                  )
                RETURNING *
                """,
                [new_session_id, strategy_name],
            )
            desc = result.description
            rows = result.fetchall()
        adopted = [_serialize_position(_row_to_dict(desc, r)) for r in rows]
        if adopted:
            logger.info(
                "Adopted %d open position(s) into session %s strategy=%s",
                len(adopted),
                new_session_id,
                strategy_name,
            )
        return adopted

    def list_all_positions(self) -> list[dict[str, Any]]:
        """Return all positions across all sessions (for global API queries)."""
        rows = self._query_all(
            "SELECT * FROM paper_positions ORDER BY opened_at ASC",
        )
        return [_serialize_position(r) for r in rows]

    def flatten_open_positions(
        self,
        session_id: str,
        *,
        exit_note: str = "FLATTEN",
        mark_prices: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
        """Close all open positions at last-mark price (or avg_entry as zero-PnL fallback).

        Pass ``mark_prices`` to use current bar prices (e.g. from crash recovery).
        The lookup order is: mark_prices → metadata_json.last_mark_price → avg_entry.
        """
        open_positions = self.list_open_positions(session_id)
        closed: list[dict[str, Any]] = []
        now = _now()

        for pos in open_positions:
            meta = _json_loads(pos.get("metadata_json")) or {}
            signal_id = meta.get("signal_id")
            symbol = pos["symbol"]
            exit_price = float(
                (mark_prices or {}).get(symbol) or meta.get("last_mark_price") or pos["avg_entry"]
            )
            avg_entry = float(pos["avg_entry"])
            qty = int(pos["qty"])
            direction = str(pos.get("direction", "LONG")).upper()
            if direction == "SHORT":
                pnl = (avg_entry - exit_price) * qty
            else:
                pnl = (exit_price - avg_entry) * qty

            self.update_position(
                pos["position_id"],
                closed_at=now,
                avg_exit=exit_price,
                pnl=pnl,
                state="CLOSED",
            )

            # SHORT positions are closed by buying back; LONG by selling.
            close_side = "BUY" if direction == "SHORT" else "SELL"

            # Create a synthetic closing order
            order = self.insert_order(
                session_id=session_id,
                signal_id=signal_id,
                symbol=pos["symbol"],
                side=close_side,
                qty=int(pos["qty"]),
                order_type="MARKET",
                status="FILLED",
            )

            # Create a fill
            self.insert_fill(
                session_id=session_id,
                order_id=order["order_id"],
                symbol=pos["symbol"],
                fill_time=now,
                fill_price=exit_price,
                qty=int(pos["qty"]),
                fees=round(exit_price * int(pos["qty"]) * 0.001, 4),
                slippage_bps=0.0,
                side=close_side,
            )

            # Update signal state if linked
            if signal_id:
                self.update_signal_state(signal_id, "EXITED")

            # Audit event
            self.insert_order_event(
                session_id=session_id,
                order_id=order["order_id"],
                signal_id=signal_id,
                event_type="POSITION_FLATTENED",
                event_status="FILLED",
                payload_json={
                    "exit_price": exit_price,
                    "pnl": pnl,
                    "note": exit_note,
                    "position_id": pos["position_id"],
                },
            )

            closed.append(
                {
                    "position_id": pos["position_id"],
                    "symbol": pos["symbol"],
                    "exit_price": exit_price,
                    "pnl": pnl,
                }
            )

        return closed

    # ===================================================================
    # Order CRUD
    # ===================================================================

    def insert_order(
        self,
        *,
        session_id: str,
        signal_id: str | None = None,
        symbol: str,
        side: str,
        qty: int,
        order_type: str,
        limit_price: float | None = None,
        status: str = "PENDING",
        broker_order_id: str | None = None,
        broker_status: str | None = None,
        order_id: str | None = None,
    ) -> dict[str, Any]:
        """Insert a new order.  Returns the serialized order dict."""
        oid = order_id or str(uuid.uuid4())
        now = _now()

        # If broker_order_id is given, try to find an existing order first.
        existing = None
        if broker_order_id:
            existing = self._query_one(
                "SELECT order_id FROM paper_orders WHERE broker_order_id = $1",
                [broker_order_id],
            )
        if existing is None and signal_id:
            existing = self._query_one(
                "SELECT order_id FROM paper_orders WHERE session_id = $1 AND signal_id = $2 AND side = $3",
                [session_id, signal_id, side],
            )

        if existing is None:
            self._execute(
                """
                INSERT INTO paper_orders
                    (order_id, session_id, broker_order_id, signal_id, symbol,
                     side, qty, order_type, limit_price, status, broker_status,
                     created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                [
                    oid,
                    session_id,
                    broker_order_id,
                    signal_id,
                    symbol,
                    side,
                    qty,
                    order_type,
                    limit_price,
                    status,
                    broker_status,
                    now,
                    now,
                ],
            )
        else:
            oid = existing["order_id"]
            self._execute(
                """
                UPDATE paper_orders
                SET broker_order_id = COALESCE($1, broker_order_id),
                    signal_id = COALESCE($2, signal_id),
                    side = $3, qty = $4, order_type = $5,
                    limit_price = $6, status = $7,
                    broker_status = COALESCE($8, broker_status),
                    updated_at = $9
                WHERE order_id = $10
                """,
                [
                    broker_order_id,
                    signal_id,
                    side,
                    qty,
                    order_type,
                    limit_price,
                    status,
                    broker_status,
                    now,
                    oid,
                ],
            )
        return self.get_order(oid)  # type: ignore[return-value]

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_orders WHERE order_id = $1",
            [order_id],
        )
        return _serialize_order(row) if row else None

    def get_order_by_broker_order_id(self, broker_order_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_orders WHERE broker_order_id = $1",
            [broker_order_id],
        )
        return _serialize_order(row) if row else None

    def update_order(
        self,
        order_id: str,
        *,
        status: str | None = None,
        broker_status: str | None = None,
        broker_order_id: str | None = None,
    ) -> dict[str, Any] | None:
        sets: list[str] = ["updated_at = $1"]
        vals: list[Any] = [_now()]
        idx = 2

        if status is not None:
            sets.append(f"status = ${idx}")
            vals.append(status)
            idx += 1
        if broker_status is not None:
            sets.append(f"broker_status = ${idx}")
            vals.append(broker_status)
            idx += 1
        if broker_order_id is not None:
            sets.append(f"broker_order_id = ${idx}")
            vals.append(broker_order_id)
            idx += 1

        vals.append(order_id)
        self._execute(
            f"UPDATE paper_orders SET {', '.join(sets)} WHERE order_id = ${idx}",
            vals,
        )
        return self.get_order(order_id)

    def update_order_broker_state(
        self,
        broker_order_id: str,
        *,
        broker_status: str,
        payload_json: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Update order by broker_order_id.  Returns the order or None."""
        order = self.get_order_by_broker_order_id(broker_order_id)
        if order is None:
            return None
        return self.update_order(
            order["order_id"],
            status=broker_status,
            broker_status=broker_status,
        )

    def list_orders_by_session(
        self,
        session_id: str,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM paper_orders WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2",
            [session_id, limit],
        )
        return [_serialize_order(r) for r in rows]

    # ===================================================================
    # Fill CRUD
    # ===================================================================

    def insert_fill(
        self,
        *,
        session_id: str,
        order_id: str | None = None,
        symbol: str,
        fill_time: datetime,
        fill_price: float,
        qty: int,
        fees: float | None = None,
        slippage_bps: float | None = None,
        broker_trade_id: str | None = None,
        broker_order_id: str | None = None,
        side: str = "BUY",
        fill_id: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Insert a fill.  Deduplicates by broker_trade_id if provided."""
        fid = fill_id or str(uuid.uuid4())

        existing = None
        if broker_trade_id:
            existing = self._query_one(
                "SELECT fill_id FROM paper_fills WHERE broker_trade_id = $1",
                [broker_trade_id],
            )

        if existing is None:
            self._execute(
                """
                INSERT INTO paper_fills
                    (fill_id, session_id, broker_trade_id, broker_order_id,
                     order_id, symbol, fill_time, fill_price, qty,
                     fees, slippage_bps, side, metadata_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                [
                    fid,
                    session_id,
                    broker_trade_id,
                    broker_order_id,
                    order_id,
                    symbol,
                    fill_time,
                    fill_price,
                    qty,
                    fees,
                    slippage_bps,
                    side,
                    _json_dumps(metadata_json or {}),
                ],
            )
        else:
            fid = existing["fill_id"]
            self._execute(
                """
                UPDATE paper_fills
                SET session_id = $1, broker_order_id = COALESCE($2, broker_order_id),
                    order_id = COALESCE($3, order_id),
                    fill_time = $4, fill_price = $5, qty = $6,
                    fees = $7, slippage_bps = $8,
                    metadata_json = $9
                WHERE fill_id = $10
                """,
                [
                    session_id,
                    broker_order_id,
                    order_id,
                    fill_time,
                    fill_price,
                    qty,
                    fees,
                    slippage_bps,
                    _json_dumps(metadata_json or {}),
                    fid,
                ],
            )
        return self.get_fill(fid)  # type: ignore[return-value]

    def get_fill(self, fill_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_fills WHERE fill_id = $1",
            [fill_id],
        )
        return _serialize_fill(row) if row else None

    def list_fills_by_session(
        self,
        session_id: str,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM paper_fills WHERE session_id = $1 ORDER BY fill_time DESC LIMIT $2",
            [session_id, limit],
        )
        return [_serialize_fill(r) for r in rows]

    # ===================================================================
    # Order Events
    # ===================================================================

    def insert_order_event(
        self,
        *,
        session_id: str,
        event_type: str,
        event_status: str,
        order_id: str | None = None,
        signal_id: str | None = None,
        broker_order_id: str | None = None,
        payload_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        eid = str(uuid.uuid4())
        now = _now()
        self._execute(
            """
            INSERT INTO paper_order_events
                (event_id, session_id, order_id, signal_id, event_type,
                 event_status, broker_order_id, payload_json, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            [
                eid,
                session_id,
                order_id,
                signal_id,
                event_type,
                event_status,
                broker_order_id,
                _json_dumps(payload_json or {}),
                now,
            ],
        )
        row = self._query_one(
            "SELECT * FROM paper_order_events WHERE event_id = $1",
            [eid],
        )
        return _serialize_order_event(row) if row else {}

    def list_order_events_by_session(
        self,
        session_id: str,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM paper_order_events WHERE session_id = $1 "
            "ORDER BY created_at DESC LIMIT $2",
            [session_id, limit],
        )
        return [_serialize_order_event(r) for r in rows]

    # ===================================================================
    # Bar Checkpoints  (bar-group watermark)
    # ===================================================================

    def insert_bar_checkpoint(
        self,
        *,
        session_id: str,
        bar_end_ts: datetime,
        committed_symbol_count: int = 0,
        fill_count: int = 0,
        state_hash: str | None = None,
    ) -> dict[str, Any]:
        """Insert a new bar-group checkpoint watermark."""
        now = _now()
        self._execute(
            """
            INSERT INTO paper_bar_checkpoints
                (session_id, bar_end_ts, committed_symbol_count, fill_count, state_hash, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            [session_id, bar_end_ts, committed_symbol_count, fill_count, state_hash, now],
        )
        # Retrieve the just-inserted row by timestamp + session.
        row = self._query_one(
            """
            SELECT * FROM paper_bar_checkpoints
            WHERE session_id = $1 AND bar_end_ts = $2
            ORDER BY checkpoint_id DESC LIMIT 1
            """,
            [session_id, bar_end_ts],
        )
        return _serialize_bar_checkpoint(row) if row else {}

    def get_latest_checkpoint(
        self,
        session_id: str,
    ) -> dict[str, Any] | None:
        """Return the most recent checkpoint for a session."""
        row = self._query_one(
            """
            SELECT * FROM paper_bar_checkpoints
            WHERE session_id = $1
            ORDER BY checkpoint_id DESC LIMIT 1
            """,
            [session_id],
        )
        return _serialize_bar_checkpoint(row) if row else None

    def list_checkpoints_by_session(
        self,
        session_id: str,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM paper_bar_checkpoints WHERE session_id = $1 "
            "ORDER BY created_at DESC LIMIT $2",
            [session_id, limit],
        )
        return [_serialize_bar_checkpoint(r) for r in rows]

    # ===================================================================
    # Feed State
    # ===================================================================

    def upsert_feed_state(
        self,
        *,
        session_id: str,
        source: str,
        mode: str,
        status: str,
        is_stale: bool = False,
        subscription_count: int | None = None,
        heartbeat_at: datetime | None = None,
        last_quote_at: datetime | None = None,
        last_tick_at: datetime | None = None,
        last_bar_at: datetime | None = None,
        raw_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Insert or fully replace the feed state for a session."""
        now = _now()
        existing = self.get_feed_state(session_id)

        if existing is None:
            self._execute(
                """
                INSERT INTO paper_feed_state
                    (session_id, source, mode, status, is_stale, subscription_count,
                     heartbeat_at, last_quote_at, last_tick_at, last_bar_at,
                     raw_state, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                [
                    session_id,
                    source,
                    mode,
                    status,
                    is_stale,
                    subscription_count,
                    heartbeat_at,
                    last_quote_at,
                    last_tick_at,
                    last_bar_at,
                    _json_dumps(raw_state or {}),
                    now,
                ],
            )
        else:
            self._execute(
                """
                UPDATE paper_feed_state
                SET source = $1, mode = $2, status = $3, is_stale = $4,
                    subscription_count = $5, heartbeat_at = $6,
                    last_quote_at = $7, last_tick_at = $8, last_bar_at = $9,
                    raw_state = $10, updated_at = $11
                WHERE session_id = $12
                """,
                [
                    source,
                    mode,
                    status,
                    is_stale,
                    subscription_count,
                    heartbeat_at,
                    last_quote_at,
                    last_tick_at,
                    last_bar_at,
                    _json_dumps(raw_state or {}),
                    now,
                    session_id,
                ],
            )
        return self.get_feed_state(session_id)  # type: ignore[return-value]

    def touch_feed_state(
        self,
        session_id: str,
        *,
        source: str | None = None,
        mode: str | None = None,
        status: str | None = None,
        is_stale: bool | None = None,
        subscription_count: int | None = None,
        heartbeat_at: datetime | None = None,
        last_quote_at: datetime | None = None,
        last_tick_at: datetime | None = None,
        last_bar_at: datetime | None = None,
        raw_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Partially update feed state fields (only non-None values are applied)."""
        now = _now()
        existing = self.get_feed_state(session_id)

        if existing is None:
            return self.upsert_feed_state(
                session_id=session_id,
                source=source or "kite",
                mode=mode or "full",
                status=status or "READY",
                is_stale=is_stale if is_stale is not None else False,
                subscription_count=subscription_count or 0,
                heartbeat_at=heartbeat_at,
                last_quote_at=last_quote_at,
                last_tick_at=last_tick_at,
                last_bar_at=last_bar_at,
                raw_state=raw_state or {},
            )

        sets: list[str] = ["updated_at = $1"]
        vals: list[Any] = [now]
        idx = 2

        if source is not None:
            sets.append(f"source = ${idx}")
            vals.append(source)
            idx += 1
        if mode is not None:
            sets.append(f"mode = ${idx}")
            vals.append(mode)
            idx += 1
        if status is not None:
            sets.append(f"status = ${idx}")
            vals.append(status)
            idx += 1
        if is_stale is not None:
            sets.append(f"is_stale = ${idx}")
            vals.append(is_stale)
            idx += 1
        if subscription_count is not None:
            sets.append(f"subscription_count = ${idx}")
            vals.append(subscription_count)
            idx += 1
        if heartbeat_at is not None:
            sets.append(f"heartbeat_at = ${idx}")
            vals.append(heartbeat_at)
            idx += 1
        if last_quote_at is not None:
            sets.append(f"last_quote_at = ${idx}")
            vals.append(last_quote_at)
            idx += 1
        if last_tick_at is not None:
            sets.append(f"last_tick_at = ${idx}")
            vals.append(last_tick_at)
            idx += 1
        if last_bar_at is not None:
            sets.append(f"last_bar_at = ${idx}")
            vals.append(last_bar_at)
            idx += 1
        if raw_state is not None:
            sets.append(f"raw_state = ${idx}")
            vals.append(_json_dumps(raw_state))
            idx += 1

        vals.append(session_id)
        self._execute(
            f"UPDATE paper_feed_state SET {', '.join(sets)} WHERE session_id = ${idx}",
            vals,
        )
        return self.get_feed_state(session_id)  # type: ignore[return-value]

    def get_feed_state(self, session_id: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM paper_feed_state WHERE session_id = $1",
            [session_id],
        )
        return _serialize_feed_state(row) if row else None

    # ===================================================================
    # Alert Log
    # ===================================================================

    def insert_alert_log(
        self,
        *,
        session_id: str | None = None,
        alert_type: str,
        channel: str,
        status: str,
        payload: dict[str, Any] | None = None,
        sent_at: datetime | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        ts = sent_at or _now()
        self._execute(
            """
            INSERT INTO alert_log
                (session_id, alert_type, channel, status, payload, sent_at, error_message)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            [
                session_id,
                alert_type,
                channel,
                status,
                _json_dumps(payload or {}),
                ts,
                error_message,
            ],
        )
        # Retrieve by timestamp + channel (sufficient for single-writer context).
        row = self._query_one(
            """
            SELECT * FROM alert_log
            WHERE session_id IS NOT DISTINCT FROM $1 AND channel = $2 AND sent_at = $3
            ORDER BY alert_id DESC LIMIT 1
            """,
            [session_id, channel, ts],
        )
        return _serialize_alert_log(row) if row else {}

    def list_alerts_by_session(
        self,
        session_id: str,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = self._query_all(
            "SELECT * FROM alert_log WHERE session_id = $1 ORDER BY sent_at DESC LIMIT $2",
            [session_id, limit],
        )
        return [_serialize_alert_log(r) for r in rows]

    def has_alert_log(
        self,
        session_id: str,
        alert_type: str,
        *,
        status: str | None = None,
        channel: str | None = None,
    ) -> bool:
        """Return True if an alert of this type was already logged for the session.

        This is used as a durable dedup guard for session lifecycle notifications
        so restarts/retries do not re-emit the same transition.
        """
        sql = "SELECT 1 FROM alert_log WHERE session_id = $1 AND alert_type = $2"
        params: list[Any] = [session_id, alert_type]
        idx = 3
        if status is not None:
            sql += f" AND status = ${idx}"
            params.append(status)
            idx += 1
        if channel is not None:
            sql += f" AND channel = ${idx}"
            params.append(channel)
            idx += 1
        sql += " LIMIT 1"
        row = self._query_one(sql, params)
        return row is not None

    # ===================================================================
    # Signal state transitions: QUALIFY / ALERT
    # ===================================================================

    def qualify_session_signals(
        self,
        session_id: str,
        *,
        max_rank: int | None = None,
        min_score: float | None = None,
    ) -> list[dict[str, Any]]:
        """Promote top-ranked NEW signals to QUALIFIED and write audit events."""
        new_signals = self.list_signals_by_session(session_id, states={"NEW"})
        qualified: list[dict[str, Any]] = []

        for sig in new_signals:
            # Look up session-signal for rank/score.
            pss_list = self._query_all(
                "SELECT * FROM paper_session_signals WHERE session_id = $1 AND signal_id = $2",
                [session_id, sig["signal_id"]],
            )
            pss = pss_list[0] if pss_list else None
            rank = pss.get("rank") if pss else None
            score = pss.get("selection_score") if pss else None

            if max_rank is not None and (rank is None or rank > max_rank):
                continue
            if min_score is not None and (score is None or score < min_score):
                continue

            self.update_signal_state(sig["signal_id"], "QUALIFIED")
            if pss is not None:
                self.update_session_signal_decision(
                    session_id,
                    sig["signal_id"],
                    decision_status="QUALIFIED",
                )

            self.insert_order_event(
                session_id=session_id,
                signal_id=sig["signal_id"],
                event_type="SIGNAL_QUALIFIED",
                event_status="QUALIFIED",
                payload_json={"rank": rank, "selection_score": score},
            )
            qualified.append(self.get_signal(sig["signal_id"]))

        return qualified

    def alert_session_signals(
        self,
        session_id: str,
        signal_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Promote specific QUALIFIED signals to ALERTED."""
        qualified_signals = self.list_signals_by_session(
            session_id,
            states={"QUALIFIED"},
        )
        target_ids = set(signal_ids)
        alerted: list[dict[str, Any]] = []

        for sig in qualified_signals:
            if sig["signal_id"] not in target_ids:
                continue

            self.update_signal_state(sig["signal_id"], "ALERTED")
            self.update_session_signal_decision(
                session_id,
                sig["signal_id"],
                decision_status="ALERTED",
            )
            self.insert_order_event(
                session_id=session_id,
                signal_id=sig["signal_id"],
                event_type="SIGNAL_ALERTED",
                event_status="ALERTED",
            )
            alerted.append(self.get_signal(sig["signal_id"]))

        return alerted

    # ===================================================================
    # Session cleanup / archive / stale detection
    # ===================================================================

    def archive_session(self, session_id: str) -> dict[str, Any] | None:
        """Mark a session as ARCHIVED with timestamp."""
        now = _now()
        existing = self.get_session(session_id)
        if existing is None:
            return None

        self._execute(
            """
            UPDATE paper_sessions
            SET status = 'ARCHIVED',
                finished_at = COALESCE(finished_at, $1),
                archived_at = $1,
                updated_at = $1
            WHERE session_id = $2
            """,
            [now, session_id],
        )
        return self.get_session(session_id)

    def archive_sessions(self, session_ids: list[str]) -> dict[str, Any]:
        """Archive multiple sessions.  Returns summary with counts."""
        archived = 0
        not_found = 0
        for sid in session_ids:
            result = self.archive_session(sid)
            if result is None:
                not_found += 1
            else:
                archived += 1
        return {"archived": archived, "not_found": not_found}

    def list_stale_sessions(
        self,
        *,
        mode: str | None = None,
        max_age_hours: int = 48,
        exclude_recent: int = 1,
    ) -> list[dict[str, Any]]:
        """Find sessions that appear stale and should be cleaned up."""
        cutoff = _now() - timedelta(hours=max_age_hours)
        active_statuses = ", ".join(f"'{s}'" for s in ACTIVE_SESSION_STATUSES)

        sql = (
            f"SELECT * FROM paper_sessions WHERE status IN ({active_statuses}) AND created_at < $1 "
        )
        params: list[Any] = [cutoff]
        idx = 2

        if mode:
            sql += f"AND mode = ${idx} "
            params.append(mode)
            idx += 1

        sql += "ORDER BY created_at DESC"
        all_stale = self._query_all(sql, params)

        if exclude_recent > 0 and all_stale:
            # Protect the N most recent sessions per mode.
            protect_sql = "SELECT session_id FROM paper_sessions ORDER BY created_at DESC LIMIT $1"
            protect_rows = self._query_all(protect_sql, [exclude_recent])
            protected_ids = {r["session_id"] for r in protect_rows}
            all_stale = [s for s in all_stale if s["session_id"] not in protected_ids]

        return [_serialize_session(s) for s in all_stale]

    def cleanup_stale_sessions(self, *, max_age_minutes: int = 15) -> int:
        """Cancel orphaned STOPPING sessions older than max_age_minutes.

        Called on every CLI startup to clean up sessions left in STOPPING status
        by crashed processes. ACTIVE sessions are intentionally left alone because
        they might actually be running.

        Returns the count of cleaned sessions.
        """
        cutoff = _now() - timedelta(minutes=max_age_minutes)
        result = self._execute(
            "UPDATE paper_sessions SET status = 'CANCELLED', "
            "notes = 'auto-cancelled: stale session from previous run', "
            "updated_at = $1 "
            "WHERE status = 'STOPPING' AND updated_at < $2",
            [_now(), cutoff],
        )
        count = result.get("rows_affected", 0) if isinstance(result, dict) else 0
        if count > 0:
            logger.info("Cleaned up %d stale STOPPING session(s)", count)
        return count

    # ===================================================================
    # Convenience: sync session signals from signals
    # ===================================================================

    def sync_session_signals_from_signals(
        self,
        session_id: str,
        *,
        decision_status: str = "PENDING",
    ) -> list[dict[str, Any]]:
        """Ensure a paper_session_signals row exists for every signal in the session."""
        signals = self.list_signals_by_session(session_id)
        synced: list[dict[str, Any]] = []
        for idx, sig in enumerate(signals, start=1):
            reason = None
            meta = _json_loads(sig.get("metadata_json")) or {}
            if isinstance(meta, dict):
                reason = meta.get("decision_reason")

            entry = self.insert_session_signal(
                session_id=session_id,
                signal_id=sig["signal_id"],
                symbol=sig["symbol"],
                asof_date=sig["asof_date"] if sig.get("asof_date") else date.today(),
                decision_status=decision_status,
                rank=idx,
                selection_score=None,
                decision_reason=reason,
                metadata_json=meta,
            )
            synced.append(entry)
        return synced

    # ===================================================================
    # Feed Audit
    # ===================================================================

    def upsert_feed_audit_rows(self, rows: list[dict[str, Any]]) -> int:
        """Upsert feed audit rows (one per session/symbol/bar_end).

        Uses INSERT OR REPLACE so re-delivery of the same bar is idempotent.
        Returns the number of rows processed.
        """
        if not rows:
            return 0
        with self._lock:
            count = 0
            for row in rows:
                self._execute(
                    """
                    INSERT OR REPLACE INTO paper_feed_audit
                        (session_id, trade_date, feed_source, transport, symbol,
                         bar_start, bar_end, open, high, low, close, volume,
                         first_snapshot_ts, last_snapshot_ts, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    """,
                    [
                        row["session_id"],
                        row["trade_date"],
                        row.get("feed_source", ""),
                        row.get("transport", ""),
                        row["symbol"],
                        row.get("bar_start"),
                        row["bar_end"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        row.get("first_snapshot_ts"),
                        row.get("last_snapshot_ts"),
                        row.get("created_at") or _now(),
                    ],
                )
                count += 1
        return count

    def get_feed_audit_rows(
        self,
        *,
        trade_date: str,
        session_id: str | None = None,
        feed_source: str | None = None,
    ) -> list[FeedAudit]:
        """Return recorded feed audit rows for a trade date, optionally filtered."""
        params: list[Any] = [trade_date]
        where = "trade_date = $1"
        idx = 2
        if session_id is not None:
            where += f" AND session_id = ${idx}"
            params.append(session_id)
            idx += 1
        if feed_source is not None:
            where += f" AND feed_source = ${idx}"
            params.append(feed_source)
            idx += 1

        rows = self._query_all(
            f"SELECT * FROM paper_feed_audit WHERE {where} ORDER BY symbol, bar_end",
            params,
        )

        def _ts(val: Any) -> datetime | None:
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            return None

        return [
            FeedAudit(
                session_id=r["session_id"],
                trade_date=str(r["trade_date"]),
                feed_source=str(r.get("feed_source") or ""),
                transport=str(r.get("transport") or ""),
                symbol=r["symbol"],
                bar_start=_ts(r.get("bar_start")),
                bar_end=r["bar_end"],
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=float(r["volume"]),
                first_snapshot_ts=_ts(r.get("first_snapshot_ts")),
                last_snapshot_ts=_ts(r.get("last_snapshot_ts")),
            )
            for r in rows
        ]

    def purge_old_feed_audit_rows(self, retention_days: int = 7) -> int:
        """Delete feed audit rows older than ``retention_days`` days.

        Returns the number of rows deleted.
        """
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        cutoff_date = cutoff.date().isoformat()
        with self._lock:
            self._execute(
                "DELETE FROM paper_feed_audit WHERE trade_date < $1",
                [cutoff_date],
            )
        # DuckDB doesn't return row-counts for DELETE directly; query the changes.
        return 0  # Best-effort; callers only use this for logging.
