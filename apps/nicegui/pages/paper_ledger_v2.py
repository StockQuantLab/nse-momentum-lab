"""Paper Ledger v2 page — DuckDB-backed paper trading dashboard.

Reads from the paper_dashboard.duckdb replica so the live writer is never blocked.
Shows sessions, positions, P&L, fills, and alerts for v2 paper sessions.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_apps_root = Path(__file__).resolve().parent.parent
_project_root = _apps_root.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))
if str(_apps_root) not in sys.path:
    sys.path.insert(0, str(_apps_root))

from nicegui import ui

from apps.nicegui.components import (
    empty_state,
    kpi_grid,
    page_layout,
    paginated_table,
    safe_timer,
    SPACE_MD,
    theme_text_muted,
    theme_text_primary,
)
from nse_momentum_lab.db.versioned_replica_consumer import VersionedReplicaConsumer


def _get_consumer() -> VersionedReplicaConsumer:
    return VersionedReplicaConsumer(
        replica_dir=_project_root / "data" / "paper_replica",
        prefix="paper_replica",
        fallback_path=_project_root / "data" / "paper_dashboard.duckdb",
    )


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except TypeError, ValueError:
        return None


def _fmt_float(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):,.{digits}f}"
    except TypeError, ValueError:
        return str(value)


def _strategy_label(strategy: str | None) -> str:
    labels = {
        "2lynchbreakout": "2LYNCH Breakout",
        "thresholdbreakout": "2LYNCH Breakout",
        "2lynchbreakdown": "2LYNCH Breakdown",
        "episodicpivot": "EP Pivot",
    }
    raw = str(strategy or "").strip().lower()
    return labels.get(raw, str(strategy or "-"))


def _load_sessions(consumer: VersionedReplicaConsumer) -> list[dict[str, Any]]:
    rows = consumer.execute("SELECT * FROM paper_sessions ORDER BY updated_at DESC")
    return rows if rows is not None else []


def _load_positions(consumer: VersionedReplicaConsumer, session_id: str) -> list[dict[str, Any]]:
    rows = consumer.execute(
        "SELECT * FROM paper_positions WHERE session_id = ? ORDER BY opened_at DESC",
        [session_id],
    )
    return rows if rows is not None else []


def _load_fills(consumer: VersionedReplicaConsumer, session_id: str) -> list[dict[str, Any]]:
    rows = consumer.execute(
        "SELECT * FROM paper_fills WHERE session_id = ? ORDER BY fill_time DESC",
        [session_id],
    )
    return rows if rows is not None else []


def _load_alerts(
    consumer: VersionedReplicaConsumer, session_id: str, limit: int = 50
) -> list[dict[str, Any]]:
    rows = consumer.execute(
        "SELECT * FROM alert_log WHERE session_id = ? ORDER BY sent_at DESC LIMIT ?",
        [session_id, limit],
    )
    return rows if rows is not None else []


def _compute_session_pnl(positions: list[dict[str, Any]]) -> dict[str, float]:
    realized = 0.0
    unrealized = 0.0
    for p in positions:
        pnl = float(p.get("pnl", 0) or 0)
        state = str(p.get("state", "")).upper()
        if state == "CLOSED":
            realized += pnl
        elif state == "OPEN":
            unrealized += pnl
    return {"realized": realized, "unrealized": unrealized, "total": realized + unrealized}


def _session_label(session: dict[str, Any]) -> str:
    trade_date = session.get("trade_date", "na")
    strategy = _strategy_label(session.get("strategy_name"))
    status = str(session.get("status", "?")).upper()
    sid = str(session.get("session_id", "-"))[:12]
    return f"{trade_date} | {strategy} | {status} | {sid}"


def _position_rows(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for p in positions:
        rows.append(
            {
                "symbol": p.get("symbol", "-"),
                "direction": p.get("direction", "-"),
                "state": p.get("state", "-"),
                "qty": p.get("qty", 0),
                "avg_entry": _to_float(p.get("avg_entry")),
                "avg_exit": _to_float(p.get("avg_exit")),
                "pnl": _to_float(p.get("pnl")),
                "opened_at": str(p.get("opened_at", "-"))[:19],
            }
        )
    return rows


def _fill_rows(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for f in fills:
        rows.append(
            {
                "symbol": f.get("symbol", "-"),
                "side": f.get("side", "-"),
                "qty": f.get("qty", 0),
                "price": _to_float(f.get("fill_price")),
                "pnl": _to_float(f.get("pnl")),
                "fill_time": str(f.get("fill_time", "-"))[:19],
            }
        )
    return rows


def _alert_rows(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for a in alerts:
        rows.append(
            {
                "type": a.get("alert_type", "-"),
                "level": a.get("alert_level", "-"),
                "status": a.get("status", "-"),
                "channel": a.get("channel", "-"),
                "subject": str(a.get("subject", a.get("payload", "-")))[:80],
                "sent_at": str(a.get("sent_at", "-"))[:19],
            }
        )
    return rows


async def paper_ledger_v2_page() -> None:
    """Paper Ledger v2 page — reads from DuckDB replica."""
    with page_layout("Paper Ledger v2", "receipt_long"):
        consumer = _get_consumer()

        # Load sessions.
        sessions = _load_sessions(consumer)
        if not sessions:
            empty_state("No v2 paper sessions found", "Create one with nseml-paper-v2 prepare")
            return

        # Staleness indicator.
        stale_sec = consumer.get_stale_seconds()
        if stale_sec is not None and stale_sec > 60:
            ui.label(f"Replica is {stale_sec:.0f}s behind live").classes(
                f"text-sm {theme_text_muted()}"
            )

        # Session selector.
        session_options = {s.get("session_id", ""): _session_label(s) for s in sessions}
        active_sid = sessions[0].get("session_id", "")

        select = ui.select(
            options=session_options,
            value=active_sid,
            with_input=True,
            label="Session",
        ).classes("w-full")

        # KPI row.
        kpi_container = ui.column().classes("w-full")

        # Content container.
        content = ui.column().classes("w-full")

        def render_session() -> None:
            sid = select.value
            if not sid:
                return

            session = None
            for s in sessions:
                if s.get("session_id") == sid:
                    session = s
                    break
            if session is None:
                return

            # Reload from replica for fresh data.
            fresh_sessions = _load_sessions(consumer)
            for s in fresh_sessions:
                if s.get("session_id") == sid:
                    session = s
                    break

            positions = _load_positions(consumer, sid)
            pnl = _compute_session_pnl(positions)
            fills = _load_fills(consumer, sid)
            alerts = _load_alerts(consumer, sid)
            open_count = sum(1 for p in positions if str(p.get("state", "")).upper() == "OPEN")

            # KPI cards.
            kpi_container.clear()
            with kpi_container:
                kpi_grid(
                    [
                        {"label": "Status", "value": str(session.get("status", "-")).upper()},
                        {"label": "Open Positions", "value": str(open_count)},
                        {"label": "Fills", "value": str(len(fills))},
                        {"label": "Realized P&L", "value": _fmt_float(pnl["realized"])},
                        {"label": "Unrealized P&L", "value": _fmt_float(pnl["unrealized"])},
                        {"label": "Total P&L", "value": _fmt_float(pnl["total"])},
                    ]
                )

            # Content.
            content.clear()
            with content:
                # Session details.
                with ui.expansion("Session Details", value=False).classes("w-full"):
                    ui.label(f"Session ID: {session.get('session_id', '-')}").classes(
                        f"text-sm {theme_text_muted()}"
                    )
                    ui.label(f"Strategy: {_strategy_label(session.get('strategy_name'))}").classes(
                        f"text-sm {theme_text_muted()}"
                    )
                    ui.label(f"Trade Date: {session.get('trade_date', '-')}").classes(
                        f"text-sm {theme_text_muted()}"
                    )
                    ui.label(f"Status: {session.get('status', '-')}").classes(
                        f"text-sm {theme_text_muted()}"
                    )

                # Positions.
                ui.space(SPACE_MD)
                ui.label("Positions").classes(f"text-lg font-semibold {theme_text_primary()}")
                pos_cols = [
                    {"name": "symbol", "label": "Symbol", "field": "symbol", "sortable": True},
                    {
                        "name": "direction",
                        "label": "Direction",
                        "field": "direction",
                        "sortable": True,
                    },
                    {"name": "state", "label": "State", "field": "state", "sortable": True},
                    {"name": "qty", "label": "Qty", "field": "qty", "sortable": True},
                    {
                        "name": "avg_entry",
                        "label": "Entry",
                        "field": "avg_entry",
                        "sortable": True,
                        "format": "val => val == null ? '-' : val.toFixed(2)",
                    },
                    {
                        "name": "avg_exit",
                        "label": "Exit",
                        "field": "avg_exit",
                        "sortable": True,
                        "format": "val => val == null ? '-' : val.toFixed(2)",
                    },
                    {
                        "name": "pnl",
                        "label": "P&L",
                        "field": "pnl",
                        "sortable": True,
                        "format": "val => val == null ? '-' : val.toFixed(2)",
                    },
                    {
                        "name": "opened_at",
                        "label": "Opened",
                        "field": "opened_at",
                        "sortable": True,
                    },
                ]
                paginated_table(pos_cols, _position_rows(positions))

                # Fills.
                ui.space(SPACE_MD)
                ui.label("Fills").classes(f"text-lg font-semibold {theme_text_primary()}")
                fill_cols = [
                    {"name": "symbol", "label": "Symbol", "field": "symbol", "sortable": True},
                    {"name": "side", "label": "Side", "field": "side", "sortable": True},
                    {"name": "qty", "label": "Qty", "field": "qty"},
                    {
                        "name": "price",
                        "label": "Price",
                        "field": "price",
                        "format": "val => val == null ? '-' : val.toFixed(2)",
                    },
                    {
                        "name": "pnl",
                        "label": "P&L",
                        "field": "pnl",
                        "format": "val => val == null ? '-' : val.toFixed(2)",
                    },
                    {"name": "fill_time", "label": "Time", "field": "fill_time", "sortable": True},
                ]
                paginated_table(fill_cols, _fill_rows(fills))

                # Alerts.
                if alerts:
                    ui.space(SPACE_MD)
                    ui.label("Alerts").classes(f"text-lg font-semibold {theme_text_primary()}")
                    alert_cols = [
                        {"name": "type", "label": "Type", "field": "type", "sortable": True},
                        {"name": "level", "label": "Level", "field": "level"},
                        {"name": "status", "label": "Status", "field": "status"},
                        {"name": "channel", "label": "Channel", "field": "channel"},
                        {"name": "subject", "label": "Subject", "field": "subject"},
                        {
                            "name": "sent_at",
                            "label": "Time",
                            "field": "sent_at",
                            "sortable": True,
                        },
                    ]
                    paginated_table(alert_cols, _alert_rows(alerts))

        select.on_value_change(lambda: render_session())
        render_session()

        # Auto-refresh.
        safe_timer(30, lambda: render_session(), once=False)
