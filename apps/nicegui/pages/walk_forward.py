"""Walk Forward page - Dedicated validation history and rerun workspace."""

from __future__ import annotations

import asyncio
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
    COLORS,
    THEME,
    divider,
    empty_state,
    info_box,
    kpi_grid,
    page_layout,
    paginated_table,
)
from apps.nicegui.pages.paper_ledger import (
    _decision_reason_label,
    _decision_status_label,
    _fmt_float,
    _fmt_ts,
    _mode_label,
    _session_label,
    _status_color,
    _status_label,
    _strategy_label,
    _walk_forward_rows,
    _walk_forward_rows_from_db,
)
from apps.nicegui.state import (
    aget_paper_session_summary,
    aget_paper_sessions,
    aget_walk_forward_folds,
)


def _format_walk_forward_summary_text(session: dict[str, Any]) -> str:
    strategy_params = session.get("strategy_params") or {}
    walk_forward = strategy_params.get("walk_forward")
    if not isinstance(walk_forward, dict):
        return (
            "This is a completed walk-forward validation session. "
            "Use it to inspect the rolling folds that were already run and to rerun the same "
            "validation protocol when you need a fresh gate."
        )

    requested_range = walk_forward.get("requested_date_range") or {}
    start = requested_range.get("start") or "unknown start"
    end = requested_range.get("end") or "unknown end"
    summary = walk_forward.get("summary") or {}
    decision = walk_forward.get("decision") or {}
    return (
        f"Validation window {start} -> {end}. "
        f"Decision: {_decision_status_label(decision.get('status'))}. "
        f"{_decision_reason_label(decision.get('reason'))}. "
        f"Average return: {_fmt_float(summary.get('avg_return_pct'))}%. "
        f"Worst drawdown: {_fmt_float(summary.get('worst_drawdown_pct'))}%."
    )


def _walk_forward_command_lines(session: dict[str, Any]) -> list[str]:
    strategy_params = session.get("strategy_params") or {}
    walk_forward = strategy_params.get("walk_forward")
    if not isinstance(walk_forward, dict):
        return [
            "doppler run -- uv run nseml-paper cleanup-walk-forward --yes",
            "doppler run -- uv run nseml-paper walk-forward --strategy thresholdbreakout --start-date YYYY-MM-DD --end-date YYYY-MM-DD --train-days 5 --test-days 3 --roll-interval-days 1 --force",
        ]

    requested_range = walk_forward.get("requested_date_range") or {}
    base_params = walk_forward.get("base_params")
    if not isinstance(base_params, dict):
        base_params = strategy_params
    train_days = int(strategy_params.get("train_days") or 252)
    test_days = int(strategy_params.get("test_days") or 63)
    roll_days = int(strategy_params.get("roll_interval_days") or test_days)
    strategy = str(session.get("strategy_name") or "thresholdbreakout")
    start_date = str(requested_range.get("start") or "YYYY-MM-DD")
    end_date = str(requested_range.get("end") or "YYYY-MM-DD")

    params_json = ""
    try:
        breakout_threshold = base_params.get("breakout_threshold")
        if breakout_threshold is not None and abs(float(breakout_threshold) - 0.04) > 1e-9:
            params_json = (
                f" --params-json '{{\"breakout_threshold\": {float(breakout_threshold):.2f}}}'"
            )
    except TypeError, ValueError:
        params_json = ""

    session_id = str(session.get("session_id") or "<SESSION_ID>")
    return [
        "doppler run -- uv run nseml-paper cleanup-walk-forward --yes",
        (
            "doppler run -- uv run nseml-paper walk-forward "
            f"--session-id {session_id} --strategy {strategy} "
            f"--start-date {start_date} --end-date {end_date} "
            f"--train-days {train_days} --test-days {test_days} --roll-interval-days {roll_days}"
            f"{params_json} --force"
        ),
    ]


async def walk_forward_page() -> None:
    """Render the dedicated walk-forward validation page."""

    with page_layout("Walk Forward", "view_week"):
        ui.label("Walk-Forward Validation").classes("text-3xl font-bold mb-1").style(
            f"color: {THEME['text_primary']};"
        )
        ui.label(
            "Dedicated history for rolling validation runs. This page keeps promotion-gate "
            "review separate from replay/live paper trading."
        ).classes("text-lg mb-3").style(f"color: {THEME['text_secondary']};")
        ui.label(
            "Use this view to inspect fold summaries, lineage, and rerun commands after cleaning "
            "stale validation sessions."
        ).classes("text-sm mb-4").style(f"color: {THEME['text_muted']};")

        divider()

        try:
            sessions = await aget_paper_sessions(limit=100)
        except Exception as exc:
            info_box(
                f"Could not load walk-forward sessions. Ensure PostgreSQL and Doppler-backed settings are available. {exc}",
                color="red",
            )
            return

        walk_forward_sessions = [
            session
            for session in sessions
            if str(session.get("mode") or "").strip().lower() == "walk_forward"
        ]

        commands_card = ui.column().classes("kpi-card p-5 mb-6")
        with commands_card:
            ui.label("Validation Commands").classes("text-lg font-semibold mb-2").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label(
                "Run a clean walk-forward cycle from the CLI, then come back here to review the "
                "persisted fold rows and promotion decision."
            ).classes("text-sm mb-3").style(f"color: {THEME['text_secondary']};")
            for command in _walk_forward_command_lines({}):
                with ui.row().classes("w-full items-center gap-2 mb-2"):
                    ui.label("$").classes("font-mono").style(f"color: {COLORS['success']};")
                    ui.label(command).classes(
                        "flex-grow font-mono text-sm px-3 py-2 rounded"
                    ).style(
                        f"background: {THEME['surface_hover']}; border: 1px solid {THEME['surface_border']}; color: {THEME['text_primary']}; border-radius: 6px;"
                    )

        if not walk_forward_sessions:
            empty_state(
                "No walk-forward sessions yet",
                "Run nseml-paper cleanup-walk-forward --yes, then start a fresh walk-forward validation run from the CLI.",
                icon="view_week",
            )
            return

        ordered_sessions = sorted(
            walk_forward_sessions,
            key=lambda row: (str(row.get("updated_at") or row.get("created_at") or ""),),
            reverse=True,
        )
        option_map = {
            _session_label(session): session["session_id"] for session in ordered_sessions
        }
        current = {
            "label": next(iter(option_map)),
            "session_id": ordered_sessions[0]["session_id"],
        }

        async def render_session(session_id: str) -> None:
            content.clear()
            with content:
                try:
                    summary, db_folds = await asyncio.gather(
                        aget_paper_session_summary(session_id),
                        aget_walk_forward_folds(session_id),
                    )
                except Exception as exc:
                    info_box(
                        f"Could not load walk-forward session {session_id}: {exc}", color="red"
                    )
                    return

                if not summary:
                    empty_state(
                        "Walk-forward session not found",
                        f"Session {session_id} no longer exists.",
                        icon="error",
                    )
                    return

                session = summary["session"]
                strategy_params = session.get("strategy_params") or {}
                walk_forward = strategy_params.get("walk_forward")
                if not isinstance(walk_forward, dict):
                    walk_forward = {}

                wf_summary = walk_forward.get("summary") or {}
                wf_decision = walk_forward.get("decision") or {}
                decision_status = _decision_status_label(wf_decision.get("status"))
                decision_reason = _decision_reason_label(wf_decision.get("reason"))
                session_color = _status_color(session.get("status"))
                decision_color = (
                    COLORS["success"]
                    if str(wf_decision.get("status") or "").upper() == "PASS"
                    else COLORS["warning"]
                )

                commands_card.clear()
                with commands_card:
                    ui.label("Validation Commands").classes("text-lg font-semibold mb-2").style(
                        f"color: {THEME['text_primary']};"
                    )
                    ui.label(
                        "Run the selected validation protocol again from the CLI, then return here "
                        "to review the persisted fold rows and promotion decision."
                    ).classes("text-sm mb-3").style(f"color: {THEME['text_secondary']};")
                    for command in _walk_forward_command_lines(session):
                        with ui.row().classes("w-full items-center gap-2 mb-2"):
                            ui.label("$").classes("font-mono").style(f"color: {COLORS['success']};")
                            ui.label(command).classes(
                                "flex-grow font-mono text-sm px-3 py-2 rounded"
                            ).style(
                                f"background: {THEME['surface_hover']}; border: 1px solid {THEME['surface_border']}; color: {THEME['text_primary']}; border-radius: 6px;"
                            )

                ui.label("What This Validation Means").classes("text-lg font-semibold mb-2").style(
                    f"color: {THEME['text_primary']};"
                )
                ui.label(_format_walk_forward_summary_text(session)).classes(
                    "leading-6 mb-4"
                ).style(f"color: {THEME['text_secondary']};")

                kpi_grid(
                    [
                        dict(
                            title="Session Status",
                            value=_status_label(session.get("status")),
                            subtitle=_mode_label(session.get("mode")),
                            icon="schedule",
                            color=session_color,
                        ),
                        dict(
                            title="Decision",
                            value=decision_status,
                            subtitle=decision_reason,
                            icon="verified",
                            color=decision_color,
                        ),
                        dict(
                            title="Fold Coverage",
                            value=int(wf_summary.get("folds_total", 0) or 0),
                            subtitle=f"Completed: {int(wf_summary.get('folds_completed', 0) or 0)}",
                            icon="view_week",
                            color=COLORS["info"],
                        ),
                        dict(
                            title="Avg Return",
                            value=f"{_fmt_float(wf_summary.get('avg_return_pct'))}%",
                            subtitle=f"Profitable: {int(wf_summary.get('folds_profitable', 0) or 0)}",
                            icon="trending_up",
                            color=COLORS["success"]
                            if float(wf_summary.get("avg_return_pct") or 0) >= 0
                            else COLORS["error"],
                        ),
                        dict(
                            title="Worst Drawdown",
                            value=f"{_fmt_float(wf_summary.get('worst_drawdown_pct'))}%",
                            subtitle="Fold maximum DD",
                            icon="south_east",
                            color=COLORS["warning"],
                        ),
                        dict(
                            title="Total Trades",
                            value=int(wf_summary.get("total_trades", 0) or 0),
                            subtitle=f"Window pairs: {len(walk_forward.get('test_ranges') or [])}",
                            icon="swap_horiz",
                            color=COLORS["primary"],
                        ),
                    ],
                    columns=6,
                )

                with ui.expansion("Validation Details", icon="info").classes(
                    "kpi-card p-5 mb-6 w-full"
                ):
                    with ui.grid(columns=2).classes("w-full gap-4"):
                        with ui.column().classes("h-full"):
                            ui.label("Session").classes("text-lg font-semibold mb-3").style(
                                f"color: {THEME['text_primary']};"
                            )
                            requested_range = walk_forward.get("requested_date_range") or {}
                            lineage = walk_forward.get("lineage") or {}
                            for label, value in [
                                ("Session ID", session.get("session_id")),
                                ("Trade Date", session.get("trade_date")),
                                (
                                    "Strategy",
                                    _strategy_label(session.get("strategy_name"), strategy_params),
                                ),
                                ("Session Type", _mode_label(session.get("mode"))),
                                (
                                    "Requested Range",
                                    f"{requested_range.get('start') or '-'} -> {requested_range.get('end') or '-'}",
                                ),
                                ("Updated", _fmt_ts(session.get("updated_at"))),
                                (
                                    "Base Params Hash",
                                    str(walk_forward.get("base_params_hash") or "-"),
                                ),
                                (
                                    "Dataset Hashes",
                                    ", ".join((lineage.get("dataset_hashes") or [])[:3]) or "-",
                                ),
                                (
                                    "Code Hashes",
                                    ", ".join((lineage.get("code_hashes") or [])[:3]) or "-",
                                ),
                            ]:
                                with ui.row().classes("justify-between w-full gap-4"):
                                    ui.label(label).style(f"color: {THEME['text_secondary']};")
                                    ui.label(str(value or "-")).classes("font-mono").style(
                                        f"color: {THEME['text_primary']};"
                                    )

                        with ui.column().classes("h-full"):
                            ui.label("Validation Summary").classes(
                                "text-lg font-semibold mb-3"
                            ).style(f"color: {THEME['text_primary']};")
                            for label, value in [
                                ("Average Return", _fmt_float(wf_summary.get("avg_return_pct"))),
                                ("Median Return", _fmt_float(wf_summary.get("median_return_pct"))),
                                (
                                    "Worst Drawdown",
                                    _fmt_float(wf_summary.get("worst_drawdown_pct")),
                                ),
                                ("Folds Total", wf_summary.get("folds_total")),
                                ("Folds Completed", wf_summary.get("folds_completed")),
                                ("Folds Profitable", wf_summary.get("folds_profitable")),
                                (
                                    "Profitable Ratio",
                                    _fmt_float(wf_summary.get("folds_profitable_ratio"), 4),
                                ),
                                ("Total Trades", wf_summary.get("total_trades")),
                            ]:
                                with ui.row().classes("justify-between w-full gap-4"):
                                    ui.label(label).style(f"color: {THEME['text_secondary']};")
                                    ui.label(str(value or "-")).classes("font-mono").style(
                                        f"color: {THEME['text_primary']};"
                                    )

                with ui.expansion("Validation Folds", icon="table_view").classes(
                    "kpi-card p-5 mb-6 w-full"
                ):
                    fold_rows = (
                        _walk_forward_rows_from_db(db_folds)
                        if db_folds
                        else _walk_forward_rows(strategy_params)
                    )
                    if fold_rows:
                        paginated_table(
                            rows=fold_rows,
                            columns=[
                                {"name": "fold", "label": "Fold", "field": "fold"},
                                {"name": "train", "label": "Train / Lookback", "field": "train"},
                                {"name": "test", "label": "Test / Eval", "field": "test"},
                                {"name": "status", "label": "Status", "field": "status"},
                                {"name": "return_pct", "label": "Return %", "field": "return_pct"},
                                {
                                    "name": "drawdown_pct",
                                    "label": "Drawdown %",
                                    "field": "drawdown_pct",
                                },
                                {"name": "trades", "label": "Trades", "field": "trades"},
                                {"name": "exp_id", "label": "Exp ID", "field": "exp_id"},
                            ],
                            page_size=10,
                        )
                    else:
                        empty_state(
                            "No fold rows found",
                            "This session does not have persisted fold rows yet. Run the walk-forward command again to repopulate the validation history.",
                            icon="table_view",
                        )

                if db_folds:
                    ui.label(f"Fold rows loaded from PostgreSQL: {len(db_folds)}").classes(
                        "text-xs mt-2"
                    ).style(f"color: {THEME['text_muted']};")

        with ui.row().classes("items-center gap-3 mb-6 w-full"):

            async def handle_session_change(event) -> None:
                current["label"] = str(event.value)
                current["session_id"] = option_map[current["label"]]
                await render_session(current["session_id"])

            ui.select(
                options=list(option_map.keys()),
                value=current["label"],
                label="Validation Session",
                on_change=handle_session_change,
            ).classes("flex-1 min-w-[860px]")
            ui.button(
                "Refresh",
                icon="refresh",
                on_click=lambda: asyncio.create_task(render_session(current["session_id"])),
            ).props("outline")
            ui.label(f"{len(walk_forward_sessions):,} validation session(s) available").classes(
                "text-sm"
            ).style(f"color: {THEME['text_muted']};")

        content = ui.column().classes("w-full")
        await render_session(current["session_id"])
