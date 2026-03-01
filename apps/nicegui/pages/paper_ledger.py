"""Paper Ledger page - Paper trading position tracking."""

from __future__ import annotations

import sys
from pathlib import Path

_apps_root = Path(__file__).resolve().parent.parent
_project_root = _apps_root.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))
if str(_apps_root) not in sys.path:
    sys.path.insert(0, str(_apps_root))

from nicegui import ui

from apps.nicegui.components import page_layout, divider, COLORS, THEME


def paper_ledger_page() -> None:
    """Render the paper trading ledger page."""
    with page_layout("Paper Ledger", "receipt_long"):
        with ui.column().classes("kpi-card p-6 mb-6"):
            ui.label("Paper Trading").classes("text-xl font-semibold mb-2").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label(
                "This page tracks paper trading positions based on live scan signals."
            ).classes("mb-4").style(f"color: {THEME['text_secondary']};")

            ui.label("Paper trading is currently in development. Coming soon:").classes(
                "mb-2"
            ).style(f"color: {COLORS['warning']};")

            for item in [
                "Auto-enter positions from daily scans",
                "Track entry/exit with 2LYNCH rules",
                "Calculate real-time P&L",
                "Compare paper vs backtest performance",
            ]:
                with ui.row().classes("items-center gap-2"):
                    ui.icon("circle", size="8px").style(f"color: {THEME['text_muted']};")
                    ui.label(item).style(f"color: {THEME['text_muted']};")

        divider()

        with ui.column().classes("kpi-card"):
            ui.label("Coming Soon").classes("text-lg font-semibold mb-2").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label(
                "Paper trading will allow you to track positions based on actual market signals "
                "and compare real-world performance against backtest results."
            ).style(f"color: {THEME['text_muted']};")
