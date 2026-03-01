"""Daily Summary page - Daily market overview and statistics."""

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

from datetime import date
from nicegui import ui

from apps.nicegui.state import get_db_status
from apps.nicegui.components import page_layout, kpi_grid, divider, info_box, COLORS, THEME


def daily_summary_page() -> None:
    """Render the daily summary page."""
    with page_layout("Daily Summary", "today"):
        status = get_db_status()
        today = date.today().isoformat()

        ui.label(f"Market Summary for {today}").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        kpi_grid(
            [
                dict(
                    title="Data Status",
                    value="Current",
                    subtitle="Latest data available",
                    icon="check_circle",
                    color=COLORS["success"],
                ),
                dict(
                    title="Universe Size",
                    value=f"{int(status.get('symbols', 0)):,}",
                    subtitle="Active symbols",
                    icon="bar_chart",
                    color=COLORS["info"],
                ),
                dict(
                    title="Total Candles",
                    value=f"{int(status.get('total_candles', 0)):,}",
                    subtitle="In database",
                    icon="candlestick_chart",
                    color=COLORS["warning"],
                ),
                dict(
                    title="Date Range",
                    value=status.get("date_range", "-"),
                    subtitle="Coverage period",
                    icon="date_range",
                    color=COLORS["gray"],
                ),
            ]
        )

        divider()

        ui.label("Market Health").classes("text-lg font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        indicators = [
            ("4% Breakouts Today", "Scans needed", "show_chart"),
            ("2LYNCH Pass Rate", "N/A", "filter_alt"),
            ("Top Gainer", "Scan required", "arrow_upward"),
            ("Top Loser", "Scan required", "arrow_downward"),
        ]

        with ui.column().classes("gap-3"):
            for title, value, icon in indicators:
                with ui.row().classes("kpi-card items-center gap-3 w-full"):
                    ui.icon(icon).classes("text-xl").style(f"color: {THEME['primary']};")
                    with ui.column().classes("gap-0"):
                        ui.label(title).classes("text-sm").style(
                            f"color: {THEME['text_secondary']};"
                        )
                        ui.label(value).classes("text-lg font-semibold").style(
                            f"color: {THEME['text_primary']};"
                        )

        divider()

        info_box("Run daily scans to see live market indicators.")
