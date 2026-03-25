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
from apps.nicegui.components import (
    color_gray,
    color_info,
    color_success,
    color_warning,
    divider,
    info_box,
    kpi_grid,
    page_layout,
    SPACE_MD,
    SPACE_XL,
    theme_primary,
    theme_text_primary,
    theme_text_secondary,
)


def daily_summary_page() -> None:
    """Render the daily summary page."""
    with page_layout("Daily Summary", "today"):
        status = get_db_status()
        today = date.today().isoformat()

        ui.label(f"Market Summary for {today}").classes(f"text-xl font-semibold {SPACE_XL}").style(
            f"color: {theme_text_primary()};"
        )

        kpi_grid(
            [
                dict(
                    title="Data Status",
                    value="Current",
                    subtitle="Latest data available",
                    icon="check_circle",
                    color=color_success(),
                ),
                dict(
                    title="Universe Size",
                    value=f"{int(status.get('symbols', 0)):,}",
                    subtitle="Active symbols",
                    icon="bar_chart",
                    color=color_info(),
                ),
                dict(
                    title="Total Candles",
                    value=f"{int(status.get('total_candles', 0)):,}",
                    subtitle="In database",
                    icon="candlestick_chart",
                    color=color_warning(),
                ),
                dict(
                    title="Date Range",
                    value=status.get("date_range", "-"),
                    subtitle="Coverage period",
                    icon="date_range",
                    color=color_gray(),
                ),
            ]
        )

        divider()

        ui.label("Market Health").classes(f"text-lg font-semibold {SPACE_XL}").style(
            f"color: {theme_text_primary()};"
        )

        indicators = [
            ("4% Breakouts Today", "Scans needed", "show_chart"),
            ("2LYNCH Pass Rate", "N/A", "filter_alt"),
            ("Top Gainer", "Scan required", "arrow_upward"),
            ("Top Loser", "Scan required", "arrow_downward"),
        ]

        with ui.column().classes(f"{SPACE_MD}"):
            for title, value, icon in indicators:
                with ui.row().classes(f"kpi-card items-center {SPACE_MD} w-full"):
                    ui.icon(icon).classes("text-xl").style(f"color: {theme_primary()};")
                    with ui.column().classes("gap-0"):
                        ui.label(title).classes("text-sm").style(
                            f"color: {theme_text_secondary()};"
                        )
                        ui.label(value).classes("text-lg font-semibold").style(
                            f"color: {theme_text_primary()};"
                        )

        divider()

        info_box("Run daily scans to see live market indicators.")
