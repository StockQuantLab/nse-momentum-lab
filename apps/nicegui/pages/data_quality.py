"""Data Quality page - Validate data integrity."""

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

from apps.nicegui.state import get_db_status
from apps.nicegui.components import page_layout, kpi_grid, divider, COLORS, THEME


def data_quality_page() -> None:
    """Render the data quality page."""
    with page_layout("Data Quality", "verified"):
        status = get_db_status()

        # Coverage summary
        ui.label("Coverage Summary").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        kpi_grid(
            [
                dict(
                    title="Symbols",
                    value=f"{int(status.get('symbols', 0)):,}",
                    icon="show_chart",
                    color=COLORS["info"],
                ),
                dict(
                    title="Total Candles",
                    value=f"{int(status.get('total_candles', 0)):,}",
                    icon="candlestick_chart",
                    color=COLORS["warning"],
                ),
                dict(
                    title="Date Range",
                    value=status.get("date_range", "-"),
                    icon="date_range",
                    color=COLORS["gray"],
                ),
                dict(
                    title="Data Source",
                    value=status.get("data_source", "unknown").upper(),
                    icon="storage",
                    color=COLORS["success"],
                ),
            ]
        )

        divider()

        # Validation info
        ui.label("Data Validation").classes("text-lg font-semibold mb-2").style(
            f"color: {THEME['text_primary']};"
        )

        with ui.column().classes("kpi-card"):
            ui.label("Dataset Information").classes("text-sm font-medium mb-2").style(
                f"color: {THEME['text_secondary']};"
            )
            if status.get("dataset_hash"):
                ui.label(f"Dataset Hash: {status.get('dataset_hash')}").classes(
                    "text-xs font-mono mb-1"
                ).style(f"color: {THEME['text_muted']};")
            ui.label(
                "All data is validated before backtesting. Use this page to verify data integrity."
            ).classes("text-sm").style(f"color: {THEME['text_muted']};")

        divider()

        with ui.expansion("Data Sources", icon="info").classes("w-full"):
            for text in [
                "Primary: Zerodha historical data (adjusted)",
                "Format: Parquet files with OHLCV data",
                "Frequency: Daily candles",
                "Adjustments: Corporate actions applied",
            ]:
                ui.label(text).classes("mb-2").style(f"color: {THEME['text_secondary']};")
