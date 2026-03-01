"""Pipeline Status page - Job monitoring and execution."""

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

from apps.nicegui.components import page_layout, divider, THEME


def pipeline_page() -> None:
    """Render the pipeline status page."""
    with page_layout("Pipeline", "engineering"):
        with ui.column().classes("kpi-card mb-6"):
            ui.label("Pipeline Jobs").classes("text-xl font-semibold mb-4").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label("This page requires the API to be running for job status monitoring.").classes(
                "mb-4"
            ).style(f"color: {THEME['text_secondary']};")
            ui.label("Start the API server: doppler run -- uv run nseml-api").classes(
                "font-mono text-sm px-3 py-2 rounded"
            ).style(
                f"background: {THEME['surface_hover']}; border: 1px solid {THEME['surface_border']}; color: {THEME['text_primary']}; border-radius: 6px;"
            )

        divider()

        ui.label("Pipeline Jobs").classes("text-lg font-semibold mb-2").style(
            f"color: {THEME['text_primary']};"
        )

        job_types = [
            ("Ingestion", "Load OHLCV data from source", "download"),
            ("Rollup", "Compute features and indicators", "functions"),
            ("Scan", "Generate momentum signals", "radar"),
            ("Backtest", "Run strategy backtests", "science"),
        ]

        for job_name, description, icon in job_types:
            with ui.row().classes("kpi-card items-center gap-3 w-full mb-2"):
                ui.icon(icon).classes("text-xl").style(f"color: {THEME['primary']};")
                ui.label(job_name).classes("font-semibold").style(
                    f"color: {THEME['text_primary']};"
                )
                ui.label(description).classes("text-sm").style(f"color: {THEME['text_muted']};")
