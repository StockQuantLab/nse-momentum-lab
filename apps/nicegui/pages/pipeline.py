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

from apps.nicegui.components import page_layout, divider, THEME, empty_state, COLORS, kpi_grid


def pipeline_page() -> None:
    """Render the pipeline status page."""
    with page_layout("Pipeline", "engineering"):
        empty_state(
            "API Required",
            "Pipeline job monitoring requires the API server to be running.",
            action_label="Copy API Command",
            action_callback=lambda: ui.run_javascript(
                "navigator.clipboard.writeText('doppler run -- uv run nseml-api');"
            ),
            icon="engineering",
        )

        divider()

        # Show job types as informational cards
        ui.label("Pipeline Jobs").classes("text-lg font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        job_types = [
            ("Ingestion", "Load OHLCV data from source", "download", COLORS["success"]),
            ("Rollup", "Compute features and indicators", "functions", COLORS["info"]),
            ("Scan", "Generate momentum signals", "radar", COLORS["warning"]),
            ("Backtest", "Run strategy backtests", "science", COLORS["primary"]),
        ]

        kpi_grid(
            [
                {
                    "title": name,
                    "value": description,
                    "icon": icon,
                    "color": color,
                }
                for name, description, icon, color in job_types
            ],
            columns=4,
        )
