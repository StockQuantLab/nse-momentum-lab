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

from apps.nicegui.components import (
    page_layout,
    divider,
    empty_state,
    kpi_grid,
    SPACE_LG,
    theme_text_primary,
    color_success,
    color_info,
    color_warning,
    color_primary,
)


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
        ui.label("Pipeline Jobs").classes(f"text-lg font-semibold {SPACE_LG}").style(
            f"color: {theme_text_primary()};"
        )

        job_types = [
            ("Ingestion", "Load OHLCV data from source", "download", color_success()),
            ("Rollup", "Compute features and indicators", "functions", color_info()),
            ("Scan", "Generate momentum signals", "radar", color_warning()),
            ("Backtest", "Run strategy backtests", "science", color_primary()),
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
