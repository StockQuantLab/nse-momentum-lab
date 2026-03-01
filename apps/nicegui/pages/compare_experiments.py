"""Compare Experiments page - Side-by-side experiment comparison."""

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

import pandas as pd
import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.state import get_experiments
from apps.nicegui.components import page_layout, divider, apply_chart_theme, COLORS, THEME


def compare_page() -> None:
    """Render the compare experiments page."""
    with page_layout("Compare", "compare_arrows"):
        experiments_df = get_experiments()

        if experiments_df.empty:
            ui.label("No experiments available for comparison.").style(
                f"color: {THEME['text_secondary']};"
            )
            return

        if len(experiments_df) < 2:
            ui.label("Need at least 2 experiments to compare.").style(
                f"color: {COLORS['warning']};"
            )
            return

        ui.label("All Experiments").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        metrics_to_show = [
            ("total_return_pct", "Total Return", "%"),
            ("win_rate_pct", "Win Rate", "%"),
            ("max_drawdown_pct", "Max Drawdown", "%"),
            ("total_trades", "Total Trades", ""),
        ]

        comparison_rows = []
        for _, exp in experiments_df.iterrows():
            row = {
                "Experiment": str(exp["exp_id"])[:12],
                "Strategy": exp.get("strategy_name", "-"),
            }
            for col, label, suffix in metrics_to_show:
                val = exp.get(col, 0)
                if suffix == "%":
                    row[label] = f"{float(val):.1f}%" if pd.notna(val) else "N/A"
                else:
                    row[label] = f"{int(val):,}" if pd.notna(val) else "N/A"
            comparison_rows.append(row)

        ui.table(
            columns=[
                {"name": "Experiment", "label": "Exp ID", "field": "Experiment"},
                {"name": "Strategy", "label": "Strategy", "field": "Strategy"},
            ]
            + [{"name": label, "label": label, "field": label} for _, label, _ in metrics_to_show],
            rows=comparison_rows,
            pagination=10,
        ).classes("w-full mb-6")

        divider()

        ui.label("Visual Comparison").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        fig = go.Figure()
        exp_labels = [row["Experiment"] for row in comparison_rows]
        bar_colors = [COLORS["info"], COLORS["success"], COLORS["error"]]

        for i, (col, label, _) in enumerate(metrics_to_show[:3]):
            values = [float(exp.get(col, 0)) for _, exp in experiments_df.iterrows()]
            fig.add_trace(
                go.Bar(
                    name=label,
                    x=exp_labels,
                    y=values,
                    marker_color=bar_colors[i % len(bar_colors)],
                )
            )

        fig.update_layout(
            title="Key Metrics Comparison",
            barmode="group",
            xaxis_title="Experiment",
            yaxis_title="Value",
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-80")
