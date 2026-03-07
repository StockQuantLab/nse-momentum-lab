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

import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.state import get_experiments, get_experiment, build_experiment_options
from apps.nicegui.components import (
    page_layout,
    divider,
    apply_chart_theme,
    COLORS,
    THEME,
    empty_state,
    page_header,
)


def compare_page() -> None:
    """Render the compare experiments page."""
    with page_layout("Compare", "compare_arrows"):
        experiments_df = get_experiments()

        if experiments_df.empty:
            empty_state(
                "No experiments to compare",
                "Run at least 2 backtests to compare experiments.",
                icon="compare_arrows",
            )
            return

        if len(experiments_df) < 2:
            empty_state(
                "Need more experiments",
                f"You have {len(experiments_df)} experiment(s). Run at least 2 backtests.",
                icon="compare_arrows",
            )
            return

        exp_options = build_experiment_options(experiments_df)
        labels = list(exp_options.keys())

        selected = {"exp1": labels[0], "exp2": labels[1] if len(labels) > 1 else labels[0]}

        page_header(
            "Compare Experiments",
            "Select two experiments to compare side-by-side",
        )

        with ui.row().classes("kpi-card p-4 mb-6 w-full gap-4"):
            with ui.column().classes("flex-1"):
                ui.label("Experiment A").classes("text-sm font-medium mb-2").style(
                    f"color: {THEME['text_secondary']};"
                )
                ui.select(
                    labels,
                    value=selected["exp1"],
                    on_change=lambda e: selected.update(exp1=e.value) or render_comparison(),
                ).classes("w-full")

            with ui.column().classes("flex-1"):
                ui.label("Experiment B").classes("text-sm font-medium mb-2").style(
                    f"color: {THEME['text_secondary']};"
                )
                ui.select(
                    labels,
                    value=selected["exp2"],
                    on_change=lambda e: selected.update(exp2=e.value) or render_comparison(),
                ).classes("w-full")

        @ui.refreshable
        def render_comparison():
            exp1_id = exp_options.get(selected["exp1"])
            exp2_id = exp_options.get(selected["exp2"])

            if exp1_id == exp2_id:
                ui.label("Please select different experiments to compare.").classes(
                    "text-center py-8"
                ).style(f"color: {COLORS['warning']};")
                return

            exp1 = get_experiment(exp1_id)
            exp2 = get_experiment(exp2_id)

            if not exp1 or not exp2:
                ui.label("Could not load experiment details.").style(f"color: {COLORS['error']};")
                return

            metrics_to_show = [
                ("total_return_pct", "Total Return", "%"),
                ("win_rate_pct", "Win Rate", "%"),
                ("max_drawdown_pct", "Max Drawdown", "%"),
                ("total_trades", "Total Trades", ""),
                ("annualized_return_pct", "Annualized Return", "%"),
                ("profit_factor", "Profit Factor", ""),
            ]

            comparison_data = []
            for col, label, suffix in metrics_to_show:
                val1 = exp1.get(col, 0)
                val2 = exp2.get(col, 0)
                if suffix == "%":
                    comparison_data.append(
                        {
                            "Metric": label,
                            "Experiment A": f"{float(val1):.1f}%" if val1 else "N/A",
                            "Experiment B": f"{float(val2):.1f}%" if val2 else "N/A",
                        }
                    )
                else:
                    comparison_data.append(
                        {
                            "Metric": label,
                            "Experiment A": f"{int(val1):,}" if val1 else "N/A",
                            "Experiment B": f"{int(val2):,}" if val2 else "N/A",
                        }
                    )

            ui.table(
                columns=[
                    {"name": "Metric", "label": "Metric", "field": "Metric"},
                    {"name": "Experiment A", "label": "Experiment A", "field": "Experiment A"},
                    {"name": "Experiment B", "label": "Experiment B", "field": "Experiment B"},
                ],
                rows=comparison_data,
            ).classes("w-full mb-6")

            divider()

            ui.label("Visual Comparison").classes("text-xl font-semibold mb-4").style(
                f"color: {THEME['text_primary']};"
            )

            fig = go.Figure()

            for i, (col, label, _) in enumerate(metrics_to_show[:3]):
                values = [float(exp1.get(col, 0) or 0), float(exp2.get(col, 0) or 0)]
                fig.add_trace(
                    go.Bar(
                        name=label,
                        x=["Experiment A", "Experiment B"],
                        y=values,
                        marker_color=[COLORS["info"], COLORS["success"]][i % 2],
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

        render_comparison()
