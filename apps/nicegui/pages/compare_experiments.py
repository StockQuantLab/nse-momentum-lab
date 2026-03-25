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
    empty_state,
    page_header,
    loading_spinner,
    paginated_table,
    SPACE_SECTION,
    SPACE_GRID_DEFAULT,
    SPACE_SM,
    SPACE_XL,
    theme_text_secondary,
    theme_text_muted,
    theme_text_primary,
    color_success,
    color_error,
    color_info,
    color_warning,
)


async def compare_page() -> None:
    """Render the compare experiments page."""
    with page_layout("Compare", "compare_arrows"):
        try:
            with loading_spinner():
                experiments_df = get_experiments()
        except Exception as e:
            empty_state(
                "Connection Error",
                f"Could not load experiments: {e}",
                icon="error",
            )
            return

        if experiments_df.is_empty():
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

        # Create reverse lookup (exp_id -> label) for restoration
        id_to_label = {v: k for k, v in exp_options.items()}

        # Restore selections after theme toggle — read sessionStorage before building selects
        saved_exp1_id = await ui.run_javascript(
            "sessionStorage.getItem('nseml_compare_exp1') || ''", timeout=2.0
        )
        saved_exp2_id = await ui.run_javascript(
            "sessionStorage.getItem('nseml_compare_exp2') || ''", timeout=2.0
        )

        # Determine initial values (None if no saved state)
        initial_exp1 = (
            id_to_label.get(saved_exp1_id, None)
            if saved_exp1_id and saved_exp1_id in id_to_label
            else None
        )
        initial_exp2 = (
            id_to_label.get(saved_exp2_id, None)
            if saved_exp2_id and saved_exp2_id in id_to_label
            else None
        )

        # Track current selections in mutable dict (accessible in closure)
        selected = {"exp1": initial_exp1, "exp2": initial_exp2}
        has_selected = {"value": bool(initial_exp1 or initial_exp2)}

        page_header(
            "Compare Experiments",
            "Select two experiments to compare side-by-side",
        )

        with ui.row().classes(f"kpi-card p-4 {SPACE_SECTION} w-full {SPACE_GRID_DEFAULT}"):
            with ui.column().classes("flex-1"):
                exp1_label_id = "exp1-label"
                ui.label("Experiment A").props(f'id="{exp1_label_id}"').classes(
                    f"text-sm font-medium {SPACE_SM}"
                ).style(f"color: {theme_text_secondary()};")

                def on_exp1_change(e):
                    selected["exp1"] = e.value
                    has_selected["value"] = True
                    exp_id = exp_options.get(e.value, "")
                    ui.run_javascript(f"sessionStorage.setItem('nseml_compare_exp1', '{exp_id}');")
                    render_comparison.refresh()

                ui.select(
                    labels,
                    value=initial_exp1,
                    on_change=on_exp1_change,
                ).classes("w-full").props(
                    f'aria-labelledby="{exp1_label_id}" aria-label="Select first experiment to compare"'
                )

            with ui.column().classes("flex-1"):
                exp2_label_id = "exp2-label"
                ui.label("Experiment B").props(f'id="{exp2_label_id}"').classes(
                    f"text-sm font-medium {SPACE_SM}"
                ).style(f"color: {theme_text_secondary()};")

                def on_exp2_change(e):
                    selected["exp2"] = e.value
                    has_selected["value"] = True
                    exp_id = exp_options.get(e.value, "")
                    ui.run_javascript(f"sessionStorage.setItem('nseml_compare_exp2', '{exp_id}');")
                    render_comparison.refresh()

                ui.select(
                    labels,
                    value=initial_exp2,
                    on_change=on_exp2_change,
                ).classes("w-full").props(
                    f'aria-labelledby="{exp2_label_id}" aria-label="Select second experiment to compare"'
                )

        # Accessibility: Live region for dynamic comparison updates
        with ui.row().props('aria-live="polite" aria-atomic="true"').classes("sr-only"):
            ui.label("Comparison results will update here")

        @ui.refreshable
        def render_comparison():
            # Get current selections from mutable dict
            exp1_label = selected.get("exp1")
            exp2_label = selected.get("exp2")

            # Only show comparison if both selections exist
            if not exp1_label or not exp2_label:
                ui.label("Select two experiments above to compare them.").classes(
                    "text-center py-8"
                ).style(f"color: {theme_text_muted()};")
                return

            exp1_id = exp_options.get(exp1_label)
            exp2_id = exp_options.get(exp2_label)

            if exp1_id == exp2_id:
                ui.label("Please select different experiments to compare.").classes(
                    "text-center py-8"
                ).style(f"color: {color_warning()};")
                return

            exp1 = get_experiment(exp1_id)
            exp2 = get_experiment(exp2_id)

            if not exp1 or not exp2:
                ui.label("Could not load experiment details.").style(f"color: {color_error()};")
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

            paginated_table(
                columns=[
                    {"name": "Metric", "label": "Metric", "field": "Metric"},
                    {"name": "Experiment A", "label": "Experiment A", "field": "Experiment A"},
                    {"name": "Experiment B", "label": "Experiment B", "field": "Experiment B"},
                ],
                rows=comparison_data,
                page_size=20,
            )

            divider()

            ui.label("Visual Comparison").classes(f"text-xl font-semibold {SPACE_XL}").style(
                f"color: {theme_text_primary()};"
            )

            fig = go.Figure()

            for i, (col, label, _) in enumerate(metrics_to_show[:3]):
                values = [float(exp1.get(col, 0) or 0), float(exp2.get(col, 0) or 0)]
                fig.add_trace(
                    go.Bar(
                        name=label,
                        x=["Experiment A", "Experiment B"],
                        y=values,
                        marker_color=[color_info(), color_success()][i % 2],
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

        # Initial state - show prompt to select experiments
        render_comparison()
