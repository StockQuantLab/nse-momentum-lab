"""Strategy Analysis page - Parameter sensitivity analysis."""

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

from apps.nicegui.state import _strategy_display_name, get_experiments
from apps.nicegui.components import (
    color_error,
    color_success,
    divider,
    empty_state,
    info_box,
    page_header,
    page_layout,
    paginated_table,
    SPACE_GRID_DEFAULT,
    SPACE_LG,
    SPACE_SECTION,
    SPACE_SM,
    theme_text_secondary,
)


def strategy_page() -> None:
    """Render the strategy analysis page."""
    with page_layout("Strategy", "tune"):
        experiments_df = get_experiments()

        if experiments_df.is_empty():
            page_header("Strategy Analysis")
            empty_state(
                "No experiments available",
                "Run a backtest first to analyze strategy performance.",
                icon="tune",
            )
            return

        # Analysis options
        with ui.column().classes(f"kpi-card {SPACE_SECTION}"):
            ui.label("Analysis Options").classes(f"text-sm font-medium {SPACE_SM}").style(
                f"color: {theme_text_secondary()};"
            )
            info_box(
                "Run multiple backtests with different parameter values to see sensitivity charts."
            )
            ui.label(f"Found {len(experiments_df)} experiments to analyze.").classes(
                "text-sm"
            ).style(f"color: {theme_text_secondary()};")

        divider()

        # Experiments Overview — collapsible so the page isn't dominated by the table
        with ui.expansion("Experiments Overview", icon="science", value=True).classes(
            f"w-full {SPACE_LG}"
        ):
            if "strategy_name" in experiments_df.columns:
                # Show all unique strategy names (no truncation)
                unique_strategies = sorted(experiments_df["strategy_name"].unique().to_list())
                ui.label(f"Strategies: {', '.join(unique_strategies)}").classes(SPACE_LG).style(
                    f"color: {theme_text_secondary()};"
                )

            if "total_return_pct" in experiments_df.columns:
                returns = experiments_df["total_return_pct"].drop_nulls()
                with ui.row().classes(f"w-full {SPACE_GRID_DEFAULT} {SPACE_LG}"):
                    ui.label(f"Best Return: {float(returns.max()):.1f}%").style(
                        f"color: {color_success()};"
                    )
                    ui.label(f"Median Return: {float(returns.median()):.1f}%").style(
                        f"color: {theme_text_secondary()};"
                    )
                    ui.label(f"Worst Return: {float(returns.min()):.1f}%").style(
                        f"color: {color_error()};"
                    )

            paginated_table(
                columns=[
                    {"name": "exp_id", "label": "Experiment", "field": "exp_id"},
                    {"name": "strategy_name", "label": "Strategy", "field": "strategy_label"},
                    {"name": "start_year", "label": "Start", "field": "start_year"},
                    {"name": "end_year", "label": "End", "field": "end_year"},
                    {
                        "name": "total_return_pct",
                        "label": "Return",
                        "field": "total_return_pct",
                        ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                    },
                    {
                        "name": "win_rate_pct",
                        "label": "Win Rate",
                        "field": "win_rate_pct",
                        ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                    },
                    {
                        "name": "max_drawdown_pct",
                        "label": "Max DD",
                        "field": "max_drawdown_pct",
                        ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                    },
                    {"name": "total_trades", "label": "Trades", "field": "total_trades"},
                ],
                rows=[
                    {
                        "exp_id": str(row["exp_id"])[:12],
                        "strategy_label": _strategy_display_name(row),
                        "start_year": int(row["start_year"])
                        if row.get("start_year") is not None
                        else None,
                        "end_year": int(row["end_year"])
                        if row.get("end_year") is not None
                        else None,
                        "total_return_pct": float(row.get("total_return_pct") or 0),
                        "win_rate_pct": float(row.get("win_rate_pct") or 0),
                        "max_drawdown_pct": float(row.get("max_drawdown_pct") or 0),
                        "total_trades": int(row.get("total_trades") or 0),
                    }
                    for row in experiments_df.iter_rows(named=True)
                ],
                page_size=15,
            )
