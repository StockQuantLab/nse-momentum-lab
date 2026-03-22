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

import json

from nicegui import ui

from apps.nicegui.state import get_experiments
from apps.nicegui.components import (
    page_layout,
    divider,
    info_box,
    COLORS,
    THEME,
    empty_state,
    page_header,
)


def _strategy_display_name(row: dict) -> str:
    """Build a human-readable strategy label that includes threshold when applicable."""
    name = row.get("strategy_name", "-")
    params: dict = {}
    if "params_json" in row and row.get("params_json") is not None:
        try:
            params = json.loads(row["params_json"])
        except ValueError, TypeError:
            pass
    threshold = params.get("breakout_threshold")
    # Show threshold for all strategies except Indian2LYNCH (which is always 4%)
    if threshold is not None and name not in ("Indian2LYNCH",):
        pct = round(float(threshold) * 100)
        return f"{name} {pct}%"
    return name


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
        with ui.column().classes("kpi-card mb-6"):
            ui.label("Analysis Options").classes("text-sm font-medium mb-2").style(
                f"color: {THEME['text_secondary']};"
            )
            info_box(
                "Run multiple backtests with different parameter values to see sensitivity charts."
            )
            ui.label(f"Found {len(experiments_df)} experiments to analyze.").classes(
                "text-sm"
            ).style(f"color: {THEME['text_secondary']};")

        divider()

        # Experiments Overview — collapsible so the page isn't dominated by the table
        with ui.expansion("Experiments Overview", icon="science", value=True).classes(
            "w-full mb-4"
        ):
            if "strategy_name" in experiments_df.columns:
                # Show all unique strategy names (no truncation)
                unique_strategies = sorted(experiments_df["strategy_name"].unique().to_list())
                ui.label(f"Strategies: {', '.join(unique_strategies)}").classes("mb-4").style(
                    f"color: {THEME['text_secondary']};"
                )

            if "total_return_pct" in experiments_df.columns:
                returns = experiments_df["total_return_pct"].drop_nulls()
                with ui.row().classes("w-full gap-4 mb-4"):
                    ui.label(f"Best Return: {returns.max():.1f}%").style(
                        f"color: {COLORS['success']};"
                    )
                    ui.label(f"Median Return: {returns.median():.1f}%").style(
                        f"color: {THEME['text_secondary']};"
                    )
                    ui.label(f"Worst Return: {returns.min():.1f}%").style(
                        f"color: {COLORS['error']};"
                    )

            ui.table(
                columns=[
                    {"name": "exp_id", "label": "Experiment", "field": "exp_id_fmt"},
                    {"name": "strategy_name", "label": "Strategy", "field": "strategy_label"},
                    {"name": "start_year", "label": "Start", "field": "start_year"},
                    {"name": "end_year", "label": "End", "field": "end_year"},
                    {"name": "total_return_pct", "label": "Return", "field": "return_fmt"},
                    {"name": "win_rate_pct", "label": "Win Rate", "field": "win_rate_fmt"},
                    {"name": "max_drawdown_pct", "label": "Max DD", "field": "max_dd_fmt"},
                    {"name": "total_trades", "label": "Trades", "field": "trades_fmt"},
                ],
                rows=[
                    {
                        "exp_id_fmt": str(row["exp_id"])[:12],
                        "strategy_label": _strategy_display_name(row),
                        "start_year": int(row["start_year"])
                        if row.get("start_year") is not None
                        else "-",
                        "end_year": int(row["end_year"])
                        if row.get("end_year") is not None
                        else "-",
                        "return_fmt": f"{float(row.get('total_return_pct') or 0):.1f}%",
                        "win_rate_fmt": f"{float(row.get('win_rate_pct') or 0):.1f}%",
                        "max_dd_fmt": f"{float(row.get('max_drawdown_pct') or 0):.1f}%",
                        "trades_fmt": f"{int(row.get('total_trades') or 0):,}",
                    }
                    for row in experiments_df.iter_rows(named=True)
                ],
                pagination=15,
            ).classes("w-full")
