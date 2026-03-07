"""Trade Analytics page - Deep dive into trade distributions."""

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

from apps.nicegui.state import (
    get_experiments,
    get_experiment_trades,
    prepare_trades_df,
    build_experiment_options,
)
from apps.nicegui.components import (
    page_layout,
    kpi_grid,
    apply_chart_theme,
    COLORS,
    THEME,
    empty_state,
    page_header,
)


def trade_analytics_page() -> None:
    """Render the trade analytics page."""
    with page_layout("Trade Analytics", "analytics"):
        experiments_df = get_experiments()

        if experiments_df.empty:
            page_header("Trade Analytics")
            empty_state(
                "No experiments available",
                "Run a backtest first to see trade analytics.",
                icon="analytics",
            )
            return

        exp_options = build_experiment_options(experiments_df)
        labels = list(exp_options.keys())
        first_label = labels[0]

        # ── refreshable content ───────────────────────────────────
        @ui.refreshable
        def render_analytics(exp_id: str) -> None:
            trades_df = get_experiment_trades(exp_id)
            trades_df = prepare_trades_df(trades_df)

            if trades_df.empty:
                empty_state(
                    "No trades for this experiment",
                    "This experiment doesn't have any trades to analyze.",
                    icon="receipt_long",
                )
                return

            # Summary KPIs
            total_trades = len(trades_df)
            winning_trades = (
                (trades_df["pnl_pct"] > 0).sum() if "pnl_pct" in trades_df.columns else 0
            )
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

            cards = [
                dict(
                    title="Total Trades",
                    value=f"{total_trades:,}",
                    icon="bar_chart",
                    color=COLORS["info"],
                ),
                dict(
                    title="Win Rate",
                    value=f"{win_rate:.1f}%",
                    icon="target",
                    color=COLORS["success"],
                ),
            ]
            if "pnl_pct" in trades_df.columns:
                total_pnl = trades_df["pnl_pct"].sum()
                cards.append(
                    dict(
                        title="Total Return",
                        value=f"{total_pnl:.1f}%",
                        icon="attach_money",
                        color=COLORS["success"] if total_pnl > 0 else COLORS["error"],
                    )
                )
            if "pnl_r" in trades_df.columns:
                avg_r = trades_df["pnl_r"].mean()
                cards.append(
                    dict(
                        title="Avg R",
                        value=f"{avg_r:.2f}R",
                        icon="trending_up",
                        color=COLORS["warning"],
                    )
                )

            kpi_grid(cards, columns=len(cards))

            # Tabs
            tabs = ui.tabs().classes("w-full")
            with tabs:
                tab_exit = ui.tab("Exit Reason Analysis")
                tab_monthly = ui.tab("Monthly Performance")
                tab_symbol = ui.tab("Symbol Breakdown")

            with ui.tab_panels(tabs, value=tab_exit).classes("w-full"):
                # Exit Reason Analysis
                with ui.tab_panel(tab_exit):
                    if "exit_reason" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        exit_summary = (
                            trades_df.groupby("exit_reason")
                            .agg(
                                count=("pnl_pct", "count"),
                                avg_pnl=("pnl_pct", "mean"),
                                avg_r=("pnl_r", "mean"),
                                wins=("pnl_pct", lambda x: (x > 0).sum()),
                            )
                            .reset_index()
                        )
                        exit_summary["win_rate"] = (
                            exit_summary["wins"] / exit_summary["count"] * 100
                        ).round(1)
                        exit_summary["total_pnl"] = (
                            trades_df.groupby("exit_reason")["pnl_pct"].sum().values
                        )

                        ui.table(
                            columns=[
                                {"name": "exit_reason", "label": "Reason", "field": "exit_reason"},
                                {"name": "count", "label": "Trades", "field": "count"},
                                {"name": "avg_pnl_fmt", "label": "Avg %", "field": "avg_pnl_fmt"},
                                {"name": "avg_r_fmt", "label": "Avg R", "field": "avg_r_fmt"},
                                {"name": "win_rate_fmt", "label": "Win %", "field": "win_rate_fmt"},
                                {
                                    "name": "total_pnl_fmt",
                                    "label": "Total %",
                                    "field": "total_pnl_fmt",
                                },
                            ],
                            rows=[
                                {
                                    "exit_reason": row["exit_reason"],
                                    "count": int(row["count"]),
                                    "avg_pnl_fmt": f"{row['avg_pnl']:.2f}%",
                                    "avg_r_fmt": f"{row['avg_r']:.2f}R",
                                    "win_rate_fmt": f"{row['win_rate']:.1f}%",
                                    "total_pnl_fmt": f"{row['total_pnl']:.2f}%",
                                }
                                for _, row in exit_summary.iterrows()
                            ],
                        ).classes("w-full")

                        fig = go.Figure()
                        fig.add_trace(
                            go.Bar(
                                x=exit_summary["exit_reason"],
                                y=exit_summary["avg_pnl"],
                                marker_color=[
                                    COLORS["success"] if v > 0 else COLORS["error"]
                                    for v in exit_summary["avg_pnl"]
                                ],
                            )
                        )
                        fig.update_layout(
                            title="Average P&L by Exit Reason",
                            xaxis_title="Exit Reason",
                            yaxis_title="Average P&L %",
                        )
                        apply_chart_theme(fig)
                        ui.plotly(fig).classes("w-full h-64 mt-4")

                # Monthly Performance
                with ui.tab_panel(tab_monthly):
                    if "entry_date" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        trades_df["month"] = pd.to_datetime(trades_df["entry_date"]).dt.to_period(
                            "M"
                        )
                        monthly_data = (
                            trades_df.groupby("month")
                            .agg(
                                trades=("pnl_pct", "count"),
                                total_pnl=("pnl_pct", "sum"),
                                avg_pnl=("pnl_pct", "mean"),
                                win_rate=("pnl_pct", lambda x: (x > 0).mean() * 100),
                            )
                            .reset_index()
                        )
                        monthly_data["month_str"] = monthly_data["month"].astype(str)

                        ui.table(
                            columns=[
                                {"name": "month_str", "label": "Month", "field": "month_str"},
                                {"name": "trades_fmt", "label": "Trades", "field": "trades_fmt"},
                                {
                                    "name": "total_pnl_fmt",
                                    "label": "Total %",
                                    "field": "total_pnl_fmt",
                                },
                                {"name": "avg_pnl_fmt", "label": "Avg %", "field": "avg_pnl_fmt"},
                                {"name": "win_rate_fmt", "label": "Win %", "field": "win_rate_fmt"},
                            ],
                            rows=[
                                {
                                    "month_str": row["month_str"],
                                    "trades_fmt": f"{int(row['trades'])}",
                                    "total_pnl_fmt": f"{row['total_pnl']:.2f}%",
                                    "avg_pnl_fmt": f"{row['avg_pnl']:.2f}%",
                                    "win_rate_fmt": f"{row['win_rate']:.1f}%",
                                }
                                for _, row in monthly_data.iterrows()
                            ],
                            pagination=15,
                        ).classes("w-full")

                        fig = go.Figure()
                        fig.add_trace(
                            go.Scatter(
                                x=monthly_data["month_str"],
                                y=monthly_data["total_pnl"],
                                mode="lines+markers",
                                line=dict(color=COLORS["success"]),
                            )
                        )
                        fig.update_layout(
                            title="Monthly Returns", xaxis_title="Month", yaxis_title="Total P&L %"
                        )
                        apply_chart_theme(fig)
                        ui.plotly(fig).classes("w-full h-64 mt-4")

                # Symbol Breakdown
                with ui.tab_panel(tab_symbol):
                    if "symbol" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        symbol_stats = (
                            trades_df.groupby("symbol")
                            .agg(
                                trades=("pnl_pct", "count"),
                                total_pnl=("pnl_pct", "sum"),
                                avg_pnl=("pnl_pct", "mean"),
                                best=("pnl_pct", "max"),
                                worst=("pnl_pct", "min"),
                            )
                            .reset_index()
                            .sort_values("total_pnl", ascending=False)
                        )

                        ui.table(
                            columns=[
                                {"name": "symbol", "label": "Symbol", "field": "symbol"},
                                {"name": "trades_fmt", "label": "Trades", "field": "trades_fmt"},
                                {
                                    "name": "total_pnl_fmt",
                                    "label": "Total %",
                                    "field": "total_pnl_fmt",
                                },
                                {"name": "avg_pnl_fmt", "label": "Avg %", "field": "avg_pnl_fmt"},
                                {"name": "best_fmt", "label": "Best %", "field": "best_fmt"},
                                {"name": "worst_fmt", "label": "Worst %", "field": "worst_fmt"},
                            ],
                            rows=[
                                {
                                    "symbol": row["symbol"],
                                    "trades_fmt": f"{int(row['trades'])}",
                                    "total_pnl_fmt": f"{row['total_pnl']:.2f}%",
                                    "avg_pnl_fmt": f"{row['avg_pnl']:.2f}%",
                                    "best_fmt": f"{row['best']:.1f}%",
                                    "worst_fmt": f"{row['worst']:.1f}%",
                                }
                                for _, row in symbol_stats.head(30).iterrows()
                            ],
                            pagination=10,
                        ).classes("w-full")

        # ── experiment selector ───────────────────────────────────
        with ui.row().classes("kpi-card w-full items-center gap-4 mb-6"):
            ui.icon("science").classes("text-xl").style(f"color: {THEME['primary']};")
            ui.label("Experiment").classes("text-sm font-medium").style(
                f"color: {THEME['text_secondary']};"
            )

            def on_select(e):
                selected_id = exp_options.get(e.value)
                if selected_id:
                    render_analytics.refresh(selected_id)

            ui.select(
                labels,
                value=first_label,
                on_change=on_select,
            ).classes("flex-grow")

        render_analytics(exp_options[first_label])
