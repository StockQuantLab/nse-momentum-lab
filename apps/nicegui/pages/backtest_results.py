"""Backtest Results page - Full trade analysis with filters and charts."""

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
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.state import (
    get_experiments,
    get_experiment,
    get_experiment_trades,
    get_experiment_yearly_metrics,
    prepare_trades_df,
    build_experiment_options,
)
from apps.nicegui.components import (
    page_layout,
    kpi_grid,
    divider,
    apply_chart_theme,
    COLORS,
    THEME,
    empty_state,
    page_header,
    paginated_table,
    export_menu,
)


async def backtest_page() -> None:
    """Render the backtest results page."""
    with page_layout("Backtest Results", "bar_chart"):
        experiments_df = get_experiments()

        if experiments_df.empty:
            page_header("Backtest Results")
            empty_state(
                "No backtest experiments found",
                "Run a backtest first to see results here.",
                action_label="Run Backtest",
                action_callback=lambda: ui.navigate.to("/"),
                icon="science",
            )
            return

        exp_options = build_experiment_options(experiments_df)
        labels = list(exp_options.keys())
        first_label = labels[0]

        # Create reverse lookup (exp_id -> label) for restoration
        id_to_label = {v: k for k, v in exp_options.items()}

        # Restore selection after theme toggle — read sessionStorage before building the select
        # so the initial value is set server-side (no fragile DOM querying needed)
        saved_id = await ui.run_javascript(
            "sessionStorage.getItem('nseml_restore_exp_id') || ''", timeout=2.0
        )
        initial_label = id_to_label.get(saved_id, first_label) if saved_id else first_label

        @ui.refreshable
        def render_experiment(exp_id: str) -> None:
            """Render all data for the selected experiment."""
            exp = get_experiment(exp_id)
            if not exp:
                ui.label("Could not load experiment details.").style(f"color: {COLORS['error']};")
                return

            page_header(
                "Backtest Results",
                f"Experiment: {exp_id[:16]}...",
                kpi_row=[
                    dict(
                        title="Strategy",
                        value=str(exp.get("strategy_name", "-")),
                        icon="flag",
                        color=COLORS["info"],
                    ),
                    dict(
                        title="Period",
                        value=f"{exp.get('start_year', '-')}-{exp.get('end_year', '-')}",
                        icon="date_range",
                        color=COLORS["gray"],
                    ),
                    dict(
                        title="Status",
                        value=str(exp.get("status", "-")).upper(),
                        icon="check_circle",
                        color=COLORS["success"],
                    ),
                ],
            )

            ret_val = float(exp.get("total_return_pct", 0))
            kpi_grid(
                [
                    dict(
                        title="Total Return",
                        value=f"{ret_val:.1f}%",
                        icon="attach_money",
                        color=COLORS["success"] if ret_val > 0 else COLORS["error"],
                    ),
                    dict(
                        title="Annualized",
                        value=f"{float(exp.get('annualized_return_pct', 0)):.1f}%",
                        icon="trending_up",
                        color=COLORS["info"],
                    ),
                    dict(
                        title="Win Rate",
                        value=f"{float(exp.get('win_rate_pct', 0)):.1f}%",
                        icon="target",
                        color=COLORS["warning"],
                    ),
                    dict(
                        title="Max Drawdown",
                        value=f"{float(exp.get('max_drawdown_pct', 0)):.1f}",
                        icon="trending_down",
                        color=COLORS["error"],
                    ),
                    dict(
                        title="Total Trades",
                        value=f"{int(exp.get('total_trades') or 0):,}",
                        icon="bar_chart",
                        color=COLORS["primary"],
                    ),
                ],
                columns=5,
            )

            divider()

            ui.label("Yearly Breakdown").classes("text-xl font-semibold mb-4").style(
                f"color: {THEME['text_primary']};"
            )

            yearly_df = get_experiment_yearly_metrics(exp_id)

            if not yearly_df.empty:
                display_cols = {
                    "year": "Year",
                    "signals": "Signals",
                    "trades": "Trades",
                    "wins": "Wins",
                    "losses": "Losses",
                    "return_pct": "Return %",
                    "win_rate_pct": "Win Rate %",
                    "avg_r": "Avg R",
                    "max_dd_pct": "Max DD %",
                    "profit_factor": "PF",
                }
                available = [c for c in display_cols if c in yearly_df.columns]
                rename_dict = {k: v for k, v in display_cols.items() if k in available}

                if available:
                    display_df = yearly_df[available].copy().rename(columns=rename_dict)

                    def format_for_display(val, col):
                        if "Return" in col or "Rate" in col or "DD" in col:
                            return f"{float(val):.2f}%" if pd.notna(val) else "-"
                        if col in ["Avg R", "PF"]:
                            return f"{float(val):.2f}" if pd.notna(val) else "-"
                        return f"{int(val)}" if pd.notna(val) else "-"

                    paginated_table(
                        columns=[
                            {"name": col, "label": col, "field": col} for col in display_df.columns
                        ],
                        rows=[
                            {col: format_for_display(row[col], col) for col in display_df.columns}
                            for _, row in display_df.iterrows()
                        ],
                        page_size=20,
                    )

                if "return_pct" in yearly_df.columns and "year" in yearly_df.columns:
                    fig_yearly = px.bar(
                        yearly_df,
                        x="year",
                        y="return_pct",
                        color="return_pct",
                        color_continuous_scale=["#ef4444", "#f59e0b", "#22c55e"],
                        labels={"return_pct": "Return %", "year": "Year"},
                        title="Yearly Returns",
                    )
                    fig_yearly.update_layout(showlegend=False, coloraxis_showscale=False)
                    apply_chart_theme(fig_yearly)
                    ui.plotly(fig_yearly).classes("w-full h-64")

            trades_df = get_experiment_trades(exp_id)
            trades_df = prepare_trades_df(trades_df)

            if trades_df.empty:
                divider()
                empty_state(
                    "No trade data available",
                    "This experiment doesn't have any trades.",
                    icon="receipt_long",
                )
                return

            divider()

            with ui.row().classes("mb-4 gap-2"):
                export_menu(trades_df, f"{exp_id}_all_trades", "Export Trades")

            divider()

            ui.label("Trade Analytics").classes("text-xl font-semibold mb-4").style(
                f"color: {THEME['text_primary']};"
            )

            tabs = ui.tabs().classes("w-full")
            with tabs:
                tab_equity = ui.tab("Equity Curve")
                tab_exit = ui.tab("Exit Reasons")
                tab_r = ui.tab("R-Multiple")
                tab_wl = ui.tab("Winners/Losers")
                tab_stock = ui.tab("Per-Stock")
                tab_monthly = ui.tab("Monthly Heatmap")

            with ui.tab_panels(tabs, value=tab_equity).classes("w-full"):
                with ui.tab_panel(tab_equity):
                    if "pnl_pct" in trades_df.columns and "entry_date" in trades_df.columns:
                        equity = trades_df.sort_values("entry_date").copy()
                        equity["cumulative_return"] = equity["pnl_pct"].cumsum()
                        equity["cummax"] = equity["cumulative_return"].cummax()
                        equity["drawdown"] = equity["cumulative_return"] - equity["cummax"]

                        def hex_to_rgba(hex_color: str, alpha: float = 0.2) -> str:
                            hex_color = hex_color.lstrip("#")
                            r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
                            return f"rgba({r},{g},{b},{alpha})"

                        fig_eq = go.Figure()

                        fig_eq.add_trace(
                            go.Scatter(
                                x=equity["entry_date"],
                                y=equity["drawdown"],
                                fill="tozeroy",
                                fillcolor=hex_to_rgba(COLORS["error"], 0.15),
                                line_color=COLORS["error"],
                                name="Drawdown",
                                hovertemplate="%{x}<br>Drawdown: %{y:.2f}%<extra></extra>",
                            )
                        )

                        fig_eq.add_trace(
                            go.Scatter(
                                x=equity["entry_date"],
                                y=equity["cumulative_return"],
                                mode="lines",
                                name="Cumulative Return %",
                                line=dict(color=COLORS["primary"], width=2.5),
                                hovertemplate="%{x}<br>Return: %{y:.2f}%<extra></extra>",
                            )
                        )

                        fig_eq.update_layout(
                            title="Equity Curve with Drawdown",
                            xaxis_title="Date",
                            yaxis_title="Return %",
                            hovermode="x unified",
                        )
                        apply_chart_theme(fig_eq)
                        ui.plotly(fig_eq).classes("w-full h-80")

                with ui.tab_panel(tab_exit):
                    if "exit_reason" in trades_df.columns:
                        exit_pnl = (
                            trades_df.groupby("exit_reason")
                            .agg(
                                count=("pnl_pct", "count"),
                                avg_pnl=("pnl_pct", "mean"),
                                avg_r=("pnl_r", "mean"),
                            )
                            .reset_index()
                        )
                        with ui.row().classes("w-full gap-4"):
                            with ui.column().classes("flex-1"):
                                exit_counts = trades_df["exit_reason"].value_counts()
                                fig_pie = go.Figure()
                                fig_pie.add_trace(
                                    go.Pie(
                                        labels=exit_counts.index.tolist(),
                                        values=exit_counts.values.tolist(),
                                        hole=0.3,
                                    )
                                )
                                fig_pie.update_layout(title="Exit Reason Distribution")
                                apply_chart_theme(fig_pie)
                                ui.plotly(fig_pie).classes("w-full h-64")

                            with ui.column().classes("flex-1"):
                                exit_rows = [
                                    {
                                        "exit_reason": row["exit_reason"],
                                        "count": int(row["count"]),
                                        "avg_pnl_fmt": f"{row['avg_pnl']:.2f}%",
                                        "avg_r_fmt": f"{row['avg_r']:.2f}R",
                                    }
                                    for _, row in exit_pnl.iterrows()
                                ]
                                paginated_table(
                                    columns=[
                                        {
                                            "name": "exit_reason",
                                            "label": "Reason",
                                            "field": "exit_reason",
                                        },
                                        {"name": "count", "label": "Count", "field": "count"},
                                        {
                                            "name": "avg_pnl_fmt",
                                            "label": "Avg %",
                                            "field": "avg_pnl_fmt",
                                        },
                                        {
                                            "name": "avg_r_fmt",
                                            "label": "Avg R",
                                            "field": "avg_r_fmt",
                                        },
                                    ],
                                    rows=exit_rows,
                                    page_size=20,
                                )

                with ui.tab_panel(tab_r):
                    if "pnl_r" in trades_df.columns:
                        r_vals = trades_df["pnl_r"].dropna()
                        if len(r_vals) > 0:
                            fig_r = go.Figure()

                            fig_r.add_trace(
                                go.Histogram(
                                    x=r_vals,
                                    nbinsx=50,
                                    marker_color=COLORS["primary"],
                                    name="Distribution",
                                    opacity=0.7,
                                )
                            )

                            mu, sigma = r_vals.mean(), r_vals.std()
                            if sigma > 0:
                                x_norm = np.linspace(r_vals.min(), r_vals.max(), 100)
                                y_norm = (
                                    pd.Series(x_norm)
                                    .apply(
                                        lambda x: (
                                            (1 / (sigma * np.sqrt(2 * np.pi)))
                                            * np.exp(-0.5 * ((x - mu) / sigma) ** 2)
                                        )
                                    )
                                    .values
                                )
                                bin_width = (r_vals.max() - r_vals.min()) / 50
                                y_norm_scaled = y_norm * len(r_vals) * bin_width

                                fig_r.add_trace(
                                    go.Scatter(
                                        x=x_norm,
                                        y=y_norm_scaled,
                                        mode="lines",
                                        name="Normal Dist",
                                        line=dict(color=COLORS["error"], dash="dash"),
                                    )
                                )

                            fig_r.add_vline(x=0, line_dash="dash", line_color=THEME["text_muted"])
                            fig_r.add_vline(
                                x=mu,
                                line_dash="dot",
                                line_color=COLORS["success"],
                                annotation_text=f"Mean: {mu:.2f}R",
                            )

                            fig_r.update_layout(
                                title="R-Multiple Distribution",
                                xaxis_title="R-Multiple",
                                yaxis_title="Count",
                                barmode="overlay",
                            )
                            apply_chart_theme(fig_r)
                            ui.plotly(fig_r).classes("w-full h-64")

                            percentiles = [10, 25, 50, 75, 90]
                            pct_data = [
                                {"Percentile": f"P{p}", "Value": f"{np.percentile(r_vals, p):.2f}R"}
                                for p in percentiles
                            ]
                            pct_data.extend(
                                [
                                    {"Percentile": "Mean", "Value": f"{r_vals.mean():.2f}R"},
                                    {"Percentile": "Min", "Value": f"{r_vals.min():.2f}R"},
                                    {"Percentile": "Max", "Value": f"{r_vals.max():.2f}R"},
                                ]
                            )
                            paginated_table(
                                columns=[
                                    {
                                        "name": "Percentile",
                                        "label": "Percentile",
                                        "field": "Percentile",
                                    },
                                    {"name": "Value", "label": "R-Multiple", "field": "Value"},
                                ],
                                rows=pct_data,
                                page_size=20,
                            )

                with ui.tab_panel(tab_wl):
                    if "pnl_pct" in trades_df.columns:
                        trade_cols = [
                            "entry_date",
                            "entry_time",
                            "symbol",
                            "entry_price",
                            "exit_date",
                            "exit_time",
                            "exit_price",
                            "exit_reason",
                            "holding_days",
                            "pnl_pct",
                            "pnl_r",
                        ]
                        avail_cols = [c for c in trade_cols if c in trades_df.columns]

                        def _format_trade_val(val, col):
                            if col == "pnl_pct":
                                return f"{val:.2f}%"
                            if col == "pnl_r":
                                return f"{val:.2f}R"
                            if "price" in col:
                                return f"{val:.2f}"
                            if col == "holding_days":
                                return f"{int(val)}d" if pd.notna(val) else "-"
                            return str(val) if pd.notna(val) else "-"

                        def _trade_rows(df_slice):
                            return [
                                {col: _format_trade_val(row[col], col) for col in avail_cols}
                                for _, row in df_slice.iterrows()
                            ]

                        table_columns = [
                            {"name": col, "label": col.replace("_", " ").title(), "field": col}
                            for col in avail_cols
                        ]

                        with ui.row().classes("w-full gap-4"):
                            with ui.column().classes("flex-1"):
                                ui.label("Top Winners").classes("text-lg font-semibold mb-2").style(
                                    f"color: {COLORS['success']};"
                                )
                                top_winners = trades_df.nlargest(min(20, len(trades_df)), "pnl_pct")
                                with ui.element("div").style(
                                    "width: 100%; max-height: 450px; overflow-x: auto;"
                                ):
                                    ui.table(
                                        columns=table_columns,
                                        rows=_trade_rows(top_winners),
                                        pagination={
                                            "rowsPerPage": 20,
                                            "rowsPerPage_options": [10, 20, 50, 100],
                                        },
                                    ).style("min-width: max-content;")

                            with ui.column().classes("flex-1"):
                                ui.label("Top Losers").classes("text-lg font-semibold mb-2").style(
                                    f"color: {COLORS['error']};"
                                )
                                top_losers = trades_df.nsmallest(min(20, len(trades_df)), "pnl_pct")
                                with ui.element("div").style(
                                    "width: 100%; max-height: 450px; overflow-x: auto;"
                                ):
                                    ui.table(
                                        columns=table_columns,
                                        rows=_trade_rows(top_losers),
                                        pagination={
                                            "rowsPerPage": 20,
                                            "rowsPerPage_options": [10, 20, 50, 100],
                                        },
                                    ).style("min-width: max-content;")

                with ui.tab_panel(tab_stock):
                    if "symbol" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        stock_stats = (
                            trades_df.groupby("symbol")
                            .agg(
                                trades=("pnl_pct", "count"),
                                total_pnl=("pnl_pct", "sum"),
                                avg_pnl=("pnl_pct", "mean"),
                                avg_r=("pnl_r", "mean"),
                                win_rate=("pnl_pct", lambda x: (x > 0).mean() * 100),
                                best=("pnl_pct", "max"),
                                worst=("pnl_pct", "min"),
                            )
                            .reset_index()
                            .sort_values("total_pnl", ascending=False)
                        )

                        stock_rows = [
                            {
                                "symbol": row["symbol"],
                                "trades_fmt": f"{int(row['trades'])}",
                                "total_pnl_fmt": f"{row['total_pnl']:.2f}%",
                                "avg_pnl_fmt": f"{row['avg_pnl']:.2f}%",
                                "avg_r_fmt": f"{row['avg_r']:.2f}R",
                                "win_rate_fmt": f"{row['win_rate']:.1f}%",
                                "best_fmt": f"{row['best']:.1f}%",
                                "worst_fmt": f"{row['worst']:.1f}%",
                            }
                            for _, row in stock_stats.head(50).iterrows()
                        ]

                        paginated_table(
                            columns=[
                                {"name": "symbol", "label": "Symbol", "field": "symbol"},
                                {"name": "trades", "label": "Trades", "field": "trades_fmt"},
                                {"name": "total_pnl", "label": "Total %", "field": "total_pnl_fmt"},
                                {"name": "avg_pnl", "label": "Avg %", "field": "avg_pnl_fmt"},
                                {"name": "avg_r", "label": "Avg R", "field": "avg_r_fmt"},
                                {"name": "win_rate", "label": "Win %", "field": "win_rate_fmt"},
                                {"name": "best", "label": "Best", "field": "best_fmt"},
                                {"name": "worst", "label": "Worst", "field": "worst_fmt"},
                            ],
                            rows=stock_rows,
                            page_size=20,
                        )

                with ui.tab_panel(tab_monthly):
                    if "entry_date" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        trades_df_copy = trades_df.copy()
                        trades_df_copy["year"] = pd.to_datetime(
                            trades_df_copy["entry_date"]
                        ).dt.year
                        trades_df_copy["month"] = pd.to_datetime(
                            trades_df_copy["entry_date"]
                        ).dt.month

                        monthly_pivot = trades_df_copy.pivot_table(
                            index="year",
                            columns="month",
                            values="pnl_pct",
                            aggfunc="sum",
                            fill_value=0,
                        )

                        monthly_pivot = monthly_pivot.reindex(columns=range(1, 13))

                        fig = px.imshow(
                            monthly_pivot,
                            labels=dict(x="Month", y="Year", color="Return %"),
                            x=[
                                "Jan",
                                "Feb",
                                "Mar",
                                "Apr",
                                "May",
                                "Jun",
                                "Jul",
                                "Aug",
                                "Sep",
                                "Oct",
                                "Nov",
                                "Dec",
                            ],
                            color_continuous_scale=[COLORS["error"], "#f1f5f9", COLORS["success"]],
                            color_continuous_midpoint=0,
                            title="Monthly Returns Heatmap",
                        )
                        fig.update_xaxes(side="top")
                        apply_chart_theme(fig)
                        ui.plotly(fig).classes("w-full h-80")

            divider()
            with ui.expansion("Run New Backtest", icon="play_arrow").classes("w-full"):
                ui.label("Configure and launch a new backtest run.").classes("mb-4").style(
                    f"color: {THEME['text_secondary']};"
                )
                with ui.row().classes("w-full gap-4"):
                    ui.number("Universe Size", value=500, min=50, max=2000, step=50)
                    ui.number("Start Year", value=2015, min=2010, max=2025)
                    ui.number("End Year", value=2025, min=2015, max=2026)

                with ui.column().classes("kpi-card mt-4"):
                    ui.label("Run this command in your terminal:").classes("text-sm mb-2").style(
                        f"color: {THEME['text_secondary']};"
                    )
                    ui.label(
                        "doppler run -- uv run nseml-backtest --universe-size 2000 --start-year 2015 --end-year 2025"
                    ).classes("font-mono text-sm").style(f"color: {COLORS['success']};")

                ui.label("After completion, refresh this page to see the new experiment.").classes(
                    "text-sm mt-2"
                ).style(f"color: {THEME['text_muted']};")

        with ui.row().classes("kpi-card w-full items-center gap-4 mb-6"):
            ui.icon("science").classes("text-xl").style(f"color: {THEME['primary']};")
            ui.label("Experiment").classes("text-sm font-medium").style(
                f"color: {THEME['text_secondary']};"
            )

            def on_select(e):
                selected_label = e.value
                selected_id = exp_options.get(selected_label)
                if selected_id:
                    # Save current selection for theme toggle preservation
                    ui.run_javascript(
                        f"sessionStorage.setItem('nseml_restore_exp_id', '{selected_id}');"
                    )
                    render_experiment.refresh(selected_id)

            ui.select(
                labels,
                value=initial_label,
                on_change=on_select,
            ).classes("flex-grow").props("outlined")

        render_experiment(exp_options[initial_label])
