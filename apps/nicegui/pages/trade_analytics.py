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

import polars as pl
import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.state import (
    get_experiments,
    get_experiment,
    get_experiment_trades,
    prepare_trades_df,
    build_experiment_options,
    get_experiment_param_items,
)
from apps.nicegui.components import (
    page_layout,
    kpi_grid,
    apply_chart_theme,
    empty_state,
    page_header,
    paginated_table,
    loading_spinner,
    SPACE_GRID_DEFAULT,
    SPACE_SECTION,
    theme_primary,
    theme_text_secondary,
    color_success,
    color_error,
    color_info,
    color_warning,
)


async def trade_analytics_page() -> None:
    """Render the trade analytics page."""
    with page_layout("Trade Analytics", "analytics"):
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
        first_exp_id = exp_options[first_label]

        # ── refreshable content ───────────────────────────────────
        @ui.refreshable
        def render_analytics(exp_id: str) -> None:
            trades_df = get_experiment_trades(exp_id)
            exp = get_experiment(exp_id)
            strategy = str(exp.get("strategy_name", "")) if exp else ""
            trades_df = prepare_trades_df(trades_df, strategy_name=strategy)

            if trades_df.is_empty():
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
                    color=color_info(),
                ),
                dict(
                    title="Win Rate",
                    value=f"{win_rate:.1f}%",
                    icon="target",
                    color=color_success(),
                ),
            ]
            if "pnl_pct" in trades_df.columns:
                total_pnl = trades_df["pnl_pct"].sum()
                cards.append(
                    dict(
                        title="Total Return",
                        value=f"{total_pnl:.1f}%",
                        icon="attach_money",
                        color=color_success() if total_pnl > 0 else color_error(),
                    )
                )
            if "pnl_r" in trades_df.columns:
                avg_r = float(trades_df["pnl_r"].mean())
                cards.append(
                    dict(
                        title="Avg R",
                        value=f"{avg_r:.2f}R",
                        icon="trending_up",
                        color=color_warning(),
                    )
                )

            kpi_grid(cards, columns=len(cards))

            exp = get_experiment(exp_id)
            if exp:
                with ui.expansion("Run Parameters", icon="tune", value=False).classes(
                    f"w-full {SPACE_SECTION}"
                ):
                    ui.label(
                        f"{exp.get('strategy_name', '-')}"
                        f" | ID {exp_id}"
                        f" | {exp.get('start_date', '-')}"
                        f" to {exp.get('end_date', '-')}"
                    ).classes("text-sm font-medium").style(f"color: {theme_text_secondary()};")
                    params = get_experiment_param_items(exp)
                    if not params:
                        ui.label("No stored parameters found.").classes("text-sm").style(
                            f"color: {theme_text_secondary()};"
                        )
                    else:
                        with ui.column().classes("w-full gap-1 mt-2"):
                            for key, value in params:
                                with ui.row().classes("w-full items-start justify-between gap-4"):
                                    ui.label(key).classes("text-xs font-mono").style(
                                        f"color: {color_info()}; min-width: 220px;"
                                    )
                                    ui.label(value).classes(
                                        "text-xs font-mono text-right break-all"
                                    ).style(f"color: {theme_text_secondary()};")

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
                            trades_df.group_by("exit_reason")
                            .agg(
                                pl.col("pnl_pct").count().alias("count"),
                                pl.col("pnl_pct").mean().alias("avg_pnl"),
                                pl.col("pnl_r").mean().alias("avg_r"),
                                (pl.col("pnl_pct") > 0).sum().alias("wins"),
                                pl.col("pnl_pct").sum().alias("total_pnl"),
                            )
                            .with_columns(
                                (pl.col("wins") / pl.col("count") * 100).round(1).alias("win_rate"),
                            )
                        )

                        paginated_table(
                            columns=[
                                {"name": "exit_reason", "label": "Reason", "field": "exit_reason"},
                                {"name": "count", "label": "Trades", "field": "count"},
                                {
                                    "name": "avg_pnl",
                                    "label": "Avg %",
                                    "field": "avg_pnl",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                },
                                {
                                    "name": "avg_r",
                                    "label": "Avg R",
                                    "field": "avg_r",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + 'R'",
                                },
                                {
                                    "name": "win_rate",
                                    "label": "Win %",
                                    "field": "win_rate",
                                    ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                },
                                {
                                    "name": "total_pnl",
                                    "label": "Total %",
                                    "field": "total_pnl",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                },
                            ],
                            rows=[
                                {
                                    "exit_reason": row["exit_reason"],
                                    "count": int(row["count"]),
                                    "avg_pnl": float(row["avg_pnl"]),
                                    "avg_r": float(row["avg_r"]),
                                    "win_rate": float(row["win_rate"]),
                                    "total_pnl": float(row["total_pnl"]),
                                }
                                for row in exit_summary.iter_rows(named=True)
                            ],
                            page_size=20,
                        )

                        fig = go.Figure()
                        fig.add_trace(
                            go.Bar(
                                x=exit_summary["exit_reason"].to_list(),
                                y=exit_summary["avg_pnl"].to_list(),
                                marker_color=[
                                    color_success() if v > 0 else color_error()
                                    for v in exit_summary["avg_pnl"].to_list()
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
                        monthly_df = trades_df.with_columns(
                            pl.col("entry_date").cast(pl.Date, strict=False).alias("_date"),
                        ).with_columns(
                            pl.col("_date").dt.year().alias("_year"),
                            pl.col("_date").dt.month().alias("_month"),
                        )

                        monthly_data = (
                            monthly_df.group_by("_year", "_month")
                            .agg(
                                pl.col("pnl_pct").count().alias("trades"),
                                pl.col("pnl_pct").sum().alias("total_pnl"),
                                pl.col("pnl_pct").mean().alias("avg_pnl"),
                                (
                                    (pl.col("pnl_pct") > 0).sum() / pl.col("pnl_pct").count() * 100
                                ).alias("win_rate"),
                            )
                            .sort("_year", "_month")
                            .with_columns(
                                (
                                    pl.col("_year").cast(pl.Utf8)
                                    + "-"
                                    + pl.col("_month").cast(pl.Utf8).str.pad_start(2, "0")
                                ).alias("month_str"),
                            )
                        )

                        paginated_table(
                            columns=[
                                {"name": "month_str", "label": "Month", "field": "month_str"},
                                {"name": "trades", "label": "Trades", "field": "trades"},
                                {
                                    "name": "total_pnl",
                                    "label": "Total %",
                                    "field": "total_pnl",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                },
                                {
                                    "name": "avg_pnl",
                                    "label": "Avg %",
                                    "field": "avg_pnl",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                },
                                {
                                    "name": "win_rate",
                                    "label": "Win %",
                                    "field": "win_rate",
                                    ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                },
                            ],
                            rows=[
                                {
                                    "month_str": row["month_str"],
                                    "trades": int(row["trades"]),
                                    "total_pnl": float(row["total_pnl"]),
                                    "avg_pnl": float(row["avg_pnl"]),
                                    "win_rate": float(row["win_rate"]),
                                }
                                for row in monthly_data.iter_rows(named=True)
                            ],
                            page_size=15,
                        )

                        fig = go.Figure()
                        fig.add_trace(
                            go.Scatter(
                                x=monthly_data["month_str"].to_list(),
                                y=monthly_data["total_pnl"].to_list(),
                                mode="lines+markers",
                                line=dict(color=color_success()),
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
                            trades_df.group_by("symbol")
                            .agg(
                                pl.col("pnl_pct").count().alias("trades"),
                                pl.col("pnl_pct").sum().alias("total_pnl"),
                                pl.col("pnl_pct").mean().alias("avg_pnl"),
                                pl.col("pnl_pct").max().alias("best"),
                                pl.col("pnl_pct").min().alias("worst"),
                            )
                            .sort("total_pnl", descending=True)
                        )

                        paginated_table(
                            columns=[
                                {"name": "symbol", "label": "Symbol", "field": "symbol"},
                                {"name": "trades", "label": "Trades", "field": "trades"},
                                {
                                    "name": "total_pnl",
                                    "label": "Total %",
                                    "field": "total_pnl",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                },
                                {
                                    "name": "avg_pnl",
                                    "label": "Avg %",
                                    "field": "avg_pnl",
                                    ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                },
                                {
                                    "name": "best",
                                    "label": "Best %",
                                    "field": "best",
                                    ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                },
                                {
                                    "name": "worst",
                                    "label": "Worst %",
                                    "field": "worst",
                                    ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                },
                            ],
                            rows=[
                                {
                                    "symbol": row["symbol"],
                                    "trades": int(row["trades"]),
                                    "total_pnl": float(row["total_pnl"]),
                                    "avg_pnl": float(row["avg_pnl"]),
                                    "best": float(row["best"]),
                                    "worst": float(row["worst"]),
                                }
                                for row in symbol_stats.head(30).iter_rows(named=True)
                            ],
                            page_size=10,
                        )

        # ── experiment selector ───────────────────────────────────
        with ui.row().classes(f"kpi-card w-full items-center {SPACE_GRID_DEFAULT} {SPACE_SECTION}"):
            ui.icon("science").classes("text-2xl").style(f"color: {theme_primary()};")
            ui.label("Experiment").classes("text-sm font-medium").style(
                f"color: {theme_text_secondary()};"
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

        render_analytics(first_exp_id)
