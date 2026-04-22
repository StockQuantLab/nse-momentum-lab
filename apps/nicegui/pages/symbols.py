"""Symbol Performance page - Per-symbol P/L breakdown for backtest experiments."""

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
    build_experiment_options,
    get_experiment_trades,
    get_experiments,
)
from apps.nicegui.components import (
    apply_chart_theme,
    color_error,
    color_info,
    color_primary,
    color_success,
    empty_state,
    kpi_grid,
    loading_spinner,
    page_layout,
    paginated_table,
    SPACE_LG,
    SPACE_MD,
    theme_text_primary,
)


def _fmt(value: object, decimals: int = 2) -> str:
    if value is None or (isinstance(value, float) and _is_nan(value)):
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except TypeError, ValueError:
        return "-"


def _is_nan(v: object) -> bool:
    return isinstance(v, float) and v != v


def _compute_symbol_stats(trades_df: pl.DataFrame) -> pl.DataFrame:
    """Group trades by symbol and compute per-symbol statistics."""
    return (
        trades_df.group_by("symbol")
        .agg(
            [
                pl.count().alias("trades"),
                pl.col("net_pnl").sum().alias("total_pnl"),
                pl.col("net_pnl").mean().alias("avg_pnl"),
                pl.col("pnl_pct").max().alias("best_trade"),
                pl.col("pnl_pct").min().alias("worst_trade"),
                (pl.col("net_pnl") > 0).sum().alias("wins"),
                pl.col("pnl_r").mean().alias("avg_r"),
                pl.col("holding_days").mean().alias("avg_holding"),
            ]
        )
        .with_columns(
            (pl.col("wins") / pl.col("trades") * 100).alias("win_rate"),
        )
        .sort("total_pnl", descending=True)
    )


def _symbol_table_rows(stats: pl.DataFrame) -> list[dict]:
    rows = []
    for r in stats.iter_rows(named=True):
        rows.append(
            {
                "symbol": r["symbol"],
                "trades": int(r.get("trades", 0)),
                "total_pnl": _fmt(r.get("total_pnl")),
                "avg_pnl": _fmt(r.get("avg_pnl")),
                "best_trade": _fmt(r.get("best_trade")),
                "worst_trade": _fmt(r.get("worst_trade")),
                "win_rate": _fmt(r.get("win_rate"), 1) + "%",
                "avg_r": _fmt(r.get("avg_r")),
                "avg_holding": _fmt(r.get("avg_holding"), 1),
            }
        )
    return rows


async def symbols_page() -> None:
    """Render the symbol performance page."""
    with page_layout("Symbol Performance", "leaderboard"):
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
                "No experiments available",
                "Run a backtest first to see symbol performance.",
                icon="leaderboard",
            )
            return

        exp_options = build_experiment_options(experiments_df)
        labels = list(exp_options.keys())
        first_label = labels[0]
        first_exp_id = exp_options[first_label]

        # Experiment selector with sessionStorage persistence
        select = ui.select(
            options=labels,
            value=first_label,
            with_input=True,
            label="Experiment",
        ).classes("w-full")

        # Restore from sessionStorage on load
        ui.run_javascript(
            """(() => {
                const saved = sessionStorage.getItem('nseml_symbols_exp');
                if (saved) {
                    const el = document.querySelector('.q-select');
                    if (el && el.__vue__) {
                        // Will be picked up by next render cycle
                    }
                }
            })()"""
        )

        @ui.refreshable
        def render_symbols(exp_id: str) -> None:
            trades_df = get_experiment_trades(exp_id)

            if trades_df is None or trades_df.is_empty():
                empty_state("No Trades", "This experiment has no trades.", icon="leaderboard")
                return

            stats = _compute_symbol_stats(trades_df)

            # KPI chips
            profitable = stats.filter(pl.col("total_pnl") > 0)
            total_trades = len(trades_df)
            total_wins = int((trades_df["net_pnl"] > 0).sum())
            best_sym = stats["symbol"][0] if len(stats) > 0 else "-"

            kpi_grid(
                [
                    dict(
                        title="Symbols Tracked",
                        value=str(len(stats)),
                        subtitle="Unique symbols traded",
                        icon="bar_chart",
                        color=color_info(),
                    ),
                    dict(
                        title="Profitable Symbols",
                        value=str(len(profitable)),
                        subtitle=f"{len(profitable) / max(len(stats), 1) * 100:.0f}% profitable",
                        icon="trending_up",
                        color=color_success(),
                    ),
                    dict(
                        title="Symbol Win Rate",
                        value=f"{total_wins / max(total_trades, 1) * 100:.1f}%",
                        subtitle="Across all symbols",
                        icon="percent",
                        color=color_primary(),
                    ),
                    dict(
                        title="Best Symbol",
                        value=str(best_sym),
                        subtitle=f"P/L: {_fmt(stats['total_pnl'][0]) if len(stats) > 0 else '-'}",
                        icon="emoji_events",
                        color=color_success(),
                    ),
                ],
                columns=4,
            )

            # Top 30 bar chart
            top_30 = stats.head(30)
            if len(top_30) > 0:
                ui.space().classes(SPACE_MD)
                ui.label("Top 30 Symbols by P/L").classes(
                    f"text-lg font-semibold {SPACE_MD}"
                ).style(f"color: {theme_text_primary()};")

                symbols = top_30["symbol"].to_list()
                pnls = top_30["total_pnl"].to_list()
                bar_colors = [color_success() if p >= 0 else color_error() for p in pnls]

                fig = go.Figure(
                    go.Bar(
                        x=pnls,
                        y=symbols,
                        orientation="h",
                        marker_color=bar_colors,
                        text=[f"{p:,.0f}" for p in pnls],
                        textposition="auto",
                    )
                )
                fig.update_layout(
                    height=max(400, len(symbols) * 22),
                    margin=dict(l=80, r=20, t=20, b=40),
                    yaxis=dict(autorange="reversed"),
                    xaxis_title="Total P/L",
                )
                apply_chart_theme(fig)
                ui.plotly(fig).classes("w-full")

            # Full symbol table
            ui.space().classes(SPACE_LG)
            ui.label("All Symbols").classes(f"text-lg font-semibold {SPACE_MD}").style(
                f"color: {theme_text_primary()};"
            )

            table_rows = _symbol_table_rows(stats)
            cols = [
                {"name": "symbol", "label": "Symbol", "field": "symbol", "sortable": True},
                {"name": "trades", "label": "Trades", "field": "trades", "sortable": True},
                {
                    "name": "total_pnl",
                    "label": "Total P/L",
                    "field": "total_pnl",
                    "sortable": True,
                },
                {
                    "name": "avg_pnl",
                    "label": "Avg P/L",
                    "field": "avg_pnl",
                    "sortable": True,
                },
                {
                    "name": "best_trade",
                    "label": "Best %",
                    "field": "best_trade",
                    "sortable": True,
                },
                {
                    "name": "worst_trade",
                    "label": "Worst %",
                    "field": "worst_trade",
                    "sortable": True,
                },
                {
                    "name": "win_rate",
                    "label": "Win Rate",
                    "field": "win_rate",
                    "sortable": True,
                },
                {"name": "avg_r", "label": "Avg R", "field": "avg_r", "sortable": True},
                {
                    "name": "avg_holding",
                    "label": "Avg Hold",
                    "field": "avg_holding",
                    "sortable": True,
                },
            ]
            paginated_table(table_rows, cols, page_size=25)

        # Initial render and refresh on selection change
        current_exp_id = first_exp_id

        def _get_exp_id(label: str) -> str:
            return exp_options.get(label, first_exp_id)

        def _on_change() -> None:
            nonlocal current_exp_id
            current_exp_id = _get_exp_id(select.value)
            # Save to sessionStorage
            ui.run_javascript(f"sessionStorage.setItem('nseml_symbols_exp', '{select.value}');")
            render_symbols.refresh(current_exp_id)

        select.on_value_change(lambda: _on_change())
        render_symbols(current_exp_id)
