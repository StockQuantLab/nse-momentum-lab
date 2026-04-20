"""Compare Experiments page - Side-by-side experiment comparison with delta analysis."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

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

from apps.nicegui.state import (
    get_experiments,
    get_experiment,
    get_experiment_trades,
    build_experiment_options,
)
from apps.nicegui.components import (
    page_layout,
    divider,
    apply_chart_theme,
    empty_state,
    page_header,
    loading_spinner,
    kpi_grid,
    color_success,
    color_error,
    color_info,
    color_primary,
    color_warning,
    theme_text_primary,
    theme_text_secondary,
    theme_text_muted,
    theme_surface,
    theme_surface_border,
)


def _flatten_params(params: dict, parent_key: str = "") -> list[tuple[str, str]]:
    """Recursively flatten nested param dicts into (key, value) pairs."""
    items: list[tuple[str, str]] = []
    for k, v in params.items():
        full_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_params(v, full_key))
        else:
            items.append((full_key, v))
    return items


def _format_param_value(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _num_delta(a_val: str, b_val: str) -> str:
    """Extract numeric delta from formatted strings."""
    try:
        a_num = float(str(a_val).replace("₹", "").replace("%", "").replace(",", ""))
        b_num = float(str(b_val).replace("₹", "").replace("%", "").replace(",", ""))
        d = b_num - a_num
        if "%" in str(a_val):
            return f"{d:+.1f}%"
        return f"{d:+.2f}"
    except ValueError, TypeError:
        return ""


def _param_group(key: str) -> str:
    """Group flattened param keys by their root section."""
    root = key.split(".", 1)[0]
    if root in ("entry_config", "exit_config", "risk_config"):
        return root.replace("_", " ").title()
    if root in ("filters", "filter_config"):
        return "Filters"
    return "General"


def _build_param_sections(params_a: dict, params_b: dict) -> list[dict]:
    """Build grouped param comparison sections with diff counts."""
    flat_a = dict(_flatten_params(params_a))
    flat_b = dict(_flatten_params(params_b))
    all_keys = sorted(set(flat_a) | set(flat_b))
    sections: dict[str, dict] = {}
    for key in all_keys:
        group = _param_group(key)
        section = sections.setdefault(
            group,
            {"name": group, "rows": [], "diff_count": 0, "total_count": 0},
        )
        value_a = flat_a.get(key)
        value_b = flat_b.get(key)
        same = value_a == value_b
        section["total_count"] += 1
        if not same:
            section["diff_count"] += 1
        section["rows"].append(
            {
                "parameter": key,
                "run_a": value_a,
                "run_b": value_b,
                "same": same,
            }
        )
    order = ["General", "Entry Config", "Exit Config", "Risk Config", "Filters"]
    return [sections[name] for name in order if name in sections]


def _render_param_section(section: dict, *, label_a: str, label_b: str) -> None:
    """Render one grouped parameter comparison section."""
    diff_count = int(section.get("diff_count") or 0)
    total_count = int(section.get("total_count") or 0)
    rows = section.get("rows") or []
    with ui.expansion(
        f"{section['name']} ({diff_count} / {total_count} different)",
        value=diff_count > 0,
    ).classes("w-full mb-3"):
        with (
            ui.grid(columns=4)
            .classes("w-full gap-3 px-2 pb-2 items-center")
            .style(
                f"color: {theme_text_muted()}; border-bottom: 1px solid {theme_surface_border()};"
            )
        ):
            ui.label("Parameter").classes("text-xs font-semibold min-w-0")
            ui.label(label_a).classes("text-xs font-semibold min-w-0")
            ui.label(label_b).classes("text-xs font-semibold min-w-0")
            ui.label("Status").classes("text-xs font-semibold text-right min-w-0")

        for row in rows:
            same = bool(row.get("same"))
            row_style = (
                f"border-bottom: 1px solid {theme_surface_border()};"
                if same
                else f"border-bottom: 1px solid {theme_surface_border()}; background: {theme_surface()};"
            )
            with ui.grid(columns=4).classes("w-full gap-3 px-2 py-2 items-start").style(row_style):
                ui.label(str(row.get("parameter") or "")).classes("text-xs min-w-0").style(
                    "word-break: break-word; white-space: normal; "
                    + (
                        f"font-weight: 700; color: {theme_text_primary()};"
                        if not same
                        else f"color: {theme_text_secondary()};"
                    )
                )
                val_a = _format_param_value(row.get("run_a"))
                val_b = _format_param_value(row.get("run_b"))
                style = (
                    f"font-weight: 700; color: {theme_text_primary()};"
                    if not same
                    else f"color: {theme_text_secondary()};"
                )
                ui.label(val_a).classes("text-xs w-full").style(
                    "word-break: break-word; white-space: normal; " + style
                )
                ui.label(val_b).classes("text-xs w-full").style(
                    "word-break: break-word; white-space: normal; " + style
                )
                ui.label("different" if not same else "same").classes(
                    "text-right text-xs font-semibold min-w-0"
                ).style(
                    f"color: {color_error() if not same else theme_text_muted()};"
                    + (" font-weight: 700;" if not same else "")
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

        id_to_label = {v: k for k, v in exp_options.items()}

        saved_exp1_id = await ui.run_javascript(
            "sessionStorage.getItem('nseml_compare_exp1') || ''", timeout=2.0
        )
        saved_exp2_id = await ui.run_javascript(
            "sessionStorage.getItem('nseml_compare_exp2') || ''", timeout=2.0
        )

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

        if not initial_exp1:
            initial_exp1 = labels[0]
        if not initial_exp2:
            initial_exp2 = labels[1] if len(labels) > 1 else labels[0]

        # Mutable state for selections (accessible in all closures)
        selected = {"exp1": initial_exp1, "exp2": initial_exp2}

        page_header(
            "Compare Experiments",
            "Select two experiments to compare side-by-side with delta analysis",
        )

        @ui.refreshable
        def render_comparison():
            exp1_label = selected["exp1"]
            exp2_label = selected["exp2"]

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

            def _strat(e: dict) -> str:
                return str(e.get("strategy_name", "-"))

            def _period(e: dict) -> str:
                s = str(e.get("start_date", "-"))[:10]
                en = str(e.get("end_date", "-"))[:10]
                return f"{s} → {en}"

            s1 = _strat(exp1)
            s2 = _strat(exp2)

            # ── Side-by-side KPI cards ────────────────────────────────────
            with ui.row().classes("w-full gap-6 responsive-row"):
                for e, label, strat in [(exp1, "Experiment A", s1), (exp2, "Experiment B", s2)]:
                    with ui.column().classes("w-full"):
                        with ui.row().classes("items-center gap-3 mb-3"):
                            ui.label(f"**{label}**").classes("text-base font-bold").style(
                                f"color: {theme_text_primary()};"
                            )
                            ui.html(
                                f'<span style="background:{color_info()};color:#fff;padding:2px 8px;'
                                f"border-radius:3px;font-size:0.75rem;font-weight:600;"
                                f'font-family:monospace">{strat}</span>'
                            )
                            ui.label(_period(e)).classes("text-xs").style(
                                f"color: {theme_text_muted()};"
                            )

                        kpi_grid(
                            [
                                dict(
                                    title="Total Return",
                                    value=f"{float(e.get('total_return_pct') or 0):.1f}%",
                                    icon="trending_up",
                                    color=color_success()
                                    if float(e.get("total_return_pct") or 0) > 0
                                    else color_error(),
                                ),
                                dict(
                                    title="Win Rate",
                                    value=f"{float(e.get('win_rate_pct') or 0):.1f}%",
                                    icon="target",
                                    color=color_success()
                                    if float(e.get("win_rate_pct") or 0) >= 40
                                    else color_error(),
                                ),
                                dict(
                                    title="Max Drawdown",
                                    value=f"{float(e.get('max_drawdown_pct') or 0):.1f}%",
                                    icon="trending_down",
                                    color=color_error(),
                                ),
                                dict(
                                    title="Total Trades",
                                    value=f"{int(e.get('total_trades') or 0):,}",
                                    icon="swap_horiz",
                                    color=color_info(),
                                ),
                            ],
                            columns=2,
                        )

            divider()

            # ── Performance comparison bar chart ──────────────────────────
            metrics = [
                "Win Rate %",
                "Profit Factor",
                "Annualized %",
                "Total Return %",
                "Max DD %",
            ]
            v1 = [
                float(exp1.get("win_rate_pct") or 0),
                float(exp1.get("profit_factor") or 0),
                float(exp1.get("annualized_return_pct") or 0),
                float(exp1.get("total_return_pct") or 0),
                abs(float(exp1.get("max_drawdown_pct") or 0)),
            ]
            v2 = [
                float(exp2.get("win_rate_pct") or 0),
                float(exp2.get("profit_factor") or 0),
                float(exp2.get("annualized_return_pct") or 0),
                float(exp2.get("total_return_pct") or 0),
                abs(float(exp2.get("max_drawdown_pct") or 0)),
            ]

            fig = go.Figure()
            fig.add_trace(go.Bar(name=s1, x=metrics, y=v1, marker_color=color_primary()))
            fig.add_trace(go.Bar(name=s2, x=metrics, y=v2, marker_color=color_info()))
            fig.update_layout(
                title="Performance Comparison",
                barmode="group",
                xaxis_title="Metric",
                showlegend=True,
            )
            apply_chart_theme(fig)
            ui.plotly(fig).classes("w-full h-72")

            divider()

            # ── Trade-level breakdowns ─────────────────────────────────────
            trades1 = get_experiment_trades(exp1_id)
            trades2 = get_experiment_trades(exp2_id)

            # ── Exit Reason Breakdown ──────────────────────────────────────
            def _exit_breakdown(df: pl.DataFrame) -> dict[str, dict]:
                if df.is_empty() or "exit_reason" not in df.columns:
                    return {}
                return {
                    row["exit_reason"]: {
                        "count": int(row["count"]),
                        "avg_pnl": float(row["avg_pnl"]),
                        "total_pnl": float(row["total_pnl"]),
                    }
                    for row in df.group_by("exit_reason")
                    .agg(
                        pl.len().alias("count"),
                        pl.col("pnl_pct").mean().round(2).alias("avg_pnl"),
                        pl.col("pnl_pct").sum().round(2).alias("total_pnl"),
                    )
                    .iter_rows(named=True)
                }

            er1 = _exit_breakdown(trades1)
            er2 = _exit_breakdown(trades2)
            all_reasons = sorted(set(er1) | set(er2))

            if all_reasons:
                ui.label("Exit Reason Breakdown").classes("text-base font-semibold mb-3").style(
                    f"color: {theme_text_primary()};"
                )

                cnt_a = [er1.get(r, {}).get("count", 0) for r in all_reasons]
                cnt_b = [er2.get(r, {}).get("count", 0) for r in all_reasons]

                exit_fig = go.Figure()
                exit_fig.add_trace(
                    go.Bar(name=s1, x=all_reasons, y=cnt_a, marker_color=color_primary())
                )
                exit_fig.add_trace(
                    go.Bar(name=s2, x=all_reasons, y=cnt_b, marker_color=color_info())
                )
                exit_fig.update_layout(
                    title="Trades by Exit Reason",
                    barmode="group",
                    xaxis_title="Exit Reason",
                    yaxis_title="Trade Count",
                    showlegend=True,
                )
                apply_chart_theme(exit_fig)
                ui.plotly(exit_fig).classes("w-full h-64")

                total_a = max(sum(v["count"] for v in er1.values()), 1)
                total_b = max(sum(v["count"] for v in er2.values()), 1)
                er_rows = []
                for r in all_reasons:
                    ra, rb = er1.get(r, {}), er2.get(r, {})
                    ca, cb = ra.get("count", 0), rb.get("count", 0)
                    pa, pb = ra.get("avg_pnl", 0), rb.get("avg_pnl", 0)
                    er_rows.append(
                        {
                            "reason": r,
                            "a_count": f"{ca:,} ({ca / total_a * 100:.0f}%)",
                            "a_avg": f"{pa:.2f}%",
                            "b_count": f"{cb:,} ({cb / total_b * 100:.0f}%)",
                            "b_avg": f"{pb:.2f}%",
                            "delta": f"{cb - ca:+,}",
                        }
                    )
                er_columns = [
                    {"name": "reason", "label": "Exit Reason", "field": "reason", "align": "left"},
                    {
                        "name": "a_count",
                        "label": f"{s1} Count",
                        "field": "a_count",
                        "align": "right",
                    },
                    {"name": "a_avg", "label": f"{s1} Avg P/L", "field": "a_avg", "align": "right"},
                    {
                        "name": "b_count",
                        "label": f"{s2} Count",
                        "field": "b_count",
                        "align": "right",
                    },
                    {"name": "b_avg", "label": f"{s2} Avg P/L", "field": "b_avg", "align": "right"},
                    {"name": "delta", "label": "Delta", "field": "delta", "align": "right"},
                ]
                ui.table(columns=er_columns, rows=er_rows, row_key="reason").classes("w-full mt-2")

                divider()

            # ── Win/Loss Analysis ──────────────────────────────────────────
            def _win_loss_stats(df: pl.DataFrame) -> dict:
                if df.is_empty() or "pnl_pct" not in df.columns:
                    return {}
                wins = df.filter(pl.col("pnl_pct") > 0)
                losses = df.filter(pl.col("pnl_pct") <= 0)
                return {
                    "total": df.height,
                    "winners": wins.height,
                    "losers": losses.height,
                    "avg_win": float(wins["pnl_pct"].mean()) if not wins.is_empty() else 0,
                    "avg_loss": float(losses["pnl_pct"].mean()) if not losses.is_empty() else 0,
                    "best_trade": float(df["pnl_pct"].max()),
                    "worst_trade": float(df["pnl_pct"].min()),
                    "gross_profit": float(wins["pnl_pct"].sum()) if not wins.is_empty() else 0,
                    "gross_loss": float(losses["pnl_pct"].sum()) if not losses.is_empty() else 0,
                }

            wl1 = _win_loss_stats(trades1)
            wl2 = _win_loss_stats(trades2)

            if wl1 or wl2:

                def _delta_str(va: float, vb: float, fmt: str = ".2f", suffix: str = "") -> str:
                    d = vb - va
                    return f"{d:+{fmt}}{suffix}"

                ui.label("Win / Loss Analysis").classes("text-base font-semibold mb-3").style(
                    f"color: {theme_text_primary()};"
                )
                wl_rows = [
                    {
                        "metric": "Total Trades",
                        "run_a": f"{wl1.get('total', 0):,}",
                        "run_b": f"{wl2.get('total', 0):,}",
                        "delta": _delta_str(wl1.get("total", 0), wl2.get("total", 0), ".0f"),
                    },
                    {
                        "metric": "Winners / Losers",
                        "run_a": f"{wl1.get('winners', 0):,} / {wl1.get('losers', 0):,}",
                        "run_b": f"{wl2.get('winners', 0):,} / {wl2.get('losers', 0):,}",
                        "delta": "",
                    },
                    {
                        "metric": "Avg Win",
                        "run_a": f"{wl1.get('avg_win', 0):.2f}%",
                        "run_b": f"{wl2.get('avg_win', 0):.2f}%",
                        "delta": _delta_str(
                            wl1.get("avg_win", 0), wl2.get("avg_win", 0), ".2f", "%"
                        ),
                    },
                    {
                        "metric": "Avg Loss",
                        "run_a": f"{wl1.get('avg_loss', 0):.2f}%",
                        "run_b": f"{wl2.get('avg_loss', 0):.2f}%",
                        "delta": _delta_str(
                            wl1.get("avg_loss", 0), wl2.get("avg_loss", 0), ".2f", "%"
                        ),
                    },
                    {
                        "metric": "Best Trade",
                        "run_a": f"{wl1.get('best_trade', 0):.2f}%",
                        "run_b": f"{wl2.get('best_trade', 0):.2f}%",
                        "delta": _delta_str(
                            wl1.get("best_trade", 0), wl2.get("best_trade", 0), ".2f", "%"
                        ),
                    },
                    {
                        "metric": "Worst Trade",
                        "run_a": f"{wl1.get('worst_trade', 0):.2f}%",
                        "run_b": f"{wl2.get('worst_trade', 0):.2f}%",
                        "delta": _delta_str(
                            wl1.get("worst_trade", 0), wl2.get("worst_trade", 0), ".2f", "%"
                        ),
                    },
                    {
                        "metric": "Gross Profit",
                        "run_a": f"{wl1.get('gross_profit', 0):.2f}%",
                        "run_b": f"{wl2.get('gross_profit', 0):.2f}%",
                        "delta": _delta_str(
                            wl1.get("gross_profit", 0),
                            wl2.get("gross_profit", 0),
                            ".2f",
                            "%",
                        ),
                    },
                    {
                        "metric": "Gross Loss",
                        "run_a": f"{wl1.get('gross_loss', 0):.2f}%",
                        "run_b": f"{wl2.get('gross_loss', 0):.2f}%",
                        "delta": _delta_str(
                            wl1.get("gross_loss", 0),
                            wl2.get("gross_loss", 0),
                            ".2f",
                            "%",
                        ),
                    },
                ]
                wl_columns = [
                    {"name": "metric", "label": "Metric", "field": "metric", "align": "left"},
                    {"name": "run_a", "label": f"Exp A ({s1})", "field": "run_a", "align": "right"},
                    {"name": "run_b", "label": f"Exp B ({s2})", "field": "run_b", "align": "right"},
                    {"name": "delta", "label": "Delta (B-A)", "field": "delta", "align": "right"},
                ]
                ui.table(columns=wl_columns, rows=wl_rows, row_key="metric").classes("w-full")

                divider()

            # ── Detailed Metrics table with delta ─────────────────────────
            ui.label("Detailed Metrics").classes("text-base font-semibold mb-3").style(
                f"color: {theme_text_primary()};"
            )

            metric_rows = [
                ("Strategy", _strat(exp1), _strat(exp2)),
                ("Period", _period(exp1), _period(exp2)),
                (
                    "Total Trades",
                    f"{int(exp1.get('total_trades') or 0):,}",
                    f"{int(exp2.get('total_trades') or 0):,}",
                ),
                (
                    "Win Rate",
                    f"{float(exp1.get('win_rate_pct') or 0):.1f}%",
                    f"{float(exp2.get('win_rate_pct') or 0):.1f}%",
                ),
                (
                    "Total Return",
                    f"{float(exp1.get('total_return_pct') or 0):.1f}%",
                    f"{float(exp2.get('total_return_pct') or 0):.1f}%",
                ),
                (
                    "Annualized Return",
                    f"{float(exp1.get('annualized_return_pct') or 0):.1f}%",
                    f"{float(exp2.get('annualized_return_pct') or 0):.1f}%",
                ),
                (
                    "Profit Factor",
                    f"{float(exp1.get('profit_factor') or 0):.2f}",
                    f"{float(exp2.get('profit_factor') or 0):.2f}",
                ),
                (
                    "Max Drawdown",
                    f"{float(exp1.get('max_drawdown_pct') or 0):.1f}%",
                    f"{float(exp2.get('max_drawdown_pct') or 0):.1f}%",
                ),
            ]

            detail_columns = [
                {"name": "metric", "label": "Metric", "field": "metric", "align": "left"},
                {"name": "run_a", "label": f"Exp A ({s1})", "field": "run_a", "align": "right"},
                {"name": "run_b", "label": f"Exp B ({s2})", "field": "run_b", "align": "right"},
                {"name": "delta", "label": "Delta (B-A)", "field": "delta", "align": "right"},
            ]
            tbl_rows = []
            for m, a, b in metric_rows:
                delta = _num_delta(a, b)
                tbl_rows.append({"metric": m, "run_a": a, "run_b": b, "delta": delta})

            ui.table(columns=detail_columns, rows=tbl_rows, row_key="metric").classes("w-full")

            # ── Parameter Comparison ──────────────────────────────────────
            params1 = {}
            params2 = {}
            try:
                params1 = json.loads(str(exp1.get("params_json") or "{}"))
            except TypeError, ValueError:
                params1 = {}
            try:
                params2 = json.loads(str(exp2.get("params_json") or "{}"))
            except TypeError, ValueError:
                params2 = {}
            if not isinstance(params1, dict):
                params1 = {}
            if not isinstance(params2, dict):
                params2 = {}

            if params1 or params2:
                param_sections = _build_param_sections(params1, params2)
                divider()
                ui.label("Parameter Comparison").classes("text-base font-semibold mb-2").style(
                    f"color: {theme_text_primary()};"
                )
                ui.label("Parameters grouped by family. Differences are highlighted.").classes(
                    "text-sm mb-3"
                ).style(f"color: {theme_text_secondary()};")

                for section in param_sections:
                    _render_param_section(
                        section,
                        label_a=f"Exp A ({s1})",
                        label_b=f"Exp B ({s2})",
                    )

        # ── Selectors at the top (above content) ────────────────────────────
        with ui.row().classes("w-full gap-4 items-end mb-4 flex-wrap"):
            with ui.column().classes("flex-1"):
                ui.label("Experiment A").classes("text-sm font-medium mb-1").style(
                    f"color: {theme_text_secondary()};"
                )

                def on_exp1_change(e):
                    selected["exp1"] = e.value
                    exp_id = exp_options.get(e.value, "")
                    ui.run_javascript(f"sessionStorage.setItem('nseml_compare_exp1', '{exp_id}');")
                    render_comparison.refresh()

                ui.select(
                    labels,
                    value=initial_exp1,
                    on_change=on_exp1_change,
                ).props("outlined dense use-input options-dense input-debounce=0").classes("w-full")

            with ui.column().classes("flex-1"):
                ui.label("Experiment B").classes("text-sm font-medium mb-1").style(
                    f"color: {theme_text_secondary()};"
                )

                def on_exp2_change(e):
                    selected["exp2"] = e.value
                    exp_id = exp_options.get(e.value, "")
                    ui.run_javascript(f"sessionStorage.setItem('nseml_compare_exp2', '{exp_id}');")
                    render_comparison.refresh()

                ui.select(
                    labels,
                    value=initial_exp2,
                    on_change=on_exp2_change,
                ).props("outlined dense use-input options-dense input-debounce=0").classes("w-full")

        divider()
        render_comparison()
