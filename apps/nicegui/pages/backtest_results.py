"""Backtest Results page - Full trade analysis with filters and charts."""

from __future__ import annotations

from datetime import date as dt_date
from datetime import datetime, time as dt_time
import json
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

import numpy as np
import polars as pl
import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.state import (
    get_experiments,
    get_experiment,
    get_experiment_param_items,
    get_experiment_trades,
    get_experiment_execution_diagnostics,
    get_experiment_yearly_metrics,
    prepare_trades_df,
    build_experiment_options,
    enrich_experiment_metrics,
)
from apps.nicegui.components import (
    page_layout,
    kpi_grid,
    divider,
    apply_chart_theme,
    color_error,
    color_gray,
    color_info,
    color_primary,
    color_success,
    color_warning,
    empty_state,
    hex_to_rgba,
    page_header,
    paginated_table,
    strat_badge,
    trade_table_with_filters,
    export_menu,
    loading_spinner,
    SPACE_GRID_DEFAULT,
    SPACE_GROUP_TIGHT,
    SPACE_LG,
    SPACE_SECTION,
    SPACE_SM,
    SPACE_XL,
    theme_primary,
    theme_surface,
    theme_surface_border,
    theme_text_muted,
    theme_text_primary,
    theme_text_secondary,
)


async def backtest_page() -> None:
    """Render the backtest results page."""
    with page_layout("Backtest Results", "bar_chart"):
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
                ui.label("Could not load experiment details.").style(f"color: {color_error()};")
                return

            strategy = str(exp.get("strategy_name", "-"))
            start_date = str(exp.get("start_date", "-"))[:10]
            end_date = str(exp.get("end_date", "-"))[:10]

            # Load trades early for enriched metrics
            raw_trades_df = get_experiment_trades(exp_id).with_row_index("trade_row_id")
            trades_df = prepare_trades_df(raw_trades_df, strategy_name=strategy)

            # Enrich experiment with computed metrics (Total P/L, Calmar, CAGR, etc.)
            enriched = enrich_experiment_metrics(exp, trades_df)

            # ── Header: strategy badge + period + run_id ──────────────────
            with ui.row().classes("w-full justify-between items-center mb-3"):
                with ui.row().classes("items-center gap-3"):
                    ui.html(strat_badge(strategy))
                    n_syms = enriched.get("n_symbols", 0)
                    ui.label(f"{start_date} → {end_date}  ·  {n_syms} symbols").classes(
                        "text-sm mono-font"
                    ).style(f"color: {theme_text_secondary()};")
                    ui.label(f"run_id: {exp_id}").classes("text-xs mono-font").style(
                        f"color: {theme_text_muted()};"
                    )

                    async def _copy_run_id(_rid: str = exp_id) -> None:
                        await ui.run_javascript(f"navigator.clipboard.writeText({_rid!a})")
                        ui.notify("Run ID copied", type="positive", timeout=900)

                    (
                        ui.button(icon="content_copy", on_click=_copy_run_id)
                        .props("flat dense round size=sm aria-label='Copy run ID'")
                        .tooltip("Copy run ID")
                    )
                with ui.row().classes("items-center gap-2"):
                    status = str(exp.get("status", "-")).upper()
                    status_color = color_success() if status == "COMPLETED" else color_warning()
                    ui.label(status).classes("text-xs font-bold").style(f"color: {status_color};")

            # ── Param header strip — compact inline pills ─────────────────
            try:
                display_params = json.loads(exp.get("params_json") or "{}")
            except TypeError, ValueError:
                display_params = {}
            if display_params:
                key_params = ["gap_threshold", "universe_size", "holding_days", "atr_period"]
                with ui.row().classes("w-full gap-2 mb-3 flex-wrap"):
                    for pk in key_params:
                        pv = display_params.get(pk)
                        if pv is not None:
                            ui.html(
                                f'<span style="background:{hex_to_rgba(color_info(), 0.12)};'
                                f"color:{color_info()};padding:2px 8px;border-radius:3px;"
                                f"font-size:0.7rem;font-family:monospace;"
                                f'border:1px solid {hex_to_rgba(color_info(), 0.25)}">'
                                f"{pk}: {pv}</span>"
                            )

            # ── Primary KPIs (4 big cards — compact dashboard style) ──────
            win_rate = float(enriched.get("win_rate_pct") or 0)
            total_pnl = float(enriched.get("total_pnl") or 0)
            calmar = float(enriched.get("calmar_ratio") or 0)
            max_dd = abs(float(enriched.get("max_drawdown_pct") or 0))

            kpi_grid(
                [
                    dict(
                        title="Win Rate",
                        value=f"{win_rate:.1f}%",
                        subtitle="Winning trades / all trades",
                        icon="target",
                        color=color_success() if win_rate >= 40 else color_error(),
                    ),
                    dict(
                        title="Total P/L",
                        value=f"₹{total_pnl:,.0f}",
                        subtitle="Net rupees across saved trades",
                        icon="monetization_on",
                        color=color_success() if total_pnl >= 0 else color_error(),
                    ),
                    dict(
                        title="Calmar",
                        value=f"{calmar:.2f}",
                        subtitle="Return vs drawdown",
                        icon="speed",
                        color=color_success() if calmar >= 2.0 else color_warning(),
                    ),
                    dict(
                        title="Max Drawdown",
                        value=f"-{max_dd:.1f}%",
                        subtitle="Worst peak-to-trough loss",
                        icon="trending_down",
                        color=color_error(),
                    ),
                ],
                columns=4,
            )

            # ── Secondary metrics — mini card row (compact dashboard style) ─
            n_trades = int(enriched.get("total_trades") or 0)
            total_return = float(enriched.get("total_return_pct") or 0)
            pf = float(enriched.get("profit_factor") or 0)
            cagr = float(enriched.get("cagr_pct") or 0)
            allocated_capital = float(enriched.get("allocated_capital") or 1_000_000)

            def _mini_card(title: str, value: str, color: str) -> None:
                with (
                    ui.column()
                    .classes("items-center px-5 py-3")
                    .style(
                        f"background:{theme_surface()};"
                        f"border:1px solid {theme_surface_border()};"
                        f"border-top:3px solid {color};"
                        f"border-radius:6px;min-width:100px;"
                    )
                ):
                    ui.label(value).classes("text-lg font-bold mono-font tabular-nums").style(
                        f"color: {color};"
                    )
                    ui.label(title).classes("text-xs uppercase tracking-wide mt-1").style(
                        f"color: {theme_text_secondary()};"
                    )

            with ui.row().classes("w-full gap-3 mb-4 flex-wrap"):
                _mini_card("Trades", f"{n_trades:,}", color_info())
                _mini_card("Symbols", str(n_syms), color_info())
                _mini_card(
                    "Return",
                    f"{total_return:.1f}%",
                    color_success() if total_return > 0 else color_error(),
                )
                _mini_card(
                    "PF",
                    f"{pf:.2f}",
                    color_success() if pf >= 1.5 else color_warning(),
                )
                _mini_card(
                    "CAGR",
                    f"{cagr:.1f}%",
                    color_success() if cagr >= 0 else color_error(),
                )
                _mini_card("Capital", f"₹{allocated_capital:,.0f}", color_info())

            # ── Full parameters (expandable, grouped) ─────────────────────
            with ui.expansion("Run Parameters", icon="tune", value=False).classes("w-full"):
                ui.label("Stored run parameters for this experiment.").classes("text-sm").style(
                    f"color: {color_gray()};"
                )
                param_items = get_experiment_param_items(exp)
                if not param_items:
                    ui.label("No stored parameters found.").classes("text-sm").style(
                        f"color: {color_gray()};"
                    )
                else:
                    with ui.column().classes("w-full gap-1 mt-2"):
                        for key, value in param_items:
                            with ui.row().classes("w-full items-start justify-between gap-4"):
                                ui.label(key).classes("text-xs font-mono").style(
                                    f"color: {color_info()}; min-width: 220px;"
                                )
                                ui.label(value).classes(
                                    "text-xs font-mono text-right break-all"
                                ).style(f"color: {theme_text_secondary()};")

            divider()

            execution_diagnostics_df = get_experiment_execution_diagnostics(exp_id)
            raw_trade_lookup = {
                int(row["trade_row_id"]): row for row in raw_trades_df.iter_rows(named=True)
            }
            try:
                exp_params = json.loads(exp.get("params_json") or "{}")
            except TypeError, ValueError:
                exp_params = {}

            def _is_missing(value: object) -> bool:
                return value is None or (isinstance(value, float) and np.isnan(value))

            def _coerce_date(value: object) -> dt_date | None:
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, dt_date):
                    return value
                if isinstance(value, str):
                    try:
                        return dt_date.fromisoformat(value[:10])
                    except ValueError:
                        return None
                return None

            def _safe_float(value):
                try:
                    if _is_missing(value):
                        return None
                    if isinstance(value, str):
                        return float(value)
                    return float(value)
                except TypeError, ValueError:
                    return None

            def _safe_value(value):
                if _is_missing(value):
                    return "—"
                if isinstance(value, bool):
                    return "Yes" if value else "No"
                if isinstance(value, datetime):
                    return value.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(value, dt_date):
                    return value.strftime("%Y-%m-%d")
                if isinstance(value, dt_time):
                    return value.strftime("%H:%M:%S")
                if isinstance(value, (int, float)):
                    if isinstance(value, float):
                        return f"{value:.4f}"
                    return f"{value:,d}"
                if isinstance(value, (dict, list, tuple)):
                    try:
                        return json.dumps(value, indent=2)
                    except TypeError:
                        return str(value)
                return str(value)

            def _json_default(value):
                if isinstance(value, datetime):
                    return value.isoformat()
                if isinstance(value, dt_date):
                    return value.isoformat()
                if isinstance(value, dt_time):
                    return value.isoformat()
                if isinstance(value, (np.integer, np.floating)):
                    return value.item()
                if isinstance(value, (np.bool_, bool)):
                    return bool(value)
                return str(value)

            def _json_dump_safe(value) -> str:
                return json.dumps(value, indent=2, default=_json_default)

            def _render_kv_table(pairs: list[tuple[str, object]], *, mono: bool = False) -> None:
                with ui.column().classes(f"w-full {SPACE_GROUP_TIGHT}"):
                    for label, value in pairs:
                        with ui.row().classes(f"w-full justify-between {SPACE_GRID_DEFAULT}"):
                            ui.label(label).classes("text-sm").style(
                                f"color:{theme_text_secondary()};"
                            )
                            label_class = "mono-font text-right text-sm" if mono else "text-sm"
                            ui.label(_safe_value(value)).classes(label_class).style(
                                f"color:{theme_text_primary()};"
                            )

            def _format_trade_details_dict(row: dict[str, object]) -> dict:
                return {
                    "symbol": row.get("symbol"),
                    "entry_date": row.get("entry_date"),
                    "entry_time": row.get("entry_time"),
                    "exit_date": row.get("exit_date"),
                    "exit_time": row.get("exit_time"),
                    "qty": row.get("qty"),
                    "entry_price": _safe_float(row.get("entry_price")),
                    "exit_price": _safe_float(row.get("exit_price")),
                    "initial_stop": _safe_float(row.get("initial_stop")),
                    "position_value": _safe_float(row.get("position_value")),
                    "pnl_pct": _safe_float(row.get("pnl_pct")),
                    "pnl_r": _safe_float(row.get("pnl_r")),
                    "net_pnl": _safe_float(row.get("net_pnl")),
                    "total_costs": _safe_float(row.get("total_costs")),
                    "gap_pct": _safe_float(row.get("gap_pct")),
                    "exit_reason": row.get("exit_reason"),
                    "holding_days": _safe_float(row.get("holding_days")),
                    "filters_passed": row.get("filters_passed"),
                    "year": row.get("year"),
                }

            def _lookup_execution_diagnostic(row: dict[str, object]) -> dict | None:
                if execution_diagnostics_df.is_empty():
                    return None
                target_symbol = row.get("symbol")
                target_entry_date = _coerce_date(row.get("entry_date"))
                if target_symbol is None or target_entry_date is None:
                    return None

                candidates = execution_diagnostics_df.filter(
                    (pl.col("symbol") == target_symbol)
                    & (
                        pl.col("signal_date").cast(pl.Date, strict=False)
                        == pl.lit(target_entry_date)
                    )
                )
                entry_time = str(row.get("entry_time") or "")
                if not candidates.is_empty() and entry_time:
                    normalized_time = entry_time[:5]
                    timed_candidates = candidates.filter(
                        pl.col("entry_time").cast(pl.Utf8, strict=False).str.slice(0, 5)
                        == normalized_time
                    )
                    if not timed_candidates.is_empty():
                        candidates = timed_candidates
                if candidates.is_empty():
                    return None
                return candidates.to_dicts()[0]

            def _lookup_raw_trade_row(row_payload: dict) -> dict | None:
                trade_row_id = row_payload.get("trade_row_id")
                if trade_row_id is not None:
                    try:
                        mapped = raw_trade_lookup.get(int(trade_row_id))
                        if mapped is not None:
                            return mapped
                    except TypeError, ValueError:
                        pass

                symbol = row_payload.get("symbol")
                entry_time = str(row_payload.get("entry_time") or "")
                target_date = _coerce_date(row_payload.get("entry_date"))
                if symbol and target_date is not None:
                    candidates = raw_trades_df.filter(
                        (pl.col("symbol") == symbol)
                        & (pl.col("entry_date").cast(pl.Date, strict=False) == pl.lit(target_date))
                    )
                    if not candidates.is_empty() and entry_time:
                        narrowed = candidates.filter(
                            pl.col("entry_time").cast(pl.Utf8, strict=False).str.slice(0, 5)
                            == entry_time[:5]
                        )
                        if not narrowed.is_empty():
                            candidates = narrowed
                    if not candidates.is_empty():
                        return candidates.to_dicts()[0]

                return None

            def _open_trade_details_from_payload(row_payload: dict) -> None:
                matched = _lookup_raw_trade_row(row_payload)
                if matched is not None:
                    _open_trade_details(matched)

            def _first_trade_row_id_by_symbol(symbol: str | None) -> int | None:
                if not symbol or raw_trades_df.is_empty():
                    return None
                subset = raw_trades_df.filter(pl.col("symbol") == symbol)
                if subset.is_empty():
                    return None
                return int(subset.get_column("trade_row_id")[0])

            def _first_trade_row_id_by_exit_reason(reason: str | None) -> int | None:
                if not reason or raw_trades_df.is_empty():
                    return None
                subset = raw_trades_df.filter(pl.col("exit_reason") == reason)
                if subset.is_empty():
                    return None
                return int(subset.get_column("trade_row_id")[0])

            def _open_trade_details(trade_row: dict[str, object]) -> None:
                details = _format_trade_details_dict(trade_row)
                diag = _lookup_execution_diagnostic(trade_row)

                def _safe_fmt(value, suffix: str = "") -> str:
                    if _is_missing(value):
                        return "—"
                    if isinstance(value, (float, int)):
                        if suffix:
                            return f"{value:.4f}{suffix}"
                        return f"{value:.4f}"
                    return str(value)

                diag_filters: dict = {}
                diag_components: dict = {}
                if diag:
                    raw_filters = diag.get("filters_json")
                    raw_components = diag.get("selection_components_json")
                    if isinstance(raw_filters, str) and raw_filters:
                        try:
                            diag_filters = json.loads(raw_filters)
                        except json.JSONDecodeError:
                            diag_filters = {"raw": raw_filters}
                    if isinstance(raw_components, str) and raw_components:
                        try:
                            diag_components = json.loads(raw_components)
                        except json.JSONDecodeError:
                            diag_components = {"raw": raw_components}

                with ui.dialog() as dialog:
                    dialog_id = f"trade-detail-dialog-{details['symbol']}".replace("/", "_")
                    with (
                        ui.card()
                        .classes("w-full")
                        .style("padding: 20px; width:min(96vw, 1100px); max-width:none;")
                        .props(
                            f'role="dialog" aria-label="Trade details for {details["symbol"]}" aria-modal="true" id="{dialog_id}"'
                        )
                    ):
                        with ui.row().classes("items-center justify-between w-full"):
                            ui.label(f"Trade Detail · {details['symbol']}").classes(
                                "text-lg font-semibold"
                            ).style(f"color: {theme_text_primary()};")
                            ui.button("Close", icon="close", on_click=dialog.close).props(
                                "flat dense"
                            ).props('aria-label="Close trade details dialog"')

                        # Accessibility: ESC key handler for dialog (A11Y-014)
                        ui.run_javascript(f'''
                            (function() {{
                                const dialogId = "{dialog_id}";
                                const handleEscape = (e) => {{
                                    if (e.key === 'Escape') {{
                                        // More resilient selector: try multiple approaches
                                        const dialog = document.getElementById(dialogId);
                                        if (!dialog) return;
                                        const closeBtn = dialog.querySelector('[aria-label*="Close"]')
                                            || dialog.querySelector('[aria-label*="close"]')
                                            || dialog.querySelector('.q-card .q-btn:last-child')
                                            || dialog.querySelector('button');
                                        if (closeBtn) closeBtn.click();
                                    }}
                                }};
                                document.addEventListener('keydown', handleEscape);
                                setTimeout(() => {{
                                    const observer = new MutationObserver((mutations) => {{
                                        mutations.forEach((mutation) => {{
                                            if (mutation.removedNodes) {{
                                                document.removeEventListener('keydown', handleEscape);
                                                observer.disconnect();
                                            }}
                                        }});
                                    }});
                                    observer.observe(document.getElementById(dialogId), {{ childList: true }});
                                }}, 100);
                            }})();
                        ''')

                        entry_date_value = _coerce_date(details.get("entry_date"))
                        entry_date = (
                            entry_date_value.strftime("%Y-%m-%d") if entry_date_value else "—"
                        )
                        exit_date_value = _coerce_date(details.get("exit_date"))
                        exit_date = (
                            exit_date_value.strftime("%Y-%m-%d") if exit_date_value else "open"
                        )

                        tabs = ui.tabs().classes("w-full mt-4")
                        with tabs:
                            tab_overview = ui.tab("Overview")
                            tab_execution = ui.tab("Execution")
                            tab_ranking = ui.tab("Ranking")
                            tab_filters = ui.tab("Filters")
                            tab_raw = ui.tab("JSON")

                        with ui.tab_panels(tabs, value=tab_overview).classes("w-full"):
                            with ui.tab_panel(tab_overview):
                                with ui.row().classes(f"w-full {SPACE_GRID_DEFAULT}"):
                                    with ui.column().classes("flex-1"):
                                        ui.label("Trade Snapshot").classes(
                                            "text-sm font-semibold"
                                        ).style(f"color: {color_info()};")
                                        _render_kv_table(
                                            [
                                                ("Symbol", details["symbol"]),
                                                ("Qty", details["qty"] or "—"),
                                                ("Entry Date", entry_date),
                                                ("Entry Time", details["entry_time"] or "—"),
                                                ("Exit Date", exit_date),
                                                ("Exit Time", details["exit_time"] or "—"),
                                                (
                                                    "Holding Days",
                                                    f"{int(details['holding_days']) if details['holding_days'] is not None else '—'}d",
                                                ),
                                                ("Strategy", exp.get("strategy_name", "—")),
                                            ]
                                        )
                                    with ui.column().classes("flex-1"):
                                        ui.label("Trade Performance").classes(
                                            "text-sm font-semibold"
                                        ).style(f"color: {color_success()};")
                                        _render_kv_table(
                                            [
                                                ("Entry Price", _safe_fmt(details["entry_price"])),
                                                ("Exit Price", _safe_fmt(details["exit_price"])),
                                                (
                                                    "Initial Stop",
                                                    _safe_fmt(details["initial_stop"]),
                                                ),
                                                (
                                                    "Position Value",
                                                    _safe_fmt(details["position_value"]),
                                                ),
                                                (
                                                    "P&L %",
                                                    f"{_safe_fmt(details['pnl_pct'])}%"
                                                    if details["pnl_pct"] is not None
                                                    else "—",
                                                ),
                                                (
                                                    "Net P&L",
                                                    _safe_fmt(details["net_pnl"]),
                                                ),
                                                (
                                                    "Costs",
                                                    _safe_fmt(details["total_costs"]),
                                                ),
                                                (
                                                    "R Multiple",
                                                    f"{_safe_fmt(details['pnl_r'])}R"
                                                    if details["pnl_r"] is not None
                                                    else "—",
                                                ),
                                                (
                                                    "Gap %",
                                                    f"{_safe_fmt(details['gap_pct'])}%"
                                                    if details["gap_pct"] is not None
                                                    else "—",
                                                ),
                                                ("Exit Reason", details["exit_reason"] or "—"),
                                                ("Filters Passed", details["filters_passed"] or 0),
                                            ],
                                            mono=True,
                                        )

                                    with ui.column().classes("flex-1"):
                                        ui.label("Run Context").classes(
                                            "text-sm font-semibold"
                                        ).style(f"color: {color_warning()};")
                                        _render_kv_table(
                                            [
                                                (
                                                    "Window",
                                                    f"{exp.get('start_year', '-')}-{exp.get('end_year', '-')}",
                                                ),
                                                ("Status", str(exp.get("status", "—")).upper()),
                                                (
                                                    "Total Return",
                                                    f"{_safe_float(exp.get('total_return_pct')):.1f}%"
                                                    if exp.get("total_return_pct") is not None
                                                    else "—",
                                                ),
                                                (
                                                    "Annualized",
                                                    f"{_safe_float(exp.get('annualized_return_pct')):.1f}%"
                                                    if exp.get("annualized_return_pct") is not None
                                                    else "—",
                                                ),
                                            ]
                                        )

                            with ui.tab_panel(tab_execution):
                                ui.label("Execution Context").classes(
                                    "text-sm font-semibold"
                                ).style(f"color: {color_info()};")
                                if not diag:
                                    ui.label("No execution diagnostic row for this trade.").style(
                                        f"color: {theme_text_secondary()};"
                                    )
                                else:
                                    _render_kv_table(
                                        [
                                            ("Signal Status", diag.get("status", "—")),
                                            ("Rejection/Decision", diag.get("reason", "—")),
                                            (
                                                "Signal Date",
                                                (
                                                    signal_dt.strftime("%Y-%m-%d")
                                                    if (
                                                        signal_dt := _coerce_date(
                                                            diag.get("signal_date")
                                                        )
                                                    )
                                                    else "—"
                                                ),
                                            ),
                                            ("Signal Time", str(diag.get("entry_time") or "—")),
                                            (
                                                "Signal Entry Price",
                                                _safe_float(diag.get("entry_price")),
                                            ),
                                            ("Initial Stop", _safe_float(diag.get("initial_stop"))),
                                            (
                                                "Hold Quality Passed",
                                                diag.get("hold_quality_passed"),
                                            ),
                                            (
                                                "Executed Exit Reason",
                                                diag.get("executed_exit_reason") or "—",
                                            ),
                                            ("Diagnostic P&L", _safe_float(diag.get("pnl_pct"))),
                                        ],
                                        mono=True,
                                    )

                            with ui.tab_panel(tab_ranking):
                                ui.label("Selection / Ranking").classes(
                                    "text-sm font-semibold"
                                ).style(f"color: {color_info()};")
                                if not diag:
                                    ui.label(
                                        "No ranking diagnostics available for this trade."
                                    ).style(f"color: {theme_text_secondary()};")
                                else:
                                    _render_kv_table(
                                        [
                                            (
                                                "Selection Score",
                                                _safe_float(diag.get("selection_score")),
                                            ),
                                            ("Selection Rank", diag.get("selection_rank", "—")),
                                        ],
                                        mono=True,
                                    )
                                    if diag_components:
                                        ui.separator().classes("my-3")
                                        ui.label("Selection Components").classes("text-xs").style(
                                            f"color: {theme_text_secondary()};"
                                        )
                                        ui.code(
                                            _json_dump_safe(diag_components), language="json"
                                        ).classes("text-xs")

                            with ui.tab_panel(tab_filters):
                                ui.label("Filter Snapshot").classes("text-sm font-semibold").style(
                                    f"color: {color_info()};"
                                )
                                if diag_filters:
                                    for k, v in sorted(diag_filters.items()):
                                        _render_kv_table([(str(k), v)])
                                else:
                                    ui.label("No filter snapshot available for this trade.").style(
                                        f"color: {theme_text_secondary()};"
                                    )

                            with ui.tab_panel(tab_raw):
                                ui.label("Strategy Parameters").classes(
                                    "text-sm font-semibold"
                                ).style(f"color: {color_warning()};")
                                if exp_params:
                                    ui.code(_json_dump_safe(exp_params), language="json").classes(
                                        "text-xs"
                                    )
                                else:
                                    ui.label("No strategy params recorded.").style(
                                        f"color: {theme_text_secondary()};"
                                    )
                                ui.separator().classes("my-3")
                                ui.label("Trade Row (Raw)").classes("text-sm font-semibold").style(
                                    f"color: {theme_text_secondary()};"
                                )
                                ui.code(_json_dump_safe(details), language="json").classes(
                                    "text-xs"
                                )
                                if diag:
                                    ui.separator().classes("my-3")
                                    ui.label("Diagnostic Row (Raw)").classes(
                                        "text-sm font-semibold"
                                    ).style(f"color: {theme_text_secondary()};")
                                    ui.code(_json_dump_safe(diag), language="json").classes(
                                        "text-xs"
                                    )
                dialog.open()

            if trades_df.is_empty():
                divider()
                empty_state(
                    "No trade data available",
                    "This experiment doesn't have any trades.",
                    icon="receipt_long",
                )
                return

            divider()

            with ui.row().classes(f"{SPACE_LG} {SPACE_SM}"):
                export_menu(trades_df, f"{exp_id}_all_trades", "Export Trades")

            divider()

            ui.label("Trade Analytics").classes(f"text-xl font-semibold {SPACE_XL}").style(
                f"color: {theme_text_primary()};"
            )

            # --- Yearly data (for Yearly tab) ---
            yearly_df = get_experiment_yearly_metrics(exp_id)

            # --- Shared trade table helpers (used by All Trades and Winners/Losers tabs) ---
            trade_cols = [
                "entry_date",
                "symbol",
                "entry_time",
                "qty",
                "entry_price",
                "exit_price",
                "position_value",
                "net_pnl",
                "total_costs",
                "pnl_pct",
                "pnl_r",
                "exit_reason",
                "holding_days",
                "gap_pct",
            ]
            avail_cols = [c for c in trade_cols if c in trades_df.columns]

            _numeric_sort_cols = frozenset(
                {
                    "pnl_pct",
                    "pnl_r",
                    "entry_price",
                    "exit_price",
                    "holding_days",
                    "position_value",
                    "net_pnl",
                    "gross_pnl",
                    "total_costs",
                    "gap_pct",
                    "initial_stop",
                    "qty",
                }
            )

            def _format_trade_val(val, col):
                if _is_missing(val):
                    return "-"
                if col in ("pnl_pct", "gap_pct"):
                    return f"{val:.2f}%"
                if col == "pnl_r":
                    return f"{val:.2f}R"
                if col in ("position_value", "net_pnl", "total_costs", "initial_stop"):
                    return f"{val:,.2f}"
                if "price" in col:
                    return f"{val:.2f}"
                if col == "qty":
                    return str(int(val))
                if col == "holding_days":
                    return f"{int(val)}d"
                return str(val)

            def _trade_rows(df_slice: pl.DataFrame):
                return [
                    {
                        "trade_row_id": int(row["trade_row_id"]),
                        **{
                            col: (
                                row.get(col)
                                if col in _numeric_sort_cols
                                else _format_trade_val(row.get(col), col)
                            )
                            for col in avail_cols
                        },
                    }
                    for row in df_slice.to_dicts()
                ]

            _short_labels = {
                "entry_date": "Date",
                "entry_time": "In",
                "exit_time": "Out",
                "symbol": "Symbol",
                "qty": "Qty",
                "entry_price": "Entry",
                "exit_price": "Exit",
                "position_value": "Position",
                "net_pnl": "Net P&L",
                "total_costs": "Costs",
                "pnl_pct": "P&L %",
                "pnl_r": "R",
                "exit_reason": "Reason",
                "holding_days": "Days",
                "gap_pct": "Gap",
            }

            def _col_def(col: str) -> dict:
                base = {
                    "name": col,
                    "label": _short_labels.get(col, col.replace("_", " ").title()),
                    "field": col,
                }
                if col in ("pnl_pct", "gap_pct"):
                    base[":format"] = "val => val == null ? '-' : val.toFixed(2) + '%'"
                elif col == "pnl_r":
                    base[":format"] = "val => val == null ? '-' : val.toFixed(2) + 'R'"
                elif col in ("position_value", "net_pnl", "total_costs"):
                    base[":format"] = (
                        "val => val == null ? '-' : "
                        "val.toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})"
                    )
                elif col in ("entry_price", "exit_price", "initial_stop"):
                    base[":format"] = "val => val == null ? '-' : val.toFixed(2)"
                elif col == "qty":
                    base[":format"] = "val => val == null ? '-' : Math.round(val)"
                elif col == "holding_days":
                    base[":format"] = "val => val == null ? '-' : Math.round(val) + 'd'"
                return base

            table_columns = [_col_def(col) for col in avail_cols]

            # Display-sorted copy: latest trades first for table views
            _sort_cols = [c for c in ("entry_date", "entry_time") if c in trades_df.columns]
            trades_display_df = (
                trades_df.sort(_sort_cols, descending=True) if _sort_cols else trades_df
            )

            # ── 5-Tab analytics layout (grouped by workflow stage) ────────
            _months = [
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
            ]

            tabs = ui.tabs().classes("w-full")
            with tabs:
                tab_trades = ui.tab("trades", label="Trades")
                tab_top = ui.tab("top_trades", label="Top Trades")
                tab_charts = ui.tab("charts", label="Charts")
                tab_analysis = ui.tab("analysis", label="Analysis")
                tab_audit = ui.tab("audit", label="Audit")

            with ui.tab_panels(tabs, value=tab_trades).classes("w-full bg-transparent pt-4"):
                # ── Tab: Trades ──────────────────────────────────────────
                with ui.tab_panel(tab_trades):
                    with ui.expansion("All Trades", icon="table_chart", value=True).classes(
                        "w-full"
                    ):
                        with ui.row().classes(f"w-full {SPACE_GROUP_TIGHT} mb-2"):
                            n_total = trades_df.height
                            n_winners = int(
                                trades_df.filter(pl.col("pnl_pct") > 0).height
                                if "pnl_pct" in trades_df.columns
                                else 0
                            )
                            ui.label(f"{n_total:,} trades total").classes("text-sm").style(
                                f"color: {theme_text_secondary()};"
                            )
                            ui.label(f"{n_winners:,} winners").classes("text-sm").style(
                                f"color: {color_success()};"
                            )
                            ui.label(f"{n_total - n_winners:,} losers").classes("text-sm").style(
                                f"color: {color_error()};"
                            )
                        ui.label("Click any trade row to inspect details.").classes(
                            "text-xs"
                        ).style(f"color: {theme_text_muted()};")
                        trade_table_with_filters(
                            trades_df=trades_display_df,
                            columns=table_columns,
                            rows=_trade_rows(trades_display_df),
                            page_size=50,
                            row_key="trade_row_id",
                            on_row_click=_open_trade_details_from_payload,
                        )

                    with ui.expansion("Daily Summary", icon="calendar_view_day").classes("w-full"):
                        if "entry_date" in trades_df.columns and "pnl_pct" in trades_df.columns:
                            daily = (
                                trades_df.with_columns(
                                    (pl.col("pnl_pct") > 0).alias("_is_win"),
                                    pl.col("entry_date")
                                    .cast(pl.Date, strict=False)
                                    .alias("_entry_date"),
                                )
                                .drop_nulls("_entry_date")
                                .group_by("_entry_date")
                                .agg(
                                    pl.len().alias("trades"),
                                    pl.col("pnl_pct").sum().round(2).alias("day_pnl"),
                                    (
                                        pl.col("net_pnl").sum().round(2)
                                        if "net_pnl" in trades_df.columns
                                        else pl.lit(None).alias("day_net_pnl")
                                    ).alias("day_net_pnl"),
                                    pl.col("_is_win").sum().alias("wins"),
                                    (pl.col("_is_win").sum() * 100.0 / pl.len())
                                    .round(1)
                                    .alias("win_rate"),
                                )
                                .sort("_entry_date", descending=True)
                            )
                            daily_rows = [
                                {
                                    "date": str(row["_entry_date"])[:10],
                                    "trades": int(row["trades"]),
                                    "wins": int(row["wins"]),
                                    "win_rate": float(row["win_rate"]),
                                    "day_pnl": float(row["day_pnl"]),
                                    "day_net_pnl": float(row["day_net_pnl"])
                                    if row.get("day_net_pnl") is not None
                                    else None,
                                }
                                for row in daily.iter_rows(named=True)
                            ]
                            paginated_table(
                                columns=[
                                    {
                                        "name": "date",
                                        "label": "Date",
                                        "field": "date",
                                        "align": "left",
                                    },
                                    {
                                        "name": "trades",
                                        "label": "Trades",
                                        "field": "trades",
                                        "align": "right",
                                    },
                                    {
                                        "name": "wins",
                                        "label": "Wins",
                                        "field": "wins",
                                        "align": "right",
                                    },
                                    {
                                        "name": "win_rate",
                                        "label": "Win %",
                                        "field": "win_rate",
                                        "align": "right",
                                        ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                    },
                                    {
                                        "name": "day_pnl",
                                        "label": "Day P/L %",
                                        "field": "day_pnl",
                                        "align": "right",
                                        ":format": "val => val == null ? '-' : val.toFixed(2) + '%'",
                                    },
                                    {
                                        "name": "day_net_pnl",
                                        "label": "Day P/L",
                                        "field": "day_net_pnl",
                                        "align": "right",
                                        ":format": (
                                            "val => val == null ? '-' : "
                                            "'₹' + val.toLocaleString('en-IN', "
                                            "{minimumFractionDigits: 0, maximumFractionDigits: 0})"
                                        ),
                                    },
                                ],
                                rows=daily_rows,
                                row_key="date",
                                page_size=25,
                            )

                # ── Tab: Top Trades ──────────────────────────────────────
                with ui.tab_panel(tab_top):
                    if "pnl_pct" in trades_df.columns:
                        with ui.row().classes(f"w-full {SPACE_GRID_DEFAULT}"):
                            with ui.column().classes("flex-1"):
                                ui.label("Top Winners").classes(
                                    f"text-lg font-semibold {SPACE_SM}"
                                ).style(f"color: {color_success()};")
                                top_winners = trades_df.sort(
                                    "pnl_pct", descending=True, nulls_last=True
                                ).head(min(25, trades_df.height))
                                with ui.element("div").style(
                                    "width: 100%; max-height: 500px; overflow-x: auto;"
                                ):
                                    paginated_table(
                                        columns=table_columns,
                                        rows=_trade_rows(top_winners),
                                        page_size=25,
                                        row_key="trade_row_id",
                                        on_row_click=_open_trade_details_from_payload,
                                    )

                            with ui.column().classes("flex-1"):
                                ui.label("Top Losers").classes(
                                    f"text-lg font-semibold {SPACE_SM}"
                                ).style(f"color: {color_error()};")
                                top_losers = trades_df.sort(
                                    "pnl_pct", descending=False, nulls_last=True
                                ).head(min(25, trades_df.height))
                                with ui.element("div").style(
                                    "width: 100%; max-height: 500px; overflow-x: auto;"
                                ):
                                    paginated_table(
                                        columns=table_columns,
                                        rows=_trade_rows(top_losers),
                                        page_size=25,
                                        row_key="trade_row_id",
                                        on_row_click=_open_trade_details_from_payload,
                                    )

                # ── Tab: Charts (equity + monthly + daily heatmap) ───────
                with ui.tab_panel(tab_charts):
                    # Equity Curve
                    if "pnl_pct" in trades_df.columns and "entry_date" in trades_df.columns:
                        equity = (
                            trades_df.sort("entry_date")
                            .with_columns(
                                pl.col("pnl_pct")
                                .fill_null(0.0)
                                .cum_sum()
                                .alias("cumulative_return")
                            )
                            .with_columns(pl.col("cumulative_return").cum_max().alias("cummax"))
                            .with_columns(
                                (pl.col("cumulative_return") - pl.col("cummax")).alias("drawdown")
                            )
                        )

                        fig_eq = go.Figure()
                        fig_eq.add_trace(
                            go.Scatter(
                                x=equity.get_column("entry_date").to_list(),
                                y=equity.get_column("drawdown").to_list(),
                                fill="tozeroy",
                                fillcolor=hex_to_rgba(color_error(), 0.15),
                                line_color=color_error(),
                                name="Drawdown",
                                hovertemplate="%{x}<br>DD: %{y:.2f}%<extra></extra>",
                            )
                        )
                        fig_eq.add_trace(
                            go.Scatter(
                                x=equity.get_column("entry_date").to_list(),
                                y=equity.get_column("cumulative_return").to_list(),
                                mode="lines",
                                name="Cumulative Return %",
                                line=dict(color=color_primary(), width=2.5),
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

                    divider()

                    # Monthly P/L Heatmap
                    if "entry_date" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        monthly_returns = (
                            trades_df.with_columns(
                                pl.col("entry_date").cast(pl.Date, strict=False).alias("entry_date")
                            )
                            .drop_nulls("entry_date")
                            .with_columns(
                                pl.col("entry_date").dt.year().alias("year"),
                                pl.col("entry_date").dt.month().alias("month"),
                            )
                            .group_by(["year", "month"])
                            .agg(pl.col("pnl_pct").sum().alias("monthly_return"))
                            .sort(["year", "month"])
                        )
                        years = monthly_returns.get_column("year").unique().sort().to_list()
                        monthly_lookup = {
                            (row["year"], row["month"]): row["monthly_return"]
                            for row in monthly_returns.to_dicts()
                        }
                        z_data = [
                            [monthly_lookup.get((y, m), None) for m in range(1, 13)] for y in years
                        ]
                        pnl_max = max(abs(v) for row in z_data for v in row if v is not None) or 1

                        fig_monthly = go.Figure(
                            go.Heatmap(
                                z=z_data,
                                x=_months,
                                y=[str(y) for y in years],
                                colorscale=[
                                    [0, color_error()],
                                    [0.5, theme_surface()],
                                    [1, color_success()],
                                ],
                                zmid=0,
                                zmin=-pnl_max,
                                zmax=pnl_max,
                                text=[
                                    [f"{v:.1f}%" if v is not None else "" for v in row]
                                    for row in z_data
                                ],
                                texttemplate="%{text}",
                                textfont={"size": 9},
                                hovertemplate="Month: %{x}<br>Year: %{y}<br>P/L: %{z:.1f}%<extra></extra>",
                                colorbar=dict(title="Return %"),
                            )
                        )
                        fig_monthly.update_layout(
                            title="Monthly P/L Heatmap",
                            height=max(300, len(years) * 38 + 100),
                        )
                        apply_chart_theme(fig_monthly)
                        ui.plotly(fig_monthly).classes("w-full")

                    divider()

                    # Daily P/L Heatmap (calendar-style: x=day, y=year-month)
                    if "entry_date" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        daily_hm = (
                            trades_df.with_columns(
                                pl.col("entry_date").cast(pl.Date, strict=False).alias("_edate")
                            )
                            .drop_nulls("_edate")
                            .group_by("_edate")
                            .agg(pl.col("pnl_pct").sum().round(2).alias("pnl"))
                            .sort("_edate")
                        )
                        if not daily_hm.is_empty():
                            daily_with_cal = daily_hm.with_columns(
                                pl.col("_edate").dt.year().alias("year"),
                                pl.col("_edate").dt.month().alias("month"),
                                pl.col("_edate").dt.day().alias("day"),
                            )
                            month_groups = (
                                daily_with_cal.group_by(["year", "month"])
                                .agg(
                                    pl.col("day").alias("days"),
                                    pl.col("pnl").alias("pnls"),
                                )
                                .sort(["year", "month"])
                            )
                            y_labels = []
                            z_daily = []
                            text_daily = []
                            for row in month_groups.iter_rows(named=True):
                                label = f"{row['year']}-{row['month']:02d}"
                                y_labels.append(label)
                                day_pnl = {
                                    int(d): float(p)
                                    for d, p in zip(row["days"], row["pnls"], strict=True)
                                }
                                z_row: list[float | None] = [None] * 31
                                t_row: list[str] = [""] * 31
                                for d in range(1, 32):
                                    if d in day_pnl:
                                        z_row[d - 1] = day_pnl[d]
                                        t_row[d - 1] = f"{day_pnl[d]:.1f}%"
                                z_daily.append(z_row)
                                text_daily.append(t_row)

                            daily_max = (
                                max(abs(v) for row in z_daily for v in row if v is not None) or 1
                            )
                            fig_daily = go.Figure(
                                go.Heatmap(
                                    z=z_daily,
                                    x=list(range(1, 32)),
                                    y=y_labels,
                                    colorscale=[
                                        [0, color_error()],
                                        [0.5, theme_surface()],
                                        [1, color_success()],
                                    ],
                                    zmid=0,
                                    zmin=-daily_max,
                                    zmax=daily_max,
                                    text=text_daily,
                                    texttemplate="%{text}",
                                    textfont={"size": 8},
                                    hovertemplate="Day %{x}<br>%{y}<br>P/L: %{z:.1f}%<extra></extra>",
                                    colorbar=dict(title="Return %"),
                                    xgap=2,
                                    ygap=2,
                                )
                            )
                            fig_daily.update_layout(
                                title="Daily P/L Heatmap",
                                xaxis_title="Day of Month",
                                xaxis=dict(dtick=1, tickfont={"size": 10}),
                                height=max(350, len(y_labels) * 28 + 120),
                            )
                            apply_chart_theme(fig_daily)
                            ui.plotly(fig_daily).classes("w-full")

                # ── Tab: Analysis (exits + R-multiple + per-stock + yearly)
                with ui.tab_panel(tab_analysis):
                    # Exit Reasons
                    if "exit_reason" in trades_df.columns:
                        exit_pnl = trades_df.group_by("exit_reason").agg(
                            pl.len().alias("count"),
                            pl.col("pnl_pct").mean().alias("avg_pnl"),
                            pl.col("pnl_r").mean().alias("avg_r"),
                        )
                        with ui.row().classes(f"w-full {SPACE_GRID_DEFAULT}"):
                            with ui.column().classes("flex-1"):
                                exit_counts = trades_df.group_by("exit_reason").agg(
                                    pl.len().alias("count")
                                )
                                fig_pie = go.Figure()
                                fig_pie.add_trace(
                                    go.Pie(
                                        labels=exit_counts.get_column("exit_reason").to_list(),
                                        values=exit_counts.get_column("count").to_list(),
                                        hole=0.3,
                                    )
                                )
                                fig_pie.update_layout(title="Exit Reason Distribution")
                                apply_chart_theme(fig_pie)
                                ui.plotly(fig_pie).classes("w-full h-64")

                            with ui.column().classes("flex-1"):
                                exit_rows = [
                                    {
                                        "trade_row_id": _first_trade_row_id_by_exit_reason(
                                            row.get("exit_reason")
                                        ),
                                        "exit_reason": row["exit_reason"],
                                        "count": int(row["count"]),
                                        "avg_pnl": float(row["avg_pnl"]),
                                        "avg_r": float(row["avg_r"]),
                                    }
                                    for row in exit_pnl.to_dicts()
                                ]
                                paginated_table(
                                    columns=[
                                        {
                                            "name": "exit_reason",
                                            "label": "Reason",
                                            "field": "exit_reason",
                                        },
                                        {
                                            "name": "count",
                                            "label": "Count",
                                            "field": "count",
                                        },
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
                                    ],
                                    rows=exit_rows,
                                    row_key="trade_row_id",
                                    on_row_click=_open_trade_details_from_payload,
                                    page_size=20,
                                )

                    divider()

                    # R-Multiple Distribution
                    if "pnl_r" in trades_df.columns:
                        r_vals = trades_df.get_column("pnl_r").drop_nulls().to_numpy()
                        if len(r_vals) > 0:
                            fig_r = go.Figure()
                            fig_r.add_trace(
                                go.Histogram(
                                    x=r_vals,
                                    nbinsx=50,
                                    marker_color=color_primary(),
                                    name="Distribution",
                                    opacity=0.7,
                                )
                            )
                            mu, sigma = r_vals.mean(), r_vals.std()
                            if sigma > 0:
                                x_norm = np.linspace(r_vals.min(), r_vals.max(), 100)
                                y_norm = (1 / (sigma * np.sqrt(2 * np.pi))) * np.exp(
                                    -0.5 * ((x_norm - mu) / sigma) ** 2
                                )
                                bin_width = (r_vals.max() - r_vals.min()) / 50
                                y_norm_scaled = y_norm * len(r_vals) * bin_width
                                fig_r.add_trace(
                                    go.Scatter(
                                        x=x_norm,
                                        y=y_norm_scaled,
                                        mode="lines",
                                        name="Normal Dist",
                                        line=dict(color=color_error(), dash="dash"),
                                    )
                                )
                            fig_r.add_vline(x=0, line_dash="dash", line_color=theme_text_muted())
                            fig_r.add_vline(
                                x=mu,
                                line_dash="dot",
                                line_color=color_success(),
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
                                {
                                    "Percentile": f"P{p}",
                                    "Value": float(np.percentile(r_vals, p)),
                                }
                                for p in percentiles
                            ]
                            pct_data.extend(
                                [
                                    {
                                        "Percentile": "Mean",
                                        "Value": float(r_vals.mean()),
                                    },
                                    {
                                        "Percentile": "Min",
                                        "Value": float(r_vals.min()),
                                    },
                                    {
                                        "Percentile": "Max",
                                        "Value": float(r_vals.max()),
                                    },
                                ]
                            )
                            paginated_table(
                                columns=[
                                    {
                                        "name": "Percentile",
                                        "label": "Percentile",
                                        "field": "Percentile",
                                    },
                                    {
                                        "name": "Value",
                                        "label": "R-Multiple",
                                        "field": "Value",
                                        ":format": "val => val == null ? '-' : val.toFixed(2) + 'R'",
                                    },
                                ],
                                rows=pct_data,
                                page_size=20,
                            )

                    divider()

                    # Per-Stock Analysis
                    if "symbol" in trades_df.columns and "pnl_pct" in trades_df.columns:
                        stock_stats = (
                            trades_df.group_by("symbol")
                            .agg(
                                pl.len().alias("trades"),
                                pl.col("pnl_pct").sum().alias("total_pnl"),
                                pl.col("pnl_pct").mean().alias("avg_pnl"),
                                pl.col("pnl_r").mean().alias("avg_r"),
                                ((pl.col("pnl_pct") > 0).cast(pl.Float64).mean() * 100).alias(
                                    "win_rate"
                                ),
                                pl.col("pnl_pct").max().alias("best"),
                                pl.col("pnl_pct").min().alias("worst"),
                            )
                            .sort("total_pnl", descending=True)
                        )
                        stock_rows = [
                            {
                                "trade_row_id": _first_trade_row_id_by_symbol(row["symbol"]),
                                "symbol": row["symbol"],
                                "trades": int(row["trades"]),
                                "total_pnl": float(row["total_pnl"]),
                                "avg_pnl": float(row["avg_pnl"]),
                                "avg_r": float(row["avg_r"]),
                                "win_rate": float(row["win_rate"]),
                                "best": float(row["best"]),
                                "worst": float(row["worst"]),
                            }
                            for row in stock_stats.head(50).to_dicts()
                        ]
                        paginated_table(
                            columns=[
                                {
                                    "name": "symbol",
                                    "label": "Symbol",
                                    "field": "symbol",
                                },
                                {
                                    "name": "trades",
                                    "label": "Trades",
                                    "field": "trades",
                                },
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
                                    "name": "best",
                                    "label": "Best",
                                    "field": "best",
                                    ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                },
                                {
                                    "name": "worst",
                                    "label": "Worst",
                                    "field": "worst",
                                    ":format": "val => val == null ? '-' : val.toFixed(1) + '%'",
                                },
                            ],
                            rows=stock_rows,
                            row_key="trade_row_id",
                            on_row_click=_open_trade_details_from_payload,
                            page_size=20,
                        )

                    divider()

                    # Yearly Performance
                    if not yearly_df.is_empty():
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
                            display_df = yearly_df.select(available).rename(rename_dict)

                            def _yearly_col_format(col: str) -> str | None:
                                if "Return" in col or "Rate" in col or "DD" in col:
                                    return "val => val == null ? '-' : val.toFixed(2) + '%'"
                                if col in ["Avg R", "PF"]:
                                    return "val => val == null ? '-' : val.toFixed(2)"
                                return None

                            paginated_table(
                                columns=[
                                    {
                                        "name": col,
                                        "label": col,
                                        "field": col,
                                        **(
                                            {":format": fmt}
                                            if (fmt := _yearly_col_format(col))
                                            else {}
                                        ),
                                    }
                                    for col in display_df.columns
                                ],
                                rows=[
                                    {
                                        col: (
                                            None
                                            if row.get(col) is None
                                            or (
                                                isinstance(row.get(col), float)
                                                and np.isnan(row.get(col))
                                            )
                                            else (
                                                float(row[col])
                                                if _yearly_col_format(col) is not None
                                                else int(row[col])
                                            )
                                        )
                                        for col in display_df.columns
                                    }
                                    for row in display_df.to_dicts()
                                ],
                                page_size=20,
                            )

                        if "return_pct" in yearly_df.columns and "year" in yearly_df.columns:
                            yearly_chart_df = yearly_df.sort("year")
                            fig_yearly = go.Figure(
                                data=[
                                    go.Bar(
                                        x=yearly_chart_df.get_column("year").to_list(),
                                        y=yearly_chart_df.get_column("return_pct").to_list(),
                                        marker=dict(
                                            color=yearly_chart_df.get_column(
                                                "return_pct"
                                            ).to_list(),
                                            colorscale=[
                                                [0.0, color_error()],
                                                [0.5, color_warning()],
                                                [1.0, color_success()],
                                            ],
                                            showscale=False,
                                        ),
                                        name="Return %",
                                    )
                                ]
                            )
                            fig_yearly.update_layout(
                                title="Yearly Returns",
                                xaxis_title="Year",
                                yaxis_title="Return %",
                                showlegend=False,
                            )
                            apply_chart_theme(fig_yearly)
                            ui.plotly(fig_yearly).classes("w-full h-64")
                    else:
                        ui.label("No yearly data available for this experiment.").style(
                            f"color: {theme_text_secondary()};"
                        )

                # ── Tab: Audit (execution shape) ─────────────────────────
                with ui.tab_panel(tab_audit):
                    enriched = trades_df.with_columns(
                        pl.col("entry_date").cast(pl.Date, strict=False).alias("_trade_date"),
                        pl.col("entry_time")
                        .cast(pl.Utf8, strict=False)
                        .str.slice(0, 5)
                        .alias("_entry_hhmm"),
                        (pl.col("pnl_pct") > 0).alias("_is_win"),
                    )

                    total_days = int(
                        enriched["_trade_date"].n_unique()
                        if "_trade_date" in enriched.columns
                        else 0
                    )

                    def _mean_or_zero(series: pl.Series | None) -> float:
                        if series is None:
                            return 0.0
                        val = series.drop_nulls().mean()
                        return float(val) if val is not None else 0.0

                    avg_entry_price = _mean_or_zero(
                        enriched.get_column("entry_price")
                        if "entry_price" in enriched.columns
                        else None
                    )
                    avg_holding = _mean_or_zero(
                        enriched.get_column("holding_days")
                        if "holding_days" in enriched.columns
                        else None
                    )
                    avg_abs_gap = _mean_or_zero(
                        enriched.get_column("gap_pct").abs()
                        if "gap_pct" in enriched.columns
                        else None
                    )
                    n_winners_audit = int(enriched.filter(pl.col("_is_win")).height)
                    _r_col = enriched.get_column("pnl_r") if "pnl_r" in enriched.columns else None
                    if _r_col is not None:
                        _r_clean = _r_col.drop_nulls().filter(_r_col.drop_nulls().abs() < 100)
                        avg_r = float(_r_clean.mean()) if len(_r_clean) > 0 else 0.0
                    else:
                        avg_r = 0.0

                    kpi_grid(
                        [
                            dict(
                                title="Trading Days",
                                value=f"{total_days:,}",
                                icon="calendar_month",
                                color=color_info(),
                            ),
                            dict(
                                title="Winners",
                                value=f"{n_winners_audit:,}",
                                icon="north_east",
                                color=color_success(),
                            ),
                            dict(
                                title="Avg Entry ₹",
                                value=f"₹{avg_entry_price:,.0f}",
                                icon="account_balance_wallet",
                                color=color_primary(),
                            ),
                            dict(
                                title="Avg Holding",
                                value=f"{avg_holding:.1f}d",
                                icon="schedule",
                                color=color_warning(),
                            ),
                            dict(
                                title="Avg |Gap| %",
                                value=f"{avg_abs_gap:.3f}%",
                                icon="difference",
                                color=color_info(),
                            ),
                            dict(
                                title="Avg R",
                                value=f"{avg_r:.2f}R",
                                icon="bar_chart",
                                color=color_success() if avg_r >= 0 else color_error(),
                            ),
                        ],
                        columns=6,
                    )

                    # Entry-Time Distribution + Exit by Holding Period
                    entry_bins = (
                        enriched.drop_nulls("_entry_hhmm")
                        .group_by("_entry_hhmm")
                        .agg(
                            pl.len().alias("trades"),
                            pl.col("pnl_pct").sum().round(2).alias("pnl"),
                        )
                        .sort("_entry_hhmm")
                    )
                    exit_by_hold = (
                        enriched.with_columns(
                            pl.col("holding_days").cast(pl.Int64, strict=False).alias("_hold")
                        )
                        .group_by(["_hold", "exit_reason"])
                        .agg(pl.len().alias("count"))
                        .sort(["_hold", "exit_reason"])
                    )

                    with ui.row().classes(f"w-full gap-4 {SPACE_GRID_DEFAULT}"):
                        with ui.column().classes("flex-1"):
                            if not entry_bins.is_empty():
                                fig_entry = go.Figure(
                                    data=[
                                        go.Bar(
                                            x=entry_bins["_entry_hhmm"].to_list(),
                                            y=entry_bins["trades"].to_list(),
                                            marker_color=color_primary(),
                                            hovertemplate="Entry %{x}<br>Trades %{y}<extra></extra>",
                                        )
                                    ]
                                )
                                fig_entry.update_layout(
                                    title="Entry-Time Distribution",
                                    xaxis_title="Entry Time (HH:MM)",
                                    yaxis_title="Trades",
                                )
                                apply_chart_theme(fig_entry)
                                ui.plotly(fig_entry).classes("w-full h-72")

                        with ui.column().classes("flex-1"):
                            if not exit_by_hold.is_empty():
                                hold_days = sorted(
                                    {
                                        int(v)
                                        for v in exit_by_hold["_hold"].to_list()
                                        if v is not None
                                    }
                                )
                                reasons = sorted(
                                    {str(v) for v in exit_by_hold["exit_reason"].to_list()}
                                )
                                fig_hold = go.Figure()
                                for reason in reasons:
                                    reason_counts = {}
                                    for row in exit_by_hold.filter(
                                        pl.col("exit_reason") == reason
                                    ).iter_rows(named=True):
                                        if row["_hold"] is not None:
                                            reason_counts[int(row["_hold"])] = int(row["count"])
                                    fig_hold.add_trace(
                                        go.Bar(
                                            name=reason,
                                            x=[str(d) for d in hold_days],
                                            y=[reason_counts.get(d, 0) for d in hold_days],
                                        )
                                    )
                                fig_hold.update_layout(
                                    title="Exit Reasons by Holding Days",
                                    xaxis_title="Holding Days",
                                    yaxis_title="Trades",
                                    barmode="group",
                                    showlegend=True,
                                )
                                apply_chart_theme(fig_hold)
                                ui.plotly(fig_hold).classes("w-full h-72")

            divider()
            with ui.expansion("Run New Backtest", icon="play_arrow").classes("w-full"):
                ui.label("Configure and launch a new backtest run.").classes(SPACE_LG).style(
                    f"color: {theme_text_secondary()};"
                )
                with ui.row().classes(f"w-full {SPACE_GRID_DEFAULT}"):
                    # Accessibility: Add autocomplete attributes for better form UX
                    ui.number("Universe Size", value=500, min=50, max=2000, step=50).props(
                        'autocomplete="off" aria-label="Universe size for backtest"'
                    )
                    ui.number("Start Year", value=2015, min=2010, max=2025).props(
                        'autocomplete="off" aria-label="Start year for backtest period"'
                    )
                    ui.number("End Year", value=2025, min=2015, max=2026).props(
                        'autocomplete="off" aria-label="End year for backtest period"'
                    )

                with ui.column().classes("kpi-card mt-4"):
                    ui.label("Run this command in your terminal:").classes(
                        f"text-sm {SPACE_SM}"
                    ).style(f"color: {theme_text_secondary()};")
                    ui.label(
                        "doppler run -- uv run nseml-backtest --universe-size 2000 --start-year 2015 --end-year 2025"
                    ).classes("font-mono text-sm").style(f"color: {color_success()};")

                ui.label("After completion, refresh this page to see the new experiment.").classes(
                    "text-sm mt-2"
                ).style(f"color: {theme_text_muted()};")

        with ui.row().classes(f"kpi-card w-full items-center {SPACE_GRID_DEFAULT} {SPACE_SECTION}"):
            ui.icon("science").classes("text-xl").style(f"color: {theme_primary()};")
            ui.label("Experiment").classes("text-sm font-medium").style(
                f"color: {theme_text_secondary()};"
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
