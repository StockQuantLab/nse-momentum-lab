"""CLI entry point for paper-session management and daily paper flows."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import logging
import sys
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path
from statistics import mean, median
from typing import Any, cast
from uuid import uuid4

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db import get_market_db, get_sessionmaker
from nse_momentum_lab.db.market_db import FEAT_DAILY_QUERY_VERSION, get_backtest_db
from nse_momentum_lab.db.paper import (
    alert_session_signals,
    create_or_update_paper_session,
    delete_walk_forward_sessions_by_ids,
    flatten_open_positions,
    get_paper_session_summary,
    get_paper_session_summary_compact,
    get_walk_forward_session_cleanup_preview,
    insert_walk_forward_fold,
    list_paper_sessions,
    list_paper_sessions_compact,
    qualify_session_signals,
    reset_walk_forward_folds,
    set_paper_session_status,
    update_paper_session,
)
from nse_momentum_lab.features import (
    FEAT_2LYNCH_DERIVED_VERSION,
    FEAT_DAILY_CORE_VERSION,
    FEAT_INTRADAY_CORE_VERSION,
)

logger = logging.getLogger(__name__)
BACKTEST_DATE_KEYS = {"start_date", "end_date", "start_year", "end_year"}
WALK_FORWARD_RUNTIME_SPECS = {
    "modular": (
        {
            "logical_name": "market_day_state",
            "table_name": "feat_daily_core",
            "date_column": "trading_date",
            "expected_query_version": FEAT_DAILY_CORE_VERSION,
            "dataset_hash_source": "daily",
        },
        {
            "logical_name": "strategy_day_state",
            "table_name": "feat_2lynch_derived",
            "date_column": "trading_date",
            "expected_query_version": FEAT_2LYNCH_DERIVED_VERSION,
            "dataset_hash_source": "daily",
        },
        {
            "logical_name": "intraday_day_pack",
            "table_name": "feat_intraday_core",
            "date_column": "trading_date",
            "expected_query_version": FEAT_INTRADAY_CORE_VERSION,
            "dataset_hash_source": "five_min",
        },
    ),
    "legacy": (
        {
            "logical_name": "market_day_state",
            "table_name": "feat_daily",
            "date_column": "trading_date",
            "expected_query_version": FEAT_DAILY_QUERY_VERSION,
            "dataset_hash_source": "overall",
        },
    ),
}


@cache
def _get_backtest_params_cls() -> type[Any]:
    from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams

    return BacktestParams


@cache
def _get_backtest_runner_cls() -> type[Any]:
    from nse_momentum_lab.services.backtest.duckdb_backtest_runner import DuckDBBacktestRunner

    return DuckDBBacktestRunner


@cache
def _get_walk_forward_framework_cls() -> type[Any]:
    from nse_momentum_lab.services.backtest.walkforward import WalkForwardFramework

    return WalkForwardFramework


@cache
def _get_kite_connect_client_cls() -> type[Any]:
    from nse_momentum_lab.services.kite.client import KiteConnectClient

    return KiteConnectClient


@cache
def _get_kite_stream_symbols() -> tuple[type[Any], type[Any]]:
    from nse_momentum_lab.services.kite.stream import KiteStreamConfig, KiteStreamRunner

    return KiteStreamConfig, KiteStreamRunner


@cache
def _get_paper_runtime_symbols() -> tuple[type[Any], type[Any], Any]:
    from nse_momentum_lab.services.paper.runtime import (
        PaperRuntimePlan,
        PaperRuntimeScaffold,
        redact_credentials,
    )

    return PaperRuntimePlan, PaperRuntimeScaffold, redact_credentials


@cache
def _backtest_param_keys() -> frozenset[str]:
    return frozenset(_get_backtest_params_cls().__dataclass_fields__)


def _utc_today() -> date:
    return datetime.now(UTC).date()


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid ISO date: {value}") from exc


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected integer value") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Expected integer greater than 0")
    return parsed


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("Expected JSON object")
    return parsed


def _parse_int_csv(value: str | None) -> list[int]:
    if not value:
        return []
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _parse_symbol_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _default_session_id(prefix: str, *parts: str) -> str:
    safe = "-".join(part.strip().lower().replace(" ", "-") for part in parts if part.strip())
    return f"{prefix}-{safe}" if safe else f"{prefix}-{uuid4().hex[:8]}"


def _format_threshold_label(value: Any) -> str:
    try:
        numeric = float(value)
    except TypeError, ValueError:
        return ""
    compact = f"{numeric:.4f}".rstrip("0").rstrip(".").replace(".", "p")
    return f"thr-{compact}" if compact else ""


def _normalize_backtest_params(
    strategy_name: str,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    backtest_params_cls = _get_backtest_params_cls()
    params = asdict(backtest_params_cls(strategy=strategy_name))
    params.update(overrides or {})
    params["strategy"] = strategy_name
    return asdict(backtest_params_cls(**params))


def _build_fast_sim_backtest_params(
    args: argparse.Namespace,
    *,
    trade_date: date,
) -> dict[str, Any]:
    backtest_params_cls = _get_backtest_params_cls()
    experiment_id = getattr(args, "experiment_id", None)
    if experiment_id:
        backtest_db = get_backtest_db(read_only=True)
        experiment = backtest_db.get_experiment(str(experiment_id))
        if experiment is None:
            raise SystemExit(f"Experiment '{experiment_id}' was not found in backtest.duckdb")
        experiment_params = _parse_json(str(experiment.get("params_json") or "{}"))
        strategy_name = str(
            experiment.get("strategy_name")
            or experiment_params.get("strategy")
            or getattr(args, "strategy", "thresholdbreakout")
        )
        params = _normalize_backtest_params(strategy_name, experiment_params)
    else:
        strategy_name = getattr(args, "strategy", "thresholdbreakout")
        params = _normalize_backtest_params(
            strategy_name, _parse_json(getattr(args, "params_json", None))
        )

    params["start_date"] = trade_date.isoformat()
    params["end_date"] = trade_date.isoformat()
    params["start_year"] = trade_date.year
    params["end_year"] = trade_date.year
    universe_size = getattr(args, "universe_size", None)
    if universe_size is None:
        universe_size = params.get("universe_size", 2000)
    params["universe_size"] = int(universe_size)
    return backtest_params_cls(**params)


def _print_fast_sim_summary(exp_id: str) -> None:
    backtest_db = get_backtest_db(read_only=True)
    row = backtest_db.con.execute(
        """SELECT strategy_name, start_year, end_year, total_trades, total_return_pct,
                  max_drawdown_pct, profit_factor
           FROM bt_experiment WHERE exp_id = ?""",
        [exp_id],
    ).fetchone()
    if not row:
        print("  (no summary available — experiment not persisted)")
        return

    strategy, start_yr, end_yr, trades, total_ret, max_dd, pf = row
    print()
    print(f"  Strategy   : {strategy}  ({start_yr}-{end_yr})")
    print(f"  Trades     : {int(trades):,}")
    print(f"  Total Ret  : {float(total_ret):.1f}%")
    print(f"  Max DD     : {float(max_dd):.2f}%")
    print(f"  Prof Factor: {float(pf):.2f}")


def _load_market_trading_sessions(start_date: date, end_date: date) -> list[date]:
    market_db = get_market_db(read_only=True)
    rows = market_db.con.execute(
        """
        SELECT DISTINCT date
        FROM v_daily
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """,
        [start_date, end_date],
    ).fetchall()
    return [row[0] for row in rows if isinstance(row[0], date)]


def _snapshot_hash(snapshot: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _runtime_coverage_spec_mode(market_db: Any) -> str:
    if market_db._table_exists("feat_daily_core"):
        return "modular"
    return "legacy"


def _load_trade_dates(trade_dates: list[date]) -> list[date]:
    return [trade_date for trade_date in trade_dates if isinstance(trade_date, date)]


def _collect_table_coverage(
    market_db: Any,
    *,
    logical_name: str,
    table_name: str,
    date_column: str,
    trade_dates: list[date],
    current_hash: str | None,
    expected_query_version: str | None,
) -> dict[str, Any]:
    trade_dates = _load_trade_dates(trade_dates)
    coverage: dict[str, Any] = {
        "logical_name": logical_name,
        "table_name": table_name,
        "date_column": date_column,
        "expected_query_version": expected_query_version,
        "state": None,
        "row_count": 0,
        "min_date": None,
        "max_date": None,
        "current_dataset_hash": current_hash,
        "state_dataset_hash": None,
        "state_query_version": None,
        "missing_trade_dates": [],
        "stale_reasons": [],
    }

    if not market_db._table_exists(table_name):
        coverage["stale_reasons"].append("missing_table")
        return coverage

    row = market_db.con.execute(
        f"""
        SELECT
            COUNT(*)::BIGINT AS rows,
            MIN({date_column})::VARCHAR AS min_date,
            MAX({date_column})::VARCHAR AS max_date
        FROM {table_name}
        """
    ).fetchone()
    if row:
        coverage["row_count"] = int(row[0]) if row[0] is not None else 0
        coverage["min_date"] = row[1]
        coverage["max_date"] = row[2]

    state = market_db._get_materialization_state(table_name)
    coverage["state"] = state
    if state is None:
        coverage["stale_reasons"].append("missing_materialization_state")
    else:
        coverage["state_dataset_hash"] = str(state.get("dataset_hash") or "")
        coverage["state_query_version"] = str(state.get("query_version") or "")
        if expected_query_version and coverage["state_query_version"] != expected_query_version:
            coverage["stale_reasons"].append("query_version_mismatch")
        if current_hash and coverage["state_dataset_hash"] != current_hash:
            coverage["stale_reasons"].append("dataset_hash_mismatch")

    if coverage["row_count"] <= 0:
        coverage["stale_reasons"].append("empty_table")
        return coverage

    if not trade_dates:
        coverage["stale_reasons"].append("no_trade_dates")
        return coverage

    available = market_db.con.execute(
        f"""
        SELECT DISTINCT {date_column}::DATE
        FROM {table_name}
        WHERE {date_column} BETWEEN ? AND ?
        ORDER BY 1
        """,
        [trade_dates[0], trade_dates[-1]],
    ).fetchall()
    available_dates = {row[0] for row in available if isinstance(row[0], date)}
    missing_trade_dates = [
        trade_date for trade_date in trade_dates if trade_date not in available_dates
    ]
    coverage["missing_trade_dates"] = [trade_date.isoformat() for trade_date in missing_trade_dates]
    if missing_trade_dates:
        coverage["stale_reasons"].append("missing_trade_dates")

    return coverage


def _build_walk_forward_runtime_coverage_report(
    market_db: Any,
    trade_dates: list[date],
    *,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    trade_dates = _load_trade_dates(trade_dates)
    dataset_snapshot = market_db.get_dataset_snapshot()
    daily_snapshot = cast(dict[str, Any], dataset_snapshot.get("daily") or {})
    five_min_snapshot = cast(dict[str, Any], dataset_snapshot.get("five_min") or {})

    current_hashes = {
        "daily": _snapshot_hash(daily_snapshot) if daily_snapshot else None,
        "five_min": _snapshot_hash(five_min_snapshot) if five_min_snapshot else None,
        "overall": str(dataset_snapshot.get("dataset_hash") or ""),
    }
    runtime_mode = _runtime_coverage_spec_mode(market_db)
    specs = WALK_FORWARD_RUNTIME_SPECS[runtime_mode]

    tables: dict[str, dict[str, Any]] = {}
    missing_by_date: dict[str, set[str]] = {}
    table_max_trade_dates: dict[str, str | None] = {}

    for spec in specs:
        current_hash = current_hashes.get(str(spec["dataset_hash_source"]))
        coverage = _collect_table_coverage(
            market_db,
            logical_name=str(spec["logical_name"]),
            table_name=str(spec["table_name"]),
            date_column=str(spec["date_column"]),
            trade_dates=trade_dates,
            current_hash=current_hash,
            expected_query_version=str(spec["expected_query_version"]),
        )
        tables[str(spec["logical_name"])] = coverage
        table_max_trade_dates[str(spec["logical_name"])] = coverage.get("max_date")
        for trade_date in coverage.get("missing_trade_dates", []):
            missing_by_date.setdefault(str(trade_date), set()).add(str(spec["logical_name"]))

    missing_by_date_rows = [
        {
            "trade_date": trade_date,
            "missing_tables": sorted(missing_tables),
            "missing_count": len(missing_tables),
        }
        for trade_date, missing_tables in sorted(missing_by_date.items())
    ]

    coverage_ready = (
        all(not table["stale_reasons"] for table in tables.values()) and not missing_by_date_rows
    )
    return {
        "runtime_mode": runtime_mode,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "trade_dates": [trade_date.isoformat() for trade_date in trade_dates],
        "coverage_ready": coverage_ready,
        "tables": tables,
        "missing_by_date": missing_by_date_rows,
        "table_max_trade_dates": table_max_trade_dates,
        "dataset_snapshot": {
            "daily": daily_snapshot,
            "five_min": five_min_snapshot,
            "dataset_hash": current_hashes["overall"],
        },
    }


def _format_walk_forward_runtime_coverage_error(report: dict[str, Any]) -> str:
    trade_dates = report.get("trade_dates") or []
    missing_by_date = report.get("missing_by_date") or []
    table_max_trade_dates = report.get("table_max_trade_dates") or {}
    tables = report.get("tables") or {}
    lines = [
        (
            "Walk-forward runtime coverage is incomplete for "
            f"{report.get('start_date')} -> {report.get('end_date')}."
        ),
        f"Trading sessions in range: {len(trade_dates)}.",
    ]
    if not trade_dates:
        lines.append("No trading sessions fall inside the requested range.")
    if missing_by_date:
        lines.append("Missing coverage by trade date:")
        for entry in missing_by_date[:10]:
            missing_tables = entry.get("missing_tables") or []
            lines.append(f"- {entry.get('trade_date')}: {', '.join(missing_tables)}")
        if len(missing_by_date) > 10:
            lines.append(f"- ... and {len(missing_by_date) - 10} more date(s)")
    lines.append("Current runtime table max trade dates:")
    for logical_name in ("market_day_state", "strategy_day_state", "intraday_day_pack"):
        lines.append(f"- {logical_name}: {table_max_trade_dates.get(logical_name) or 'missing'}")
    lines.append("Current runtime table status:")
    for logical_name in ("market_day_state", "strategy_day_state", "intraday_day_pack"):
        table = tables.get(logical_name) or {}
        stale_reasons = table.get("stale_reasons") or []
        if stale_reasons:
            lines.append(
                f"- {logical_name} ({table.get('table_name')}) stale: {', '.join(stale_reasons)}"
            )
    lines.append("Recovery flow:")
    lines.append("  doppler run -- uv run nseml-kite-ingest --from <YYYY-MM-DD> --to <YYYY-MM-DD>")
    lines.append("  doppler run -- uv run nseml-build-features")
    lines.append("  doppler run -- uv run nseml-market-monitor --incremental --since <YYYY-MM-DD>")
    return "\n".join(lines)


def _validate_walk_forward_runtime_coverage(
    trade_dates: list[date],
    *,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    market_db = get_market_db(read_only=True)
    report = _build_walk_forward_runtime_coverage_report(
        market_db,
        trade_dates,
        start_date=start_date,
        end_date=end_date,
    )
    if not report.get("coverage_ready", False):
        raise SystemExit(_format_walk_forward_runtime_coverage_error(report))
    return report


def _coerce_iso_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _extract_walk_forward_metadata(session: dict[str, Any]) -> dict[str, Any]:
    strategy_params = session.get("strategy_params")
    if not isinstance(strategy_params, dict):
        return {}
    walk_forward = strategy_params.get("walk_forward")
    return walk_forward if isinstance(walk_forward, dict) else {}


def _extract_walk_forward_base_params(session: dict[str, Any]) -> dict[str, Any]:
    strategy_params = session.get("strategy_params")
    if not isinstance(strategy_params, dict):
        return {}

    walk_forward = _extract_walk_forward_metadata(session)
    base_params = walk_forward.get("base_params")
    if isinstance(base_params, dict):
        return base_params

    valid_keys = _backtest_param_keys()
    return {key: value for key, value in strategy_params.items() if key in valid_keys}


def _extract_walk_forward_test_ranges(session: dict[str, Any]) -> list[tuple[date, date]]:
    walk_forward = _extract_walk_forward_metadata(session)
    raw_ranges: list[Any] = []
    test_ranges_value = walk_forward.get("test_ranges")
    if isinstance(test_ranges_value, list):
        raw_ranges = test_ranges_value
    else:
        folds_value = walk_forward.get("folds")
        if isinstance(folds_value, list):
            raw_ranges = folds_value

    ranges: list[tuple[date, date]] = []
    for raw_range in raw_ranges:
        if not isinstance(raw_range, dict):
            continue
        start_date = _coerce_iso_date(raw_range.get("start") or raw_range.get("test_start"))
        end_date = _coerce_iso_date(raw_range.get("end") or raw_range.get("test_end"))
        if start_date is None or end_date is None:
            continue
        ranges.append((start_date, end_date))
    return ranges


def _trade_date_is_covered(
    trade_date: date | None,
    test_ranges: list[tuple[date, date]],
) -> bool:
    if trade_date is None:
        return True
    return any(start_date <= trade_date <= end_date for start_date, end_date in test_ranges)


def _extract_walk_forward_lineage(
    session: dict[str, Any],
    *,
    backtest_db: Any | None = None,
) -> dict[str, list[str]]:
    walk_forward = _extract_walk_forward_metadata(session)
    lineage = walk_forward.get("lineage")
    fold_exp_ids_value = walk_forward.get("fold_experiment_ids")
    fold_exp_ids_source: list[Any] = (
        fold_exp_ids_value if isinstance(fold_exp_ids_value, list) else []
    )
    fold_exp_ids = [str(exp_id) for exp_id in fold_exp_ids_source if str(exp_id).strip()]

    dataset_hashes_source: list[Any] = []
    code_hashes_source: list[Any] = []
    if isinstance(lineage, dict):
        dataset_hashes_value = lineage.get("dataset_hashes")
        code_hashes_value = lineage.get("code_hashes")
        if isinstance(dataset_hashes_value, list):
            dataset_hashes_source = dataset_hashes_value
        if isinstance(code_hashes_value, list):
            code_hashes_source = code_hashes_value

    dataset_hashes = {str(item) for item in dataset_hashes_source if str(item).strip()}
    code_hashes = {str(item) for item in code_hashes_source if str(item).strip()}

    if not fold_exp_ids:
        folds = walk_forward.get("folds")
        if isinstance(folds, list):
            fold_exp_ids = [
                str(fold.get("exp_id"))
                for fold in folds
                if isinstance(fold, dict) and str(fold.get("exp_id") or "").strip()
            ]

    if backtest_db is not None and fold_exp_ids and (not dataset_hashes or not code_hashes):
        for exp_id in fold_exp_ids:
            experiment = backtest_db.get_experiment(exp_id) or {}
            if experiment.get("dataset_hash"):
                dataset_hashes.add(str(experiment["dataset_hash"]))
            if experiment.get("code_hash"):
                code_hashes.add(str(experiment["code_hash"]))

    return {
        "fold_experiment_ids": sorted(dict.fromkeys(fold_exp_ids)),
        "dataset_hashes": sorted(dataset_hashes),
        "code_hashes": sorted(code_hashes),
    }


def _dedupe_nonempty_strings(values: list[str] | None) -> list[str]:
    if not values:
        return []
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


async def _build_walk_forward_cleanup_plan(
    db_session,
    backtest_db,
    *,
    wf_run_ids: list[str] | None = None,
    run_ids: list[str] | None = None,
) -> dict[str, Any]:
    requested_wf_run_ids = _dedupe_nonempty_strings(wf_run_ids)
    requested_run_ids = _dedupe_nonempty_strings(run_ids)

    postgres_sessions: list[dict[str, Any]] = []
    missing_wf_run_ids: list[str] = []
    duckdb_experiments_by_id: dict[str, dict[str, Any]] = {}
    missing_run_ids: list[str] = []

    for wf_run_id in requested_wf_run_ids:
        preview = await get_walk_forward_session_cleanup_preview(db_session, wf_run_id)
        if preview is None:
            missing_wf_run_ids.append(wf_run_id)
            continue
        session = preview["session"]
        session["fold_count"] = preview["fold_count"]
        postgres_sessions.append(
            {
                "session": session,
                "folds": preview["folds"],
                "fold_count": preview["fold_count"],
            }
        )
        for experiment in backtest_db.list_experiments_for_wf_run_id(wf_run_id):
            exp_id = str(experiment.get("exp_id") or "").strip()
            if not exp_id or exp_id in duckdb_experiments_by_id:
                continue
            summary = backtest_db.get_experiment_cleanup_summary(exp_id)
            if summary is not None:
                duckdb_experiments_by_id[exp_id] = summary

    for run_id in requested_run_ids:
        summary = backtest_db.get_experiment_cleanup_summary(run_id)
        if summary is None:
            missing_run_ids.append(run_id)
            continue
        duckdb_experiments_by_id[run_id] = summary

    duckdb_experiments = sorted(
        duckdb_experiments_by_id.values(),
        key=lambda row: (
            str(row.get("wf_run_id") or ""),
            str(row.get("exp_id") or ""),
        ),
    )
    duckdb_rows_to_delete = sum(int(row.get("total_rows") or 0) for row in duckdb_experiments)
    postgres_fold_rows = sum(len(entry.get("folds", [])) for entry in postgres_sessions)

    return {
        "requested_wf_run_ids": requested_wf_run_ids,
        "requested_run_ids": requested_run_ids,
        "missing_wf_run_ids": missing_wf_run_ids,
        "missing_run_ids": missing_run_ids,
        "postgres": {
            "sessions": postgres_sessions,
            "session_count": len(postgres_sessions),
            "fold_row_count": postgres_fold_rows,
        },
        "duckdb": {
            "experiments": duckdb_experiments,
            "run_ids_to_delete": [str(row["exp_id"]) for row in duckdb_experiments],
            "experiment_count": len(duckdb_experiments),
            "row_count": duckdb_rows_to_delete,
        },
        "summary": {
            "requested_wf_run_ids": len(requested_wf_run_ids),
            "requested_run_ids": len(requested_run_ids),
            "postgres_sessions": len(postgres_sessions),
            "postgres_fold_rows": postgres_fold_rows,
            "duckdb_experiments": len(duckdb_experiments),
            "duckdb_rows": duckdb_rows_to_delete,
        },
    }


def _validate_experiment_against_walk_forward(
    experiment: dict[str, Any],
    session: dict[str, Any],
    *,
    backtest_db: Any | None = None,
) -> str | None:
    experiment_strategy = str(experiment.get("strategy_name") or "").strip()
    session_strategy = str(session.get("strategy_name") or "").strip()
    if experiment_strategy and session_strategy and experiment_strategy != session_strategy:
        return (
            f"experiment strategy '{experiment_strategy}' does not match validated strategy "
            f"'{session_strategy}'"
        )

    validated_params = _extract_walk_forward_base_params(session)
    if not validated_params:
        return "validated base parameters are missing"

    params_json = experiment.get("params_json")
    experiment_params = (
        json.loads(params_json) if isinstance(params_json, str) and params_json else {}
    )
    comparable_keys = _backtest_param_keys() - BACKTEST_DATE_KEYS
    validated_comparable = {key: validated_params.get(key) for key in comparable_keys}
    experiment_comparable = {key: experiment_params.get(key) for key in comparable_keys}
    if experiment_comparable != validated_comparable:
        return "experiment parameters do not match the validated walk-forward configuration"

    lineage = _extract_walk_forward_lineage(session, backtest_db=backtest_db)
    dataset_hash = str(experiment.get("dataset_hash") or "").strip()
    code_hash = str(experiment.get("code_hash") or "").strip()

    if lineage["dataset_hashes"] and dataset_hash not in lineage["dataset_hashes"]:
        return "experiment dataset hash is outside the validated walk-forward lineage"
    if lineage["code_hashes"] and code_hash not in lineage["code_hashes"]:
        return "experiment code hash is outside the validated walk-forward lineage"

    return None


def _walk_forward_gate_rejection_reason(
    *,
    session: dict[str, Any],
    trade_date: date | None,
    experiment: dict[str, Any] | None,
    backtest_db: Any | None = None,
) -> str | None:
    test_ranges = _extract_walk_forward_test_ranges(session)
    if not test_ranges:
        return "validated test ranges are missing"
    if not _trade_date_is_covered(trade_date, test_ranges):
        trade_date_display = trade_date.isoformat() if trade_date is not None else "unknown date"
        return f"trade date {trade_date_display} is outside validated test coverage"
    if experiment is not None:
        experiment_reason = _validate_experiment_against_walk_forward(
            experiment,
            session,
            backtest_db=backtest_db,
        )
        if experiment_reason is not None:
            return experiment_reason
    return None


def _session_to_json(session: Any) -> dict[str, Any]:
    if isinstance(session, dict):
        return session
    if is_dataclass(session):
        return asdict(session)
    if hasattr(session, "_asdict"):
        return dict(session._asdict())
    if hasattr(session, "__dict__"):
        return {key: value for key, value in vars(session).items() if not key.startswith("_sa_")}
    raise TypeError(f"Unsupported paper-session payload type: {type(session)!r}")


async def _warn_if_session_exists(
    db_session: Any,
    session_id: str,
    *,
    command: str,
    auto_generated: bool,
) -> None:
    if not auto_generated:
        return
    try:
        existing = await get_paper_session_summary(db_session, session_id)
    except Exception:
        logger.debug("Skipping session reuse check for %s=%s", command, session_id, exc_info=True)
        return
    if existing is not None:
        logger.warning(
            "%s is reusing existing session_id=%s; pass --session-id to avoid overwriting it.",
            command,
            session_id,
        )


def _build_runtime_plan(
    args: argparse.Namespace,
    *,
    mode: str,
    feed_source: str,
    trade_date: date | None = None,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    paper_runtime_plan_cls, _, _ = _get_paper_runtime_symbols()
    settings = get_settings()
    strategy_params = _parse_json(args.strategy_params) if hasattr(args, "strategy_params") else {}
    resolved_trade_date = trade_date
    if resolved_trade_date is None:
        resolved_trade_date = getattr(args, "trade_date", None) or (
            _utc_today() if mode == "live" else None
        )
    session_parts = [str(args.strategy)]
    threshold_value = strategy_params.get("breakout_threshold")
    if threshold_value in (None, "") and str(args.strategy).startswith("thresholdbreak"):
        threshold_value = 0.04
    threshold_label = _format_threshold_label(threshold_value)
    if threshold_label:
        session_parts.append(threshold_label)
    if mode == "live" and getattr(args, "watchlist", False):
        session_parts.append("watchlist")
    if getattr(args, "observe", False):
        session_parts.append("observe")
    session_parts.extend([resolved_trade_date.isoformat() if resolved_trade_date else "na", mode])
    session_id = args.session_id or _default_session_id("paper", *session_parts)
    resolved_symbols = (
        list(symbols) if symbols is not None else _parse_symbol_csv(getattr(args, "symbols", None))
    )
    return paper_runtime_plan_cls(
        session_id=session_id,
        strategy_name=args.strategy,
        trade_date=resolved_trade_date,
        mode=mode,  # type: ignore[arg-type]
        symbols=resolved_symbols,
        experiment_id=args.experiment_id,
        notes=args.notes,
        strategy_params=strategy_params,
        risk_config=_parse_json(args.risk_config) if hasattr(args, "risk_config") else {},
        feed_mode=getattr(args, "feed_mode", "full"),
        feed_source=feed_source,
        kite_api_key=settings.kite_api_key,
        kite_access_token=settings.kite_access_token,
        instrument_tokens=_parse_int_csv(getattr(args, "instrument_tokens", None)),
        observe_only=getattr(args, "observe", False),
    )


def _resolve_trade_date_arg(value: date | None) -> date:
    return value or _utc_today()


def _resolve_watchlist_threshold(args: argparse.Namespace) -> float:
    strategy_params = _parse_json(getattr(args, "strategy_params", None))
    raw = strategy_params.get("breakout_threshold")
    if raw in (None, ""):
        return 0.04
    try:
        return float(raw)
    except TypeError, ValueError:
        return 0.04


def _resolve_watchlist_min_filters(args: argparse.Namespace) -> int:
    strategy_params = _parse_json(getattr(args, "strategy_params", None))
    raw = strategy_params.get("watchlist_min_filters")
    if raw in (None, ""):
        return 5
    try:
        return int(raw)
    except TypeError, ValueError:
        return 5


def _build_watchlist_report(
    args: argparse.Namespace,
    *,
    trade_date: date,
    symbols: list[str],
) -> tuple[Any | None, dict[str, Any]]:
    watchlist_mode = bool(getattr(args, "watchlist", False))
    if not watchlist_mode:
        return None, {"enabled": False, "ready": True}

    from nse_momentum_lab.services.paper.live_watchlist import build_prior_day_watchlist

    threshold = _resolve_watchlist_threshold(args)
    min_filters = _resolve_watchlist_min_filters(args)
    direction = "short" if "breakdown" in str(getattr(args, "strategy", "")).lower() else "long"
    watchlist_df = build_prior_day_watchlist(
        symbols=symbols,
        trade_date=trade_date,
        strategy=args.strategy,
        threshold=threshold,
        direction=direction,
        min_filters=min_filters,
    )
    count = int(getattr(watchlist_df, "height", 0) or 0)
    report: dict[str, Any] = {
        "enabled": True,
        "ready": count > 0,
        "count": count,
        "threshold": threshold,
        "min_filters": min_filters,
        "symbol_sample": (
            watchlist_df["symbol"].to_list()[:10]
            if count > 0 and "symbol" in watchlist_df.columns
            else []
        ),
    }
    if count > 0:
        report["top_filters_passed"] = int(watchlist_df["filters_passed"].max())
        return watchlist_df, report

    report["reasons"] = ["empty_watchlist"]
    report["remediation"] = [
        "Loosen watchlist_min_filters or breakout_threshold for the live session.",
        "Run daily-live without --watchlist only if you explicitly want broad observe-only coverage.",
        "Check logs for watchlist query failures before retrying.",
    ]
    return watchlist_df, report


def _compact_paper_session_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    if not summary:
        return {}
    session = dict(summary.get("session") or {})
    counts = dict(summary.get("counts") or {})
    feed_state = dict(summary.get("feed_state") or {})
    feed_metadata = dict(feed_state.get("metadata_json") or {})
    compact_session = {
        "session_id": session.get("session_id"),
        "trade_date": session.get("trade_date"),
        "strategy_name": session.get("strategy_name"),
        "experiment_id": session.get("experiment_id"),
        "mode": session.get("mode"),
        "status": session.get("status"),
        "symbol_count": int(session.get("symbol_count") or len(session.get("symbols") or [])),
        "strategy_params": session.get("strategy_params") or {},
    }
    compact_feed = {
        "source": feed_state.get("source"),
        "mode": feed_state.get("mode"),
        "status": feed_state.get("status"),
        "is_stale": feed_state.get("is_stale"),
        "subscription_count": feed_state.get("subscription_count"),
        "token_count": int(
            feed_state.get("token_count") or len(feed_metadata.get("instrument_tokens") or [])
        ),
        "last_quote_at": feed_state.get("last_quote_at"),
        "last_tick_at": feed_state.get("last_tick_at"),
        "heartbeat_at": feed_state.get("heartbeat_at"),
        "observe_only": bool(feed_state.get("observe_only", feed_metadata.get("observe_only"))),
    }
    return {
        "session": compact_session,
        "counts": counts,
        "feed_state": compact_feed,
    }


def _compact_runtime_result(result: dict[str, Any]) -> dict[str, Any]:
    session = dict(result.get("session") or {})
    feed_state = dict(result.get("feed_state") or {})
    feed_plan = dict(result.get("feed_plan") or {})
    signals = list(result.get("signals") or [])
    return {
        "session": {
            "session_id": session.get("session_id"),
            "trade_date": session.get("trade_date"),
            "strategy_name": session.get("strategy_name"),
            "mode": session.get("mode"),
            "status": session.get("status"),
            "symbol_count": len(session.get("symbols") or []),
            "strategy_params": session.get("strategy_params") or {},
        },
        "feed_state": {
            "source": feed_state.get("source"),
            "mode": feed_state.get("mode"),
            "status": feed_state.get("status"),
            "subscription_count": feed_state.get("subscription_count"),
            "is_stale": feed_state.get("is_stale"),
            "token_count": len(feed_plan.get("instrument_tokens") or []),
            "batch_count": len(feed_plan.get("batches") or []),
        },
        "queue_size": int(result.get("queue_size") or 0),
        "actionable_queue_size": int(result.get("actionable_queue_size") or 0),
        "signal_count": len(signals),
        "signal_sample": [
            {
                "signal_id": signal.get("signal_id"),
                "symbol": signal.get("symbol"),
                "state": signal.get("state"),
                "decision_status": signal.get("decision_status"),
            }
            for signal in signals[:10]
        ],
    }


def _resolve_all_local_symbols() -> list[str]:
    market_db = get_market_db(read_only=True)
    return sorted(
        {
            str(symbol).strip().upper()
            for symbol in market_db.get_available_symbols()
            if str(symbol).strip()
        }
    )


def _resolve_experiment_symbols(experiment_id: str, trade_date: date) -> list[str]:
    backtest_db = get_backtest_db(read_only=True)
    diagnostics_df = backtest_db.get_experiment_execution_diagnostics(experiment_id)
    if diagnostics_df.is_empty():
        raise SystemExit(f"Experiment '{experiment_id}' has no execution diagnostics")

    symbols = sorted(
        {
            str(row.get("symbol") or "").strip().upper()
            for row in diagnostics_df.to_dicts()
            if _coerce_iso_date(row.get("signal_date")) == trade_date
            and str(row.get("symbol") or "").strip()
        }
    )
    if not symbols:
        raise SystemExit(
            f"Experiment '{experiment_id}' has no execution diagnostics for {trade_date.isoformat()}"
        )
    return symbols


def _resolve_daily_symbols(
    args: argparse.Namespace,
    trade_date: date,
    *,
    live: bool = False,
) -> list[str]:
    explicit_symbols = _parse_symbol_csv(getattr(args, "symbols", None))
    use_all_symbols = bool(getattr(args, "all_symbols", False))
    if explicit_symbols and use_all_symbols:
        raise SystemExit("Use either --symbols or --all-symbols, not both.")
    if explicit_symbols:
        return explicit_symbols
    experiment_id = getattr(args, "experiment_id", None)
    if experiment_id:
        return _resolve_experiment_symbols(str(experiment_id), trade_date)
    if use_all_symbols and live:
        from nse_momentum_lab.services.paper.live_watchlist import build_operational_universe

        symbols = build_operational_universe(trade_date=trade_date)
        if not symbols:
            print(
                "WARNING: Operational universe is empty — falling back to all local symbols.",
                file=sys.stderr,
            )
            return _resolve_all_local_symbols()
        print(f"Operational universe: {len(symbols)} symbols (prior-day daily + 5-min coverage)")
        return symbols
    return _resolve_all_local_symbols()


def _build_live_runtime_coverage_report(
    market_db: Any,
    *,
    trade_date: date,
    symbols: list[str],
) -> dict[str, Any]:
    requested_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    if requested_symbols:
        placeholders = ", ".join("?" for _ in requested_symbols)
        daily_rows = market_db.con.execute(
            f"""
            SELECT symbol, MAX(date)::VARCHAR AS prev_daily_date
            FROM v_daily
            WHERE symbol IN ({placeholders}) AND date < ?
            GROUP BY symbol
            """,
            [*requested_symbols, trade_date],
        ).fetchall()
        five_min_rows = market_db.con.execute(
            f"""
            SELECT symbol, MAX(date)::VARCHAR AS prev_5min_date
            FROM v_5min
            WHERE symbol IN ({placeholders}) AND date < ?
            GROUP BY symbol
            """,
            [*requested_symbols, trade_date],
        ).fetchall()
    else:
        daily_rows = []
        five_min_rows = []

    prev_daily_by_symbol = {
        str(row[0]).strip().upper(): str(row[1]) for row in daily_rows if row[0]
    }
    prev_5min_by_symbol = {
        str(row[0]).strip().upper(): str(row[1]) for row in five_min_rows if row[0]
    }
    matched_prev_trade_dates: dict[str, int] = {}
    missing_by_symbol: dict[str, list[str]] = {}

    for symbol in requested_symbols:
        prev_daily_date = prev_daily_by_symbol.get(symbol)
        prev_5min_date = prev_5min_by_symbol.get(symbol)

        missing: list[str] = []
        if prev_daily_date is None:
            missing.append("v_daily")
        if prev_5min_date is None:
            missing.append("v_5min")
        if (
            prev_daily_date is not None
            and prev_5min_date is not None
            and prev_daily_date != prev_5min_date
        ):
            missing.append("date_mismatch")
        if missing:
            missing_by_symbol[symbol] = missing
            continue
        if prev_daily_date is not None:
            matched_prev_trade_dates[prev_daily_date] = (
                matched_prev_trade_dates.get(prev_daily_date, 0) + 1
            )

    return {
        "mode": "live",
        "trade_date": trade_date.isoformat(),
        "requested_symbol_count": len(requested_symbols),
        "requested_symbols_sample": requested_symbols[:20],
        "coverage_ready": not missing_by_symbol,
        "matched_prev_trade_dates": dict(sorted(matched_prev_trade_dates.items())),
        "missing_symbol_count": len(missing_by_symbol),
        "missing_symbol_sample": [
            {"symbol": symbol, "reasons": reasons}
            for symbol, reasons in sorted(missing_by_symbol.items())[:20]
        ],
    }


def _build_daily_prepare_report(
    *,
    trade_date: date,
    symbols: list[str],
    mode: str,
) -> dict[str, Any]:
    market_db = get_market_db(read_only=True)

    if mode == "live":
        data_report = _build_live_runtime_coverage_report(
            market_db,
            trade_date=trade_date,
            symbols=symbols,
        )
        return _compose_live_readiness_verdict(
            data_report=data_report,
            trade_date=trade_date,
        )

    return _build_walk_forward_runtime_coverage_report(
        market_db,
        [trade_date],
        start_date=trade_date,
        end_date=trade_date,
    )


def _compose_live_readiness_verdict(
    *,
    data_report: dict[str, Any],
    trade_date: date,
) -> dict[str, Any]:
    """Compose a structured readiness verdict from individual check sections.

    Produces a report with explicit sections and a single overall decision:
    ``READY``, ``OBSERVE_ONLY``, or ``BLOCKED``.
    """
    data_ready = data_report.get("coverage_ready", False)
    requested_count = data_report.get("requested_symbol_count", 0)
    missing_count = data_report.get("missing_symbol_count", 0)

    checks: list[str] = []
    reasons: list[str] = []
    remediation: list[str] = []

    # --- Data readiness ---
    if data_ready:
        checks.append("data_ready")
    else:
        reasons.append("data_coverage_gap")
        if requested_count == 0:
            remediation.append(
                "No symbols resolved. Pass --symbols CSV or ensure --all-symbols "
                "has prior-day daily and 5-min data in DuckDB."
            )
        else:
            pct = round(100 * missing_count / requested_count, 1) if requested_count else 0
            remediation.append(
                f"{missing_count}/{requested_count} symbols ({pct}%) lack prior-day "
                "daily or 5-min coverage. Run kite ingestion and feature refresh, "
                "or narrow the symbol list."
            )

    # --- Overall verdict ---
    if data_ready:
        verdict = "READY"
    elif requested_count > 0 and missing_count > 0:
        # Check if most symbols have coverage (>= 50% matched)
        matched_count = requested_count - missing_count
        if matched_count >= requested_count * 0.5:
            verdict = "OBSERVE_ONLY"
            reasons.append("partial_data_coverage")
            remediation.append(
                f"{missing_count} symbols have coverage gaps. Session will proceed "
                "in observe-only mode with {matched_count} available symbols."
            )
        else:
            verdict = "BLOCKED"
            reasons.append("data_coverage_gap")
            pct = round(100 * missing_count / requested_count, 1)
            remediation.append(
                f"{missing_count}/{requested_count} symbols ({pct}%) lack prior-day "
                "daily or 5-min coverage. Run kite ingestion and feature refresh."
            )
    else:
        verdict = "BLOCKED"
        reasons.append("data_coverage_gap")
        remediation.append(
            "No symbols resolved. Pass --symbols CSV or ensure --all-symbols "
            "has prior-day daily and 5-min data in DuckDB."
        )

    return {
        "verdict": verdict,
        "trade_date": trade_date.isoformat(),
        "mode": "live",
        "checks": checks,
        "reasons": reasons,
        "remediation": remediation,
        "data_readiness": {
            "ready": data_ready,
            "requested_symbol_count": requested_count,
            "matched_symbol_count": requested_count - missing_count,
            "missing_symbol_count": missing_count,
            "matched_prev_trade_dates": data_report.get("matched_prev_trade_dates", {}),
            "missing_symbol_sample": data_report.get("missing_symbol_sample", []),
        },
        "coverage_ready": data_ready,  # backward compat
    }


def _build_stream_runner(
    args: argparse.Namespace,
    session_id: str,
    *,
    instrument_tokens: list[int] | None = None,
    tick_handler: Any | None = None,
) -> dict[str, Any]:
    kite_stream_config_cls, kite_stream_runner_cls = _get_kite_stream_symbols()
    settings = get_settings()
    sessionmaker = get_sessionmaker()
    return kite_stream_runner_cls(
        sessionmaker=sessionmaker,
        session_id=session_id,
        config=kite_stream_config_cls(
            api_key=settings.kite_api_key or "",
            access_token=settings.kite_access_token or "",
            instrument_tokens=(
                list(instrument_tokens)
                if instrument_tokens is not None
                else _parse_int_csv(getattr(args, "instrument_tokens", None))
            ),
            mode=getattr(args, "feed_mode", "full"),
        ),
        tick_handler=tick_handler,
    )


def _summarize_folds(folds: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [
        float(fold["total_return_pct"])
        for fold in folds
        if fold.get("total_return_pct") is not None
    ]
    drawdowns = [
        float(fold["max_drawdown_pct"])
        for fold in folds
        if fold.get("max_drawdown_pct") is not None
    ]
    trades = [int(fold.get("total_trades") or 0) for fold in folds]
    completed = [fold for fold in folds if str(fold.get("status") or "").lower() == "completed"]
    profitable = [value for value in returns if value > 0]
    return {
        "folds_total": len(folds),
        "folds_completed": len(completed),
        "folds_profitable": len(profitable),
        "folds_profitable_ratio": round(len(profitable) / len(returns), 4) if returns else None,
        "avg_return_pct": round(mean(returns), 4) if returns else None,
        "median_return_pct": round(median(returns), 4) if returns else None,
        "worst_drawdown_pct": round(max(drawdowns), 4) if drawdowns else None,
        "total_trades": sum(trades),
    }


def _evaluate_walk_forward(summary: dict[str, Any]) -> dict[str, Any]:
    min_avg_return_pct = 0.0
    min_profitable_ratio = 0.5
    max_drawdown_pct = 15.0

    folds_total = int(summary.get("folds_total") or 0)
    folds_completed = int(summary.get("folds_completed") or 0)
    folds_profitable_ratio = summary.get("folds_profitable_ratio")
    avg_return_pct = summary.get("avg_return_pct")
    worst_drawdown_pct = summary.get("worst_drawdown_pct")

    reasons = []
    if folds_total <= 0:
        reasons.append("no_folds")
    if folds_completed != folds_total:
        reasons.append("incomplete_folds")
    if avg_return_pct is None or avg_return_pct <= min_avg_return_pct:
        reasons.append("non_positive_average_return")
    if folds_profitable_ratio is None or folds_profitable_ratio < min_profitable_ratio:
        reasons.append("insufficient_profitable_folds")
    if worst_drawdown_pct is None or worst_drawdown_pct >= max_drawdown_pct:
        reasons.append("excessive_drawdown")

    passed = not reasons
    return {
        "status": "PASS" if passed else "FAIL",
        "reason": "all_thresholds_met" if passed else ",".join(reasons),
        "thresholds": {
            "min_avg_return_pct": min_avg_return_pct,
            "min_profitable_ratio": min_profitable_ratio,
            "max_drawdown_pct": max_drawdown_pct,
        },
    }


async def _cmd_prepare(args: argparse.Namespace) -> None:
    session_id = args.session_id or _default_session_id(
        "paper",
        args.strategy,
        args.trade_date.isoformat() if args.trade_date else "na",
        args.mode,
    )
    auto_generated = args.session_id is None
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        await _warn_if_session_exists(
            db_session,
            session_id,
            command="prepare",
            auto_generated=auto_generated,
        )
        row = await create_or_update_paper_session(
            db_session,
            session_id=session_id,
            trade_date=args.trade_date,
            strategy_name=args.strategy,
            mode=args.mode,
            status=args.status,
            experiment_id=args.experiment_id,
            symbols=[s.strip().upper() for s in args.symbols.split(",") if s.strip()],
            strategy_params=_parse_json(args.strategy_params),
            risk_config=_parse_json(args.risk_config),
            notes=args.notes,
        )
    print(json.dumps(_session_to_json(row), default=str, indent=2))


async def _cmd_status(args: argparse.Namespace) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        if args.session_id:
            if args.summary:
                payload = await get_paper_session_summary_compact(db_session, args.session_id)
                print(json.dumps(payload or {}, default=str, indent=2))
                return
            summary = await get_paper_session_summary(db_session, args.session_id)
            payload = _compact_paper_session_summary(summary) if args.summary else (summary or {})
            print(json.dumps(payload, default=str, indent=2))
            return

        sessions = (
            await list_paper_sessions_compact(db_session, status=args.status, limit=args.limit)
            if args.summary
            else await list_paper_sessions(db_session, status=args.status, limit=args.limit)
        )
    print(json.dumps({"sessions": sessions}, default=str, indent=2))


async def _transition_session(args: argparse.Namespace, status: str) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        row = await set_paper_session_status(
            db_session,
            session_id=args.session_id,
            status=status,
            notes=args.notes,
        )
    print(json.dumps(_session_to_json(row) if row else {}, default=str, indent=2))


async def _cmd_pause(args: argparse.Namespace) -> None:
    await _transition_session(args, "PAUSED")


async def _cmd_resume(args: argparse.Namespace) -> None:
    await _transition_session(args, "ACTIVE")


async def _cmd_stop(args: argparse.Namespace) -> None:
    await _transition_session(args, "COMPLETED" if args.complete else "STOPPING")


async def _cmd_flatten(args: argparse.Namespace) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        closed = await flatten_open_positions(db_session, args.session_id)
        await set_paper_session_status(
            db_session,
            session_id=args.session_id,
            status="STOPPING",
            notes=args.notes,
        )
    print(
        json.dumps(
            {
                "session_id": args.session_id,
                "flattened_positions": len(closed),
                "positions": closed,
                "status": "STOPPING",
            },
            default=str,
            indent=2,
        )
    )


async def _cmd_archive(args: argparse.Namespace) -> None:
    await _transition_session(args, "ARCHIVED")


async def _cmd_cleanup(args: argparse.Namespace) -> None:
    from nse_momentum_lab.db.paper import archive_sessions, list_stale_sessions

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        stale = await list_stale_sessions(
            db_session,
            mode=getattr(args, "mode", None),
            max_age_hours=getattr(args, "max_age_hours", 48),
        )

    if not stale:
        print(json.dumps({"stale_count": 0, "archived": 0}, indent=2))
        return

    session_ids = [s["session_id"] for s in stale]
    print(json.dumps({"stale_count": len(stale), "sessions": stale}, default=str, indent=2))

    if getattr(args, "dry_run", False):
        print("DRY RUN — no sessions were archived.")
        return

    async with sessionmaker() as db_session:
        result = await archive_sessions(db_session, session_ids)

    print(f"Archived {result['archived']} session(s), {result['not_found']} not found.")


async def _cmd_qualify(args: argparse.Namespace) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        qualified = await qualify_session_signals(
            db_session,
            args.session_id,
            max_rank=getattr(args, "max_rank", None),
            min_score=getattr(args, "min_score", None),
        )
    print(
        json.dumps(
            {"session_id": args.session_id, "qualified": len(qualified), "signals": qualified},
            default=str,
            indent=2,
        )
    )


async def _cmd_alert(args: argparse.Namespace) -> None:
    signal_ids = (
        [int(x.strip()) for x in args.signal_ids.split(",") if x.strip()] if args.signal_ids else []
    )
    if not signal_ids:
        raise SystemExit("--signal-ids is required (comma-separated list of signal IDs)")
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        alerted = await alert_session_signals(db_session, args.session_id, signal_ids)
    print(
        json.dumps(
            {"session_id": args.session_id, "alerted": len(alerted), "signals": alerted},
            default=str,
            indent=2,
        )
    )


async def _cmd_daily_prepare(args: argparse.Namespace) -> None:
    trade_date = _resolve_trade_date_arg(getattr(args, "trade_date", None))
    symbols = _resolve_daily_symbols(args, trade_date)
    payload = _build_daily_prepare_report(
        trade_date=trade_date,
        symbols=symbols,
        mode=getattr(args, "mode", "replay"),
    )
    print(json.dumps(payload, default=str, indent=2))


async def _cmd_daily_sim(args: argparse.Namespace) -> None:
    trade_date = _resolve_trade_date_arg(getattr(args, "trade_date", None))
    symbols = _resolve_daily_symbols(args, trade_date, live=False)
    preparation = _build_daily_prepare_report(
        trade_date=trade_date,
        symbols=symbols,
        mode="replay",
    )
    if not preparation.get("coverage_ready", False):
        print(json.dumps({"mode": "daily-sim", "preparation": preparation}, default=str, indent=2))
        return

    params = _build_fast_sim_backtest_params(args, trade_date=trade_date)
    duckdb_backtest_runner_cls = _get_backtest_runner_cls()
    runner = duckdb_backtest_runner_cls()
    progress_file = (
        Path(args.progress_file).expanduser() if getattr(args, "progress_file", None) else None
    )

    try:
        exp_id = await asyncio.to_thread(
            runner.run,
            params,
            force=getattr(args, "force", False),
            snapshot=getattr(args, "snapshot", False),
            progress_file=progress_file,
        )
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(f"[FAST-SIM FAILED] {exc}") from exc

    print(
        json.dumps(
            {
                "mode": "daily-sim",
                "trade_date": trade_date.isoformat(),
                "symbols": len(symbols),
                "preparation": preparation,
                "experiment_id": exp_id,
            },
            default=str,
            indent=2,
        )
    )
    _print_fast_sim_summary(exp_id)


async def _cmd_daily_replay(args: argparse.Namespace) -> None:
    _, paper_runtime_scaffold_cls, _ = _get_paper_runtime_symbols()
    trade_date = _resolve_trade_date_arg(getattr(args, "trade_date", None))
    symbols = _resolve_daily_symbols(args, trade_date)
    preparation = _build_daily_prepare_report(trade_date=trade_date, symbols=symbols, mode="replay")
    if not preparation.get("coverage_ready", False):
        print(json.dumps({"mode": "replay", "preparation": preparation}, default=str, indent=2))
        return

    plan = _build_runtime_plan(
        args,
        mode="replay",
        feed_source="duckdb",
        trade_date=trade_date,
        symbols=symbols,
    )

    # Watchlist mode for replay: build from prior-day features
    watchlist_mode = getattr(args, "watchlist", False)
    watchlist_df = None
    if watchlist_mode:
        from nse_momentum_lab.services.paper.live_watchlist import build_prior_day_watchlist

        watchlist_df = build_prior_day_watchlist(
            symbols=symbols,
            trade_date=trade_date,
            strategy=args.strategy,
            threshold=_resolve_watchlist_threshold(args),
            min_filters=_resolve_watchlist_min_filters(args),
        )
        if watchlist_df.is_empty():
            print(
                json.dumps(
                    {
                        "mode": "replay",
                        "trade_date": trade_date.isoformat(),
                        "watchlist": {
                            "enabled": True,
                            "ready": False,
                            "count": 0,
                            "reasons": ["empty_watchlist"],
                        },
                        "status": "BLOCKED",
                        "reason": "empty_watchlist",
                    },
                    default=str,
                    indent=2,
                )
            )
            return

        watchlist_rows = watchlist_df.to_dicts()
        plan = _build_runtime_plan(
            args,
            mode="replay",
            feed_source="duckdb",
            trade_date=trade_date,
            symbols=sorted(watchlist_df["symbol"].to_list()),
        )
        plan.strategy_params = {
            **(plan.strategy_params or {}),
            "_live_watchlist_rows": watchlist_rows,
            "_watchlist_mode": True,
            "breakout_threshold": _resolve_watchlist_threshold(args),
            "watchlist_min_filters": _resolve_watchlist_min_filters(args),
        }
        print(f"[WATCHLIST] {len(watchlist_df)} symbols from prior-day features for replay")
    runtime = paper_runtime_scaffold_cls(feed_batch_size=get_settings().kite_ws_max_tokens)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        await _warn_if_session_exists(
            db_session,
            plan.session_id,
            command="daily-replay",
            auto_generated=args.session_id is None,
        )
    result = await runtime.prepare_session(sessionmaker, plan, status="RUNNING")
    execution = None
    if getattr(args, "execute", False):
        execution = await runtime.execute_replay_cycle(sessionmaker, plan.session_id)
    print(
        json.dumps(
            {
                "session_id": plan.session_id,
                "mode": "replay",
                "preparation": preparation,
                "result": result,
                "execution": execution,
            },
            default=str,
            indent=2,
        )
    )


async def _cmd_daily_start(args: argparse.Namespace) -> None:
    """One-command daily paper: readiness → bootstrap → optional live run.

    Prints a consolidated operator summary at the end.
    """
    trade_date = _resolve_trade_date_arg(getattr(args, "trade_date", None))
    symbols = _resolve_daily_symbols(args, trade_date, live=True)
    preparation = _build_daily_prepare_report(trade_date=trade_date, symbols=symbols, mode="live")

    verdict = preparation.get("verdict", "BLOCKED")
    print(f"\n{'=' * 60}")
    print(f"  DAILY START — {trade_date.isoformat()}")
    print(f"{'=' * 60}")
    print(f"  Verdict:  {verdict}")
    print(f"  Symbols:  {len(symbols)}")
    if preparation.get("reasons"):
        print(f"  Reasons:  {', '.join(preparation['reasons'])}")
    if preparation.get("remediation"):
        for r in preparation["remediation"]:
            print(f"  FIX: {r}")
    print(f"{'=' * 60}\n")

    if verdict == "BLOCKED":
        print("Session blocked. Address the issues above before retrying.")
        return

    if verdict == "OBSERVE_ONLY":
        print("Proceeding in observe-only mode due to partial data coverage.")

    # Delegate to daily-live for session bootstrap and optional run
    await _cmd_daily_live(args)


async def _cmd_daily_live(args: argparse.Namespace) -> None:
    _, paper_runtime_scaffold_cls, redact_credentials = _get_paper_runtime_symbols()
    kite_connect_client_cls = _get_kite_connect_client_cls()
    settings = get_settings()
    trade_date = _resolve_trade_date_arg(getattr(args, "trade_date", None))
    symbols = _resolve_daily_symbols(args, trade_date, live=True)
    preparation = _build_daily_prepare_report(trade_date=trade_date, symbols=symbols, mode="live")
    if not preparation.get("coverage_ready", False):
        print(json.dumps({"mode": "live", "preparation": preparation}, default=str, indent=2))
        return

    watchlist_df, watchlist_report = _build_watchlist_report(
        args,
        trade_date=trade_date,
        symbols=symbols,
    )
    watchlist_mode = bool(watchlist_report.get("enabled"))
    if watchlist_mode and not watchlist_report.get("ready", False):
        print(
            json.dumps(
                {
                    "mode": "live",
                    "verdict": "BLOCKED",
                    "preparation": preparation,
                    "watchlist": watchlist_report,
                },
                default=str,
                indent=2,
            )
        )
        return
    if watchlist_mode:
        print(
            f"[WATCHLIST] {watchlist_report['count']} symbols from prior-day features "
            f"(top filters_passed={watchlist_report['top_filters_passed']})"
        )

    plan = _build_runtime_plan(
        args,
        mode="live",
        feed_source="kite",
        trade_date=trade_date,
        symbols=(
            sorted(watchlist_df["symbol"].to_list())
            if watchlist_df is not None and not watchlist_df.is_empty()
            else symbols
        ),
    )
    if watchlist_mode:
        if watchlist_df is not None and not watchlist_df.is_empty():
            plan.strategy_params = {
                **(plan.strategy_params or {}),
                "_live_watchlist_rows": watchlist_df.to_dicts(),
                "_watchlist_mode": True,
                "breakout_threshold": _resolve_watchlist_threshold(args),
            }
    runtime = paper_runtime_scaffold_cls(feed_batch_size=settings.kite_ws_max_tokens)
    sessionmaker = get_sessionmaker()
    status = "ACTIVE" if settings.has_kite_credentials() else "PLANNING"
    async with sessionmaker() as db_session:
        await _warn_if_session_exists(
            db_session,
            plan.session_id,
            command="daily-live",
            auto_generated=args.session_id is None,
        )
    result = await runtime.prepare_session(sessionmaker, plan, status=status)
    execution = None
    observe_only = bool(getattr(args, "observe", False) or plan.observe_only)
    if observe_only:
        print("[OBSERVE] Observe-only mode: feed will be monitored, no trades will be executed")
    if getattr(args, "execute", False) and not observe_only and not getattr(args, "run", False):
        if not settings.has_kite_credentials():
            raise SystemExit("Kite credentials are required to execute live paper entries")
        with kite_connect_client_cls(
            api_key=settings.kite_api_key or "",
            access_token=settings.kite_access_token,
            api_secret=settings.kite_api_secret,
            login_url=settings.kite_login_url,
            api_root=settings.kite_api_root,
        ) as kite_client:
            execution = await runtime.execute_live_cycle(
                sessionmaker,
                plan.session_id,
                kite_client=kite_client,
            )
    print(
        json.dumps(
            {
                "session_id": plan.session_id,
                "mode": "live",
                "observe_only": observe_only,
                "status": status,
                "kite_ready": settings.has_kite_credentials(),
                "preparation": preparation,
                "watchlist": watchlist_report,
                "result": _compact_runtime_result(
                    {
                        **result,
                        "feed_plan": redact_credentials(result.get("feed_plan", {})),
                    }
                ),
                "execution": execution,
            },
            default=str,
            indent=2,
        )
    )

    if getattr(args, "run", False):
        if not settings.has_kite_credentials():
            raise SystemExit("Kite credentials are required to start the live stream")
        # Resolve tokens from prepared feed plan (prefer resolved list, fall back to plan)
        resolved_tokens = list(
            result.get("resolved_instrument_tokens")
            or result.get("feed_plan", {}).get("instrument_tokens", [])
        )
        if not resolved_tokens and not getattr(args, "observe", False):
            raise SystemExit(
                "No instrument tokens resolved — live stream would subscribe to nothing. "
                "Check symbol universe and instrument cache. "
                "Use --observe to start in observe-only mode without tokens."
            )
        print(
            f"[STREAM] Resolved {len(resolved_tokens)} instrument tokens for session {plan.session_id}"
        )
        runner = _build_stream_runner(
            args,
            plan.session_id,
            instrument_tokens=resolved_tokens,
            tick_handler=(
                (
                    lambda ticks: runtime.process_live_ticks(
                        sessionmaker,
                        plan.session_id,
                        ticks,
                        observe_only=observe_only,
                    )
                )
                if watchlist_mode
                else None
            ),
        )
        await runner.run()


async def _cmd_replay_day(args: argparse.Namespace) -> None:
    _, paper_runtime_scaffold_cls, _ = _get_paper_runtime_symbols()
    plan = _build_runtime_plan(args, mode="replay", feed_source="duckdb")
    runtime = paper_runtime_scaffold_cls(feed_batch_size=get_settings().kite_ws_max_tokens)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        await _warn_if_session_exists(
            db_session,
            plan.session_id,
            command="replay-day",
            auto_generated=args.session_id is None,
        )
    result = await runtime.prepare_session(sessionmaker, plan, status="RUNNING")
    execution = None
    if getattr(args, "execute", False):
        execution = await runtime.execute_replay_cycle(sessionmaker, plan.session_id)
    print(
        json.dumps(
            {
                "session_id": plan.session_id,
                "mode": "replay",
                "result": result,
                "execution": execution,
            },
            default=str,
            indent=2,
        )
    )


async def _cmd_live(args: argparse.Namespace) -> None:
    _, paper_runtime_scaffold_cls, redact_credentials = _get_paper_runtime_symbols()
    kite_connect_client_cls = _get_kite_connect_client_cls()
    settings = get_settings()
    plan = _build_runtime_plan(args, mode="live", feed_source="kite")
    runtime = paper_runtime_scaffold_cls(feed_batch_size=settings.kite_ws_max_tokens)
    sessionmaker = get_sessionmaker()
    status = "ACTIVE" if settings.has_kite_credentials() else "PLANNING"
    async with sessionmaker() as db_session:
        await _warn_if_session_exists(
            db_session,
            plan.session_id,
            command="live",
            auto_generated=args.session_id is None,
        )
    result = await runtime.prepare_session(sessionmaker, plan, status=status)
    execution = None
    observe_only = getattr(args, "observe", False)
    if observe_only:
        print("[OBSERVE] Observe-only mode: feed will be monitored, no trades will be executed")
    if getattr(args, "execute", False) and not observe_only:
        if not settings.has_kite_credentials():
            raise SystemExit("Kite credentials are required to execute live paper entries")
        with kite_connect_client_cls(
            api_key=settings.kite_api_key or "",
            access_token=settings.kite_access_token,
            api_secret=settings.kite_api_secret,
            login_url=settings.kite_login_url,
            api_root=settings.kite_api_root,
        ) as kite_client:
            execution = await runtime.execute_live_cycle(
                sessionmaker,
                plan.session_id,
                kite_client=kite_client,
            )
    print(
        json.dumps(
            {
                "session_id": plan.session_id,
                "mode": "live",
                "observe_only": observe_only,
                "status": status,
                "kite_ready": settings.has_kite_credentials(),
                "result": {**result, "feed_plan": redact_credentials(result.get("feed_plan", {}))},
                "execution": execution,
            },
            default=str,
            indent=2,
        )
    )

    if getattr(args, "run", False):
        if not settings.has_kite_credentials():
            raise SystemExit("Kite credentials are required to start the live stream")
        resolved_tokens = list(
            result.get("resolved_instrument_tokens")
            or result.get("feed_plan", {}).get("instrument_tokens", [])
        )
        if not resolved_tokens and not getattr(args, "observe", False):
            raise SystemExit(
                "No instrument tokens resolved — live stream would subscribe to nothing. "
                "Check symbol universe and instrument cache. "
                "Use --observe to start in observe-only mode without tokens."
            )
        print(
            f"[STREAM] Resolved {len(resolved_tokens)} instrument tokens for session {plan.session_id}"
        )
        runner = _build_stream_runner(
            args,
            plan.session_id,
            instrument_tokens=resolved_tokens,
        )
        await runner.run()


async def _cmd_stream(args: argparse.Namespace) -> None:
    args.run = True
    await _cmd_live(args)


async def _cmd_walk_forward(args: argparse.Namespace) -> None:
    backtest_params_cls = _get_backtest_params_cls()
    duckdb_backtest_runner_cls = _get_backtest_runner_cls()
    walk_forward_framework_cls = _get_walk_forward_framework_cls()
    framework = walk_forward_framework_cls(strategy_name=args.strategy)
    trading_sessions = _load_market_trading_sessions(args.start_date, args.end_date)
    windows = list(
        framework.generate_rolling_windows_from_sessions(
            trading_sessions,
            train_sessions=args.train_days,
            test_sessions=args.test_days,
            roll_interval_sessions=args.roll_interval_days,
        )
    )
    if args.max_folds is not None:
        windows = windows[: args.max_folds]
    if not windows:
        raise SystemExit("No walk-forward windows generated for the requested date range")

    # CLI safeguard: confirm for large fold counts
    if len(windows) > 20 and not args.yes:
        if sys.stdin.isatty():
            response = (
                input(f"Running {len(windows)} walk-forward folds. Continue? [y/N] ")
                .strip()
                .lower()
            )
            if response not in ("y", "yes"):
                print("Aborted.")
                return
        elif not args.yes:
            print(
                f"Warning: {len(windows)} folds will run without confirmation (not a TTY). Use --yes to skip."
            )
            return

    runtime_coverage = _validate_walk_forward_runtime_coverage(
        trading_sessions,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info(
        "Walk-forward runtime coverage ready in %s mode (%d trade dates).",
        runtime_coverage.get("runtime_mode"),
        len(runtime_coverage.get("trade_dates") or []),
    )

    session_id = args.session_id or _default_session_id(
        "wf", args.strategy, args.start_date.isoformat(), args.end_date.isoformat()
    )
    base_params = _normalize_backtest_params(args.strategy, _parse_json(args.params_json))
    base_params_hash = backtest_params_cls(**base_params).to_hash()
    test_ranges = [
        {"start": window.test_start.isoformat(), "end": window.test_end.isoformat()}
        for window in windows
    ]

    runner = duckdb_backtest_runner_cls()
    folds: list[dict[str, Any]] = []
    sessionmaker = get_sessionmaker()

    summary: dict[str, Any] | None = None
    decision: dict[str, Any] | None = None
    async with sessionmaker() as db_session:
        await _warn_if_session_exists(
            db_session,
            session_id,
            command="walk-forward",
            auto_generated=args.session_id is None,
        )
        await create_or_update_paper_session(
            db_session,
            session_id=session_id,
            trade_date=args.end_date,
            strategy_name=args.strategy,
            mode="walk_forward",
            status="RUNNING",
            experiment_id=None,
            symbols=[],
            strategy_params={
                "train_days": args.train_days,
                "test_days": args.test_days,
                "roll_interval_days": args.roll_interval_days,
                "window_mode": "trading_sessions",
                "requested_date_range": {
                    "start": args.start_date.isoformat(),
                    "end": args.end_date.isoformat(),
                },
                "base_params": base_params,
                "base_params_hash": base_params_hash,
                "test_ranges": test_ranges,
            },
            risk_config={},
            notes=args.notes,
        )

        try:
            await reset_walk_forward_folds(db_session, session_id)
            for fold_index, window in enumerate(windows, start=1):
                fold_params = dict(base_params)
                fold_params["start_date"] = window.test_start.isoformat()
                fold_params["end_date"] = window.test_end.isoformat()
                fold_params["start_year"] = window.test_start.year
                fold_params["end_year"] = window.test_end.year
                exp_id = runner.run(
                    backtest_params_cls(**fold_params),
                    force=args.force,
                    snapshot=args.snapshot,
                    wf_run_id=session_id,
                )
                exp = runner.results_db.get_experiment(exp_id) or {}
                fold: dict[str, Any] = {
                    "wf_run_id": session_id,
                    "train_start": window.train_start.isoformat(),
                    "train_end": window.train_end.isoformat(),
                    "test_start": window.test_start.isoformat(),
                    "test_end": window.test_end.isoformat(),
                    "exp_id": exp_id,
                    "status": exp.get("status"),
                    "total_return_pct": exp.get("total_return_pct"),
                    "max_drawdown_pct": exp.get("max_drawdown_pct"),
                    "profit_factor": exp.get("profit_factor"),
                    "total_trades": exp.get("total_trades"),
                    "params_hash": exp.get("params_hash"),
                    "dataset_hash": exp.get("dataset_hash"),
                    "code_hash": exp.get("code_hash"),
                }
                folds.append(fold)
                await insert_walk_forward_fold(
                    db_session,
                    wf_session_id=session_id,
                    fold_index=fold_index,
                    train_start=window.train_start,
                    train_end=window.train_end,
                    test_start=window.test_start,
                    test_end=window.test_end,
                    exp_id=exp_id,
                    status=fold["status"],
                    total_return_pct=fold["total_return_pct"],
                    max_drawdown_pct=fold["max_drawdown_pct"],
                    profit_factor=fold["profit_factor"],
                    total_trades=fold["total_trades"],
                )
                await db_session.commit()

            summary = _summarize_folds(folds)
            decision = _evaluate_walk_forward(summary)
            fold_experiment_ids = sorted(
                {str(fold["exp_id"]) for fold in folds if str(fold.get("exp_id") or "").strip()}
            )
            dataset_hashes = sorted(
                {
                    str(fold["dataset_hash"])
                    for fold in folds
                    if str(fold.get("dataset_hash") or "").strip()
                }
            )
            code_hashes = sorted(
                {
                    str(fold["code_hash"])
                    for fold in folds
                    if str(fold.get("code_hash") or "").strip()
                }
            )
            await update_paper_session(
                db_session,
                session_id=session_id,
                strategy_params={
                    **base_params,
                    "walk_forward": {
                        "window_mode": "trading_sessions",
                        "requested_date_range": {
                            "start": args.start_date.isoformat(),
                            "end": args.end_date.isoformat(),
                        },
                        "base_params": base_params,
                        "base_params_hash": base_params_hash,
                        "test_ranges": test_ranges,
                        "fold_experiment_ids": fold_experiment_ids,
                        "lineage": {
                            "dataset_hashes": dataset_hashes,
                            "code_hashes": code_hashes,
                        },
                        "summary": summary,
                        "decision": decision,
                        "folds": folds,
                    },
                },
            )
            await set_paper_session_status(
                db_session,
                session_id=session_id,
                status="COMPLETED" if decision["status"] == "PASS" else "FAILED",
                notes=args.notes,
            )
        except Exception:
            logger.exception("Walk-forward session failed: session_id=%s", session_id)
            await set_paper_session_status(
                db_session,
                session_id=session_id,
                status="FAILED",
                notes=args.notes,
            )
            raise

    print(
        json.dumps(
            {
                "session_id": session_id,
                "strategy": args.strategy,
                "windows": len(windows),
                "summary": summary,
                "decision": decision,
                "folds": folds,
            },
            default=str,
            indent=2,
        )
    )


async def _cmd_walk_forward_cleanup(args: argparse.Namespace) -> None:
    wf_run_ids = _dedupe_nonempty_strings(getattr(args, "wf_run_ids", []))
    run_ids = _dedupe_nonempty_strings(getattr(args, "run_ids", []))
    if not wf_run_ids and not run_ids:
        raise SystemExit("Provide at least one --wf-run-id or --run-id.")

    sessionmaker = get_sessionmaker()
    backtest_db = get_backtest_db(read_only=not args.apply)
    plan: dict[str, Any] = {}
    postgres_deleted: dict[str, Any] = {"deleted_count": 0, "session_ids": []}
    duckdb_deleted: list[str] = []
    async with sessionmaker() as db_session:
        plan = await _build_walk_forward_cleanup_plan(
            db_session,
            backtest_db,
            wf_run_ids=wf_run_ids,
            run_ids=run_ids,
        )

        if not args.apply:
            print(
                json.dumps(
                    {
                        "applied": False,
                        "plan": plan,
                    },
                    default=str,
                    indent=2,
                )
            )
            return

        postgres_deleted = await delete_walk_forward_sessions_by_ids(
            db_session,
            [str(session["session_id"]) for session in plan["postgres"]["sessions"]],
        )
        for exp_id in plan["duckdb"]["run_ids_to_delete"]:
            if backtest_db.experiment_exists(exp_id):
                backtest_db.delete_experiment(exp_id)
                duckdb_deleted.append(exp_id)

    print(
        json.dumps(
            {
                "applied": True,
                "requested_wf_run_ids": wf_run_ids,
                "requested_run_ids": run_ids,
                "plan": plan,
                "deleted": {
                    "postgres": postgres_deleted,
                    "duckdb": {
                        "deleted_count": len(duckdb_deleted),
                        "exp_ids": duckdb_deleted,
                    },
                },
            },
            default=str,
            indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Paper session workflow")
    sub = parser.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser("prepare", help="Create or update a paper session")
    prepare.add_argument("--session-id", default=None)
    prepare.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    prepare.add_argument("--strategy", default="thresholdbreakout")
    prepare.add_argument("--mode", default="replay", choices=["replay", "live"])
    prepare.add_argument("--status", default="PLANNING")
    prepare.add_argument("--experiment-id", default=None)
    prepare.add_argument("--symbols", default="")
    prepare.add_argument("--strategy-params", default=None)
    prepare.add_argument("--risk-config", default=None)
    prepare.add_argument("--notes", default=None)
    prepare.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    prepare.add_argument("--instrument-tokens", default="")
    prepare.set_defaults(handler=_cmd_prepare)

    status = sub.add_parser("status", help="Show one session or list sessions")
    status.add_argument("--session-id", default=None)
    status.add_argument("--status", default=None)
    status.add_argument("--limit", type=_positive_int, default=20)
    status.add_argument(
        "--summary",
        action="store_true",
        help="Show compact counts/feed state without full symbol and signal payloads",
    )
    status.set_defaults(handler=_cmd_status)

    daily_prepare = sub.add_parser(
        "daily-prepare",
        help="Check daily paper runtime readiness",
    )
    daily_prepare.add_argument(
        "--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD"
    )
    daily_prepare.add_argument("--experiment-id", default=None)
    daily_prepare.add_argument("--symbols", default="")
    daily_prepare.add_argument(
        "--all-symbols",
        action="store_true",
        help="Use the full local symbol universe instead of an explicit symbol list",
    )
    daily_prepare.add_argument("--mode", default="replay", choices=["replay", "live"])
    daily_prepare.set_defaults(handler=_cmd_daily_prepare)

    daily_sim = sub.add_parser(
        "daily-sim",
        help="Run a fast single-day historical simulation using the backtest engine",
    )
    daily_sim.add_argument("--trade-date", required=True, type=_parse_iso_date, help="YYYY-MM-DD")
    daily_sim.add_argument("--strategy", default="thresholdbreakout")
    daily_sim.add_argument("--experiment-id", default=None)
    daily_sim.add_argument("--params-json", default=None)
    daily_sim.add_argument(
        "--universe-size",
        type=_positive_int,
        default=None,
        help="Backtest universe size for the one-day simulation",
    )
    daily_sim.add_argument(
        "--progress-file",
        default=None,
        help="Optional NDJSON progress file for the backtest run",
    )
    daily_sim.add_argument("--force", action="store_true")
    daily_sim.add_argument("--snapshot", action="store_true")
    daily_sim.set_defaults(handler=_cmd_daily_sim)

    replay = sub.add_parser("replay-day", help="Bootstrap a replay-day paper session")
    replay.add_argument("--session-id", default=None)
    replay.add_argument("--trade-date", required=True, type=_parse_iso_date, help="YYYY-MM-DD")
    replay.add_argument("--strategy", default="thresholdbreakout")
    replay.add_argument("--experiment-id", default=None)
    replay.add_argument("--symbols", default="")
    replay.add_argument("--strategy-params", default=None)
    replay.add_argument("--risk-config", default=None)
    replay.add_argument("--notes", default=None)
    replay.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    replay.add_argument("--instrument-tokens", default="")
    replay.add_argument("--execute", action="store_true", help="Execute the replay queue once")
    replay.set_defaults(handler=_cmd_replay_day)

    daily_replay = sub.add_parser(
        "daily-replay",
        help="Run daily readiness checks, then bootstrap a replay-day paper session",
    )
    daily_replay.add_argument("--session-id", default=None)
    daily_replay.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    daily_replay.add_argument("--strategy", default="thresholdbreakout")
    daily_replay.add_argument("--experiment-id", default=None)
    daily_replay.add_argument("--symbols", default="")
    daily_replay.add_argument(
        "--all-symbols",
        action="store_true",
        help="Use the full local symbol universe instead of an explicit symbol list",
    )
    daily_replay.add_argument("--strategy-params", default=None)
    daily_replay.add_argument("--risk-config", default=None)
    daily_replay.add_argument("--notes", default=None)
    daily_replay.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    daily_replay.add_argument("--instrument-tokens", default="")
    daily_replay.add_argument(
        "--execute", action="store_true", help="Execute the replay queue once"
    )
    daily_replay.add_argument(
        "--watchlist",
        action="store_true",
        help="Build watchlist from prior-day features (no same-day data required)",
    )
    daily_replay.set_defaults(handler=_cmd_daily_replay)

    live = sub.add_parser("live", help="Bootstrap a live Kite-backed paper session")
    live.add_argument("--session-id", default=None)
    live.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    live.add_argument("--strategy", default="thresholdbreakout")
    live.add_argument("--experiment-id", default=None)
    live.add_argument("--symbols", default="")
    live.add_argument("--strategy-params", default=None)
    live.add_argument("--risk-config", default=None)
    live.add_argument("--notes", default=None)
    live.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    live.add_argument("--instrument-tokens", default="")
    live.add_argument("--execute", action="store_true", help="Execute live paper entries once")
    live.add_argument("--run", action="store_true", help="Start the live Kite websocket loop")
    live.add_argument(
        "--observe",
        action="store_true",
        help="Observe-only mode: subscribe to feed but do not execute trades",
    )
    live.set_defaults(handler=_cmd_live)

    daily_live = sub.add_parser(
        "daily-live",
        help="Run live daily readiness checks, then bootstrap a live Kite-backed paper session",
    )
    daily_live.add_argument("--session-id", default=None)
    daily_live.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    daily_live.add_argument("--strategy", default="thresholdbreakout")
    daily_live.add_argument("--experiment-id", default=None)
    daily_live.add_argument("--symbols", default="")
    daily_live.add_argument(
        "--all-symbols",
        action="store_true",
        help="Use the full local symbol universe instead of an explicit symbol list",
    )
    daily_live.add_argument(
        "--watchlist",
        action="store_true",
        help="Build watchlist from prior-day features (no same-day data required)",
    )
    daily_live.add_argument("--strategy-params", default=None)
    daily_live.add_argument("--risk-config", default=None)
    daily_live.add_argument("--notes", default=None)
    daily_live.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    daily_live.add_argument("--instrument-tokens", default="")
    daily_live.add_argument(
        "--execute", action="store_true", help="Execute live paper entries once"
    )
    daily_live.add_argument("--run", action="store_true", help="Start the live Kite websocket loop")
    daily_live.add_argument(
        "--observe",
        action="store_true",
        help="Observe-only mode: subscribe to feed but do not execute trades",
    )
    daily_live.set_defaults(handler=_cmd_daily_live)

    daily_start = sub.add_parser(
        "daily-start",
        help="One-command daily paper session: readiness check, session bootstrap, and optional live start",
    )
    daily_start.add_argument("--session-id", default=None)
    daily_start.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    daily_start.add_argument("--strategy", default="thresholdbreakout")
    daily_start.add_argument("--experiment-id", default=None)
    daily_start.add_argument("--symbols", default="")
    daily_start.add_argument(
        "--all-symbols",
        action="store_true",
        help="Use the full local symbol universe (live mode: operational universe only)",
    )
    daily_start.add_argument(
        "--watchlist",
        action="store_true",
        help="Build watchlist from prior-day features (no same-day data required)",
    )
    daily_start.add_argument("--strategy-params", default=None)
    daily_start.add_argument("--risk-config", default=None)
    daily_start.add_argument("--notes", default=None)
    daily_start.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    daily_start.add_argument("--instrument-tokens", default="")
    daily_start.add_argument(
        "--run",
        action="store_true",
        help="Start the live Kite websocket loop after bootstrap",
    )
    daily_start.add_argument(
        "--observe",
        action="store_true",
        help="Observe-only mode: subscribe to feed but do not execute trades",
    )
    daily_start.set_defaults(handler=_cmd_daily_start)

    stream = sub.add_parser("stream", help="Start the live Kite websocket loop")
    stream.add_argument("--session-id", default=None)
    stream.add_argument("--trade-date", default=None, type=_parse_iso_date, help="YYYY-MM-DD")
    stream.add_argument("--strategy", default="thresholdbreakout")
    stream.add_argument("--experiment-id", default=None)
    stream.add_argument("--symbols", default="")
    stream.add_argument("--strategy-params", default=None)
    stream.add_argument("--risk-config", default=None)
    stream.add_argument("--notes", default=None)
    stream.add_argument("--feed-mode", default="full", choices=["ltp", "quote", "full"])
    stream.add_argument("--instrument-tokens", default="")
    stream.add_argument(
        "--execute", action="store_true", help="Execute live paper entries once before streaming"
    )
    stream.add_argument(
        "--observe",
        action="store_true",
        help="Observe-only mode: subscribe to feed but do not execute trades",
    )
    stream.set_defaults(handler=_cmd_stream)

    pause = sub.add_parser("pause", help="Pause a paper session")
    pause.add_argument("--session-id", required=True)
    pause.add_argument("--notes", default=None)
    pause.set_defaults(handler=_cmd_pause)

    resume = sub.add_parser("resume", help="Resume a paused paper session")
    resume.add_argument("--session-id", required=True)
    resume.add_argument("--notes", default=None)
    resume.set_defaults(handler=_cmd_resume)

    stop = sub.add_parser("stop", help="Stop a paper session")
    stop.add_argument("--session-id", required=True)
    stop.add_argument("--notes", default=None)
    stop.add_argument("--complete", action="store_true")
    stop.set_defaults(handler=_cmd_stop)

    flatten = sub.add_parser("flatten", help="Liquidate all open positions and stop session")
    flatten.add_argument("--session-id", required=True)
    flatten.add_argument("--notes", default=None)
    flatten.set_defaults(handler=_cmd_flatten)

    archive = sub.add_parser("archive", help="Archive a completed session")
    archive.add_argument("--session-id", required=True)
    archive.add_argument("--notes", default=None)
    archive.set_defaults(handler=_cmd_archive)

    cleanup = sub.add_parser("cleanup", help="Find and archive stale sessions")
    cleanup.add_argument(
        "--mode",
        default=None,
        choices=["live", "replay"],
        help="Only consider sessions of this mode",
    )
    cleanup.add_argument(
        "--max-age-hours",
        type=int,
        default=48,
        help="Consider sessions older than N hours as stale (default: 48)",
    )
    cleanup.add_argument(
        "--dry-run",
        action="store_true",
        help="List stale sessions without archiving them",
    )
    cleanup.set_defaults(handler=_cmd_cleanup)

    qualify = sub.add_parser("qualify", help="Promote top-ranked NEW signals to QUALIFIED")
    qualify.add_argument("--session-id", required=True)
    qualify.add_argument(
        "--max-rank", type=int, default=None, help="Only qualify signals with rank <= N"
    )
    qualify.add_argument(
        "--min-score", type=float, default=None, help="Only qualify signals with score >= N"
    )
    qualify.set_defaults(handler=_cmd_qualify)

    alert = sub.add_parser("alert", help="Promote QUALIFIED signals to ALERTED")
    alert.add_argument("--session-id", required=True)
    alert.add_argument(
        "--signal-ids", required=True, help="Comma-separated list of signal IDs to alert"
    )
    alert.set_defaults(handler=_cmd_alert)

    return parser


def _run_async_handler(handler: Any, args: argparse.Namespace) -> None:
    if sys.platform == "win32":
        selector_loop_cls = getattr(asyncio, "SelectorEventLoop", None)
        if selector_loop_cls is None:
            raise RuntimeError("asyncio.SelectorEventLoop is not available on this platform")
        with asyncio.Runner(loop_factory=selector_loop_cls) as runner:
            runner.run(handler(args))
        return
    asyncio.run(handler(args))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    handler = getattr(args, "handler", None)
    if handler is None:
        raise SystemExit("No command specified")

    if inspect.iscoroutinefunction(handler):
        _run_async_handler(handler, args)
    else:
        handler(args)
