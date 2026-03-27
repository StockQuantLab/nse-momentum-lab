#!/usr/bin/env python3
"""Prune backtest experiments outside the retention window.

Retention policy:
- Keep all experiments created between ``retain_start`` and ``retain_end``.
- Keep any experiments referenced by walk-forward sessions inside the
  ``wf_start`` to ``wf_end`` window.

This script deletes the pruned experiments from DuckDB and, unless
``--duckdb-only`` is supplied, also removes their PostgreSQL lineage rows and
MinIO artifacts.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.core import get_sessionmaker
from nse_momentum_lab.db.market_db import DUCKDB_FILE, MarketDataDB
from nse_momentum_lab.db.paper import list_paper_sessions
from nse_momentum_lab.services.ingest.minio import MinioArtifactStore


@dataclass(frozen=True)
class RetainedExperiment:
    exp_id: str
    created_at: datetime | None
    reason: str


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _date_from_created_at(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_params(params_json: str) -> dict[str, Any]:
    if not params_json:
        return {}
    try:
        parsed = json.loads(params_json)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    try:
        parsed = ast.literal_eval(params_json)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def _load_market_db(db_path: Path, read_only: bool) -> MarketDataDB:
    return MarketDataDB(db_path=db_path, read_only=read_only)


def _validate_external_targets() -> MinioArtifactStore:
    settings = get_settings()
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required for retention cleanup.")

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

    return MinioArtifactStore()


def _load_postgres_run_ids() -> set[str]:
    settings = get_settings()
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required for retention cleanup.")

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT exp_hash FROM nseml.exp_run")
            return {str(r[0]) for r in cur.fetchall()}


def _has_minio_artifacts(exp_id: str, store: MinioArtifactStore) -> bool:
    prefix = f"experiments/{exp_id}/"
    for _ in store._client.list_objects(
        MinioArtifactStore.BUCKET_ARTIFACTS,
        prefix=prefix,
        recursive=True,
    ):
        return True
    return False


def _delete_postgres(exp_ids: list[str]) -> int:
    settings = get_settings()
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required for retention cleanup.")

    deleted = 0
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            for exp_id in exp_ids:
                cur.execute("DELETE FROM nseml.exp_run WHERE exp_hash = %s", (exp_id,))
                deleted += int(cur.rowcount)
        conn.commit()
    return deleted


def _delete_minio(exp_ids: list[str], store: MinioArtifactStore) -> int:
    deleted = 0
    for exp_id in exp_ids:
        prefix = f"experiments/{exp_id}/"
        objects = list(
            store._client.list_objects(
                MinioArtifactStore.BUCKET_ARTIFACTS,
                prefix=prefix,
                recursive=True,
            )
        )
        for obj in objects:
            store._client.remove_object(MinioArtifactStore.BUCKET_ARTIFACTS, obj.object_name)
            deleted += 1
    return deleted


def _delete_duckdb(db: MarketDataDB, exp_ids: list[str]) -> int:
    deleted = 0
    for exp_id in exp_ids:
        if db.experiment_exists(exp_id):
            db.delete_experiment(exp_id)
            deleted += 1
    return deleted


def _collect_preserved_walk_forward_exp_ids(
    session_rows: list[dict[str, Any]],
    *,
    wf_start: date,
    wf_end: date,
) -> set[str]:
    preserved: set[str] = set()
    for row in session_rows:
        if str(row.get("mode") or "").strip().lower() != "walk_forward":
            continue
        trade_date = _date_from_created_at(row.get("trade_date"))
        if trade_date is None or trade_date < wf_start or trade_date > wf_end:
            continue
        strategy_params = row.get("strategy_params") or {}
        walk_forward = strategy_params.get("walk_forward") if isinstance(strategy_params, dict) else {}
        if not isinstance(walk_forward, dict):
            continue
        fold_ids = walk_forward.get("fold_experiment_ids")
        if isinstance(fold_ids, list):
            preserved.update(str(exp_id) for exp_id in fold_ids if exp_id)
        folds = walk_forward.get("folds")
        if isinstance(folds, list):
            preserved.update(
                str(fold.get("exp_id") or "")
                for fold in folds
                if isinstance(fold, dict) and fold.get("exp_id")
            )
    return preserved


async def _load_walk_forward_sessions() -> list[dict[str, Any]]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db_session:
        sessions = await list_paper_sessions(db_session, limit=1000)
    return sessions


def _build_retention_plan(
    experiments: list[dict[str, Any]],
    *,
    retain_start: date,
    retain_end: date,
    preserved_walk_forward_exp_ids: set[str],
) -> tuple[list[RetainedExperiment], list[RetainedExperiment]]:
    keep: list[RetainedExperiment] = []
    delete: list[RetainedExperiment] = []

    for row in experiments:
        exp_id = str(row.get("exp_id") or "")
        if not exp_id:
            continue
        created_at = row.get("created_at")
        created_date = _date_from_created_at(created_at)
        if created_date is not None and retain_start <= created_date <= retain_end:
            keep.append(RetainedExperiment(exp_id=exp_id, created_at=created_at, reason="in_retain_window"))
            continue
        if exp_id in preserved_walk_forward_exp_ids:
            keep.append(
                RetainedExperiment(exp_id=exp_id, created_at=created_at, reason="walk_forward_lineage")
            )
            continue
        delete.append(
            RetainedExperiment(exp_id=exp_id, created_at=created_at, reason="outside_retention_window")
        )

    return keep, delete


async def main() -> None:
    parser = argparse.ArgumentParser(description="Prune backtest experiments by retention window.")
    parser.add_argument("--apply", action="store_true", help="Apply deletions. Dry-run by default.")
    parser.add_argument(
        "--duckdb-only",
        action="store_true",
        help="Only clean the DuckDB catalog; skip Postgres and MinIO cleanup.",
    )
    parser.add_argument("--retain-start", type=_parse_date, default=_parse_date("2025-01-01"))
    parser.add_argument("--retain-end", type=_parse_date, default=_parse_date("2026-02-28"))
    parser.add_argument("--wf-start", type=_parse_date, default=_parse_date("2026-03-01"))
    parser.add_argument("--wf-end", type=_parse_date, default=_parse_date("2026-03-20"))
    args = parser.parse_args()

    market_path = Path(os.getenv("DUCKDB_PATH", str(DUCKDB_FILE)))
    read_db = _load_market_db(market_path, read_only=True)
    try:
        experiments = read_db.list_experiments().to_dicts()
    finally:
        read_db.close()

    sessions = await _load_walk_forward_sessions()
    preserved_walk_forward_exp_ids = _collect_preserved_walk_forward_exp_ids(
        sessions,
        wf_start=args.wf_start,
        wf_end=args.wf_end,
    )

    keep, delete = _build_retention_plan(
        experiments,
        retain_start=args.retain_start,
        retain_end=args.retain_end,
        preserved_walk_forward_exp_ids=preserved_walk_forward_exp_ids,
    )

    print(
        json.dumps(
            {
                "total_experiments": len(experiments),
                "kept": len(keep),
                "deleted": len(delete),
                "retain_window": {
                    "start": args.retain_start.isoformat(),
                    "end": args.retain_end.isoformat(),
                },
                "walk_forward_window": {
                    "start": args.wf_start.isoformat(),
                    "end": args.wf_end.isoformat(),
                },
                "preserved_walk_forward_exp_ids": sorted(preserved_walk_forward_exp_ids),
                "delete_sample": [row.exp_id for row in delete[:20]],
            },
            indent=2,
        )
    )

    if not delete:
        print("[RETENTION] Nothing to prune.")
        return

    if not args.apply:
        print("[RETENTION] Dry-run complete. Re-run with --apply to delete.")
        return

    exp_ids = [row.exp_id for row in delete]

    if not args.duckdb_only:
        store = _validate_external_targets()
        pg_ids = _load_postgres_run_ids()
        present_pg_ids = [exp_id for exp_id in exp_ids if exp_id in pg_ids]
        minio_ids = [exp_id for exp_id in exp_ids if _has_minio_artifacts(exp_id, store)]
        deleted_pg = _delete_postgres(present_pg_ids)
        deleted_minio = _delete_minio(minio_ids, store)
        print(f"[RETENTION] Postgres exp_run rows deleted: {deleted_pg}")
        print(f"[RETENTION] MinIO objects deleted: {deleted_minio}")

    write_db = _load_market_db(market_path, read_only=False)
    try:
        deleted_duckdb = _delete_duckdb(write_db, exp_ids)
        write_db.refresh_backtest_read_snapshot()
    finally:
        write_db.close()

    print(f"[RETENTION] DuckDB experiments deleted: {deleted_duckdb}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
