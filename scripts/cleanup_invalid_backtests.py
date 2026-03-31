#!/usr/bin/env python3
"""Clean invalid backtest runs across DuckDB, Postgres, and MinIO.

Default invalid criteria:
- run does not have ``entry_timeframe=5min``, or
- run is missing lineage/artifacts in Postgres/MinIO.

Use ``--apply`` to execute deletions. Without it, this script is dry-run only.
"""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.ingest.minio import MinioArtifactStore


@dataclass(frozen=True)
class InvalidRun:
    exp_id: str
    created_at: datetime | None
    reason: str


@dataclass(frozen=True)
class RunRecord:
    exp_id: str
    created_at: datetime | None
    status: str
    params: dict[str, Any]


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


def _list_runs() -> list[RunRecord]:
    db = get_market_db()
    rows = db.list_experiments().iter_rows(named=True)
    out: list[RunRecord] = []
    for row in rows:
        exp_id = str(row["exp_id"])
        exp = db.get_experiment(exp_id)
        if exp is None:
            continue
        out.append(
            RunRecord(
                exp_id=exp_id,
                created_at=row.get("created_at"),
                status=str(row.get("status") or ""),
                params=_parse_params(str(exp.get("params_json") or "")),
            )
        )
    return out


def _validate_external_targets() -> MinioArtifactStore:
    settings = get_settings()
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required for Postgres cleanup.")

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

    return MinioArtifactStore()


def _load_postgres_run_ids() -> set[str]:
    settings = get_settings()
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required for Postgres cleanup.")

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
        raise RuntimeError("DATABASE_URL is required for Postgres cleanup.")

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


def _delete_duckdb(exp_ids: list[str]) -> int:
    db = get_market_db()
    deleted = 0
    for exp_id in exp_ids:
        if db.experiment_exists(exp_id):
            db.delete_experiment(exp_id)
            deleted += 1
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete invalid legacy backtest runs.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletions. Without this flag the script is dry-run.",
    )
    parser.add_argument(
        "--duckdb-only",
        action="store_true",
        help="Only clean DuckDB bt_* tables; skip Postgres/MinIO cleanup.",
    )
    parser.add_argument(
        "--skip-external-lineage-check",
        action="store_true",
        help="Skip checking for matching Postgres+MinIO lineage/artifacts.",
    )
    parser.add_argument(
        "--exp-id",
        action="append",
        default=[],
        help="Delete explicit experiment ID(s), regardless of auto-detection rules.",
    )
    args = parser.parse_args()

    runs = _list_runs()
    run_by_id = {r.exp_id: r for r in runs}
    invalid_map: dict[str, InvalidRun] = {}

    for run in runs:
        status = run.status.lower().strip()
        if status != "completed":
            invalid_map[run.exp_id] = InvalidRun(
                exp_id=run.exp_id,
                created_at=run.created_at,
                reason=f"status={run.status or 'unknown'}",
            )

        entry_timeframe = str(run.params.get("entry_timeframe") or "").lower().strip()
        if entry_timeframe != "5min":
            reason = (
                "missing entry_timeframe"
                if not entry_timeframe
                else f"entry_timeframe={entry_timeframe}"
            )
            invalid_map[run.exp_id] = InvalidRun(
                exp_id=run.exp_id,
                created_at=run.created_at,
                reason=reason,
            )

    store: MinioArtifactStore | None = None
    if not args.duckdb_only and not args.skip_external_lineage_check:
        store = _validate_external_targets()
        pg_ids = _load_postgres_run_ids()
        for run in runs:
            has_pg = run.exp_id in pg_ids
            has_minio = _has_minio_artifacts(run.exp_id, store)
            if not has_pg or not has_minio:
                invalid_map[run.exp_id] = InvalidRun(
                    exp_id=run.exp_id,
                    created_at=run.created_at,
                    reason="missing lineage in postgres/minio",
                )

    if args.exp_id:
        explicit_invalid: dict[str, InvalidRun] = {}
        for exp_id in args.exp_id:
            record = run_by_id.get(exp_id)
            explicit_invalid[exp_id] = InvalidRun(
                exp_id=exp_id,
                created_at=record.created_at if record else None,
                reason="explicit selection",
            )
        invalid_map = explicit_invalid

    invalid_runs = sorted(invalid_map.values(), key=lambda r: (r.created_at is None, r.created_at))
    if not invalid_runs:
        print("[CLEANUP] No invalid runs found.")
        return

    print("[CLEANUP] Invalid runs:")
    for run in invalid_runs:
        print(f"  - {run.exp_id} | created_at={run.created_at} | {run.reason}")

    if not args.apply:
        print("\n[CLEANUP] Dry-run complete. Re-run with --apply to delete.")
        return

    exp_ids = [r.exp_id for r in invalid_runs]

    if not args.duckdb_only:
        if store is None:
            store = _validate_external_targets()
        deleted_pg = _delete_postgres(exp_ids)
        deleted_minio = _delete_minio(exp_ids, store)
        print(f"[CLEANUP] Postgres exp_run rows deleted: {deleted_pg}")
        print(f"[CLEANUP] MinIO objects deleted: {deleted_minio}")

    deleted_duckdb = _delete_duckdb(exp_ids)
    print(f"[CLEANUP] DuckDB experiments deleted: {deleted_duckdb}")
    print("[CLEANUP] Done.")


if __name__ == "__main__":
    main()
