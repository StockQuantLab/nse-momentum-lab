from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import polars as pl
import psycopg

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.services.ingest.minio import MinioArtifactStore
from nse_momentum_lab.utils import compute_short_hash


@dataclass(frozen=True)
class ExperimentArtifact:
    artifact_name: str
    uri: str
    sha256: str
    size_bytes: int


def build_strategy_hash(strategy_name: str, params_hash: str) -> str:
    return compute_short_hash(
        {"strategy_name": strategy_name, "params_hash": params_hash}, length=16
    )


class BacktestArtifactPublisher:
    """Publishes backtest artifacts to MinIO object storage."""

    def __init__(self, store: MinioArtifactStore | None = None) -> None:
        self.store = store or MinioArtifactStore()

    @staticmethod
    def _sha256(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _sha256_file(file_path: Path) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _to_csv_bytes(df: pl.DataFrame) -> bytes:
        return df.write_csv().encode("utf-8")

    @staticmethod
    def _to_parquet_bytes(df: pl.DataFrame) -> bytes:
        out = BytesIO()
        df.write_parquet(out)
        return out.getvalue()

    def _publish_bytes(
        self,
        *,
        exp_id: str,
        object_name: str,
        data: bytes,
        content_type: str,
    ) -> ExperimentArtifact:
        path = f"experiments/{exp_id}/{object_name}"
        uri = self.store.put_bytes(
            MinioArtifactStore.BUCKET_ARTIFACTS,
            path,
            data,
            content_type=content_type,
        )
        return ExperimentArtifact(
            artifact_name=object_name,
            uri=uri,
            sha256=self._sha256(data),
            size_bytes=len(data),
        )

    def publish_run_artifacts(
        self,
        *,
        exp_id: str,
        trades_df: pl.DataFrame,
        yearly_df: pl.DataFrame,
        equity_df: pl.DataFrame,
        summary: dict[str, Any],
    ) -> list[ExperimentArtifact]:
        artifacts: list[ExperimentArtifact] = []

        summary_bytes = json.dumps(summary, sort_keys=True, indent=2).encode("utf-8")
        artifacts.append(
            self._publish_bytes(
                exp_id=exp_id,
                object_name="summary.json",
                data=summary_bytes,
                content_type="application/json",
            )
        )

        for object_name, df in [
            ("trades", trades_df),
            ("yearly_metrics", yearly_df),
            ("equity_curve", equity_df),
        ]:
            artifacts.append(
                self._publish_bytes(
                    exp_id=exp_id,
                    object_name=f"{object_name}.csv",
                    data=self._to_csv_bytes(df),
                    content_type="text/csv",
                )
            )
            artifacts.append(
                self._publish_bytes(
                    exp_id=exp_id,
                    object_name=f"{object_name}.parquet",
                    data=self._to_parquet_bytes(df),
                    content_type="application/octet-stream",
                )
            )

        return artifacts

    def publish_duckdb_snapshot(
        self,
        *,
        exp_id: str,
        dataset_hash: str,
        snapshot_path: Path,
    ) -> ExperimentArtifact:
        object_name = f"snapshots/market_{dataset_hash}.duckdb"
        path = f"experiments/{exp_id}/{object_name}"
        uri = self.store.put_file(
            MinioArtifactStore.BUCKET_ARTIFACTS,
            path,
            snapshot_path,
            content_type="application/octet-stream",
        )
        return ExperimentArtifact(
            artifact_name=object_name,
            uri=uri,
            sha256=self._sha256_file(snapshot_path),
            size_bytes=snapshot_path.stat().st_size,
        )


def _ensure_experiment_tables_sync(cur: psycopg.Cursor) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS nseml")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nseml.exp_run (
          exp_run_id bigserial PRIMARY KEY,
          exp_hash text NOT NULL UNIQUE,
          strategy_name text NOT NULL,
          strategy_hash text NOT NULL,
          dataset_hash text NOT NULL,
          params_json jsonb NOT NULL,
          code_sha text,
          started_at timestamptz,
          finished_at timestamptz,
          status text NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nseml.exp_metric (
          exp_run_id bigint NOT NULL REFERENCES nseml.exp_run(exp_run_id) ON DELETE CASCADE,
          metric_name text NOT NULL,
          metric_value numeric,
          PRIMARY KEY(exp_run_id, metric_name)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nseml.exp_artifact (
          exp_run_id bigint NOT NULL REFERENCES nseml.exp_run(exp_run_id) ON DELETE CASCADE,
          artifact_name text NOT NULL,
          uri text NOT NULL,
          sha256 text,
          PRIMARY KEY(exp_run_id, artifact_name)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exp_run_strategy_dataset
          ON nseml.exp_run(strategy_hash, dataset_hash)
        """
    )
    cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS progress_stage text")
    cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS progress_message text")
    cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS progress_pct numeric")
    cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exp_run_status_heartbeat
          ON nseml.exp_run(status, heartbeat_at DESC)
        """
    )


def upsert_exp_run_with_artifacts_sync(
    *,
    exp_hash: str,
    strategy_name: str,
    strategy_hash: str,
    dataset_hash: str,
    params_json: str,
    code_sha: str,
    status: str,
    started_at: datetime,
    finished_at: datetime | None,
    metrics: dict[str, float],
    artifacts: list[ExperimentArtifact],
    progress_stage: str | None = None,
    progress_message: str | None = None,
    progress_pct: float | None = None,
    heartbeat_at: datetime | None = None,
) -> int:
    settings = get_settings()
    if settings.database_url is None:
        raise ValueError("database_url is not configured")

    run_sql = """
    INSERT INTO nseml.exp_run
      (exp_hash, strategy_name, strategy_hash, dataset_hash, params_json,
       code_sha, started_at, finished_at, status,
       progress_stage, progress_message, progress_pct, heartbeat_at)
    VALUES
      (%(exp_hash)s, %(strategy_name)s, %(strategy_hash)s, %(dataset_hash)s, %(params_json)s::jsonb,
       %(code_sha)s, %(started_at)s, %(finished_at)s, %(status)s,
       %(progress_stage)s, %(progress_message)s, %(progress_pct)s, %(heartbeat_at)s)
    ON CONFLICT (exp_hash)
    DO UPDATE SET
      strategy_name = EXCLUDED.strategy_name,
      strategy_hash = EXCLUDED.strategy_hash,
      dataset_hash = EXCLUDED.dataset_hash,
      params_json = EXCLUDED.params_json,
      code_sha = EXCLUDED.code_sha,
      started_at = EXCLUDED.started_at,
      finished_at = EXCLUDED.finished_at,
      status = EXCLUDED.status,
      progress_stage = EXCLUDED.progress_stage,
      progress_message = EXCLUDED.progress_message,
      progress_pct = EXCLUDED.progress_pct,
      heartbeat_at = EXCLUDED.heartbeat_at
    RETURNING exp_run_id;
    """

    metric_sql = """
    INSERT INTO nseml.exp_metric (exp_run_id, metric_name, metric_value)
    VALUES (%(exp_run_id)s, %(metric_name)s, %(metric_value)s)
    ON CONFLICT (exp_run_id, metric_name)
    DO UPDATE SET metric_value = EXCLUDED.metric_value;
    """

    artifact_sql = """
    INSERT INTO nseml.exp_artifact (exp_run_id, artifact_name, uri, sha256)
    VALUES (%(exp_run_id)s, %(artifact_name)s, %(uri)s, %(sha256)s)
    ON CONFLICT (exp_run_id, artifact_name)
    DO UPDATE SET
      uri = EXCLUDED.uri,
      sha256 = EXCLUDED.sha256;
    """

    run_params = {
        "exp_hash": exp_hash,
        "strategy_name": strategy_name,
        "strategy_hash": strategy_hash,
        "dataset_hash": dataset_hash,
        "params_json": params_json,
        "code_sha": code_sha,
        "started_at": started_at.astimezone(UTC),
        "finished_at": finished_at.astimezone(UTC) if finished_at else None,
        "status": status,
        "progress_stage": progress_stage,
        "progress_message": progress_message,
        "progress_pct": float(progress_pct) if progress_pct is not None else None,
        "heartbeat_at": (heartbeat_at or datetime.now(UTC)).astimezone(UTC),
    }

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            _ensure_experiment_tables_sync(cur)
            cur.execute(run_sql, run_params)
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Failed to upsert exp_run")
            exp_run_id = int(row[0])

            cur.execute("DELETE FROM nseml.exp_metric WHERE exp_run_id = %s", (exp_run_id,))
            for metric_name, metric_value in metrics.items():
                cur.execute(
                    metric_sql,
                    {
                        "exp_run_id": exp_run_id,
                        "metric_name": metric_name,
                        "metric_value": float(metric_value),
                    },
                )

            cur.execute("DELETE FROM nseml.exp_artifact WHERE exp_run_id = %s", (exp_run_id,))
            for artifact in artifacts:
                cur.execute(
                    artifact_sql,
                    {
                        "exp_run_id": exp_run_id,
                        "artifact_name": artifact.artifact_name,
                        "uri": artifact.uri,
                        "sha256": artifact.sha256,
                    },
                )
        conn.commit()
    return exp_run_id
