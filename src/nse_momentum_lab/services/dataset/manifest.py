from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.models import DatasetManifest
from nse_momentum_lab.utils import compute_short_hash


def build_code_hash(tag: str, extra: dict[str, Any] | None = None) -> str:
    """Build a code hash from tag and optional extra metadata."""
    return compute_short_hash({"tag": tag, "extra": extra or {}}, length=16)


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class DatasetManifestPayload:
    dataset_kind: str
    dataset_hash: str
    code_hash: str
    params_hash: str
    source_uri: str | None
    row_count: int | None
    min_trading_date: date | None
    max_trading_date: date | None
    metadata_json: dict[str, Any]

    def normalized(self) -> DatasetManifestPayload:
        return DatasetManifestPayload(
            dataset_kind=self.dataset_kind.strip(),
            dataset_hash=self.dataset_hash.strip(),
            code_hash=self.code_hash.strip() or "default",
            params_hash=self.params_hash.strip() or "default",
            source_uri=self.source_uri,
            row_count=self.row_count,
            min_trading_date=self.min_trading_date,
            max_trading_date=self.max_trading_date,
            metadata_json=self.metadata_json,
        )


def build_manifest_payload_from_snapshot(
    *,
    dataset_kind: str,
    snapshot: dict[str, Any],
    code_hash: str,
    params_hash: str = "default",
    source_uri: str | None = None,
) -> DatasetManifestPayload:
    daily = snapshot.get("daily", {})
    if not source_uri:
        source_uri = str(snapshot.get("daily_glob") or snapshot.get("five_min_glob") or "")

    return DatasetManifestPayload(
        dataset_kind=dataset_kind,
        dataset_hash=str(snapshot.get("dataset_hash", "")),
        code_hash=code_hash,
        params_hash=params_hash,
        source_uri=source_uri,
        row_count=int(daily.get("rows", 0)) if daily.get("rows") is not None else None,
        min_trading_date=_to_date(daily.get("min_date")),
        max_trading_date=_to_date(daily.get("max_date")),
        metadata_json=snapshot,
    ).normalized()


class DatasetManifestRepository:
    """Async repository for dataset manifest metadata."""

    @staticmethod
    async def _ensure_table(session: AsyncSession) -> None:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS nseml"))
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS nseml.dataset_manifest (
                  dataset_id bigserial PRIMARY KEY,
                  dataset_kind text NOT NULL,
                  dataset_hash text NOT NULL,
                  code_hash text,
                  params_hash text,
                  source_uri text,
                  row_count bigint,
                  min_trading_date date,
                  max_trading_date date,
                  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  UNIQUE(dataset_kind, dataset_hash, code_hash, params_hash)
                )
                """
            )
        )
        await session.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_dataset_manifest_kind_created
                ON nseml.dataset_manifest(dataset_kind, created_at)
                """
            )
        )

    async def upsert(
        self,
        session: AsyncSession,
        payload: DatasetManifestPayload,
    ) -> DatasetManifest:
        await self._ensure_table(session)
        p = payload.normalized()
        query = select(DatasetManifest).where(
            DatasetManifest.dataset_kind == p.dataset_kind,
            DatasetManifest.dataset_hash == p.dataset_hash,
            DatasetManifest.code_hash == p.code_hash,
            DatasetManifest.params_hash == p.params_hash,
        )
        result = await session.execute(query)
        existing = result.scalar_one_or_none()
        if existing is not None:
            existing.source_uri = p.source_uri
            existing.row_count = p.row_count
            existing.min_trading_date = p.min_trading_date
            existing.max_trading_date = p.max_trading_date
            existing.metadata_json = p.metadata_json
            await session.flush()
            return existing

        row = DatasetManifest(
            dataset_kind=p.dataset_kind,
            dataset_hash=p.dataset_hash,
            code_hash=p.code_hash,
            params_hash=p.params_hash,
            source_uri=p.source_uri,
            row_count=p.row_count,
            min_trading_date=p.min_trading_date,
            max_trading_date=p.max_trading_date,
            metadata_json=p.metadata_json,
        )
        session.add(row)
        await session.flush()
        return row

    async def list_manifests(
        self,
        session: AsyncSession,
        *,
        dataset_kind: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DatasetManifest], int]:
        count_q = select(func.count(DatasetManifest.dataset_id))
        data_q = select(DatasetManifest)
        if dataset_kind:
            count_q = count_q.where(DatasetManifest.dataset_kind == dataset_kind)
            data_q = data_q.where(DatasetManifest.dataset_kind == dataset_kind)

        total = (await session.execute(count_q)).scalar() or 0
        rows = (
            (
                await session.execute(
                    data_q.order_by(DatasetManifest.created_at.desc()).offset(offset).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return rows, int(total)

    async def get_latest(
        self,
        session: AsyncSession,
        *,
        dataset_kind: str,
    ) -> DatasetManifest | None:
        result = await session.execute(
            select(DatasetManifest)
            .where(DatasetManifest.dataset_kind == dataset_kind)
            .order_by(DatasetManifest.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


def upsert_dataset_manifest_sync(payload: DatasetManifestPayload) -> None:
    """Sync upsert helper for non-async flows (for example CLI backtest runner)."""
    p = payload.normalized()
    settings = get_settings()
    if settings.database_url is None:
        raise ValueError("database_url is not configured")

    sql = """
    INSERT INTO nseml.dataset_manifest
      (dataset_kind, dataset_hash, code_hash, params_hash, source_uri,
       row_count, min_trading_date, max_trading_date, metadata_json)
    VALUES
      (%(dataset_kind)s, %(dataset_hash)s, %(code_hash)s, %(params_hash)s, %(source_uri)s,
       %(row_count)s, %(min_trading_date)s, %(max_trading_date)s, %(metadata_json)s::jsonb)
    ON CONFLICT (dataset_kind, dataset_hash, code_hash, params_hash)
    DO UPDATE SET
      source_uri = EXCLUDED.source_uri,
      row_count = EXCLUDED.row_count,
      min_trading_date = EXCLUDED.min_trading_date,
      max_trading_date = EXCLUDED.max_trading_date,
      metadata_json = EXCLUDED.metadata_json;
    """
    params = {
        "dataset_kind": p.dataset_kind,
        "dataset_hash": p.dataset_hash,
        "code_hash": p.code_hash,
        "params_hash": p.params_hash,
        "source_uri": p.source_uri,
        "row_count": p.row_count,
        "min_trading_date": p.min_trading_date,
        "max_trading_date": p.max_trading_date,
        "metadata_json": json.dumps(p.metadata_json, sort_keys=True),
    }

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS nseml")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nseml.dataset_manifest (
                  dataset_id bigserial PRIMARY KEY,
                  dataset_kind text NOT NULL,
                  dataset_hash text NOT NULL,
                  code_hash text,
                  params_hash text,
                  source_uri text,
                  row_count bigint,
                  min_trading_date date,
                  max_trading_date date,
                  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  UNIQUE(dataset_kind, dataset_hash, code_hash, params_hash)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dataset_manifest_kind_created
                ON nseml.dataset_manifest(dataset_kind, created_at)
                """
            )
            cur.execute(sql, params)
        conn.commit()
