from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

from nse_momentum_lab.services.dataset.manifest import (
    DatasetManifestPayload,
    DatasetManifestRepository,
    build_code_hash,
    build_manifest_payload_from_snapshot,
)


def test_build_code_hash_is_deterministic() -> None:
    h1 = build_code_hash("tag", {"a": 1, "b": 2})
    h2 = build_code_hash("tag", {"b": 2, "a": 1})
    assert h1 == h2


def test_build_code_hash_changes_on_inputs() -> None:
    h1 = build_code_hash("tag_a", {"x": 1})
    h2 = build_code_hash("tag_b", {"x": 1})
    assert h1 != h2


def test_build_manifest_payload_from_snapshot() -> None:
    snapshot = {
        "dataset_hash": "abc123",
        "daily_glob": "s3://market-data/parquet/daily/*/*.parquet",
        "daily": {
            "rows": 2000,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31",
        },
    }
    payload = build_manifest_payload_from_snapshot(
        dataset_kind="duckdb_market_daily",
        snapshot=snapshot,
        code_hash="codehash",
        params_hash="default",
    )
    assert payload.dataset_hash == "abc123"
    assert payload.row_count == 2000
    assert payload.min_trading_date == date(2024, 1, 1)
    assert payload.max_trading_date == date(2024, 12, 31)


def test_manifest_payload_normalization_defaults() -> None:
    payload = DatasetManifestPayload(
        dataset_kind="  kind  ",
        dataset_hash="  hash  ",
        code_hash="",
        params_hash="",
        source_uri=None,
        row_count=None,
        min_trading_date=None,
        max_trading_date=None,
        metadata_json={},
    ).normalized()
    assert payload.dataset_kind == "kind"
    assert payload.dataset_hash == "hash"
    assert payload.code_hash == "default"
    assert payload.params_hash == "default"


async def test_repository_upsert_insert_new_row() -> None:
    repo = DatasetManifestRepository()
    session = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=execute_result)
    session.flush = AsyncMock()
    session.add = MagicMock()

    payload = DatasetManifestPayload(
        dataset_kind="duckdb_market_daily",
        dataset_hash="hash",
        code_hash="code",
        params_hash="default",
        source_uri="s3://market-data/path",
        row_count=10,
        min_trading_date=date(2024, 1, 1),
        max_trading_date=date(2024, 1, 31),
        metadata_json={"k": "v"},
    )

    row = await repo.upsert(session, payload)
    session.add.assert_called_once()
    session.flush.assert_awaited_once()
    assert row.dataset_hash == "hash"


async def test_repository_upsert_updates_existing_row() -> None:
    repo = DatasetManifestRepository()
    session = AsyncMock()
    existing = MagicMock()
    existing.dataset_hash = "hash"
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = existing
    session.execute = AsyncMock(return_value=execute_result)
    session.flush = AsyncMock()

    payload = DatasetManifestPayload(
        dataset_kind="duckdb_market_daily",
        dataset_hash="hash",
        code_hash="code",
        params_hash="default",
        source_uri="s3://market-data/new-path",
        row_count=20,
        min_trading_date=date(2024, 2, 1),
        max_trading_date=date(2024, 2, 29),
        metadata_json={"n": "v"},
    )

    row = await repo.upsert(session, payload)
    session.flush.assert_awaited_once()
    assert row is existing
    assert existing.source_uri == "s3://market-data/new-path"
    assert existing.row_count == 20
