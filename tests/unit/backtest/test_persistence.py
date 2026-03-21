from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from nse_momentum_lab.services.backtest.persistence import (
    BacktestArtifactPublisher,
    ExperimentArtifact,
    build_strategy_hash,
    upsert_exp_run_with_artifacts_sync,
)


class _FakeStore:
    def __init__(self) -> None:
        self.put_bytes_calls: list[tuple[str, str, bytes, str]] = []
        self.put_file_calls: list[tuple[str, str, str, str | None]] = []

    def put_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> str:
        self.put_bytes_calls.append((bucket, object_name, data, content_type))
        return f"s3://{bucket}/{object_name}"

    def put_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        *,
        content_type: str | None = None,
    ) -> str:
        self.put_file_calls.append((bucket, object_name, str(file_path), content_type))
        return f"s3://{bucket}/{object_name}"


def test_build_strategy_hash_is_deterministic() -> None:
    h1 = build_strategy_hash("Indian2LYNCH", "params_hash_1")
    h2 = build_strategy_hash("Indian2LYNCH", "params_hash_1")
    assert h1 == h2


def test_publish_run_artifacts() -> None:
    store = _FakeStore()
    publisher = BacktestArtifactPublisher(store=store)
    trades_df = pl.DataFrame([{"symbol": "TCS", "pnl_pct": 1.2}])
    yearly_df = pl.DataFrame([{"year": 2024, "return_pct": 12.3}])
    equity_df = pl.DataFrame([{"entry_date": "2024-01-01", "cumulative_return_pct": 1.2}])

    artifacts = publisher.publish_run_artifacts(
        exp_id="exp123",
        trades_df=trades_df,
        yearly_df=yearly_df,
        equity_df=equity_df,
        summary={"exp_id": "exp123"},
    )

    names = {a.artifact_name for a in artifacts}
    assert len(artifacts) == 7
    assert "summary.json" in names
    assert "trades.csv" in names
    assert "trades.parquet" in names
    assert "yearly_metrics.csv" in names
    assert "yearly_metrics.parquet" in names
    assert "equity_curve.csv" in names
    assert "equity_curve.parquet" in names
    assert len(store.put_bytes_calls) == 7


def test_publish_duckdb_snapshot(tmp_path) -> None:
    store = _FakeStore()
    publisher = BacktestArtifactPublisher(store=store)
    snapshot_file = tmp_path / "market.duckdb"
    snapshot_file.write_bytes(b"duckdb-snapshot")

    artifact = publisher.publish_duckdb_snapshot(
        exp_id="exp123",
        dataset_hash="abc123",
        snapshot_path=snapshot_file,
    )

    assert artifact.artifact_name == "snapshots/market_abc123.duckdb"
    assert artifact.uri.endswith("/experiments/exp123/snapshots/market_abc123.duckdb")
    assert len(store.put_file_calls) == 1


def test_upsert_exp_run_with_artifacts_sync(monkeypatch) -> None:
    class _DummySettings:
        database_url = "postgresql://user:pass@localhost:5432/test"

    class _DummyCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        def execute(self, sql, params=None) -> None:
            self.calls.append((str(sql), params))

        def fetchone(self):
            return (11,)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class _DummyConn:
        def __init__(self) -> None:
            self.cursor_obj = _DummyCursor()
            self.committed = False

        def cursor(self):
            return self.cursor_obj

        def commit(self) -> None:
            self.committed = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    dummy_conn = _DummyConn()

    monkeypatch.setattr(
        "nse_momentum_lab.services.backtest.persistence.get_settings",
        lambda: _DummySettings(),
    )
    monkeypatch.setattr(
        "nse_momentum_lab.services.backtest.persistence.psycopg.connect",
        lambda _url: dummy_conn,
    )

    exp_run_id = upsert_exp_run_with_artifacts_sync(
        exp_hash="exp123",
        strategy_name="Indian2LYNCH",
        strategy_hash="strat_hash",
        dataset_hash="dataset_hash",
        params_json='{"a":1}',
        code_sha="code_hash",
        status="SUCCEEDED",
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        metrics={"total_return_pct": 10.5},
        artifacts=[
            ExperimentArtifact(
                artifact_name="summary.json",
                uri="s3://artifacts/experiments/exp123/summary.json",
                sha256="abc",
                size_bytes=10,
            )
        ],
    )

    assert exp_run_id == 11
    assert dummy_conn.committed is True
    assert any("INSERT INTO nseml.exp_run" in sql for sql, _ in dummy_conn.cursor_obj.calls)
    assert any("INSERT INTO nseml.exp_metric" in sql for sql, _ in dummy_conn.cursor_obj.calls)
    assert any("INSERT INTO nseml.exp_artifact" in sql for sql, _ in dummy_conn.cursor_obj.calls)
