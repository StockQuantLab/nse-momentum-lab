from __future__ import annotations

from pathlib import Path

from nse_momentum_lab.db.versioned_replica_consumer import VersionedReplicaConsumer


def test_get_replica_path_uses_pointer_without_open_connection(tmp_path: Path) -> None:
    replica_dir = tmp_path / "paper_replica"
    replica_dir.mkdir()
    replica_path = replica_dir / "paper_replica_v3.duckdb"
    replica_path.write_text("stub")
    (replica_dir / "paper_replica_latest").write_text("v3")

    consumer = VersionedReplicaConsumer(replica_dir=replica_dir, prefix="paper_replica")

    assert consumer.get_latest_version() == 3
    assert consumer.get_replica_path() == replica_path
    assert consumer.get_stale_seconds() >= 0.0


def test_get_replica_path_scans_disk_when_pointer_missing(tmp_path: Path) -> None:
    replica_dir = tmp_path / "paper_replica"
    replica_dir.mkdir()
    (replica_dir / "paper_replica_v2.duckdb").write_text("old")
    newest = replica_dir / "paper_replica_v4.duckdb"
    newest.write_text("new")

    consumer = VersionedReplicaConsumer(replica_dir=replica_dir, prefix="paper_replica")

    assert consumer.get_latest_version() == 4
    assert consumer.get_replica_path() == newest
