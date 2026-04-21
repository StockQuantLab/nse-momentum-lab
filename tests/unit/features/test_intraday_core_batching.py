from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest

from nse_momentum_lab.features.intraday_core import (
    _build_feat_intraday_core_yearly,
    _build_parquet_manifest,
    _build_symbol_source_select,
    _iter_symbol_batches,
    _split_symbols_with_required_parquet,
)


def test_iter_symbol_batches_splits_symbols_evenly() -> None:
    batches = _iter_symbol_batches(["AAA", "BBB", "CCC", "DDD", "EEE"], 2)

    assert batches == [["AAA", "BBB"], ["CCC", "DDD"], ["EEE"]]


def test_split_symbols_with_required_parquet_requires_daily_and_5min(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "parquet"
    (parquet_dir / "5min" / "AAA").mkdir(parents=True)
    (parquet_dir / "5min" / "AAA" / "2025.parquet").write_text("x")
    (parquet_dir / "daily" / "AAA").mkdir(parents=True)
    (parquet_dir / "daily" / "AAA" / "all.parquet").write_text("x")
    (parquet_dir / "5min" / "BBB").mkdir(parents=True)
    (parquet_dir / "5min" / "BBB" / "2025.parquet").write_text("x")

    buildable, missing = _split_symbols_with_required_parquet(parquet_dir, ["AAA", "BBB"])

    assert buildable == ["AAA"]
    assert missing == ["BBB"]


def test_build_symbol_source_select_uses_symbol_specific_parquets(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir(parents=True)

    sql = _build_symbol_source_select(
        parquet_dir=parquet_dir,
        subdir="5min",
        symbols=["AAA", "BBB"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        fallback_view="v_5min",
    )

    assert "read_parquet([" in sql
    assert "5min/AAA/*.parquet" in sql
    assert "5min/BBB/*.parquet" in sql
    assert "date >= DATE '2025-01-01'" in sql
    assert "date <= DATE '2025-12-31'" in sql


def test_legacy_yearly_intraday_helper_is_hard_gated() -> None:
    with pytest.raises(RuntimeError, match="Legacy feat_intraday_core yearly rebuild is disabled"):
        _build_feat_intraday_core_yearly(object(), dataset_hash="deadbeef")


def test_build_parquet_manifest_returns_none_for_missing_dir(tmp_path: Path) -> None:
    result = _build_parquet_manifest(tmp_path / "nonexistent")
    assert result is None


def test_build_parquet_manifest_maps_symbols_to_files(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "parquet"
    (parquet_dir / "5min" / "RELIANCE").mkdir(parents=True)
    (parquet_dir / "5min" / "RELIANCE" / "2025.parquet").write_text("x")
    (parquet_dir / "5min" / "TCS").mkdir(parents=True)
    (parquet_dir / "5min" / "TCS" / "2024.parquet").write_text("x")
    (parquet_dir / "daily" / "RELIANCE").mkdir(parents=True)
    (parquet_dir / "daily" / "RELIANCE" / "all.parquet").write_text("x")

    manifest = _build_parquet_manifest(parquet_dir)

    assert manifest is not None
    assert "RELIANCE" in manifest["five_min"]
    assert "TCS" in manifest["five_min"]
    assert "RELIANCE" in manifest["daily"]
    assert "TCS" not in manifest["daily"]
    assert manifest["symbols_5min"] == ["RELIANCE", "TCS"]
    assert manifest["symbols_daily"] == ["RELIANCE"]


def test_split_symbols_with_manifest_uses_dict_lookups() -> None:
    manifest = {
        "five_min": {"AAA": ["5min/AAA/2025.parquet"], "BBB": ["5min/BBB/2025.parquet"]},
        "daily": {"AAA": ["daily/AAA/all.parquet"]},
        "symbols_5min": ["AAA", "BBB"],
        "symbols_daily": ["AAA"],
    }

    buildable, missing = _split_symbols_with_required_parquet(
        Path("/nonexistent"), ["AAA", "BBB", "CCC"], manifest
    )

    assert buildable == ["AAA"]
    assert missing == ["BBB", "CCC"]


def test_build_symbol_source_select_with_manifest_uses_explicit_paths(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()
    five_min_dir = parquet_dir / "5min" / "AAA"
    five_min_dir.mkdir(parents=True)
    (five_min_dir / "2025.parquet").write_text("x")
    (five_min_dir / "2024.parquet").write_text("x")
    bbb_dir = parquet_dir / "5min" / "BBB"
    bbb_dir.mkdir(parents=True)
    (bbb_dir / "2025.parquet").write_text("x")

    manifest = _build_parquet_manifest(parquet_dir)
    assert manifest is not None

    sql = _build_symbol_source_select(
        parquet_dir=parquet_dir,
        subdir="5min",
        symbols=["AAA", "BBB"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        fallback_view="v_5min",
        manifest=manifest,
    )

    assert "read_parquet([" in sql
    assert "AAA" in sql
    assert "BBB" in sql
    assert "*.parquet" not in sql
    assert "date >= DATE '2025-01-01'" in sql
    # Verify explicit file paths are present (not glob patterns)
    for symbol in ["AAA", "BBB"]:
        assert f"5min{os.sep}{symbol}" in sql or f"5min/{symbol}" in sql
