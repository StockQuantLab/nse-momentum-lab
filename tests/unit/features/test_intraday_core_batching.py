from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from nse_momentum_lab.features.intraday_core import (
    _build_feat_intraday_core_yearly,
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
