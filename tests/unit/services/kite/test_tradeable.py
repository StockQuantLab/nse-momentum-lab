from __future__ import annotations

from pathlib import Path

from nse_momentum_lab.services.kite.tradeable import get_dead_symbols, get_parquet_symbols


def test_get_parquet_symbols_supports_daily_and_five_min_layouts(tmp_path: Path) -> None:
    daily_dir = tmp_path / "daily"
    five_min_dir = tmp_path / "5min"

    (daily_dir / "AAA").mkdir(parents=True)
    (daily_dir / "AAA" / "all.parquet").write_text("")
    (daily_dir / "BBB").mkdir(parents=True)
    (daily_dir / "BBB" / "kite.parquet").write_text("")
    (five_min_dir / "CCC").mkdir(parents=True)
    (five_min_dir / "CCC" / "2015.parquet").write_text("")
    (five_min_dir / "DDD" / "nested").mkdir(parents=True)
    (five_min_dir / "DDD" / "nested" / "2016.parquet").write_text("")

    assert get_parquet_symbols(daily_dir, layout="daily") == {"AAA", "BBB"}
    assert get_parquet_symbols(five_min_dir, layout="5min") == {"CCC", "DDD"}


def test_get_dead_symbols_honors_layout(tmp_path: Path) -> None:
    daily_dir = tmp_path / "daily"
    five_min_dir = tmp_path / "5min"

    (daily_dir / "AAA").mkdir(parents=True)
    (daily_dir / "AAA" / "all.parquet").write_text("")
    (daily_dir / "BBB").mkdir(parents=True)
    (daily_dir / "BBB" / "all.parquet").write_text("")
    (five_min_dir / "CCC").mkdir(parents=True)
    (five_min_dir / "CCC" / "2015.parquet").write_text("")

    tradeable = {"AAA", "CCC"}

    assert get_dead_symbols(daily_dir, tradeable, layout="daily") == {"BBB"}
    assert get_dead_symbols(five_min_dir, tradeable, layout="5min") == set()
