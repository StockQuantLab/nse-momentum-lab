from __future__ import annotations

from pathlib import Path


def test_iter_candles_csv_day(tmp_path: Path) -> None:
    from nse_momentum_lab.services.ingest.candle_csv import iter_candles_csv

    p = tmp_path / "SBIN.csv"
    p.write_text(
        "Date,Open,High,Low,Close,Volume\n"
        "2026-02-05,500,510,495,508,1000\n"
        "2026-02-06,508,515,505,512,1200\n",
        encoding="utf-8",
    )

    rows = list(iter_candles_csv(p, timeframe="day"))
    assert len(rows) == 2
    assert rows[0].trading_date.isoformat() == "2026-02-05"
    assert rows[0].ts is None
    assert rows[0].open == 500.0
    assert rows[0].volume == 1000


def test_iter_candles_csv_minute_and_aggregate(tmp_path: Path) -> None:
    from nse_momentum_lab.services.ingest.candle_csv import aggregate_to_daily, iter_candles_csv

    p = tmp_path / "SBIN_1min.csv"
    p.write_text(
        "Date,Open,High,Low,Close,Volume\n"
        "2026-02-06 09:15:00,500,501,499,500.5,100\n"
        "2026-02-06 09:16:00,500.5,503,500,502,150\n"
        "2026-02-06 15:29:00,502,504,501,503,200\n"
        "2026-02-07 09:15:00,503,506,502,505,50\n",
        encoding="utf-8",
    )

    rows = list(iter_candles_csv(p, timeframe="minute"))
    assert len(rows) == 4
    assert rows[0].ts is not None

    daily = aggregate_to_daily(rows)
    assert len(daily) == 2

    d0 = daily[0]
    assert d0.trading_date.isoformat() == "2026-02-06"
    assert d0.open == 500.0
    assert d0.high == 504.0
    assert d0.low == 499.0
    assert d0.close == 503.0
    assert d0.volume == 450


def test_iter_candles_csv_tsv_timezone_offset(tmp_path: Path) -> None:
    from nse_momentum_lab.services.ingest.candle_csv import aggregate_to_daily, iter_candles_csv

    # Matches the user-provided broker/dataset style: tab-separated, ISO timestamp with +0530 offset.
    p = tmp_path / "INFY.tsv"
    p.write_text(
        "Date\tOpen\tHigh\tLow\tClose\tVolume\n"
        "2015-04-01T09:15:00+0530\t2008\t2015\t2007.25\t2011.25\t6152\n"
        "2015-04-01T09:16:00+0530\t2011.15\t2011.15\t2004.45\t2007.5\t6729\n",
        encoding="utf-8",
    )

    rows = list(iter_candles_csv(p, timeframe="auto"))
    assert len(rows) == 2
    assert rows[0].ts is not None
    assert rows[0].trading_date.isoformat() == "2015-04-01"

    daily = aggregate_to_daily(rows)
    assert len(daily) == 1
    assert daily[0].open == 2008.0
    assert daily[0].high == 2015.0
    assert daily[0].low == 2004.45
    assert daily[0].close == 2007.5
    assert daily[0].volume == 6152 + 6729
