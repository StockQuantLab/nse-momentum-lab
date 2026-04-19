from __future__ import annotations

import os
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

os.environ.setdefault("POLARS_IGNORE_TIMEZONE_PARSE_ERROR", "1")

import polars as pl

from nse_momentum_lab.services.kite.writer import KiteWriter


def _fetch_daily_empty(fetch_calls: list[tuple[object, object]], start: object, end: object) -> pl.DataFrame:
    fetch_calls.append((start, end))
    return pl.DataFrame()


def _fetch_daily_frame(
    fetch_calls: list[tuple[object, object]], start: object, end: object
) -> pl.DataFrame:
    fetch_calls.append((start, end))
    return pl.DataFrame(
        {
            "symbol": ["RELIANCE"],
            "date": [date(2026, 3, 26)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
        }
    )


def test_normalize_5min_frame_uses_naive_ist_timestamp() -> None:
    writer = KiteWriter()
    sample = pl.DataFrame(
        {
            "symbol": ["RELIANCE"],
            "date": [date(2026, 3, 23)],
            "candle_time": [datetime(2026, 3, 23, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
        }
    )

    frame = writer._normalize_5min_frame(sample)

    assert frame.schema["candle_time"] == pl.Datetime("ns")
    assert frame["candle_time"][0].isoformat() == "2026-03-23T09:15:00"


def test_normalize_ist_candle_time_converts_legacy_offset_to_naive_ist() -> None:
    writer = KiteWriter()
    frame = pl.DataFrame(
        {
            "symbol": ["RELIANCE"],
            "date": [date(2026, 3, 23)],
            "candle_time": [datetime(2026, 3, 23, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
        }
    )

    normalized = writer._normalize_ist_candle_time(frame)

    assert normalized.schema["candle_time"] == pl.Datetime("ns")
    assert normalized["candle_time"][0].isoformat() == "2026-03-23T09:15:00"


def test_fetch_and_write_daily_skips_already_ingested_backfill(monkeypatch) -> None:
    fetch_calls: list[tuple[object, object]] = []
    writer = KiteWriter(
        fetcher=SimpleNamespace(
            fetch_daily_ohlcv=lambda symbol, start, end: _fetch_daily_empty(fetch_calls, start, end)
        )
    )
    monkeypatch.setattr(
        writer,
        "_get_existing_date_range",
        lambda **kwargs: (date(2026, 3, 1), date(2026, 3, 25)),
    )

    written = writer.fetch_and_write_daily(
        symbol="RELIANCE",
        start_date=date(2026, 3, 20),
        end_date=date(2026, 3, 25),
    )

    assert written == 0
    assert fetch_calls == []


def test_fetch_and_write_daily_advances_start_date_for_partial_backfill(monkeypatch) -> None:
    fetch_calls: list[tuple[object, object]] = []
    writer = KiteWriter(
        fetcher=SimpleNamespace(
            fetch_daily_ohlcv=lambda symbol, start, end: _fetch_daily_frame(fetch_calls, start, end)
        )
    )
    monkeypatch.setattr(
        writer,
        "_get_existing_date_range",
        lambda **kwargs: (date(2026, 3, 1), date(2026, 3, 25)),
    )
    monkeypatch.setattr(writer, "write_daily", lambda symbol, df, mode="append": df.height)

    written = writer.fetch_and_write_daily(
        symbol="RELIANCE",
        start_date=date(2026, 3, 20),
        end_date=date(2026, 3, 30),
    )

    assert written == 1
    assert fetch_calls == [(date(2026, 3, 26), date(2026, 3, 30))]


def test_fetch_and_write_daily_skips_same_day_rerun_when_already_present(monkeypatch) -> None:
    fetch_calls: list[tuple[object, object]] = []
    writer = KiteWriter(
        fetcher=SimpleNamespace(
            fetch_daily_ohlcv=lambda symbol, start, end: _fetch_daily_empty(fetch_calls, start, end)
        )
    )
    monkeypatch.setattr(
        writer,
        "_get_existing_date_range",
        lambda **kwargs: (date(2026, 3, 1), date(2026, 3, 30)),
    )

    written = writer.fetch_and_write_daily(
        symbol="RELIANCE",
        start_date=date(2026, 3, 30),
        end_date=date(2026, 3, 30),
    )

    assert written == 0
    assert fetch_calls == []


def test_normalize_5min_frame_filters_out_session_candles() -> None:
    writer = KiteWriter()
    sample = pl.DataFrame(
        {
            "symbol": ["RELIANCE"] * 4,
            "date": [date(2026, 3, 23)] * 4,
            "candle_time": [
                datetime(2026, 3, 23, 9, 10),   # before session start
                datetime(2026, 3, 23, 9, 15),   # session start
                datetime(2026, 3, 23, 15, 25),  # session end
                datetime(2026, 3, 23, 15, 35),  # after session end
            ],
            "open": [1.0, 2.0, 3.0, 4.0],
            "high": [1.0, 2.0, 3.0, 4.0],
            "low": [1.0, 2.0, 3.0, 4.0],
            "close": [1.0, 2.0, 3.0, 4.0],
            "volume": [1, 1, 1, 1],
        }
    )

    frame = writer._normalize_5min_frame(sample)

    assert frame.height == 2
    times = frame["candle_time"].dt.time().to_list()
    assert times[0] == datetime(2026, 3, 23, 9, 15).time()
    assert times[1] == datetime(2026, 3, 23, 15, 25).time()
