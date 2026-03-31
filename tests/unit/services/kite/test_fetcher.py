from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import httpx
import polars as pl
import pytest

import nse_momentum_lab.services.kite.fetcher as fetcher_module
from nse_momentum_lab.services.kite.fetcher import KiteFetcher, TokenBucketRateLimiter


class _RetryingHistoricalClient:
    def __init__(self) -> None:
        self.calls = 0

    def historical_data(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            request = httpx.Request(
                "GET",
                "https://api.kite.trade/instruments/historical/101/day",
            )
            response = httpx.Response(
                429,
                request=request,
                headers={"Retry-After": "1.5"},
            )
            raise httpx.HTTPStatusError("Too Many Requests", request=request, response=response)
        return [{"date": "2026-03-21", "close": 100.0}]


class _AllowAllRateLimiter:
    def __init__(self) -> None:
        self.calls = 0

    def acquire(self) -> float:
        self.calls += 1
        return 0.0


def test_fetch_historical_data_retries_429_with_retry_after(monkeypatch) -> None:
    sleeps: list[float] = []

    monkeypatch.setattr(fetcher_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    client = _RetryingHistoricalClient()
    limiter = _AllowAllRateLimiter()
    fetcher = KiteFetcher(
        auth=SimpleNamespace(get_kite_client=lambda: client),
        historical_rate_limiter=limiter,
    )

    candles = fetcher._fetch_historical_data(
        instrument_token=101,
        interval="day",
        start_date=date(2026, 3, 21),
        end_date=date(2026, 3, 21),
    )

    assert client.calls == 2
    assert limiter.calls == 2
    assert candles == [{"date": "2026-03-21", "close": 100.0}]
    assert sleeps[0] >= 1.5


def test_token_bucket_rate_limiter_waits_until_tokens_refill(monkeypatch) -> None:
    now = {"value": 0.0}
    sleeps: list[float] = []

    monkeypatch.setattr(fetcher_module.time, "monotonic", lambda: now["value"])

    def _sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now["value"] += seconds

    monkeypatch.setattr(fetcher_module.time, "sleep", _sleep)

    limiter = TokenBucketRateLimiter(rate_per_second=2.0, burst_capacity=1.0)

    first_wait = limiter.acquire()
    second_wait = limiter.acquire()

    assert first_wait == 0.0
    assert second_wait == pytest.approx(0.5)
    assert sleeps == [pytest.approx(0.5)]


def test_normalize_daily_candles_returns_polars_frame() -> None:
    fetcher = KiteFetcher(auth=SimpleNamespace())

    frame = fetcher._normalize_daily_candles(
        "RELIANCE",
        [{"date": "2026-03-21T00:00:00+05:30", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}],
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.to_dicts() == [
        {
            "symbol": "RELIANCE",
            "date": date(2026, 3, 21),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10,
        }
    ]


def test_normalize_5min_candles_converts_utc_to_naive_ist() -> None:
    fetcher = KiteFetcher(auth=SimpleNamespace())

    frame = fetcher._normalize_5min_candles(
        "RELIANCE",
        [{"date": "2026-03-21T03:45:00+00:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}],
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.schema["candle_time"] == pl.Datetime("ns")
    assert frame["candle_time"][0].isoformat() == "2026-03-21T09:15:00"
