from __future__ import annotations

import logging
import random
import threading
import time
from datetime import date, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from nse_momentum_lab.services.kite.auth import KiteAuth, get_kite_auth
from nse_momentum_lab.services.kite.client import KiteAPIError

logger = logging.getLogger(__name__)

KITE_DAILY_INTERVAL = "day"
KITE_5MIN_INTERVAL = "5minute"
MAX_DAILY_CANDLES = 2000
MAX_5MIN_CANDLES_PER_CALL = 10080
MAX_5MIN_DAYS_PER_CHUNK = 60
HISTORICAL_REQUESTS_PER_SECOND = 2.85
HISTORICAL_RATE_LIMIT_BURST = 3.0
RATE_LIMIT_BACKOFF_BASE_SECONDS = 2.0
MAX_API_RETRIES = 5
RETRY_BASE_DELAY_SECONDS = 1.0
RETRY_MAX_DELAY_SECONDS = 30.0
RETRY_JITTER_SECONDS = 0.5
IST = ZoneInfo("Asia/Kolkata")


class TokenBucketRateLimiter:
    """Thread-safe token bucket limiter for Kite historical requests."""

    def __init__(self, rate_per_second: float, burst_capacity: float | None = None) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self.rate_per_second = float(rate_per_second)
        self.burst_capacity = float(burst_capacity or rate_per_second)
        if self.burst_capacity <= 0:
            raise ValueError("burst_capacity must be positive")
        self._tokens = self.burst_capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        if tokens <= 0:
            raise ValueError("tokens must be positive")

        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last_refill)
                self._last_refill = now
                self._tokens = min(
                    self.burst_capacity,
                    self._tokens + elapsed * self.rate_per_second,
                )
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited

                deficit = tokens - self._tokens
                wait_for = deficit / self.rate_per_second

            time.sleep(wait_for)
            waited += wait_for


HISTORICAL_RATE_LIMITER = TokenBucketRateLimiter(
    rate_per_second=HISTORICAL_REQUESTS_PER_SECOND,
    burst_capacity=HISTORICAL_RATE_LIMIT_BURST,
)


class KiteFetcher:
    def __init__(
        self,
        auth: KiteAuth | None = None,
        historical_rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        self.auth = auth or get_kite_auth()
        self._historical_rate_limiter = historical_rate_limiter or HISTORICAL_RATE_LIMITER

    def is_authenticated(self) -> bool:
        return self.auth.is_authenticated()

    def fetch_daily_ohlcv(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        instrument_token = self.auth.get_instrument_token(symbol, exchange)
        if instrument_token is None:
            logger.warning("No Kite instrument token found for %s on %s", symbol, exchange)
            return self._empty_daily_frame()

        candles = self._fetch_historical_data(
            instrument_token=instrument_token,
            interval=KITE_DAILY_INTERVAL,
            start_date=start_date,
            end_date=end_date,
        )
        return self._normalize_daily_candles(symbol, candles)

    def fetch_5min_ohlcv(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        instrument_token = self.auth.get_instrument_token(symbol, exchange)
        if instrument_token is None:
            logger.warning("No Kite instrument token found for %s on %s", symbol, exchange)
            return self._empty_5min_frame()

        frames: list[pd.DataFrame] = []
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = min(end_date, chunk_start + timedelta(days=MAX_5MIN_DAYS_PER_CHUNK - 1))
            candles = self._fetch_historical_data(
                instrument_token=instrument_token,
                interval=KITE_5MIN_INTERVAL,
                start_date=chunk_start,
                end_date=chunk_end,
            )
            frame = self._normalize_5min_candles(symbol, candles)
            if not frame.empty:
                frames.append(frame)
            chunk_start = chunk_end + timedelta(days=1)

        if not frames:
            return self._empty_5min_frame()

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "candle_time"], keep="last")
        combined = combined.sort_values(["symbol", "candle_time"], kind="stable").reset_index(
            drop=True
        )
        return combined

    def _fetch_historical_data(
        self,
        *,
        instrument_token: int,
        interval: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]] | list[list[Any]]:
        client = self.auth.get_kite_client()
        attempt = 0
        while True:
            attempt += 1
            try:
                waited = self._historical_rate_limiter.acquire()
                if waited > 0 and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Kite historical limiter waited %.3fs for token=%s interval=%s",
                        waited,
                        instrument_token,
                        interval,
                    )
                candles = client.historical_data(
                    instrument_token=instrument_token,
                    interval=interval,
                    from_date=start_date,
                    to_date=end_date,
                )
                return candles
            except Exception as exc:
                if attempt >= MAX_API_RETRIES or not self._should_retry(exc):
                    raise
                backoff = self._retry_delay_seconds(exc, attempt)
                logger.warning(
                    "Kite historical fetch failed for token=%s interval=%s attempt=%d/%d: %s",
                    instrument_token,
                    interval,
                    attempt,
                    MAX_API_RETRIES,
                    exc,
                )
                time.sleep(backoff)

    def _should_retry(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code == 429 or status_code >= 500
        if isinstance(exc, (TimeoutError, ConnectionError)):
            return True
        if isinstance(exc, KiteAPIError):
            message = str(exc).lower()
            non_retryable_markers = ("token", "permission", "input", "invalid")
            return not any(marker in message for marker in non_retryable_markers)
        return True

    def _retry_delay_seconds(self, exc: Exception, attempt: int) -> float:
        retry_after = self._retry_after_seconds(exc)
        if self._is_rate_limit_error(exc):
            return min(
                RETRY_MAX_DELAY_SECONDS,
                max(
                    retry_after,
                    RATE_LIMIT_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
                ),
            )

        jittered_backoff = (
            RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)) + random.random() * RETRY_JITTER_SECONDS
        )
        return min(RETRY_MAX_DELAY_SECONDS, max(retry_after, jittered_backoff))

    def _retry_after_seconds(self, exc: Exception) -> float:
        if not isinstance(exc, httpx.HTTPStatusError):
            return 0.0
        retry_after = exc.response.headers.get("Retry-After")
        if not retry_after:
            return 0.0
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            return 0.0

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code == 429
        if isinstance(exc, KiteAPIError):
            message = str(exc).lower()
            return "too many requests" in message or "rate limit" in message
        return False

    def _normalize_daily_candles(
        self,
        symbol: str,
        candles: list[dict[str, Any]] | list[list[Any]],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for candle in candles:
            normalized = self._normalize_candle(symbol, candle)
            if normalized is None:
                continue
            candle_time = normalized["candle_time"]
            rows.append(
                {
                    "symbol": symbol,
                    "date": candle_time.date(),
                    "open": normalized["open"],
                    "high": normalized["high"],
                    "low": normalized["low"],
                    "close": normalized["close"],
                    "volume": normalized["volume"],
                }
            )
        if not rows:
            return self._empty_daily_frame()
        frame = pd.DataFrame(
            rows,
            columns=["symbol", "date", "open", "high", "low", "close", "volume"],
        )
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["volume"] = frame["volume"].astype("int64")
        return frame

    def _normalize_5min_candles(
        self,
        symbol: str,
        candles: list[dict[str, Any]] | list[list[Any]],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for candle in candles:
            normalized = self._normalize_candle(symbol, candle)
            if normalized is None:
                continue
            candle_time = normalized["candle_time"]
            rows.append(
                {
                    "symbol": symbol,
                    "date": candle_time.date(),
                    "candle_time": candle_time,
                    "open": normalized["open"],
                    "high": normalized["high"],
                    "low": normalized["low"],
                    "close": normalized["close"],
                    "volume": normalized["volume"],
                }
            )
        if not rows:
            return self._empty_5min_frame()
        frame = pd.DataFrame(
            rows,
            columns=["symbol", "date", "candle_time", "open", "high", "low", "close", "volume"],
        )
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["candle_time"] = pd.to_datetime(frame["candle_time"])
        frame["volume"] = frame["volume"].astype("int64")
        return frame

    def _normalize_candle(
        self,
        symbol: str,
        candle: dict[str, Any] | list[Any],
    ) -> dict[str, Any] | None:
        if isinstance(candle, dict):
            raw_time = candle.get("date")
            open_price = candle.get("open")
            high_price = candle.get("high")
            low_price = candle.get("low")
            close_price = candle.get("close")
            volume = candle.get("volume")
        elif isinstance(candle, list) and len(candle) >= 6:
            raw_time, open_price, high_price, low_price, close_price, volume = candle[:6]
        else:
            logger.debug("Skipping malformed Kite candle for %s: %r", symbol, candle)
            return None

        candle_time = pd.Timestamp(raw_time)
        if candle_time.tzinfo is None:
            candle_time = candle_time.tz_localize(IST)
        else:
            candle_time = candle_time.tz_convert(IST)
        candle_time = candle_time.tz_localize(None)
        return {
            "candle_time": candle_time.to_pydatetime(),
            "open": float(open_price),
            "high": float(high_price),
            "low": float(low_price),
            "close": float(close_price),
            "volume": int(volume or 0),
        }

    def _empty_daily_frame(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])

    def _empty_5min_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=["symbol", "date", "candle_time", "open", "high", "low", "close", "volume"]
        )


_kite_fetcher: KiteFetcher | None = None


def get_kite_fetcher() -> KiteFetcher:
    global _kite_fetcher
    if _kite_fetcher is None:
        _kite_fetcher = KiteFetcher()
    return _kite_fetcher
