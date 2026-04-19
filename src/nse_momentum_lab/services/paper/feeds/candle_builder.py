"""Thread-safe N-minute OHLCV candle aggregator.

Ingests MarketSnapshot ticks and emits ClosedCandle objects when a bar
boundary is crossed. Pull-based: callers invoke drain_closed() to retrieve
completed candles.

Adapted from cpr-pivot-lab's FiveMinuteCandleBuilder pattern.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from nse_momentum_lab.services.paper.feeds.candle_types import (
    ClosedCandle,
    MarketSnapshot,
)

logger = logging.getLogger(__name__)


@dataclass
class _CandleState:
    """Mutable in-progress bar for a single symbol."""

    bar_start: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    first_snapshot_ts: float = 0.0
    last_snapshot_ts: float = 0.0


class FiveMinuteCandleBuilder:
    """Aggregates ticks into N-minute OHLCV candles.

    Thread-safe. Feed ticks via ingest()/ingest_many(), retrieve completed
    candles via drain_closed().
    """

    def __init__(self, interval_minutes: int = 5) -> None:
        self._interval = interval_minutes
        self._lock = threading.Lock()
        self._states: dict[str, _CandleState] = {}
        self._pending_closed: list[ClosedCandle] = []
        self._prev_cumulative_vol: dict[str, float] = {}

    @property
    def interval_minutes(self) -> int:
        return self._interval

    def ingest(self, snapshot: MarketSnapshot) -> list[ClosedCandle]:
        """Feed one tick. Returns any newly closed candles (also buffered)."""
        with self._lock:
            self._ingest_locked(snapshot)
            return list(self._pending_closed)

    def ingest_many(self, snapshots: list[MarketSnapshot]) -> list[ClosedCandle]:
        """Feed multiple ticks under a single lock acquisition."""
        with self._lock:
            for s in snapshots:
                self._ingest_locked(s)
            return list(self._pending_closed)

    def flush(self, symbol: str | None = None) -> list[ClosedCandle]:
        """Force-close the current in-progress bar, optionally per symbol."""
        with self._lock:
            return self._flush_locked(symbol)

    def drain_closed(self) -> list[ClosedCandle]:
        """Return all accumulated closed candles and clear the buffer."""
        with self._lock:
            closed = list(self._pending_closed)
            self._pending_closed.clear()
            return closed

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _bucket_start(self, ts: float) -> float:
        """Floor epoch seconds to the nearest interval boundary."""
        dt = datetime.fromtimestamp(ts, tz=UTC)
        total_minutes = dt.hour * 60 + dt.minute
        bucket_minute = (total_minutes // self._interval) * self._interval
        bucket_dt = dt.replace(
            minute=bucket_minute % 60,
            second=0,
            microsecond=0,
            hour=bucket_minute // 60,
        )
        return bucket_dt.timestamp()

    def _ingest_locked(self, snapshot: MarketSnapshot) -> None:
        symbol = snapshot.symbol
        price = snapshot.last_price
        ts = snapshot.ts
        bucket = self._bucket_start(ts)

        # Volume delta from cumulative volume.
        vol_delta = 0.0
        prev_vol = self._prev_cumulative_vol.get(symbol)
        if prev_vol is not None:
            vol_delta = max(0.0, snapshot.volume - prev_vol)
        self._prev_cumulative_vol[symbol] = snapshot.volume

        state = self._states.get(symbol)

        if state is None:
            # First tick for this symbol.
            self._states[symbol] = _CandleState(
                bar_start=bucket,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=vol_delta,
                first_snapshot_ts=ts,
                last_snapshot_ts=ts,
            )
            return

        if bucket == state.bar_start:
            # Same bar — update running OHLCV.
            state.high = max(state.high, price)
            state.low = min(state.low, price)
            state.close = price
            state.volume += vol_delta
            state.last_snapshot_ts = ts
        elif bucket > state.bar_start:
            # New bar — close the old one, start fresh.
            candle = self._state_to_candle(symbol, state)
            self._pending_closed.append(candle)
            self._states[symbol] = _CandleState(
                bar_start=bucket,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=vol_delta,
                first_snapshot_ts=ts,
                last_snapshot_ts=ts,
            )
        # Out-of-order ticks (bucket < state.bar_start) are silently dropped.

    def _flush_locked(self, symbol: str | None = None) -> list[ClosedCandle]:
        closed: list[ClosedCandle] = []
        if symbol is not None:
            state = self._states.pop(symbol, None)
            if state is not None:
                closed.append(self._state_to_candle(symbol, state))
        else:
            for sym in list(self._states):
                state = self._states.pop(sym)
                closed.append(self._state_to_candle(sym, state))
        self._pending_closed.clear()
        return closed

    def _state_to_candle(self, symbol: str, state: _CandleState) -> ClosedCandle:
        interval_sec = self._interval * 60
        return ClosedCandle(
            symbol=symbol,
            bar_start=state.bar_start,
            bar_end=state.bar_start + interval_sec,
            open=state.open,
            high=state.high,
            low=state.low,
            close=state.close,
            volume=state.volume,
            first_snapshot_ts=state.first_snapshot_ts,
            last_snapshot_ts=state.last_snapshot_ts,
        )
