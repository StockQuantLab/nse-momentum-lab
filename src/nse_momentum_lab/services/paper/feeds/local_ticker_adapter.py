"""DuckDB-backed replay feed adapter for paper trading.

Reads historical 5-min candles from market.duckdb and replays them as
ClosedCandle objects, mimicking the KiteTickerAdapter interface for
seamless replay/live switching.

Adapted from cpr-pivot-lab's LocalTickerAdapter pattern.
"""

from __future__ import annotations

import logging
from typing import Any

from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
from nse_momentum_lab.services.paper.feeds.candle_types import ClosedCandle

logger = logging.getLogger(__name__)


class LocalTickerAdapter:
    """Replays historical 5-min candles from DuckDB for paper replay mode.

    Provides the same public interface as KiteTickerAdapter so the session
    runner can switch between replay and live with no code changes.

    Data is read once on construction, then emitted one bar-group at a time
    via drain_closed().
    """

    # Marker so callers can detect local mode.
    _local_feed = True

    def __init__(
        self,
        *,
        trade_date: str,
        symbols: list[str],
        candle_interval_minutes: int = 5,
        market_db: Any = None,
        pack_source: str = "market",
        paper_db: Any = None,
        session_id: str | None = None,
    ) -> None:
        self._trade_date = trade_date
        self._symbols = list(symbols)
        self._interval = candle_interval_minutes
        self._market_db = market_db
        self._pack_source = pack_source
        self._paper_db = paper_db
        self._session_id = session_id

        # Loaded candle data: {symbol: list[ClosedCandle]}
        self._candles_by_symbol: dict[str, list[ClosedCandle]] = {}
        # Sorted union of all bar_end times across all symbols.
        self._sorted_bar_ends: list[float] = []
        # Cursor position.
        self._bar_idx = -1
        self._exhausted = False
        # Per-session pending candles from fan-out.
        self._pending: dict[str, list[ClosedCandle]] = {}
        # Registered sessions.
        self._sessions: dict[str, set[str]] = {}

        # Stats.
        self._tick_count = 0

        if self._pack_source == "feed_audit" and self._paper_db is not None:
            self._load_candles_from_feed_audit()
        elif market_db is not None:
            self._load_candles()

    # ------------------------------------------------------------------
    # Public API (KiteTickerAdapter-compatible)
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return True

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def last_tick_ts(self) -> float | None:
        return None

    @property
    def reconnect_count(self) -> int:
        return 0

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    def get_last_ltp(self, symbol: str) -> float | None:
        candles = self._candles_by_symbol.get(symbol, [])
        if candles:
            return candles[-1].close
        return None

    def register_session(
        self,
        session_id: str,
        symbols: list[str],
        builder: FiveMinuteCandleBuilder | None = None,
    ) -> None:
        """Register a session. Builder is kept for API parity but unused."""
        self._sessions[session_id] = set(symbols)
        self._pending.setdefault(session_id, [])

    def unregister_session(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)
        self._pending.pop(session_id, None)

    def update_symbols(self, session_id: str, symbols: list[str]) -> None:
        self._sessions[session_id] = set(symbols)

    def synthesize_quiet_symbols(self, session_id: str, symbols: list[str], now: float) -> None:
        """No-op: all data comes from pre-loaded candles."""
        pass

    def drain_closed(self, session_id: str) -> list[ClosedCandle]:
        """Advance the global cursor by one bar and return candles for this session.

        A single drain_closed call from any session advances the cursor for all
        sessions. Other sessions retrieve their queued data on their next call.
        """
        if session_id not in self._sessions:
            return []

        # Return any pending candles from a prior fan-out.
        if self._pending.get(session_id):
            pending = self._pending[session_id]
            self._pending[session_id] = []
            self._tick_count += len(pending)
            return pending

        if self._exhausted:
            return []

        # Advance global cursor.
        self._bar_idx += 1
        if self._bar_idx >= len(self._sorted_bar_ends):
            self._exhausted = True
            return []

        bar_end = self._sorted_bar_ends[self._bar_idx]

        # Fan out to all sessions.
        for sid, sym_set in self._sessions.items():
            bar_candles: list[ClosedCandle] = []
            for symbol in sym_set:
                symbol_candles = self._candles_by_symbol.get(symbol, [])
                for c in symbol_candles:
                    if c.bar_end == bar_end:
                        bar_candles.append(c)
                        break
            if sid == session_id:
                self._tick_count += len(bar_candles)
                result = bar_candles
            else:
                self._pending.setdefault(sid, []).extend(bar_candles)
                result = []

        return result

    def close(self) -> None:
        """No-op."""
        pass

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_candles(self) -> None:
        """Load historical candles from DuckDB for all registered symbols."""
        if self._market_db is None:
            return

        bar_end_set: set[float] = set()

        for symbol in self._symbols:
            try:
                df = self._market_db.query_5min_candles(
                    symbol=symbol,
                    start_date=self._trade_date,
                    end_date=self._trade_date,
                )
                if df.is_empty():
                    logger.warning("No 5-min data for %s on %s", symbol, self._trade_date)
                    continue

                candles: list[ClosedCandle] = []
                for row in df.iter_rows(named=True):
                    candle_time = row.get("candle_time")
                    if candle_time is None:
                        continue

                    # candle_time is a datetime or timestamp.
                    if hasattr(candle_time, "timestamp"):
                        bar_end_epoch = candle_time.timestamp()
                    else:
                        continue

                    interval_sec = self._interval * 60
                    candle = ClosedCandle(
                        symbol=symbol,
                        bar_start=bar_end_epoch - interval_sec,
                        bar_end=bar_end_epoch,
                        open=float(row.get("open", 0)),
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=float(row.get("close", 0)),
                        volume=float(row.get("volume", 0)),
                    )
                    candles.append(candle)
                    bar_end_set.add(bar_end_epoch)

                self._candles_by_symbol[symbol] = candles

            except Exception:
                logger.exception("Failed to load candles for %s", symbol)

        self._sorted_bar_ends = sorted(bar_end_set)
        logger.info(
            "LocalTickerAdapter: loaded %d symbols, %d bars for %s (source=market)",
            len(self._candles_by_symbol),
            len(self._sorted_bar_ends),
            self._trade_date,
        )

    def _load_candles_from_feed_audit(self) -> None:
        """Load candles from ``paper_feed_audit`` table for parity replay.

        Reconstructs ClosedCandle objects from the recorded live feed data
        instead of the EOD-built v_5min view, enabling exact reproduction
        of what the live engine saw during a session.
        """
        if self._paper_db is None:
            return

        audit_rows = self._paper_db.get_feed_audit_rows(
            trade_date=self._trade_date,
            session_id=self._session_id,
        )

        if not audit_rows:
            raise ValueError(
                f"No feed audit rows for session={self._session_id} "
                f"date={self._trade_date}. Cannot replay from feed_audit "
                f"without recorded live data. Use --pack-source market instead."
            )

        bar_end_set: set[float] = set()
        interval_sec = self._interval * 60

        for row in audit_rows:
            symbol = row.symbol
            if symbol not in self._symbols:
                continue

            bar_end_dt = row.bar_end
            if not hasattr(bar_end_dt, "timestamp"):
                continue

            bar_end_epoch = bar_end_dt.timestamp()
            candle = ClosedCandle(
                symbol=symbol,
                bar_start=bar_end_epoch - interval_sec,
                bar_end=bar_end_epoch,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
            )
            self._candles_by_symbol.setdefault(symbol, []).append(candle)
            bar_end_set.add(bar_end_epoch)

        self._sorted_bar_ends = sorted(bar_end_set)
        logger.info(
            "LocalTickerAdapter: loaded %d symbols, %d bars for %s (source=feed_audit, session=%s)",
            len(self._candles_by_symbol),
            len(self._sorted_bar_ends),
            self._trade_date,
            self._session_id,
        )
