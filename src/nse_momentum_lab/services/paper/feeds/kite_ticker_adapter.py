"""Live Kite WebSocket feed adapter for paper trading.

Thread-safe wrapper around kiteconnect.KiteTicker that fans out live ticks
to per-session FiveMinuteCandleBuilder instances.

Adapted from cpr-pivot-lab's KiteTickerAdapter pattern.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
from nse_momentum_lab.services.paper.feeds.candle_types import MarketSnapshot

try:
    from kiteconnect import KiteTicker  # type: ignore
except Exception:
    KiteTicker = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class KiteTickerAdapter:
    """Live WebSocket feed adapter wrapping kiteconnect.KiteTicker.

    Manages instrument token resolution, subscription lifecycle, tick fan-out,
    and connection recovery.
    """

    _local_feed = False

    def __init__(
        self,
        *,
        api_key: str | None = None,
        access_token: str | None = None,
        exchange: str = "NSE",
    ) -> None:
        self._api_key = api_key
        self._access_token = access_token
        self._exchange = exchange

        self._lock = threading.Lock()
        self._connected = threading.Event()
        self._kite: Any = None

        # Symbol <-> token mapping.
        self._symbol_to_token: dict[str, int] = {}
        self._token_to_symbol: dict[int, str] = {}

        # Per-session state: {session_id: (symbols, builder)}.
        self._sessions: dict[str, tuple[set[str], FiveMinuteCandleBuilder]] = {}
        self._subscribed_tokens: set[int] = set()

        # Telemetry.
        self._tick_count = 0
        self._last_tick_ts: float | None = None
        self._reconnect_count = 0
        self._last_ltp: dict[str, float] = {}
        self._symbol_last_tick_ts: dict[str, float] = {}

        # Connection recovery.
        self._disconnected_at: float | None = None
        self._last_recovery_at: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def last_tick_ts(self) -> float | None:
        return self._last_tick_ts

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    def get_last_ltp(self, symbol: str) -> float | None:
        return self._last_ltp.get(symbol)

    def register_session(
        self,
        session_id: str,
        symbols: list[str],
        builder: FiveMinuteCandleBuilder,
    ) -> None:
        """Register a session with its symbols and candle builder."""
        with self._lock:
            self._sessions[session_id] = (set(symbols), builder)
            self._reconcile_subscriptions()

        if not self._connected.is_set():
            self.connect(symbols)

    def unregister_session(self, session_id: str) -> None:
        """Remove a session and reconcile subscriptions."""
        with self._lock:
            self._sessions.pop(session_id, None)
            self._reconcile_subscriptions()

        if not self._sessions:
            self.close()

    def update_symbols(self, session_id: str, symbols: list[str]) -> None:
        """Update symbol set for a session and reconcile."""
        with self._lock:
            if session_id in self._sessions:
                _, builder = self._sessions[session_id]
                self._sessions[session_id] = (set(symbols), builder)
                self._reconcile_subscriptions()

    def synthesize_quiet_symbols(self, session_id: str, symbols: list[str], now: float) -> None:
        """Inject synthetic snapshots for symbols that haven't ticked recently."""
        with self._lock:
            entry = self._sessions.get(session_id)
            if entry is None:
                return
            _, builder = entry
            for symbol in symbols:
                last_ts = self._symbol_last_tick_ts.get(symbol, 0)
                if now - last_ts > 300:  # 5 min quiet threshold.
                    ltp = self._last_ltp.get(symbol)
                    if ltp is not None:
                        snapshot = MarketSnapshot(
                            symbol=symbol, ts=now, last_price=ltp, source="synthetic"
                        )
                        builder.ingest(snapshot)

    def drain_closed(self, session_id: str) -> list:
        """Return closed candles from this session's builder."""
        with self._lock:
            entry = self._sessions.get(session_id)
            if entry is None:
                return []
            _, builder = entry
            return builder.drain_closed()

    def connect(self, symbols: list[str] | None = None) -> None:
        """Create and connect the KiteTicker WebSocket."""
        if KiteTicker is None:
            raise RuntimeError("kiteconnect not installed — cannot use live feed")

        if self._api_key is None or self._access_token is None:
            raise ValueError("api_key and access_token required for live feed")

        self._kite = KiteTicker(self._api_key, self._access_token)
        self._kite.on_ticks = self._on_ticks
        self._kite.on_connect = self._on_connect
        self._kite.on_close = self._on_close
        self._kite.on_error = self._on_error
        self._kite.on_reconnect = self._on_reconnect

        self._kite.connect(threaded=True)
        self._connected.wait(timeout=15)

    def close(self) -> None:
        """Tear down the WebSocket."""
        if self._kite is not None:
            try:
                self._kite.close()
            except Exception:
                pass
            self._kite = None
        self._connected.clear()

    def recover_connection(
        self,
        *,
        now: float | None = None,
        reconnect_after_sec: float = 30.0,
        cooldown_sec: float = 30.0,
    ) -> dict[str, Any]:
        """Watchdog: recreate the WebSocket if it has been down too long."""
        if now is None:
            now = time.time()

        if self._connected.is_set():
            return {"action": "noop"}

        if self._disconnected_at is None:
            return {"action": "noop"}

        down_duration = now - self._disconnected_at
        if down_duration < reconnect_after_sec:
            return {"action": "noop", "down_sec": down_duration}

        if now - self._last_recovery_at < cooldown_sec:
            return {
                "action": "cooldown",
                "cooldown_remaining": cooldown_sec - (now - self._last_recovery_at),
            }

        self._last_recovery_at = now
        self.close()

        all_symbols: set[str] = set()
        for syms, _ in self._sessions.values():
            all_symbols.update(syms)

        try:
            self.connect(list(all_symbols))
            self._reconnect_count += 1
            return {"action": "recovered"}
        except Exception as e:
            return {"action": "failed", "error": str(e)}

    def health_stats(self) -> dict[str, Any]:
        """Return telemetry snapshot."""
        return {
            "connected": self._connected.is_set(),
            "tick_count": self._tick_count,
            "last_tick_ts": self._last_tick_ts,
            "reconnect_count": self._reconnect_count,
            "sessions": len(self._sessions),
            "subscribed_tokens": len(self._subscribed_tokens),
        }

    def symbol_coverage(self, symbols: list[str], within_sec: float = 300.0) -> dict[str, Any]:
        """Per-symbol tick freshness stats."""
        now = time.time()
        fresh = 0
        stale = 0
        missing = 0
        for s in symbols:
            last = self._symbol_last_tick_ts.get(s)
            if last is None:
                missing += 1
            elif now - last <= within_sec:
                fresh += 1
            else:
                stale += 1
        return {"fresh": fresh, "stale": stale, "missing": missing}

    def set_instrument_map(self, symbol_to_token: dict[str, int]) -> None:
        """Set the symbol -> instrument_token mapping for subscription."""
        with self._lock:
            self._symbol_to_token = dict(symbol_to_token)
            self._token_to_symbol = {v: k for k, v in symbol_to_token.items()}

    # ------------------------------------------------------------------
    # KiteTicker callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, ws: Any) -> None:
        """Called when WebSocket connects."""
        self._connected.set()
        self._disconnected_at = None
        self._reconcile_subscriptions()
        logger.info("KiteTickerAdapter: connected")

    def _on_close(self, ws: Any, code: int | None = None, reason: str | None = None) -> None:
        """Called when WebSocket closes."""
        self._connected.clear()
        self._disconnected_at = time.time()
        logger.warning("KiteTickerAdapter: disconnected code=%s reason=%s", code, reason)

    def _on_error(self, ws: Any, code: int | None = None, reason: str | None = None) -> None:
        logger.error("KiteTickerAdapter: error code=%s reason=%s", code, reason)

    def _on_reconnect(self, ws: Any, attempts: int | None = None) -> None:
        self._reconnect_count += 1
        logger.info("KiteTickerAdapter: reconnect attempt %s", attempts)

    def _on_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        """Fan out live ticks to per-session builders."""
        now = time.time()
        with self._lock:
            for tick in ticks:
                token = tick.get("instrument_token", 0)
                symbol = self._token_to_symbol.get(token)
                if symbol is None:
                    continue

                ltp = tick.get("last_price", 0.0)
                volume = tick.get("volume", 0.0) or tick.get("last_quantity", 0.0)

                # Best timestamp: exchange_timestamp > timestamp > now.
                ts = self._coerce_tick_timestamp(tick, now)

                snapshot = MarketSnapshot(
                    symbol=symbol, ts=ts, last_price=ltp, volume=volume, source="websocket"
                )

                self._last_ltp[symbol] = ltp
                self._symbol_last_tick_ts[symbol] = now
                self._tick_count += 1
                self._last_tick_ts = now

                # Fan out to sessions that subscribe to this symbol.
                for _sid, (syms, builder) in self._sessions.items():
                    if symbol in syms:
                        builder.ingest(snapshot)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _coerce_tick_timestamp(self, tick: dict[str, Any], fallback: float) -> float:
        """Pick the best timestamp from a tick dict."""
        for key in ("exchange_timestamp", "timestamp", "last_trade_time"):
            val = tick.get(key)
            if val is not None:
                if hasattr(val, "timestamp"):
                    return val.timestamp()
        return fallback

    def _reconcile_subscriptions(self) -> None:
        """Update WebSocket subscriptions to match union of all session symbols."""
        if self._kite is None or not self._connected.is_set():
            return

        needed_tokens: set[int] = set()
        for syms, _ in self._sessions.values():
            for s in syms:
                token = self._symbol_to_token.get(s)
                if token is not None:
                    needed_tokens.add(token)

        to_subscribe = needed_tokens - self._subscribed_tokens
        to_unsubscribe = self._subscribed_tokens - needed_tokens

        if to_subscribe:
            try:
                self._kite.subscribe(list(to_subscribe))
                mode = getattr(self._kite, "MODE_QUOTE", "quote")
                self._kite.set_mode(mode, list(to_subscribe))
            except Exception:
                logger.exception("Failed to subscribe to %d tokens", len(to_subscribe))

        if to_unsubscribe:
            try:
                self._kite.unsubscribe(list(to_unsubscribe))
            except Exception:
                logger.exception("Failed to unsubscribe %d tokens", len(to_unsubscribe))

        self._subscribed_tokens = needed_tokens
