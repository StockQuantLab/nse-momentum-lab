from __future__ import annotations

import asyncio
import logging
import sys
import threading
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from nse_momentum_lab.db.paper import (
    touch_paper_feed_state,
    update_paper_order_broker_state,
    upsert_paper_fill,
    upsert_paper_order_event,
)
from nse_momentum_lab.services.kite.ticker import (
    SubscriptionMode,
    build_subscription_frames,
    build_websocket_url,
    plan_subscription_batches,
)

try:  # pragma: no cover - optional dependency for live runs
    from kiteconnect import KiteTicker  # type: ignore
except Exception:  # pragma: no cover - optional dependency for live runs
    KiteTicker = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class KiteStreamConfig:
    api_key: str
    access_token: str
    instrument_tokens: list[int] = field(default_factory=list)
    mode: SubscriptionMode = "full"
    reconnect: bool = True
    reconnect_max_tries: int = 50
    reconnect_max_delay: int = 60
    connect_timeout: int = 30


class KiteStreamRunner:
    """Official KiteTicker-backed paper/live feed runner scaffold."""

    def __init__(
        self,
        *,
        sessionmaker: async_sessionmaker[Any],
        session_id: str,
        config: KiteStreamConfig,
        tick_handler: Callable[[list[dict[str, Any]]], Awaitable[None]] | None = None,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.session_id = session_id
        self.config = config
        self.tick_handler = tick_handler
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ticker: Any | None = None
        self._stop_requested = False

    @staticmethod
    def _resolve_mode_constant(ws: Any, mode: str) -> Any:
        mode_name = mode.lower().strip()
        if mode_name == "ltp":
            return getattr(ws, "MODE_LTP", mode_name)
        if mode_name == "quote":
            return getattr(ws, "MODE_QUOTE", mode_name)
        return getattr(ws, "MODE_FULL", mode_name)

    async def bootstrap(self) -> dict[str, Any]:
        async with self.sessionmaker() as db_session:
            feed_state = await touch_paper_feed_state(
                db_session,
                self.session_id,
                source="kite",
                mode=self.config.mode,
                status="CONNECTING",
                is_stale=False,
                subscription_count=len(self.config.instrument_tokens),
                metadata_json={
                    "instrument_tokens": self.config.instrument_tokens,
                    "mode": self.config.mode,
                },
            )

        return {
            "session_id": self.session_id,
            "status": feed_state.status,
            "subscription_count": feed_state.subscription_count,
            "mode": feed_state.mode,
        }

    async def run(self) -> None:
        if KiteTicker is None:  # pragma: no cover - depends on optional package
            raise RuntimeError(
                "kiteconnect is not installed. Install the official Kite Connect v4 package "
                "to run the live websocket loop."
            )

        # Windows guard: KiteTicker uses Twisted, which calls signal.signal() —
        # this only works in the main thread. Detect early and fail with a clear message.
        if sys.platform == "win32" and threading.current_thread() is not threading.main_thread():
            raise RuntimeError(
                "KiteTicker must be started from the main thread on Windows. "
                "The Twisted reactor calls signal.signal() which is main-thread-only."
            )

        await self.bootstrap()
        self._loop = asyncio.get_running_loop()
        self._ticker = KiteTicker(
            self.config.api_key,
            self.config.access_token,
            reconnect=self.config.reconnect,
            reconnect_max_tries=self.config.reconnect_max_tries,
            reconnect_max_delay=self.config.reconnect_max_delay,
            connect_timeout=self.config.connect_timeout,
        )
        self._ticker.on_connect = self._on_connect
        self._ticker.on_ticks = self._on_ticks
        self._ticker.on_order_update = self._on_order_update
        self._ticker.on_close = self._on_close
        self._ticker.on_error = self._on_error

        token_count = len(self.config.instrument_tokens)
        logger.info(
            "[WS-START] session=%s tokens=%d mode=%s threaded=True",
            self.session_id,
            token_count,
            self.config.mode,
        )
        try:
            self._ticker.connect(threaded=True)
        except Exception:
            logger.exception(
                "[WS-START] KiteTicker.connect() failed for session %s", self.session_id
            )
            await self._touch_feed_state("ERROR", is_stale=True)
            raise
        try:
            while not self._stop_requested:
                await asyncio.sleep(1)
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._stop_requested = True
        if self._ticker is not None:
            try:
                self._ticker.stop_retry()
            except Exception:  # pragma: no cover - best-effort cleanup
                logger.exception("Failed to stop Kite retry loop cleanly")
            try:
                self._ticker.stop()
            except Exception:  # pragma: no cover - best-effort cleanup
                logger.exception("Failed to stop Kite stream cleanly")
            self._ticker = None

    def _submit(self, coro: Coroutine[Any, Any, Any]) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _on_connect(self, ws: Any, _response: Any) -> None:
        try:
            subscribed_total = 0
            for batch in plan_subscription_batches(
                self.config.instrument_tokens, mode=self.config.mode
            ):
                if not batch.tokens:
                    continue
                ws.subscribe(batch.tokens)
                ws.set_mode(self._resolve_mode_constant(ws, batch.mode), batch.tokens)
                subscribed_total += len(batch.tokens)
            logger.info(
                "[WS-CONNECT] session=%s subscribed=%d batches=%d",
                self.session_id,
                subscribed_total,
                max(
                    1,
                    len(
                        list(
                            plan_subscription_batches(
                                self.config.instrument_tokens, mode=self.config.mode
                            )
                        )
                    ),
                ),
            )
            self._submit(self._touch_feed_state("CONNECTED", is_stale=False))
        except Exception:
            logger.exception("[WS-CONNECT] Kite on_connect failed for session %s", self.session_id)
            self._submit(self._touch_feed_state("ERROR", is_stale=True))

    def _on_ticks(self, _ws: Any, ticks: list[dict[str, Any]]) -> None:
        self._submit(self._handle_ticks(ticks))

    def _on_order_update(self, _ws: Any, order_update: dict[str, Any]) -> None:
        self._submit(self._record_order_update(order_update))

    def _on_close(self, _ws: Any, _code: int, reason: str) -> None:
        logger.warning("[WS-CLOSE] session=%s code=%s reason=%s", self.session_id, _code, reason)
        self._submit(self._touch_feed_state("DISCONNECTED", is_stale=True))

    def _on_error(self, _ws: Any, code: Any, reason: Any) -> None:
        logger.error("[WS-ERROR] session=%s code=%s reason=%s", self.session_id, code, reason)
        self._submit(self._touch_feed_state("ERROR", is_stale=True))

    async def _touch_feed_state(
        self,
        status: str,
        *,
        is_stale: bool,
        heartbeat_at: datetime | None = None,
        last_tick_at: datetime | None = None,
        last_quote_at: datetime | None = None,
    ) -> None:
        async with self.sessionmaker() as db_session:
            await touch_paper_feed_state(
                db_session,
                self.session_id,
                source="kite",
                mode=self.config.mode,
                status=status,
                is_stale=is_stale,
                subscription_count=len(self.config.instrument_tokens),
                heartbeat_at=heartbeat_at or datetime.now(tz=UTC),
                last_tick_at=last_tick_at,
                last_quote_at=last_quote_at,
                metadata_json={
                    "instrument_tokens": self.config.instrument_tokens,
                    "mode": self.config.mode,
                    "stream_status": status,
                },
            )

    async def _record_ticks(self, ticks: list[dict[str, Any]]) -> None:
        now = datetime.now(tz=UTC)
        instrument_tokens = sorted(
            {
                int(token)
                for token in (tick.get("instrument_token") for tick in ticks)
                if token is not None
            }
        )
        async with self.sessionmaker() as db_session:
            await touch_paper_feed_state(
                db_session,
                self.session_id,
                source="kite",
                mode=self.config.mode,
                status="CONNECTED",
                is_stale=False,
                heartbeat_at=now,
                last_tick_at=now,
                last_quote_at=now,
                metadata_json={
                    "tick_count": len(ticks),
                    "instrument_tokens": self.config.instrument_tokens,
                    "last_tick_tokens": instrument_tokens[:25],
                    "mode": self.config.mode,
                },
            )

    async def _handle_ticks(self, ticks: list[dict[str, Any]]) -> None:
        await self._record_ticks(ticks)
        if self.tick_handler is not None:
            try:
                await self.tick_handler(ticks)
            except Exception:
                logger.exception(
                    "[WS-TICKS] Tick handler failed for session %s",
                    self.session_id,
                )

    async def _record_order_update(self, order_update: dict[str, Any]) -> None:
        broker_order_id = str(
            order_update.get("order_id")
            or order_update.get("exchange_order_id")
            or order_update.get("broker_order_id")
            or ""
        ).strip()
        status = str(order_update.get("status") or "UNKNOWN")
        now = datetime.now(tz=UTC)

        async with self.sessionmaker() as db_session:
            order_row = None
            if broker_order_id:
                order_row = await update_paper_order_broker_state(
                    db_session,
                    broker_order_id=broker_order_id,
                    broker_status=status,
                    payload_json=order_update,
                )

            await upsert_paper_order_event(
                db_session,
                session_id=self.session_id,
                event_type="ORDER_UPDATE",
                event_status=status,
                order_id=order_row.order_id if order_row else None,
                broker_order_id=broker_order_id or None,
                payload_json=order_update,
            )

            filled_quantity = order_update.get("filled_quantity")
            average_price = order_update.get("average_price")
            if (
                order_row
                and filled_quantity not in (None, 0, "0")
                and average_price not in (None, "")
            ):
                fill_time = now
                if isinstance(order_update.get("exchange_timestamp"), str):
                    try:
                        fill_time = datetime.fromisoformat(order_update["exchange_timestamp"])
                    except ValueError:
                        fill_time = now

                await upsert_paper_fill(
                    db_session,
                    session_id=self.session_id,
                    order_id=order_row.order_id,
                    fill_time=fill_time,
                    fill_price=float(average_price),
                    qty=float(filled_quantity),
                    fees=None,
                    slippage_bps=None,
                    broker_trade_id=str(order_update.get("trade_id") or "") or None,
                    broker_order_id=broker_order_id or None,
                    broker_payload_json=order_update,
                )

        logger.info(
            "Recorded Kite order update for session %s order %s status %s",
            self.session_id,
            broker_order_id or "<unknown>",
            status,
        )

    async def snapshot(self) -> dict[str, Any]:
        async with self.sessionmaker() as db_session:
            feed_state = await touch_paper_feed_state(
                db_session,
                self.session_id,
                source="kite",
                mode=self.config.mode,
                status="CONNECTED" if not self._stop_requested else "STOPPED",
                is_stale=self._stop_requested,
                subscription_count=len(self.config.instrument_tokens),
                heartbeat_at=datetime.now(tz=UTC),
                metadata_json={
                    "instrument_tokens": self.config.instrument_tokens,
                    "mode": self.config.mode,
                },
            )
        return {
            "session_id": self.session_id,
            "feed_state": {
                "session_id": feed_state.session_id,
                "status": feed_state.status,
                "mode": feed_state.mode,
                "is_stale": feed_state.is_stale,
                "subscription_count": feed_state.subscription_count,
            },
            "websocket_url": build_websocket_url(self.config.api_key, self.config.access_token),
            "subscription_frames": build_subscription_frames(
                self.config.instrument_tokens, mode=self.config.mode
            ),
        }
