"""Async alert dispatcher with retry for paper trading notifications.

Provides a fire-and-forget enqueue/dequeue API backed by asyncio.Queue.
Alerts are sent to registered notifiers (Telegram, etc.) with exponential
backoff retry. Every send attempt is logged to the paper DB alert_log table.

Adapted from cpr-pivot-lab's AlertDispatcher pattern.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 100
MAX_RETRIES = 3
RETRY_BACKOFF = (1.0, 2.0, 4.0)

# Redact Telegram bot-token URLs (https://api.telegram.org/bot<TOKEN>/...)
_BOT_TOKEN_RE = re.compile(r"(https?://[^/]+/bot)[^/\s]+(/?)", re.IGNORECASE)


def _redact_url(text: str) -> str:
    """Replace bot token in Telegram API URLs with a placeholder."""
    return _BOT_TOKEN_RE.sub(r"\1<REDACTED>\2", text)


class AlertType(StrEnum):
    """Paper trading alert types."""

    TRADE_OPENED = "TRADE_OPENED"
    TRADE_CLOSED = "TRADE_CLOSED"
    SL_HIT = "SL_HIT"
    TRAIL_STOP = "TRAIL_STOP"
    TARGET_HIT = "TARGET_HIT"
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_COMPLETED = "SESSION_COMPLETED"
    SESSION_ERROR = "SESSION_ERROR"
    FEED_STALE = "FEED_STALE"
    FEED_RECOVERED = "FEED_RECOVERED"
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    DRAWDOWN_LIMIT = "DRAWDOWN_LIMIT"
    FLATTEN_EOD = "FLATTEN_EOD"
    DAILY_PNL_SUMMARY = "DAILY_PNL_SUMMARY"


@dataclass
class AlertEvent:
    """A single alert to be dispatched."""

    alert_type: AlertType | str
    session_id: str
    subject: str
    body: str
    level: str = "info"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertConfig:
    """Toggle which alert types produce notifications."""

    trade_open: bool = True
    trade_close: bool = True
    session_lifecycle: bool = True
    risk_limits: bool = True
    daily_summary: bool = True


def _should_send(alert_type: AlertType | str, config: AlertConfig) -> bool:
    """Check if an alert type is enabled in config."""
    mapping: dict[AlertType, bool] = {
        AlertType.TRADE_OPENED: config.trade_open,
        AlertType.TRADE_CLOSED: config.trade_close,
        AlertType.SL_HIT: config.trade_close,
        AlertType.TRAIL_STOP: config.trade_close,
        AlertType.TARGET_HIT: config.trade_close,
        AlertType.SESSION_STARTED: config.session_lifecycle,
        AlertType.SESSION_COMPLETED: config.session_lifecycle,
        AlertType.SESSION_ERROR: config.session_lifecycle,
        AlertType.FEED_STALE: config.session_lifecycle,
        AlertType.FEED_RECOVERED: config.session_lifecycle,
        AlertType.DAILY_LOSS_LIMIT: config.risk_limits,
        AlertType.DRAWDOWN_LIMIT: config.risk_limits,
        AlertType.FLATTEN_EOD: config.risk_limits,
        AlertType.DAILY_PNL_SUMMARY: config.daily_summary,
    }
    key = alert_type if isinstance(alert_type, AlertType) else None
    if key is None:
        return True
    return mapping.get(key, True)


class AlertDispatcher:
    """Async queue-based alert dispatcher with retry and DB audit."""

    def __init__(
        self,
        *,
        notifiers: list[Any] | None = None,
        paper_db: Any = None,
        config: AlertConfig | None = None,
    ) -> None:
        self._notifiers = notifiers or []
        self._paper_db = paper_db
        self._config = config or AlertConfig()
        self._queue: asyncio.Queue[AlertEvent] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None

    def add_notifier(self, notifier: Any) -> None:
        """Register a notifier (must have async send(subject, body) method)."""
        self._notifiers.append(notifier)

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def enqueue(self, event: AlertEvent) -> None:
        """Fire-and-forget: add an alert to the queue."""
        if not _should_send(event.alert_type, self._config):
            return

        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Alert queue full, dropping %s", event.alert_type)
            self._log_alert(event, status="failed", error="queue_full")

    async def start(self) -> None:
        """Start the background consumer loop."""
        if self._running:
            return
        self._running = True
        self._consumer_task = asyncio.create_task(self._consumer_loop())

    async def shutdown(self) -> None:
        """Stop the consumer and drain remaining alerts."""
        self._running = False
        if self._consumer_task is not None:
            for _ in range(240):  # up to 120s
                if self._consumer_task.done():
                    break
                await asyncio.sleep(0.5)
            self._consumer_task = None

        # Drain remaining synchronously.
        while not self._queue.empty():
            try:
                event = self._queue.get_nowait()
                await self._send_with_retry(event)
            except Exception:
                pass

    async def dispatch(self, event: AlertEvent) -> None:
        """Direct dispatch (bypass queue) with retry."""
        await self._send_with_retry(event)

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    async def _consumer_loop(self) -> None:
        while self._running or not self._queue.empty():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            await self._send_with_retry(event)

    async def _send_with_retry(self, event: AlertEvent) -> None:
        """Try each notifier with exponential backoff, skipping already-succeeded ones."""
        succeeded: set[int] = set()
        for attempt in range(MAX_RETRIES):
            all_ok = True
            for i, notifier in enumerate(self._notifiers):
                if i in succeeded:
                    continue
                if not getattr(notifier, "enabled", True):
                    succeeded.add(i)
                    continue
                try:
                    await notifier.send(event.subject, event.body)
                    self._log_alert(event, status="sent", channel=type(notifier).__name__)
                    succeeded.add(i)
                except Exception as e:
                    all_ok = False
                    if attempt == MAX_RETRIES - 1:
                        self._log_alert(
                            event,
                            status="failed",
                            error=_redact_url(str(e))[:200],
                            channel=type(notifier).__name__,
                        )

            if all_ok or len(succeeded) == len(self._notifiers):
                return

            if attempt < MAX_RETRIES - 1:
                backoff = RETRY_BACKOFF[attempt]
                logger.warning(
                    "Alert send failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    MAX_RETRIES,
                    backoff,
                    event.alert_type,
                )
                await asyncio.sleep(backoff)

    def _log_alert(
        self,
        event: AlertEvent,
        *,
        status: str,
        channel: str = "ALL",
        error: str | None = None,
    ) -> None:
        """Write alert attempt to paper DB alert_log."""
        if self._paper_db is None:
            return
        try:
            self._paper_db.insert_alert_log(
                session_id=event.session_id,
                alert_type=str(event.alert_type),
                channel=channel,
                status=status,
                payload={"level": event.level, "subject": event.subject, "body": event.body},
                error_message=error,
            )
        except Exception:
            logger.exception("Failed to log alert to DB")
