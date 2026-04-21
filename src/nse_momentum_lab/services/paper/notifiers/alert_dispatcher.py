"""Async alert dispatcher with retry for paper trading notifications.

Provides a fire-and-forget enqueue/dequeue API backed by asyncio.Queue.
Alerts are sent to registered notifiers (Telegram) with exponential
backoff retry. Every send attempt is logged to the paper DB alert_log table.

Pattern adapted from cpr-pivot-lab's AlertDispatcher:
- AlertConfig holds both connection credentials and per-type toggles.
- get_alert_config() reads from Settings (Doppler env vars).
- AlertDispatcher wires TelegramNotifier internally from AlertConfig.
- HTML-formatted body functions (_format_*_alert) keep trading logic separate.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from html import escape
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
class AlertConfig:
    """Connection credentials and per-type alert toggles.

    Holds Telegram credentials so AlertDispatcher can wire TelegramNotifier
    internally — same pattern as cpr-pivot-lab.
    """

    # Telegram connection
    telegram_bot_token: str | None = None
    telegram_chat_ids: list[str] = field(default_factory=list)

    # Email connection
    email_smtp_host: str | None = None
    email_smtp_port: int = 587
    email_from: str | None = None
    email_to: str | None = None
    email_password: str | None = None
    email_use_tls: bool = True

    # Per-category toggles
    trade_open: bool = True
    trade_close: bool = True
    session_lifecycle: bool = True
    risk_limits: bool = True
    daily_summary: bool = True


def get_alert_config() -> AlertConfig:
    """Build AlertConfig from Doppler-backed Settings (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_IDS)."""
    from nse_momentum_lab.config import get_settings

    s = get_settings()
    chat_ids = [
        c.strip() for c in (getattr(s, "telegram_chat_ids", None) or "").split(",") if c.strip()
    ]
    return AlertConfig(
        telegram_bot_token=getattr(s, "telegram_bot_token", None),
        telegram_chat_ids=chat_ids,
        email_smtp_host=getattr(s, "email_smtp_host", None),
        email_smtp_port=getattr(s, "email_smtp_port", 587),
        email_from=getattr(s, "email_from", None),
        email_to=getattr(s, "email_to", None),
        email_password=getattr(s, "email_password", None),
        email_use_tls=getattr(s, "email_use_tls", True),
    )


@dataclass
class AlertEvent:
    """A single alert to be dispatched."""

    alert_type: AlertType | str
    session_id: str
    subject: str
    body: str
    level: str = "info"
    metadata: dict[str, Any] = field(default_factory=dict)


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


# ---------------------------------------------------------------------------
# HTML alert formatters (body is pre-formatted HTML — not escaped by notifier)
# ---------------------------------------------------------------------------


def _format_event_time(event_time: datetime | None) -> str:
    if event_time is None:
        return ""
    return event_time.strftime("%H:%M %d-%b")


def format_trade_opened_alert(
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    initial_stop: float,
    qty: int,
    session_id: str,
    strategy: str = "",
    event_time: datetime | None = None,
) -> tuple[str, str]:
    """Return (subject, HTML body) for a TRADE_OPENED alert."""
    icon = "🟢" if direction == "LONG" else "🔴"
    subject = f"{icon} {direction} OPENED: {symbol}"
    risk_per_unit = abs(entry_price - initial_stop)
    risk_rupees = risk_per_unit * qty
    time_str = _format_event_time(event_time)
    chart_link = (
        f"📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:{escape(symbol)}'>Chart</a>"
    )
    body = (
        f"📥 Entry: <code>₹{entry_price:.2f}</code> | 🛡️ SL: <code>₹{initial_stop:.2f}</code>\n"
        f"📏 Qty: <code>{qty}</code> | 💰 Risk: ₹{risk_rupees:,.0f}"
        + (f" | 🕒 {time_str}" if time_str else "")
        + f"\n{chart_link}"
        + (f"\n<i>{escape(strategy)} · {session_id[:16]}</i>" if strategy else "")
    )
    return subject, body


def format_trade_closed_alert(
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    reason: str,
    realized_pnl: float,
    qty: int = 0,
    session_id: str = "",
    strategy: str = "",
    event_time: datetime | None = None,
) -> tuple[str, str]:
    """Return (subject, HTML body) for a TRADE_CLOSED alert."""
    pnl_pct = (
        (
            ((exit_price - entry_price) / entry_price * 100)
            if direction == "LONG"
            else ((entry_price - exit_price) / entry_price * 100)
        )
        if entry_price
        else 0.0
    )
    is_win = realized_pnl >= 0
    result_tag = "WIN" if is_win else "LOSS"
    icon = "✅" if is_win else "❌"
    trend_icon = "📈" if is_win else "📉"
    subject = f"{icon} [{result_tag}] {symbol} {direction} {reason}"
    time_str = _format_event_time(event_time)
    pnl_display = f"{'+' if is_win else '-'}₹{abs(realized_pnl):,.0f}"
    chart_link = (
        f"📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:{escape(symbol)}'>Chart</a>"
    )
    body = (
        f"💰 P&amp;L: <code>{pnl_display}</code> ({pnl_pct:+.2f}%)\n"
        f"🏁 Reason: {escape(reason)}\n"
        f"{trend_icon} Exit: <code>{entry_price:.2f}</code> → <code>{exit_price:.2f}</code>"
        + (f"\n🕒 {time_str}" if time_str else "")
        + f"\n{chart_link}"
        + (f"\n<i>{escape(strategy)} · {session_id[:16]}</i>" if strategy else "")
    )
    return subject, body


def format_risk_alert(
    *,
    reason: str,
    net_pnl: float,
    session_id: str,
    positions_closed: int = 0,
    total_trades: int | None = None,
    trade_date: str | None = None,
) -> tuple[str, str]:
    """Return (subject, HTML body) for a risk-limit or EOD summary alert."""
    pnl_emoji = "📈" if net_pnl >= 0 else "📉"
    date_str = ""
    if trade_date and len(trade_date) == 10:
        try:
            date_str = datetime.strptime(trade_date, "%Y-%m-%d").strftime("%d-%b-%Y")
        except ValueError:
            date_str = trade_date
    subject = f"📊 {escape(reason)}" + (f" — {date_str}" if date_str else "")
    body = (
        f"Session: <code>{session_id[:16]}</code>\n"
        f"Net P&amp;L: <code>{net_pnl:+,.2f}</code> {pnl_emoji}\n"
        f"Trades closed: {total_trades if total_trades is not None else positions_closed}"
    )
    return subject, body


def format_session_alert(
    *,
    session_id: str,
    event: str,
    strategy: str = "",
    details: str | None = None,
) -> tuple[str, str]:
    """Return (subject, HTML body) for session lifecycle alerts."""
    subject = f"📋 SESSION_{escape(event.upper())} {session_id[:16]}"
    body = f"Session: <code>{session_id[:16]}</code>\nEvent: {escape(event)}"
    if strategy:
        body += f"\nStrategy: {escape(strategy)}"
    if details:
        body += f"\n{escape(details)}"
    return subject, body


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class AlertDispatcher:
    """Async queue-based alert dispatcher with retry and DB audit.

    Wires TelegramNotifier internally from AlertConfig — same as cpr-pivot-lab.
    """

    def __init__(
        self,
        *,
        paper_db: Any = None,
        config: AlertConfig | None = None,
        # Legacy: accept bare notifiers list for backward compat with old callers.
        notifiers: list[Any] | None = None,
        enabled: bool = True,
    ) -> None:
        self._paper_db = paper_db
        self._config = config or AlertConfig()
        self._enabled = enabled
        self._queue: asyncio.Queue[AlertEvent] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None

        # Wire TelegramNotifier from config if credentials present.
        from nse_momentum_lab.services.paper.notifiers.telegram import TelegramNotifier

        self._notifiers: list[Any] = list(notifiers or [])
        if self._config.telegram_bot_token and self._config.telegram_chat_ids:
            telegram = TelegramNotifier(
                self._config.telegram_bot_token,
                self._config.telegram_chat_ids,
            )
            if telegram.enabled:
                self._notifiers.append(telegram)

        # Wire EmailNotifier from config if credentials present.
        from nse_momentum_lab.services.paper.notifiers.email_notifier import EmailNotifier

        email_to = [e.strip() for e in (self._config.email_to or "").split(",") if e.strip()]
        if self._config.email_smtp_host and self._config.email_from and email_to:
            email_notifier = EmailNotifier(
                self._config.email_smtp_host,
                self._config.email_smtp_port,
                self._config.email_from,
                email_to,
                password=self._config.email_password,
                use_tls=self._config.email_use_tls,
            )
            if email_notifier.enabled:
                self._notifiers.append(email_notifier)

    def add_notifier(self, notifier: Any) -> None:
        """Register an additional notifier."""
        self._notifiers.append(notifier)

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def enqueue(self, event: AlertEvent) -> None:
        """Fire-and-forget: add an alert to the queue."""
        if not self._enabled:
            return
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
        logger.info(
            "AlertDispatcher started: notifiers=%d telegram=%s",
            len(self._notifiers),
            any(type(n).__name__ == "TelegramNotifier" for n in self._notifiers),
        )

    async def shutdown(self) -> None:
        """Stop the consumer, drain remaining alerts, close notifier clients."""
        self._running = False
        if self._consumer_task is not None:
            for _ in range(240):  # up to 120 s
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

        # Release persistent HTTP clients.
        for notifier in self._notifiers:
            if hasattr(notifier, "close"):
                try:
                    await notifier.close()
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
