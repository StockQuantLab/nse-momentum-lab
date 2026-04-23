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

import httpx

logger = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 100
MAX_RETRIES = 6
RETRY_BACKOFF = (1.0, 2.0, 4.0, 30.0, 120.0, 300.0)

# Redact Telegram bot-token URLs (https://api.telegram.org/bot<TOKEN>/...)
_BOT_TOKEN_RE = re.compile(r"(https?://[^/]+/bot)[^/\s]+(/?)", re.IGNORECASE)


def _redact_url(text: str) -> str:
    """Replace bot token in Telegram API URLs with a placeholder."""
    return _BOT_TOKEN_RE.sub(r"\1<REDACTED>\2", text)


def _is_transient_delivery_error(exc: Exception) -> bool:
    """Return True when the failure is likely temporary and worth retrying."""
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = getattr(exc.response, "status_code", None)
        if status in {429, 500, 502, 503, 504}:
            return True
    message = str(exc).lower()
    transient_tokens = (
        "getaddrinfo failed",
        "temporary failure",
        "timed out",
        "timeout",
        "connection reset",
        "connection aborted",
        "too many requests",
        "503 service unavailable",
        "502 bad gateway",
        "504 gateway timeout",
    )
    return any(token in message for token in transient_tokens)


class AlertType(StrEnum):
    """Paper trading alert types."""

    TRADE_OPENED = "TRADE_OPENED"
    TRADE_CLOSED = "TRADE_CLOSED"
    SL_HIT = "SL_HIT"
    TRAIL_STOP = "TRAIL_STOP"
    TARGET_HIT = "TARGET_HIT"
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_PAUSED = "SESSION_PAUSED"
    SESSION_RESUMED = "SESSION_RESUMED"
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
        AlertType.SESSION_PAUSED: config.session_lifecycle,
        AlertType.SESSION_RESUMED: config.session_lifecycle,
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


def _format_session_context(strategy: str, session_id: str) -> str:
    if not strategy and not session_id:
        return ""
    parts = []
    if strategy:
        parts.append(escape(strategy))
    if session_id:
        parts.append(session_id[:16])
    return " · ".join(parts)


def _friendly_exit_reason(reason: str) -> str:
    value = str(reason or "").upper()
    mapping = {
        "STOP_BREAKEVEN": "BREAKEVEN_SL",
        "STOP_TRAIL": "TRAIL_SL",
        "STOP_INITIAL": "INITIAL_SL",
        "GAP_THROUGH_STOP": "GAP_SL",
    }
    return mapping.get(value, value or "EXIT")


def _estimate_fee(price: float, qty: int) -> float:
    return round(float(price) * int(qty) * 0.001, 4) if price and qty else 0.0


def _net_trade_return_pct(*, realized_pnl: float, entry_price: float, qty: int) -> float:
    basis = float(entry_price) * int(qty)
    if basis <= 0:
        return 0.0
    return (float(realized_pnl) / basis) * 100.0


def format_trade_opened_alert(
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    initial_stop: float,
    qty: int,
    session_id: str,
    strategy: str = "",
    target_price: float | None = None,
    event_time: datetime | None = None,
) -> tuple[str, str]:
    """Return (subject, HTML body) for a TRADE_OPENED alert."""
    subject = f"{direction} OPENED: {symbol}"
    risk_per_unit = abs(entry_price - initial_stop)
    risk_rupees = risk_per_unit * qty
    reward_r = None
    if target_price is not None and risk_per_unit > 0:
        reward_r = abs(float(target_price) - entry_price) / risk_per_unit
    time_str = _format_event_time(event_time)
    session_context = _format_session_context(strategy, session_id)
    body = (
        f"📥 Entry: <code>₹{entry_price:.2f}</code> | 🛡️ SL: <code>₹{initial_stop:.2f}</code>\n"
        + (
            f"🎯 Target: <code>₹{float(target_price):.2f}</code> | "
            if target_price is not None
            else ""
        )
        + f"📏 Qty: <code>{qty}</code>\n"
        + f"💰 Risk: <code>₹{risk_rupees:,.0f}</code>"
        + (f" ({reward_r:.1f}R)" if reward_r is not None else "")
        + (f"\n🕒 {time_str}" if time_str else "")
        + "\n<a href='https://www.tradingview.com/chart/?symbol=NSE:"
        + f"{escape(symbol)}'>Chart</a>"
        + (f"\n<i>{session_context}</i>" if session_context else "")
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
    pnl_pct = _net_trade_return_pct(
        realized_pnl=realized_pnl,
        entry_price=entry_price,
        qty=qty,
    )
    is_win = realized_pnl >= 0
    result_tag = "WIN" if is_win else "LOSS"
    icon = "✅" if is_win else "❌"
    friendly_reason = _friendly_exit_reason(reason)
    subject = f"{icon} [{result_tag}] {symbol} {direction} {friendly_reason}"
    time_str = _format_event_time(event_time)
    pnl_display = f"{'+' if is_win else '-'}₹{abs(realized_pnl):,.0f}"
    session_context = _format_session_context(strategy, session_id)
    body = (
        f"💰 P&amp;L: <code>{pnl_display}</code> ({pnl_pct:+.2f}%)\n"
        + (f"📏 Qty: <code>{qty}</code>\n" if qty else "")
        + f"🏁 Reason: {escape(friendly_reason)}\n"
        + f"📤 Exit: <code>{entry_price:.2f}</code> → <code>{exit_price:.2f}</code>"
        + (f"\n🕒 {time_str}" if time_str else "")
        + "\n<a href='https://www.tradingview.com/chart/?symbol=NSE:"
        + f"{escape(symbol)}'>Chart</a>"
        + (f"\n<i>{session_context}</i>" if session_context else "")
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


def format_daily_pnl_summary(
    *,
    session_id: str,
    strategy: str = "",
    trade_date: str = "",
    realized_pnl: float = 0.0,
    unrealized_pnl: float = 0.0,
    trades_closed_today: int = 0,
    winners: int = 0,
    losers: int = 0,
    open_positions: list[dict[str, Any]] | None = None,
    portfolio_value: float = 0.0,
    max_dd_used_pct: float = 0.0,
) -> tuple[str, str]:
    """Return (subject, HTML body) for a swing-trading DAILY_PNL_SUMMARY.

    Unlike CPR's FLATTEN_EOD (day trading, all positions closed), this shows
    both realized and unrealized P&L with per-position breakdowns including
    days_held — the daily scorecard for a multi-day swing system.
    """
    net_pnl = realized_pnl + unrealized_pnl
    date_str = ""
    if trade_date and len(trade_date) == 10:
        try:
            date_str = datetime.strptime(trade_date, "%Y-%m-%d").strftime("%d-%b-%Y")
        except ValueError:
            date_str = trade_date

    realized_icon = "📈" if realized_pnl >= 0 else "📉"
    unrealized_icon = "📈" if unrealized_pnl >= 0 else "📉"
    net_icon = "📈" if net_pnl >= 0 else "📉"
    realized_sign = "+" if realized_pnl >= 0 else "-"
    unrealized_sign = "+" if unrealized_pnl >= 0 else "-"
    net_sign = "+" if net_pnl >= 0 else "-"
    realized_pct = (realized_pnl / portfolio_value * 100) if portfolio_value else 0.0
    unrealized_pct = (unrealized_pnl / portfolio_value * 100) if portfolio_value else 0.0
    net_pct = (net_pnl / portfolio_value * 100) if portfolio_value else 0.0

    subject = f"📊 Daily Summary — {date_str}"
    body = (
        f"Session: <code>{session_id[:16]}</code>"
        + (f" | {escape(strategy)}" if strategy else "")
        + f"\n\n{realized_icon} Realized P&amp;L: <code>{realized_sign}₹{abs(realized_pnl):,.0f}</code>"
        f" ({realized_pct:+.2f}%)"
        f"\n  Trades closed today: {trades_closed_today} (✅{winners} ❌{losers})"
        f"\n\n{unrealized_icon} Unrealized P&amp;L: <code>{unrealized_sign}₹{abs(unrealized_pnl):,.0f}</code>"
        f" ({unrealized_pct:+.2f}%)"
        f"\n  Open positions: {len(open_positions) if open_positions else 0}"
    )

    if open_positions:
        pos_lines = []
        for p in open_positions[:10]:  # cap at 10 to avoid message overflow
            sym = p.get("symbol", "?")
            pnl = p.get("unrealized_pnl", 0.0)
            days = p.get("days_held", 0)
            sign = "+" if pnl >= 0 else "-"
            pos_lines.append(f"  {sym} {sign}₹{abs(pnl):,.0f} ({days}D)")
        body += "\n" + "\n".join(pos_lines)
        if len(open_positions) > 10:
            body += f"\n  ... +{len(open_positions) - 10} more"

    body += (
        f"\n\n{net_icon} Net P&amp;L: <code>{net_sign}₹{abs(net_pnl):,.0f}</code> ({net_pct:+.2f}%)"
        f"\nPortfolio: ₹{portfolio_value:,.0f} | DD used: {max_dd_used_pct:.1f}%"
    )
    return subject, body


def format_manual_flatten_alert(
    *,
    session_id: str,
    strategy: str = "",
    trade_date: str = "",
    flattened_positions: int = 0,
    net_pnl: float = 0.0,
    session_status: str = "PAUSED",
) -> tuple[str, str]:
    """Return (subject, HTML body) for a manual flatten operator alert."""
    date_str = ""
    if trade_date and len(trade_date) == 10:
        try:
            date_str = datetime.strptime(trade_date, "%Y-%m-%d").strftime("%d-%b-%Y")
        except ValueError:
            date_str = trade_date
    subject = "🛑 MANUAL FLATTEN" + (f" — {date_str}" if date_str else "")
    session_context = _format_session_context(strategy, session_id)
    body = (
        f"Session: <code>{session_id[:16]}</code>\n"
        f"Positions flattened: <code>{int(flattened_positions)}</code>\n"
        f"Session status: <code>{escape(session_status)}</code>\n"
        f"Net P&amp;L: <code>{net_pnl:+,.2f}</code>"
    )
    if session_context:
        body += f"\n<i>{session_context}</i>"
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


def summarize_session_pnl(
    *,
    paper_db: Any,
    session_id: str,
    mark_prices: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compute current session P&L using the paper engine's fee approximation."""
    realized_pnl = float(paper_db.get_session_realized_pnl(session_id))
    unrealized_pnl = 0.0
    trades_closed_today = 0
    winners = 0
    losers = 0
    open_pos_details: list[dict[str, Any]] = []

    for position in paper_db.list_positions_by_session(session_id):
        qty = int(position.get("qty", 0) or 0)
        avg_entry = float(position.get("avg_entry", 0) or 0.0)
        direction = str(position.get("direction", "LONG") or "LONG").upper()
        meta = position.get("metadata_json") or {}
        symbol = str(position.get("symbol", "") or "")

        if str(position.get("state", "")).upper() == "CLOSED":
            trades_closed_today += 1
            avg_exit = float(position.get("avg_exit", avg_entry) or avg_entry)
            gross_pnl = float(position.get("pnl", 0) or 0.0)
            net_pnl = gross_pnl - _estimate_fee(avg_entry, qty) - _estimate_fee(avg_exit, qty)
            if net_pnl >= 0:
                winners += 1
            else:
                losers += 1
            continue

        mark = (mark_prices or {}).get(symbol)
        if mark is None:
            mark = float(meta.get("last_mark_price", avg_entry) or avg_entry)
        if direction == "SHORT":
            gross_upnl = (avg_entry - mark) * qty
        else:
            gross_upnl = (mark - avg_entry) * qty
        entry_fee = _estimate_fee(avg_entry, qty)
        net_upnl = gross_upnl - entry_fee - _estimate_fee(mark, qty)
        unrealized_pnl += net_upnl
        open_pos_details.append(
            {
                "symbol": symbol,
                "unrealized_pnl": net_upnl,
                "days_held": meta.get("days_held", 0),
            }
        )

    return {
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "trades_closed_today": trades_closed_today,
        "winners": winners,
        "losers": losers,
        "open_positions": open_pos_details,
    }


def enqueue_daily_pnl_summary(
    *,
    alert_dispatcher: AlertDispatcher,
    session_id: str,
    paper_db: Any,
    strategy: str,
    trade_date: str,
    portfolio_value: float,
    mark_prices: dict[str, float] | None = None,
    alerts_sent: set[str] | None = None,
) -> None:
    """Compute and enqueue DAILY_PNL_SUMMARY with net-of-modeled-fees P&L."""
    if not alert_dispatcher._enabled:
        return
    if paper_db.has_alert_log(session_id, AlertType.DAILY_PNL_SUMMARY.value, status="sent"):
        return
    summary_key = f"DAILY_PNL_SUMMARY:{session_id}"
    if alerts_sent is not None and summary_key in alerts_sent:
        return
    if alerts_sent is not None:
        alerts_sent.add(summary_key)
    try:
        summary = summarize_session_pnl(
            paper_db=paper_db,
            session_id=session_id,
            mark_prices=mark_prices,
        )
        realized_pnl = float(summary["realized_pnl"])
        unrealized_pnl = float(summary["unrealized_pnl"])
        max_dd_used_pct = (
            abs(realized_pnl + unrealized_pnl) / portfolio_value * 100 if portfolio_value else 0.0
        )
        subject, body = format_daily_pnl_summary(
            session_id=session_id,
            strategy=strategy,
            trade_date=trade_date,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            trades_closed_today=int(summary["trades_closed_today"]),
            winners=int(summary["winners"]),
            losers=int(summary["losers"]),
            open_positions=list(summary["open_positions"]),
            portfolio_value=portfolio_value,
            max_dd_used_pct=max_dd_used_pct,
        )
        alert_dispatcher.enqueue(
            AlertEvent(
                alert_type=AlertType.DAILY_PNL_SUMMARY,
                session_id=session_id,
                subject=subject,
                body=body,
            )
        )
    except Exception:
        logger.exception("Failed to dispatch DAILY_PNL_SUMMARY session=%s", session_id)


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
        terminal_failed: set[int] = set()
        for attempt in range(MAX_RETRIES):
            all_ok = True
            any_transient_failure = False
            for i, notifier in enumerate(self._notifiers):
                if i in succeeded or i in terminal_failed:
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
                    if _is_transient_delivery_error(e):
                        any_transient_failure = True
                    else:
                        terminal_failed.add(i)
                        self._log_alert(
                            event,
                            status="failed",
                            error=_redact_url(str(e))[:200],
                            channel=type(notifier).__name__,
                        )

            if all_ok or len(succeeded) + len(terminal_failed) == len(self._notifiers):
                return

            if not any_transient_failure:
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
