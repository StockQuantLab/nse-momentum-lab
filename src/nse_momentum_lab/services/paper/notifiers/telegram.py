"""Telegram Bot API notifier for paper trading alerts.

Sends HTML-formatted trade, risk, and session lifecycle alerts
to configured Telegram chat IDs via the Bot API.

Body is expected to be pre-formatted HTML (same as cpr-pivot-lab pattern).
Subject is always HTML-escaped. An inline TradingView chart button is
auto-attached when a symbol is detected in the subject.
"""

from __future__ import annotations

import logging
import re
from html import escape
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 10.0

# Match symbol after "OPENED: SYMBOL" or after "[WIN/LOSS] SYMBOL DIRECTION"
_SYMBOL_RE = re.compile(r"(?:OPENED:\s*|\]\s*)([A-Z]{3,})")


class TelegramNotifier:
    """Send Telegram messages to one or more chat IDs via the Bot API.

    Uses a persistent httpx.AsyncClient (created lazily on first send).
    Failures are logged and re-raised so the dispatcher can retry.
    """

    def __init__(self, bot_token: str | None, chat_ids: list[str]) -> None:
        self._bot_token = bot_token
        self._chat_ids = chat_ids
        self._client: httpx.AsyncClient | None = None
        self._enabled = bool(bot_token and chat_ids)

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def close(self) -> None:
        """Release the underlying HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, subject: str, body: str) -> None:
        """Send an alert to all configured chat IDs.

        Subject is HTML-escaped. Body is sent as-is (pre-formatted HTML).
        """
        if not self._enabled:
            return
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=_TIMEOUT)

        url = _TELEGRAM_API.format(token=self._bot_token)
        # Subject: plain text → escape for HTML. Body: pre-formatted HTML → send raw.
        text = f"<b>{escape(subject)}</b>\n\n{body}"
        reply_markup = self._chart_button(subject)

        for chat_id in self._chat_ids:
            payload: dict[str, Any] = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = await self._client.post(url, json=payload)
            if not resp.is_success:
                safe_url = url.replace(self._bot_token or "", "***")
                logger.error(
                    "Telegram send failed chat_id=%s url=%s: HTTP %s",
                    chat_id,
                    safe_url,
                    resp.status_code,
                )
                resp.raise_for_status()

    @staticmethod
    def _chart_button(subject: str) -> dict[str, Any] | None:
        """Attach a TradingView chart button if the subject contains a symbol."""
        m = _SYMBOL_RE.search(subject)
        if not m:
            return None
        symbol = m.group(1)
        return {
            "inline_keyboard": [
                [
                    {
                        "text": "📊 View Chart",
                        "url": f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}",
                    }
                ]
            ]
        }
