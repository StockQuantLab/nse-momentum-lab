"""Telegram Bot API notifier for paper trading alerts.

Sends HTML-formatted trade, risk, and session lifecycle alerts
to configured Telegram chat IDs via the Bot API.

Adapted from cpr-pivot-lab's TelegramNotifier pattern.
"""

from __future__ import annotations

import html
import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_SYMBOL_RE = re.compile(r"(?:OPENED:\s*|\]\s*)([A-Z]{3,})")


class TelegramNotifier:
    """Sends alerts to Telegram chats via Bot API."""

    def __init__(
        self,
        *,
        bot_token: str,
        chat_ids: list[str],
        timeout: float = 10.0,
    ) -> None:
        self._bot_token = bot_token
        self._chat_ids = chat_ids
        self._timeout = timeout
        self._base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._enabled = bool(bot_token and chat_ids)

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def send(self, subject: str, body: str) -> None:
        """Send an alert to all configured chat IDs."""
        if not self._enabled:
            return

        text = f"<b>{html.escape(subject)}</b>\n\n{html.escape(body)}"
        reply_markup = self._chart_button(subject)

        payload: dict[str, Any] = {
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "text": text,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for chat_id in self._chat_ids:
                payload["chat_id"] = chat_id
                try:
                    resp = await client.post(self._base_url, json=payload)
                    resp.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "Telegram send failed chat=%s status=%d body=%s",
                        chat_id,
                        exc.response.status_code,
                        exc.response.text[:200],
                    )
                    raise
                except httpx.HTTPError as exc:
                    # Avoid logging the full URL which contains the bot token.
                    logger.error(
                        "Telegram send error chat=%s type=%s message=%s",
                        chat_id,
                        type(exc).__name__,
                        str(exc).split("?")[0],  # strip query params that may include token
                    )
                    raise

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
                        "text": "View Chart",
                        "url": f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}",
                    }
                ]
            ]
        }
