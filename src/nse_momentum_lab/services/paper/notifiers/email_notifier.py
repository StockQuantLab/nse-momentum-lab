"""SMTP email notifier for paper trading alerts.

Sends HTML-formatted trade and session alerts via SMTP.
Configured via Doppler env vars: EMAIL_SMTP_HOST, EMAIL_SMTP_PORT,
EMAIL_FROM, EMAIL_TO (comma-separated), EMAIL_PASSWORD, EMAIL_USE_TLS.

Failures are logged and re-raised so AlertDispatcher can retry.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html.parser import HTMLParser

logger = logging.getLogger(__name__)


class _HTMLStripper(HTMLParser):
    """Strip HTML tags for plain-text fallback."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def _strip_html(html: str) -> str:
    s = _HTMLStripper()
    s.feed(html)
    return s.get_text()


class EmailNotifier:
    """Send email alerts via SMTP with HTML + plain-text multipart.

    Uses a new SMTP connection per send (stateless — avoids idle disconnects).
    Failures are logged and re-raised so AlertDispatcher can retry.
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        from_addr: str,
        to_addrs: list[str],
        password: str | None = None,
        use_tls: bool = True,
    ) -> None:
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._from_addr = from_addr
        self._to_addrs = to_addrs
        self._password = password
        self._use_tls = use_tls
        self._enabled = bool(smtp_host and from_addr and to_addrs)

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def send(self, subject: str, body: str) -> None:
        """Send an HTML email to all configured recipients.

        Subject is used as the email subject line.
        Body is expected to be pre-formatted HTML.
        """
        if not self._enabled:
            return

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._from_addr
        msg["To"] = ", ".join(self._to_addrs)

        plain_text = _strip_html(body)
        msg.attach(MIMEText(plain_text, "plain"))
        msg.attach(MIMEText(f"<html><body>{body}</body></html>", "html"))

        try:
            if self._use_tls:
                with smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=15) as smtp:
                    smtp.starttls()
                    if self._password:
                        smtp.login(self._from_addr, self._password)
                    smtp.sendmail(self._from_addr, self._to_addrs, msg.as_string())
            else:
                with smtplib.SMTP_SSL(self._smtp_host, self._smtp_port, timeout=15) as smtp:
                    if self._password:
                        smtp.login(self._from_addr, self._password)
                    smtp.sendmail(self._from_addr, self._to_addrs, msg.as_string())
        except Exception as exc:
            logger.error(
                "Email send failed to %s via %s:%d: %s",
                self._to_addrs,
                self._smtp_host,
                self._smtp_port,
                exc,
            )
            raise
