"""Unit tests for TelegramNotifier and AlertDispatcher."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import (
    AlertConfig,
    AlertDispatcher,
    AlertEvent,
    AlertType,
    _redact_url,
    _should_send,
    format_partial_exit_alert,
)
from nse_momentum_lab.services.paper.notifiers.telegram import TelegramNotifier

# --- _redact_url ---


def test_redact_url_strips_bot_token():
    url = "https://api.telegram.org/bot1234567890:ABCdef_xyz/sendMessage"
    result = _redact_url(url)
    assert "1234567890" not in result
    assert "ABCdef_xyz" not in result
    assert "<REDACTED>" in result


def test_redact_url_leaves_normal_text_unchanged():
    assert _redact_url("no url here") == "no url here"


# --- TelegramNotifier ---


@pytest.mark.asyncio
async def test_telegram_notifier_disabled_when_no_token():
    notifier = TelegramNotifier(None, [])
    assert not notifier.enabled
    await notifier.send("subject", "body")  # Must not raise


@pytest.mark.asyncio
async def test_telegram_notifier_raises_on_http_error():
    notifier = TelegramNotifier("fake_token", ["123"])
    mock_resp = MagicMock()
    mock_resp.is_success = False
    mock_resp.status_code = 400
    mock_resp.raise_for_status = MagicMock(side_effect=Exception("HTTP 400"))

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
        with pytest.raises(Exception):  # noqa: B017
            await notifier.send("subject", "body")


@pytest.mark.asyncio
async def test_telegram_notifier_sends_html():
    notifier = TelegramNotifier("fake_token", ["123"])
    sent_payloads = []

    mock_resp = MagicMock()
    mock_resp.is_success = True

    async def fake_post(url, *, json, **_):
        sent_payloads.append(json)
        return mock_resp

    with patch("httpx.AsyncClient.post", side_effect=fake_post):
        await notifier.send("My Subject", "<b>body</b>")

    assert len(sent_payloads) == 1
    assert sent_payloads[0]["parse_mode"] == "HTML"
    assert "My Subject" in sent_payloads[0]["text"]


# --- AlertDispatcher ---


@pytest.mark.asyncio
async def test_dispatcher_enqueue_and_deliver():
    config = AlertConfig(telegram_bot_token=None, telegram_chat_ids=[])
    dispatcher = AlertDispatcher(config=config)

    received = []

    class FakeNotifier:
        enabled = True

        async def send(self, subject, body):
            received.append((subject, body))

    dispatcher.add_notifier(FakeNotifier())
    await dispatcher.start()
    event = AlertEvent(AlertType.SESSION_STARTED, "sess1", "Subject", "Body")
    dispatcher.enqueue(event)
    await asyncio.sleep(0.2)
    await dispatcher.shutdown()

    assert len(received) == 1
    assert received[0][0] == "Subject"


@pytest.mark.asyncio
async def test_dispatcher_best_effort_on_max_retries():
    """Dispatcher must not raise even when all retries fail."""
    config = AlertConfig(telegram_bot_token=None, telegram_chat_ids=[])
    dispatcher = AlertDispatcher(config=config)

    class AlwaysFailNotifier:
        enabled = True

        async def send(self, subject, body):
            raise RuntimeError("simulated failure")

    dispatcher.add_notifier(AlwaysFailNotifier())
    event = AlertEvent(AlertType.SESSION_ERROR, "sess1", "Subject", "Body")
    # Mock asyncio.sleep to avoid slow retry backoffs
    with patch("asyncio.sleep", new_callable=AsyncMock):
        await dispatcher._send_with_retry(event)  # Should not raise — best-effort


@pytest.mark.asyncio
async def test_dispatcher_queue_full_drops_alert():
    config = AlertConfig(telegram_bot_token=None, telegram_chat_ids=[])
    dispatcher = AlertDispatcher(config=config)
    # Fill queue to capacity
    for i in range(100):
        dispatcher._queue.put_nowait(
            AlertEvent(AlertType.SESSION_STARTED, "s", f"sub{i}", "body")
        )
    # Enqueuing one more must not raise — it should drop
    event = AlertEvent(AlertType.SESSION_STARTED, "s", "overflow", "body")
    dispatcher.enqueue(event)  # Must not raise


def test_session_pause_resume_alerts_follow_session_lifecycle_toggle():
    config = AlertConfig(telegram_bot_token=None, telegram_chat_ids=[], session_lifecycle=False)

    assert not _should_send(AlertType.SESSION_PAUSED, config)
    assert not _should_send(AlertType.SESSION_RESUMED, config)


def test_partial_exit_formatter_includes_remaining_qty_and_carry_stop():
    subject, body = format_partial_exit_alert(
        symbol="RELIANCE",
        direction="LONG",
        entry_price=100.0,
        exit_price=120.0,
        realized_pnl=1580.4,
        exited_qty=80,
        remaining_qty=20,
        carry_stop=114.0,
        session_id="sess1",
        strategy="2lynchbreakout",
    )

    assert "[PARTIAL]" in subject
    assert "RELIANCE" in subject
    assert "Exited: <code>80</code>" in body
    assert "Remaining: <code>20</code>" in body
    assert "Carry SL: <code>₹114.00</code>" in body
