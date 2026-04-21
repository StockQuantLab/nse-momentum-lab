"""Test Telegram bot connectivity — sends sample paper-trading alerts.

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS from Doppler (Settings).

Usage:
    doppler run -- uv run python scripts/test_telegram.py
"""

from __future__ import annotations

import asyncio

import httpx

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 10.0


async def test_telegram() -> None:
    from nse_momentum_lab.config import get_settings

    settings = get_settings()
    token = settings.telegram_bot_token
    chat_ids_raw = settings.telegram_chat_ids

    if not token or not chat_ids_raw:
        print("ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS not configured in Doppler")
        print("  doppler secrets set TELEGRAM_BOT_TOKEN=<token> TELEGRAM_CHAT_IDS=<chat_id>")
        return

    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    if not chat_ids:
        print("ERROR: TELEGRAM_CHAT_IDS is set but empty after parsing")
        return

    print("Bot token : [SET]")
    print(f"Chat IDs  : {chat_ids}")
    print()

    url = _TELEGRAM_API.format(token=token)

    msgs = [
        (
            "<b>🟢 LONG OPENED: RELIANCE</b>\n\n"
            "📥 Entry: <code>₹2,450.75</code> | 🛡️ SL: <code>₹2,401.74</code>\n"
            "📏 Qty: <code>40</code> | 💰 Risk: ₹1,964\n"
            "🕒 09:25 19-Apr\n"
            "📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:RELIANCE'>Chart</a>\n"
            "<i>thresholdbreakout · test-session-0001</i>",
            "LONG OPENED",
        ),
        (
            "<b>🔴 SHORT OPENED: WIPRO</b>\n\n"
            "📥 Entry: <code>₹482.30</code> | 🛡️ SL: <code>₹491.95</code>\n"
            "📏 Qty: <code>207</code> | 💰 Risk: ₹1,997\n"
            "🕒 09:30 19-Apr\n"
            "📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:WIPRO'>Chart</a>\n"
            "<i>2lynchbreakdown · test-session-0001</i>",
            "SHORT OPENED",
        ),
        (
            "<b>✅ [WIN] RELIANCE LONG TRAIL_STOP</b>\n\n"
            "💰 P&amp;L: <code>+₹3,924</code> (+4.00%)\n"
            "🏁 Reason: TRAIL_STOP\n"
            "📈 Exit: <code>2,450.75</code> → <code>2,548.78</code>\n"
            "🕒 13:45 19-Apr\n"
            "📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:RELIANCE'>Chart</a>\n"
            "<i>thresholdbreakout · test-session-0001</i>",
            "WIN (trail stop)",
        ),
        (
            "<b>❌ [LOSS] WIPRO SHORT INITIAL_SL</b>\n\n"
            "💰 P&amp;L: <code>-₹1,997</code> (-2.00%)\n"
            "🏁 Reason: INITIAL_SL\n"
            "📉 Exit: <code>482.30</code> → <code>491.95</code>\n"
            "🕒 09:55 19-Apr\n"
            "📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:WIPRO'>Chart</a>\n"
            "<i>2lynchbreakdown · test-session-0001</i>",
            "LOSS (initial SL)",
        ),
        (
            "<b>📊 FLATTEN_EOD — 19-Apr-2026</b>\n\n"
            "Session: <code>test-session-0001</code>\n"
            "Net P&amp;L: <code>+1,927.00</code> 📈\n"
            "Trades closed: 2",
            "EOD flatten summary",
        ),
        (
            "<b>📊 DAILY_LOSS_LIMIT — 19-Apr-2026</b>\n\n"
            "Session: <code>test-session-0001</code>\n"
            "Net P&amp;L: <code>-3,500.00</code> 📉\n"
            "Trades closed: 4",
            "Daily loss limit hit",
        ),
        (
            "<b>📋 SESSION_COMPLETED test-session-0001</b>\n\n"
            "Session: <code>test-session-0001</code>\n"
            "Event: COMPLETED\n"
            "Strategy: thresholdbreakout",
            "Session completed",
        ),
    ]

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for chat_id in chat_ids:
            print(f"--- Sending to chat_id={chat_id} ---")
            for text, label in msgs:
                try:
                    resp = await client.post(
                        url,
                        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                              "disable_web_page_preview": True},
                    )
                    if resp.status_code == 200:
                        print(f"  OK   {label}")
                    else:
                        print(f"  FAIL {label} — HTTP {resp.status_code}: {resp.text[:200]}")
                except Exception as exc:
                    print(f"  FAIL {label} — {exc}")
            print()

    print("Done. Check your Telegram chat for the sample messages.")


if __name__ == "__main__":
    asyncio.run(test_telegram())
