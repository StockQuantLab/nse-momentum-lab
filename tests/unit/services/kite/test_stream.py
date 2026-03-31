from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from nse_momentum_lab.services.kite.stream import KiteStreamConfig, KiteStreamRunner


def _sessionmaker_mock() -> MagicMock:
    session = AsyncMock()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock()
    sessionmaker = MagicMock()
    sessionmaker.return_value = context
    return sessionmaker


class TestKiteStreamRunner:
    @patch("nse_momentum_lab.services.kite.stream.touch_paper_feed_state", new_callable=AsyncMock)
    def test_bootstrap_records_feed_state(self, mock_touch: AsyncMock) -> None:
        mock_touch.return_value = SimpleNamespace(
            session_id="paper-live",
            status="CONNECTING",
            subscription_count=2,
            mode="full",
        )
        runner = KiteStreamRunner(
            sessionmaker=_sessionmaker_mock(),
            session_id="paper-live",
            config=KiteStreamConfig(
                api_key="kite-key",
                access_token="kite-token",
                instrument_tokens=[101, 102],
            ),
        )

        result = asyncio.run(runner.bootstrap())

        assert result == {
            "session_id": "paper-live",
            "status": "CONNECTING",
            "subscription_count": 2,
            "mode": "full",
        }
        mock_touch.assert_awaited_once()
        assert mock_touch.await_args.kwargs["metadata_json"] == {
            "token_count": 2,
            "mode": "full",
        }

    @patch("nse_momentum_lab.services.kite.stream.touch_paper_feed_state", new_callable=AsyncMock)
    def test_record_ticks_throttles_feed_state_writes(self, mock_touch: AsyncMock) -> None:
        runner = KiteStreamRunner(
            sessionmaker=_sessionmaker_mock(),
            session_id="paper-live",
            config=KiteStreamConfig(
                api_key="kite-key",
                access_token="kite-token",
                instrument_tokens=[101, 102],
            ),
        )

        asyncio.run(runner._record_ticks([{"instrument_token": 101, "last_price": 100.0}]))
        asyncio.run(runner._record_ticks([{"instrument_token": 102, "last_price": 101.0}]))

        mock_touch.assert_awaited_once()
        assert mock_touch.await_args.kwargs["metadata_json"] == {
            "tick_count": 1,
            "token_count": 2,
            "last_tick_tokens": [101],
            "mode": "full",
        }

    @patch("nse_momentum_lab.services.kite.stream.upsert_paper_fill", new_callable=AsyncMock)
    @patch("nse_momentum_lab.services.kite.stream.upsert_paper_order_event", new_callable=AsyncMock)
    @patch(
        "nse_momentum_lab.services.kite.stream.update_paper_order_broker_state",
        new_callable=AsyncMock,
    )
    def test_record_order_update_persists_broker_state(
        self,
        mock_update_order: AsyncMock,
        mock_upsert_event: AsyncMock,
        mock_upsert_fill: AsyncMock,
    ) -> None:
        mock_update_order.return_value = SimpleNamespace(order_id=77)
        runner = KiteStreamRunner(
            sessionmaker=_sessionmaker_mock(),
            session_id="paper-live",
            config=KiteStreamConfig(
                api_key="kite-key",
                access_token="kite-token",
                instrument_tokens=[101, 102],
            ),
        )

        asyncio.run(
            runner._record_order_update(
                {
                    "order_id": "BRK-123",
                    "status": "COMPLETE",
                    "filled_quantity": 25,
                    "average_price": 101.5,
                    "trade_id": "TRD-1",
                    "exchange_timestamp": datetime.now(tz=UTC).isoformat(),
                }
            )
        )

        mock_update_order.assert_awaited_once()
        mock_upsert_event.assert_awaited_once()
        mock_upsert_fill.assert_awaited_once()
