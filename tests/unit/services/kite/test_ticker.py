from __future__ import annotations

from nse_momentum_lab.services.kite.ticker import (
    build_subscription_frames,
    build_websocket_url,
    chunk_instrument_tokens,
)


class TestKiteTickerScaffold:
    def test_chunk_instrument_tokens(self) -> None:
        chunks = chunk_instrument_tokens([101, 102, 103, 104], chunk_size=2)
        assert chunks == [[101, 102], [103, 104]]

    def test_build_subscription_frames(self) -> None:
        frames = build_subscription_frames([101, 102], mode="quote", chunk_size=1)
        assert frames == [
            {"a": "subscribe", "v": [101]},
            {"a": "mode", "v": ["quote", [101]]},
            {"a": "subscribe", "v": [102]},
            {"a": "mode", "v": ["quote", [102]]},
        ]

    def test_build_websocket_url(self) -> None:
        url = build_websocket_url("api-key", "access-token")
        assert url.startswith("wss://ws.kite.trade?")
        assert "api_key=api-key" in url
        assert "access_token=access-token" in url
