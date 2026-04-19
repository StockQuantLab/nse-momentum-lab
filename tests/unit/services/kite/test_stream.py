from __future__ import annotations

import asyncio

import pytest

from nse_momentum_lab.services.kite.stream import KiteStreamConfig, KiteStreamRunner


class TestKiteStreamRunnerDeprecation:
    """Verify that KiteStreamRunner behaves as a deprecation shim."""

    def test_init_stores_config(self) -> None:
        config = KiteStreamConfig(
            api_key="kite-key",
            access_token="kite-token",
            instrument_tokens=[101, 102],
        )
        runner = KiteStreamRunner(
            session_id="paper-live",
            config=config,
        )
        assert runner.session_id == "paper-live"
        assert runner.config is config

    def test_run_raises_not_implemented(self) -> None:
        runner = KiteStreamRunner(
            session_id="paper-live",
            config=KiteStreamConfig(api_key="k", access_token="t"),
        )
        with pytest.raises(NotImplementedError, match="KiteTickerAdapter"):
            asyncio.run(runner.run())

    def test_stop_is_noop(self) -> None:
        runner = KiteStreamRunner(
            session_id="paper-live",
            config=KiteStreamConfig(api_key="k", access_token="t"),
        )
        # Should not raise
        asyncio.run(runner.stop())

    def test_snapshot_returns_deprecated_status(self) -> None:
        runner = KiteStreamRunner(
            session_id="paper-live",
            config=KiteStreamConfig(api_key="k", access_token="t"),
        )
        result = asyncio.run(runner.snapshot())
        assert result["session_id"] == "paper-live"
        assert result["status"] == "DEPRECATED"
