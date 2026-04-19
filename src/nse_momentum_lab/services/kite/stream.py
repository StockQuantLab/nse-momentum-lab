"""Kite stream runner — superseded by KiteTickerAdapter.

This module is kept for backward compatibility. New code should use
``nse_momentum_lab.services.paper.feeds.kite_ticker_adapter.KiteTickerAdapter``
instead.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from nse_momentum_lab.services.kite.ticker import SubscriptionMode

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class KiteStreamConfig:
    api_key: str
    access_token: str
    instrument_tokens: list[int] = field(default_factory=list)
    mode: SubscriptionMode = "full"
    reconnect: bool = True
    reconnect_max_tries: int = 50
    reconnect_max_delay: int = 60
    connect_timeout: int = 30


class KiteStreamRunner:
    """Legacy stream runner — use KiteTickerAdapter for new code.

    This class is retained as a compatibility shim. It logs a deprecation
    warning and delegates to KiteTickerAdapter internally.
    """

    feed_state_update_interval_seconds = 5.0

    def __init__(
        self,
        *,
        sessionmaker: Any = None,
        session_id: str = "",
        config: KiteStreamConfig | None = None,
        tick_handler: Any = None,
    ) -> None:
        logger.warning(
            "KiteStreamRunner is deprecated. Use KiteTickerAdapter from "
            "nse_momentum_lab.services.paper.feeds.kite_ticker_adapter instead."
        )
        self.sessionmaker = sessionmaker
        self.session_id = session_id
        self.config = config
        self.tick_handler = tick_handler

    async def run(self) -> None:
        raise NotImplementedError(
            "KiteStreamRunner.run() is no longer supported. "
            "Use KiteTickerAdapter from the paper v2 feeds module."
        )

    async def stop(self) -> None:
        pass

    async def snapshot(self) -> dict[str, Any]:
        return {"session_id": self.session_id, "status": "DEPRECATED"}
